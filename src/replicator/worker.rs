use std::sync::Arc;

use foundationdb::{FdbBindingError, RetryableTransaction};
use futures_util::Future;
use tokio::time::Duration;

use rhino::{
    region_server::{ClientRequestPayload, ClientResponsePayload},
    regional_multiplexed_ws, FdbClient, FdbQueueError, GlobalTableSchemasReplicationQueue, LeaseId,
    Region, RegionInfo, RegionalStreams, ShardRoutingReplicationQueue, TokenReplicationQueue,
};
use tracing::{info, warn};
use ulid::Ulid;

pub async fn run(db: Arc<FdbClient>, regions: Vec<RegionInfo>) {
    for (region, streams) in regional_multiplexed_ws(regions) {
        tokio::spawn(replicate_tokens(db.clone(), region, streams.clone()));

        tokio::spawn(replicate_shard_routing(db.clone(), region, streams.clone()));

        tokio::spawn(replicate_table_schemas(db.clone(), region, streams.clone()));
    }

    // Replication futures never return, so we just go into infinite wait.
    futures_util::future::pending().await
}

async fn replicate_tokens(db: Arc<FdbClient>, region: Region, streams: RegionalStreams) {
    replicate(
        "tokens",
        db,
        region,
        streams,
        |db, tx, region| async move {
            Ok(TokenReplicationQueue::new(&db, &tx, region)
                .await?
                .dequeue(Duration::from_secs(5))
                .await?
                .map(|(lease_id, tenant_id, token)| {
                    (
                        lease_id,
                        ClientRequestPayload::AddLocalTenant { tenant_id, token },
                    )
                }))
        },
        |payload| matches!(payload, ClientResponsePayload::LocalTenantAdded),
        |db, tx, region, lease_id| async move {
            let queue = TokenReplicationQueue::new(&db, &tx, region).await?;
            queue.complete(lease_id).await
        },
    )
    .await;
}

async fn replicate_shard_routing(db: Arc<FdbClient>, region: Region, streams: RegionalStreams) {
    replicate(
        "shard_routing",
        db,
        region,
        streams,
        |db, tx, region| async move {
            Ok(ShardRoutingReplicationQueue::new(&db, &tx, region)
                .await?
                .dequeue(Duration::from_secs(5))
                .await?
                .map(|(lease_id, tenant_id, shard_name, region)| {
                    (
                        lease_id,
                        ClientRequestPayload::AddShardToLocalRouting {
                            tenant_id,
                            shard_name,
                            region,
                        },
                    )
                }))
        },
        |payload| matches!(payload, ClientResponsePayload::ShardAddedToLocalRouting),
        |db, tx, region, lease_id| async move {
            let queue = ShardRoutingReplicationQueue::new(&db, &tx, region).await?;
            queue.complete(lease_id).await
        },
    )
    .await;
}

async fn replicate_table_schemas(db: Arc<FdbClient>, region: Region, streams: RegionalStreams) {
    replicate(
        "table_schemas",
        db,
        region,
        streams,
        |db, tx, region| async move {
            Ok(GlobalTableSchemasReplicationQueue::new(&db, &tx, region)
                .await?
                .dequeue(Duration::from_secs(5))
                .await?
                .map(|(lease_id, tenant_id, table_name, pk)| {
                    (
                        lease_id,
                        ClientRequestPayload::AddLocalTableSchema {
                            tenant_id,
                            table_name,
                            pk,
                        },
                    )
                }))
        },
        |payload| matches!(payload, ClientResponsePayload::LocalTableSchemaAdded),
        |db, tx, region, lease_id| async move {
            let queue = GlobalTableSchemasReplicationQueue::new(&db, &tx, region).await?;
            queue.complete(lease_id).await
        },
    )
    .await;
}

#[derive(Debug)]
enum ReplicationError {
    FdbError,
    MultiplexerFailure,
    RegionUnavailable,
    LeaseExpired,
}

async fn replicate<'a, DequeueFn, DequeueFut, ResponseCheckFn, CompleteFn, CompleteFut>(
    stream: &str,
    db: Arc<FdbClient>,
    region: Region,
    streams: RegionalStreams,
    dequeue: DequeueFn,
    response_check: ResponseCheckFn,
    complete: CompleteFn,
) where
    DequeueFn: Fn(Arc<FdbClient>, RetryableTransaction, Region) -> DequeueFut,
    DequeueFut: Future<Output = Result<Option<(LeaseId, ClientRequestPayload)>, FdbBindingError>>,
    ResponseCheckFn: Fn(&ClientResponsePayload) -> bool,
    CompleteFn: Fn(Arc<FdbClient>, RetryableTransaction, Region, LeaseId) -> CompleteFut,
    CompleteFut: Future<Output = Result<Result<(), FdbQueueError>, FdbBindingError>>,
{
    info!(?stream, %region, "replicator started");
    let mut error_streak = 0;
    loop {
        // TODO: introduce exponential backoff for errors.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let replicate_fut = async {
            // Dequeue an item from the queue.
            let item = db
                .run(|tx| dequeue(db.clone(), tx, region))
                .await
                .map_err(|_| ReplicationError::FdbError)?;

            let (lease_id, payload) = match item {
                Some(item) => item,
                None => {
                    // Replication queue is empty.
                    info!(%region, "replication queue is empty");
                    return Ok(());
                }
            };

            // Send replication request to the region.
            let correlation_id = Ulid::new().to_string();
            let mut handler = streams
                .create_stream(correlation_id.clone())
                .await
                .map_err(|_| ReplicationError::MultiplexerFailure)?;

            handler
                .send(payload.clone().correlate(correlation_id))
                .map_err(|_| ReplicationError::MultiplexerFailure)?;

            // Confirm the success of the replication request.
            let response = handler
                .recv()
                .await
                .map_err(|_| ReplicationError::MultiplexerFailure)?;

            if !response_check(&response.payload) {
                return Err(ReplicationError::RegionUnavailable);
            }

            // Mark item as completed in the queue.
            let lease_id = &lease_id;
            db.run(|tx| complete(db.clone(), tx, region, lease_id.clone()))
                .await
                .map_err(|_| ReplicationError::FdbError)?
                .map_err(|_| ReplicationError::LeaseExpired)?;

            info!(?stream, %region, "replicated single item");
            Ok(())
        };

        if let Err(err) = replicate_fut.await {
            error_streak += 1;
            warn!(?stream, %region, ?error_streak, ?err, "failed to replicate");
        } else {
            if error_streak > 0 {
                info!(?stream, %region, "replication recovered");
            }
            error_streak = 0;
        }
    }
}
