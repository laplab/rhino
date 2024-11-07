use std::sync::Arc;

use mahogany::{
    control_server::{ClientRequest, ClientRequestPayload, ClientResponse, ClientResponsePayload},
    recv_or_stop, run_ws_server, send_or_stop, AuthManager, FdbClient,
    GlobalTableSchemasReplicationQueue, Region, RoutingManager, RoutingManagerError, SchemaManager,
    SchemaManagerError, ShardRoutingReplicationQueue, TokenReplicationQueue,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

struct ControlHandler {
    requests: UnboundedReceiver<ClientRequest>,
    responses: UnboundedSender<ClientResponse>,

    db: Arc<FdbClient>,
    regions: Vec<Region>,
}

impl ControlHandler {
    fn new(
        requests: UnboundedReceiver<ClientRequest>,
        responses: UnboundedSender<ClientResponse>,
        db: Arc<FdbClient>,
        regions: Vec<Region>,
    ) -> Self {
        ControlHandler {
            requests,
            responses,
            db,
            regions,
        }
    }

    fn state(&self, correlation_id: String) -> ControlHandlerState {
        ControlHandlerState {
            responses: self.responses.clone(),
            db: self.db.clone(),
            regions: self.regions.clone(),
            correlation_id,
        }
    }

    async fn handle(mut self) {
        loop {
            let ClientRequest {
                payload,
                correlation_id,
            } = recv_or_stop!(self.requests);

            let state = self.state(correlation_id);

            use ClientRequestPayload::*;
            match payload {
                AddGlobalTenant { tenant_id, token } => {
                    tokio::spawn(state.handle_add_global_tenant(tenant_id, token))
                }
                AddShardToGlobalRouting {
                    tenant_id,
                    shard_name,
                    region,
                } => tokio::spawn(
                    state.handle_add_shard_to_global_routing(tenant_id, shard_name, region),
                ),
                AddGlobalTableSchema {
                    tenant_id,
                    table_name,
                    pk,
                } => tokio::spawn(state.handle_add_global_table_schema(tenant_id, table_name, pk)),
                GetShardRegion {
                    tenant_id,
                    shard_name,
                } => tokio::spawn(state.handle_get_shard_region(tenant_id, shard_name)),
                GetTableSchema {
                    tenant_id,
                    table_name,
                } => tokio::spawn(state.handle_get_table_schema(tenant_id, table_name)),
            };
        }
    }
}

struct ControlHandlerState {
    responses: UnboundedSender<ClientResponse>,
    db: Arc<FdbClient>,
    regions: Vec<Region>,
    correlation_id: String,
}

impl ControlHandlerState {
    async fn handle_add_global_tenant(self, tenant_id: String, token: String) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let regions = self.regions.as_slice();
                let tenant_id = &tenant_id;
                let token = &token;
                async move {
                    let auth = AuthManager::new(db, &tx).await?;
                    auth.add_tenant(&tenant_id, &token);

                    for region in regions {
                        let queue = TokenReplicationQueue::new(db, &tx, *region).await?;
                        queue.add_token(&tenant_id, &token);
                    }
                    Ok(())
                }
            })
            .await;
        let response = match result {
            Ok(..) => ClientResponsePayload::GlobalTenantAdded,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_add_shard_to_global_routing(
        self,
        tenant_id: String,
        shard_name: String,
        shard_region: Region,
    ) {
        // TODO: check shard name length.
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let regions = self.regions.as_slice();
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                async move {
                    let routing = RoutingManager::new(db, &tx).await?;
                    if let Err(err) = routing
                        .add_shard(tenant_id, shard_name, shard_region, false)
                        .await?
                    {
                        Ok(Err(err))
                    } else {
                        for replication_region in regions {
                            let queue =
                                ShardRoutingReplicationQueue::new(db, &tx, *replication_region)
                                    .await?;
                            queue.add_shard(tenant_id, shard_name, shard_region);
                        }
                        Ok(Ok(()))
                    }
                }
            })
            .await;

        let response = match result {
            Ok(Ok(())) => ClientResponsePayload::ShardAddedToGlobalRouting,
            Ok(Err(RoutingManagerError::RouteAlreadyExists)) => {
                ClientResponsePayload::ShardRoutingAlreadyExists
            }
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_add_global_table_schema(
        self,
        tenant_id: String,
        table_name: String,
        pk: Vec<String>,
    ) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let regions = self.regions.as_slice();
                let tenant_id = &tenant_id;
                let table_name = &table_name;
                let pk = &pk;
                async move {
                    let schema = SchemaManager::new(db, &tx).await?;
                    if let Err(err) = schema
                        .add_table(tenant_id, &table_name, pk.clone(), false)
                        .await?
                    {
                        Ok(Err(err))
                    } else {
                        for replication_region in regions {
                            let queue = GlobalTableSchemasReplicationQueue::new(
                                db,
                                &tx,
                                *replication_region,
                            )
                            .await?;
                            queue.add_table(tenant_id, &table_name, pk.clone());
                        }
                        Ok(Ok(()))
                    }
                }
            })
            .await;

        let response = match result {
            Ok(Ok(())) => ClientResponsePayload::GlobalTableSchemaAdded,
            Ok(Err(SchemaManagerError::TableAlreadyExists)) => {
                ClientResponsePayload::TableAlreadyExists
            }
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_get_shard_region(self, tenant_id: String, shard_name: String) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let shard_name = &shard_name;
                async move {
                    let routing = RoutingManager::new(db, &tx).await?;
                    routing.get_shard_region(tenant_id, shard_name).await
                }
            })
            .await;

        let response = match result {
            Ok(Some(region)) => ClientResponsePayload::ShardLocated { region },
            Ok(None) => ClientResponsePayload::ShardNotFound,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }

    async fn handle_get_table_schema(self, tenant_id: String, table_name: String) {
        let result = self
            .db
            .run(|tx| {
                let db = &self.db;
                let tenant_id = &tenant_id;
                let table_name = &table_name;
                async move {
                    let schema = SchemaManager::new(db, &tx).await?;
                    schema.get_table_pk(tenant_id, table_name).await
                }
            })
            .await;

        let response = match result {
            Ok(Some(pk)) => ClientResponsePayload::TableSchema { pk },
            Ok(None) => ClientResponsePayload::TableNotFound,
            Err(..) => ClientResponsePayload::FdbError,
        };

        send_or_stop!(self.responses, response.correlate(self.correlation_id));
    }
}

pub async fn run(address: &str, db: Arc<FdbClient>, regions: Vec<Region>) {
    run_ws_server(address, move |requests, responses| {
        ControlHandler::new(requests, responses, db.clone(), regions.clone()).handle()
    })
    .await;
}
