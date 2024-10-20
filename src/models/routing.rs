//! Table schemas:
//!  - routing/table/Shards
//!    PK: tenant_id, shard name
//!    Values: region
//! Queue schemas:
//!  - routing/queue/<region>/GlobalShards
//!    Values: tenant_id, shard_name, region

use std::{str::FromStr, time::Duration};

use foundationdb::{FdbBindingError, RetryableTransaction};

use crate::{
    queue_path, table_path, FdbClient, FdbQueue, FdbQueueError, FdbTable, FdbValue, LeaseId, Region,
};

const ROUTING_DIR: &'static str = "routing";

pub struct RoutingManager<'a> {
    table: FdbTable<'a>,
}

pub enum RoutingManagerError {
    RouteAlreadyExists,
}

impl<'a> RoutingManager<'a> {
    const SHARDS_TABLE: &'static str = "Shards";
    const REGION_COLUMN: &'static str = "region";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            table: db
                .open_table(&tx, &table_path(ROUTING_DIR, Self::SHARDS_TABLE))
                .await?,
        })
    }

    pub async fn get_shard_region(
        &self,
        tenant_id: &str,
        shard_name: &str,
    ) -> Result<Option<Region>, FdbBindingError> {
        let tenant_id = self
            .table
            .get_one_column(
                vec![
                    FdbValue::String(tenant_id.into()),
                    FdbValue::String(shard_name.into()),
                ],
                Self::REGION_COLUMN.into(),
            )
            .await?;

        Ok(tenant_id
            .map(FdbValue::unwrap_string)
            .map(|region| Region::from_str(&region).unwrap()))
    }

    pub async fn add_shard(
        &self,
        tenant_id: &str,
        shard_name: &str,
        region: Region,
        force: bool,
    ) -> Result<Result<(), RoutingManagerError>, FdbBindingError> {
        let primary_key = vec![
            FdbValue::String(tenant_id.into()),
            FdbValue::String(shard_name.into()),
        ];

        if !force {
            let existing_routing = self
                .table
                .get_one_column(primary_key.clone(), Self::REGION_COLUMN.into())
                .await?;
            if existing_routing.is_some() {
                return Ok(Err(RoutingManagerError::RouteAlreadyExists));
            }
        }

        self.table.set(
            primary_key,
            [(
                Self::REGION_COLUMN.into(),
                FdbValue::String(region.to_string()),
            )],
        );

        Ok(Ok(()))
    }
}

pub struct ShardRoutingReplicationQueue<'a> {
    queue: FdbQueue<'a>,
}

impl<'a> ShardRoutingReplicationQueue<'a> {
    const GLOBAL_SHARDS_QUEUE: &'static str = "GlobalShards";
    const TENANT_ID_COLUMN: &'static str = "tenant_id";
    const SHARD_NAME_COLUMN: &'static str = "shard_name";
    const REGION_COLUMN: &'static str = "region";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
        region: Region,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            queue: db
                .open_queue(
                    tx,
                    &queue_path(ROUTING_DIR, region, Self::GLOBAL_SHARDS_QUEUE),
                )
                .await?,
        })
    }

    pub fn add_shard(&self, tenant_id: &str, shard_name: &str, region: Region) {
        self.queue.enqueue([
            (
                Self::TENANT_ID_COLUMN.into(),
                FdbValue::String(tenant_id.into()),
            ),
            (
                Self::SHARD_NAME_COLUMN.into(),
                FdbValue::String(shard_name.into()),
            ),
            (
                Self::REGION_COLUMN.into(),
                FdbValue::String(region.to_string()),
            ),
        ]);
    }

    pub async fn dequeue(
        &self,
        lease_duration: Duration,
    ) -> Result<Option<(LeaseId, String, String, Region)>, FdbBindingError> {
        Ok(self
            .queue
            .dequeue(lease_duration)
            .await?
            .map(|(lease_id, mut values)| {
                (
                    lease_id,
                    values
                        .remove(Self::TENANT_ID_COLUMN)
                        .unwrap()
                        .unwrap_string(),
                    values
                        .remove(Self::SHARD_NAME_COLUMN)
                        .unwrap()
                        .unwrap_string(),
                    Region::from_str(&values.remove(Self::REGION_COLUMN).unwrap().unwrap_string())
                        .unwrap(),
                )
            }))
    }

    pub async fn complete(
        &self,
        lease_id: LeaseId,
    ) -> Result<Result<(), FdbQueueError>, FdbBindingError> {
        self.queue.complete(lease_id).await
    }
}
