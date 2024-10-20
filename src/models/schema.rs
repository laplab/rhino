//! Table schemas:
//!  - schema/table/TableSchemas
//!    PK: tenant_id, table name
//!    Values: pk
//! Queue schemas:
//!  - schema/queue/<region>/GlobalTableSchemas
//!    Values: tenant_id, table_name, pk

use std::time::Duration;

use foundationdb::{FdbBindingError, RetryableTransaction};
use itertools::Itertools;

use crate::{
    queue_path, table_path, FdbClient, FdbQueue, FdbQueueError, FdbTable, FdbValue, LeaseId, Region,
};

const SCHEMA_DIR: &'static str = "schema";

pub struct SchemaManager<'a> {
    table: FdbTable<'a>,
}

pub enum SchemaManagerError {
    TableAlreadyExists,
}

impl<'a> SchemaManager<'a> {
    const SCHEMAS_TABLE: &'static str = "TableSchemas";
    const PK_COLUMN: &'static str = "pk";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            table: db
                .open_table(&tx, &table_path(SCHEMA_DIR, Self::SCHEMAS_TABLE))
                .await?,
        })
    }

    pub async fn get_table_pk(
        &self,
        tenant_id: &str,
        table_name: &str,
    ) -> Result<Option<Vec<String>>, FdbBindingError> {
        let tenant_id = self
            .table
            .get_one_column(
                vec![
                    FdbValue::String(tenant_id.into()),
                    FdbValue::String(table_name.into()),
                ],
                Self::PK_COLUMN.into(),
            )
            .await?;

        Ok(tenant_id
            .map(FdbValue::unwrap_string)
            .map(|pk| pk.split(",").map(<_>::into).collect()))
    }

    pub async fn add_table(
        &self,
        tenant_id: &str,
        table_name: &str,
        pk: Vec<String>,
        force: bool,
    ) -> Result<Result<(), SchemaManagerError>, FdbBindingError> {
        let primary_key = vec![
            FdbValue::String(tenant_id.into()),
            FdbValue::String(table_name.into()),
        ];

        if !force {
            let existing_table = self
                .table
                .get_one_column(primary_key.clone(), Self::PK_COLUMN.into())
                .await?;
            if existing_table.is_some() {
                return Ok(Err(SchemaManagerError::TableAlreadyExists));
            }
        }

        self.table.set(
            primary_key,
            [(
                Self::PK_COLUMN.into(),
                FdbValue::String(pk.into_iter().join(",")),
            )],
        );

        Ok(Ok(()))
    }
}

pub struct GlobalTableSchemasReplicationQueue<'a> {
    queue: FdbQueue<'a>,
}

impl<'a> GlobalTableSchemasReplicationQueue<'a> {
    const GLOBAL_TABLE_SCHEMAS_QUEUE: &'static str = "GlobalTableSchemas";
    const TENANT_ID_COLUMN: &'static str = "tenant_id";
    const TABLE_NAME_COLUMN: &'static str = "table_name";
    const PK_COLUMN: &'static str = "pk";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
        region: Region,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            queue: db
                .open_queue(
                    tx,
                    &queue_path(SCHEMA_DIR, region, Self::GLOBAL_TABLE_SCHEMAS_QUEUE),
                )
                .await?,
        })
    }

    pub fn add_table(&self, tenant_id: &str, table_name: &str, pk: Vec<String>) {
        self.queue.enqueue([
            (
                Self::TENANT_ID_COLUMN.into(),
                FdbValue::String(tenant_id.into()),
            ),
            (
                Self::TABLE_NAME_COLUMN.into(),
                FdbValue::String(table_name.into()),
            ),
            (
                Self::PK_COLUMN.into(),
                FdbValue::String(pk.into_iter().join(",")),
            ),
        ]);
    }

    pub async fn dequeue(
        &self,
        lease_duration: Duration,
    ) -> Result<Option<(LeaseId, String, String, Vec<String>)>, FdbBindingError> {
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
                        .remove(Self::TABLE_NAME_COLUMN)
                        .unwrap()
                        .unwrap_string(),
                    values
                        .remove(Self::PK_COLUMN)
                        .unwrap()
                        .unwrap_string()
                        .split(",")
                        .map(<_>::into)
                        .collect(),
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
