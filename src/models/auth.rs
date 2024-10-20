//! Table schemas:
//!  - auth/table/Tokens
//!    PK: token
//!    Values: tenant_id
//! Queues:
//!  - auth/queue/<region>/GlobalTokens
//!    Values: tenant_id, token

use std::time::Duration;

use foundationdb::{FdbBindingError, RetryableTransaction};

use crate::{
    queue_path, table_path, FdbClient, FdbQueue, FdbQueueError, FdbTable, FdbValue, LeaseId, Region,
};

const AUTH_DIR: &'static str = "auth";

pub struct AuthManager<'a> {
    table: FdbTable<'a>,
}

impl<'a> AuthManager<'a> {
    const TOKENS_TABLE: &'static str = "Tokens";
    const TENANT_ID_COLUMN: &'static str = "tenant_id";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            table: db
                .open_table(tx, &table_path(AUTH_DIR, Self::TOKENS_TABLE))
                .await?,
        })
    }

    pub async fn get_tenant_id_by_token(
        &self,
        token: &str,
    ) -> Result<Option<String>, FdbBindingError> {
        let tenant_id = self
            .table
            .get_one_column(
                vec![FdbValue::String(token.into())],
                Self::TENANT_ID_COLUMN.into(),
            )
            .await?;

        Ok(tenant_id.map(FdbValue::unwrap_string))
    }

    pub fn add_tenant(&self, tenant_id: &str, token: &str) {
        self.table.set(
            vec![FdbValue::String(token.into())],
            [(
                Self::TENANT_ID_COLUMN.into(),
                FdbValue::String(tenant_id.into()),
            )],
        );
    }
}

pub struct TokenReplicationQueue<'a> {
    queue: FdbQueue<'a>,
}

impl<'a> TokenReplicationQueue<'a> {
    const GLOBAL_TOKENS_QUEUE: &'static str = "GlobalTokens";
    const TENANT_ID_COLUMN: &'static str = "tenant_id";
    const TOKEN_COLUMN: &'static str = "token";

    pub async fn new(
        db: &FdbClient,
        tx: &'a RetryableTransaction,
        region: Region,
    ) -> Result<Self, FdbBindingError> {
        Ok(Self {
            queue: db
                .open_queue(tx, &queue_path(AUTH_DIR, region, Self::GLOBAL_TOKENS_QUEUE))
                .await?,
        })
    }

    pub fn add_token(&self, tenant_id: &str, token: &str) {
        self.queue.enqueue([
            (
                Self::TENANT_ID_COLUMN.into(),
                FdbValue::String(tenant_id.into()),
            ),
            (Self::TOKEN_COLUMN.into(), FdbValue::String(token.into())),
        ]);
    }

    pub async fn dequeue(
        &self,
        lease_duration: Duration,
    ) -> Result<Option<(LeaseId, String, String)>, FdbBindingError> {
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
                    values.remove(Self::TOKEN_COLUMN).unwrap().unwrap_string(),
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
