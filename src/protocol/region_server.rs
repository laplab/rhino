use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{protocol::WithCorrelationId, Region, SerialisableFdbValue};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    pub payload: ClientRequestPayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientRequestPayload {
    // Endpoints for API server.
    GetTenantIdByToken {
        token: String,
    },
    GetShardRegion {
        tenant_id: String,
        shard_name: String,
    },
    GetTableSchema {
        tenant_id: String,
        table_name: String,
    },
    SetRow {
        tenant_id: String,
        shard_name: String,
        table_name: String,
        row: HashMap<String, SerialisableFdbValue>,
    },
    GetRow {
        tenant_id: String,
        shard_name: String,
        table_name: String,
        pk: HashMap<String, SerialisableFdbValue>,
    },

    // Endpoints for replicator.
    AddLocalTenant {
        tenant_id: String,
        token: String,
    },
    AddShardToLocalRouting {
        tenant_id: String,
        shard_name: String,
        region: Region,
    },
    AddLocalTableSchema {
        tenant_id: String,
        table_name: String,
        pk: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse {
    pub payload: ClientResponsePayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponsePayload {
    /// Failed to process request due to error from FoundationDB. This outcome is considered
    /// retryable.
    FdbError,

    TenantIdByToken {
        tenant_id: String,
    },
    InvalidToken,

    ShardLocated {
        region: Region,
    },
    ShardNotFound,

    TableSchema {
        pk: Vec<String>,
    },
    TableNotFound,

    LocalTenantAdded,

    ShardAddedToLocalRouting,

    LocalTableSchemaAdded,

    SetRowCompleted,

    RowFound {
        row: HashMap<String, SerialisableFdbValue>,
    },
    RowNotFound,

    MissingPkValue {
        component: String,
    },
}

impl ClientRequestPayload {
    pub fn correlate(self, correlation_id: String) -> ClientRequest {
        ClientRequest {
            payload: self,
            correlation_id,
        }
    }
}

impl ClientResponsePayload {
    pub fn correlate(self, correlation_id: String) -> ClientResponse {
        ClientResponse {
            payload: self,
            correlation_id,
        }
    }
}

impl WithCorrelationId for ClientRequest {
    fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
}

impl WithCorrelationId for ClientResponse {
    fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
}
