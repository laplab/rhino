use serde::{Deserialize, Serialize};

use crate::{protocol::WithCorrelationId, Region};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    pub payload: ClientRequestPayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientRequestPayload {
    AddGlobalTenant {
        tenant_id: String,
        token: String,
    },
    AddShardToGlobalRouting {
        tenant_id: String,
        shard_name: String,
        region: Region,
    },
    AddGlobalTableSchema {
        tenant_id: String,
        table_name: String,
        pk: Vec<String>,
    },

    GetShardRegion {
        tenant_id: String,
        shard_name: String,
    },
    GetTableSchema {
        tenant_id: String,
        table_name: String,
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

    GlobalTenantAdded,

    ShardAddedToGlobalRouting,
    ShardRoutingAlreadyExists,

    GlobalTableSchemaAdded,
    TableAlreadyExists,

    ShardLocated {
        region: Region,
    },
    ShardNotFound,

    TableSchema {
        pk: Vec<String>,
    },
    TableNotFound,
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
