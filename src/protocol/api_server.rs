use serde::{Deserialize, Serialize};

use crate::{Region, WithCorrelationId};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientRequest {
    pub payload: ClientRequestPayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientRequestPayload {
    Login { token: String },
    CreateShard { name: String, region: Region },
    CreateTable { name: String, pk: Vec<String> },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientResponse {
    pub payload: ClientResponsePayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientResponsePayload {
    NotLoggedIn,

    InvalidToken,
    AlreadyLoggedIn,
    LoggedIn { region: Region },

    ShardCreated,
    ShardAlreadyExists,

    TableCreated,
    TableAlreadyExists,

    PleaseRetry { reason: String },
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
