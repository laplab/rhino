use std::{collections::HashMap, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::{Region, SerializableFdbValue, WithCorrelationId};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientRequest {
    pub payload: ClientRequestPayload,
    pub correlation_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientRequestPayload {
    Login {
        token: String,
    },
    CreateShard {
        name: String,
        region: Region,
    },
    CreateTable {
        name: String,
        pk: Vec<String>,
    },
    GetRow {
        shard_name: String,
        table_name: String,
        pk: HashMap<String, SerializableFdbValue>,
    },
    SetRow {
        shard_name: String,
        table_name: String,
        row: HashMap<String, SerializableFdbValue>,
    },
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
    LoggedIn {
        region: Region,
    },

    ShardCreated,
    ShardAlreadyExists,
    ShardNotFound,

    TableCreated,
    TableAlreadyExists,
    TableNotFound,

    PleaseRetry {
        reason: String,
    },

    RowFound {
        row: HashMap<String, SerializableFdbValue>,
    },
    RowNotFound,

    MissingPkValue {
        component: String,
    },

    SetRowCompleted,
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
