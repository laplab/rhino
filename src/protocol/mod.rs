use std::str::FromStr;

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::FdbValue;

pub mod api_server;
pub mod control_server;
pub mod region_server;

pub trait WithCorrelationId {
    fn correlation_id(&self) -> &str;
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, PartialEq, Eq, Display)]
pub enum Region {
    #[display(fmt = "control")]
    #[serde(rename = "control")]
    Control,
    #[display(fmt = "eu-west-2")]
    #[serde(rename = "eu-west-2")]
    EuWest2,
    #[display(fmt = "us-east-2")]
    #[serde(rename = "us-east-2")]
    UsEast2,
}

impl FromStr for Region {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "eu-west-2" => Ok(Region::EuWest2),
            "us-east-2" => Ok(Region::UsEast2),
            "control" => Err("`control` region should never be parsed from config"),
            _ => Err("invalid region"),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct RegionInfo {
    pub region: Region,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SerialisableFdbValue {
    String(String),
    U64(u64),
}

impl From<FdbValue> for SerialisableFdbValue {
    fn from(value: FdbValue) -> Self {
        match value {
            FdbValue::String(value) => Self::String(value),
            FdbValue::U64(value) => Self::U64(value),
        }
    }
}

impl From<SerialisableFdbValue> for FdbValue {
    fn from(value: SerialisableFdbValue) -> Self {
        match value {
            SerialisableFdbValue::String(value) => Self::String(value),
            SerialisableFdbValue::U64(value) => Self::U64(value),
        }
    }
}
