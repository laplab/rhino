mod auth;
mod routing;
mod schema;
mod table;

pub use crate::models::{auth::*, routing::*, schema::*, table::*};
use crate::Region;

pub(crate) fn table_path(prefix: &str, name: &str) -> [String; 3] {
    [prefix.into(), String::from("table"), name.into()]
}

pub(crate) fn queue_path(prefix: &str, region: Region, name: &str) -> [String; 4] {
    [
        prefix.into(),
        String::from("queue"),
        region.to_string(),
        name.into(),
    ]
}
