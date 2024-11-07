use serde::Deserialize;

use rhino::{Region, RegionInfo};

// TODO: validate addresses.

#[derive(Deserialize)]
pub struct Config {
    pub address: String,
    pub control_address: String,
    pub local_region: Region,
    pub regions: Vec<RegionInfo>,
}
