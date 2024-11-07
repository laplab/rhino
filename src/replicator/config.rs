use rhino::{Region, RegionInfo};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub cluster_file: String,
    pub regions: Vec<RegionInfo>,
}
