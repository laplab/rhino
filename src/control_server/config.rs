use rhino::Region;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub address: String,
    pub cluster_file: String,
    pub regions: Vec<Region>,
}
