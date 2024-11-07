use rhino::Region;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub region: Region,
    pub address: String,
    pub cluster_file: String,
}
