use std::{io::IsTerminal, sync::Arc};

use argh::FromArgs;
use tracing::error;

use crate::config::Config;
use rhino::FdbClient;

pub mod config;
pub mod worker;

#[derive(FromArgs)]
/// Replicator.
struct Arguments {
    /// config file in TOML format.
    #[argh(positional)]
    config_path: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    let args: Arguments = argh::from_env();
    let raw_config = match std::fs::read_to_string(args.config_path) {
        Ok(raw_config) => raw_config,
        Err(err) => {
            error!(?err, "failed to read config file");
            std::process::exit(1);
        }
    };
    let config: Config = match toml::from_str(&raw_config) {
        Ok(config) => config,
        Err(err) => {
            error!(?err, "invalid config format");
            std::process::exit(1);
        }
    };

    let db = Arc::new(FdbClient::new(&config.cluster_file));

    worker::run(db, config.regions).await;
}
