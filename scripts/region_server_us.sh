#!/bin/bash

set -euxo pipefail

cargo run --bin region_server -- configs/servers/region_server_us.toml
