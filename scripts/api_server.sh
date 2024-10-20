#!/bin/bash

set -euxo pipefail

cargo run --bin api_server -- configs/servers/api_server.toml
