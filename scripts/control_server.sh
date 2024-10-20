#!/bin/bash

set -euxo pipefail

cargo run --bin control_server -- configs/servers/control_server.toml
