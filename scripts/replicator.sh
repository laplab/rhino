#!/bin/bash

set -euxo pipefail

cargo run --bin replicator -- configs/servers/replicator.toml
