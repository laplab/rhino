#!/bin/bash

set -euxo pipefail

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

cargo build --bin control_server
cargo build --bin region_server
cargo build --bin api_server
cargo build --bin replicator

# Start FDB cluster.
# cd configs/fdb
# ./setup.sh
# cd ../../

rm -r logs
mkdir -p logs

pids=()
for p in control_server region_server_eu region_server_us replicator api_server; do
    "./scripts/$p.sh" > "logs/$p.txt" &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid"
done
