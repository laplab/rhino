#!/bin/bash

set -euxo pipefail

docker compose up -d --force-recreate

for name in fdb-central fdb-eu fdb-us; do
  fdbcli -C "${name}.cluster" --exec "configure new single ssd" && echo "${name}: database created" || echo "${name}: database already exists"
done
