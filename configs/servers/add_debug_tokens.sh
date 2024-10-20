#!/bin/bash

set -euxo pipefail

websocat --one-message --no-close ws://localhost:3003 < configs/servers/debug_tokens.ndjson

