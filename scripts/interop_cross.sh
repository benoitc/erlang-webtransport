#!/bin/sh
# Run the cross-implementation interop harness (erlang <-> webtransport-go).
# Requires docker + docker compose.
set -e

cd "$(dirname "$0")/.."
cd interop
exec docker compose --profile cross up --abort-on-container-exit --build
