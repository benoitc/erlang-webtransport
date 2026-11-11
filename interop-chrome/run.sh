#!/bin/bash
#
# Run WebTransport Chrome interop tests
#
# Usage:
#   ./run.sh                          # Run with browser window
#   ./run.sh --headless               # Run headless
#   ./run.sh --wt-port 4443 --headless
#
# Ports: --wt-port (UDP, default 4433) and --http-port (TCP, default 8080),
# or the WT_PORT and HTTP_PORT environment variables. VENV picks the Python
# virtual environment (default interop-chrome/venv). Other arguments go to
# interop.py.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

WT_PORT="${WT_PORT:-4433}"
HTTP_PORT="${HTTP_PORT:-8080}"
VENV="${VENV:-$SCRIPT_DIR/venv}"
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --wt-port) WT_PORT="$2"; shift 2 ;;
        --http-port) HTTP_PORT="$2"; shift 2 ;;
        *) ARGS+=("$1"); shift ;;
    esac
done

# Check for Python virtual environment
if [ ! -d "$VENV" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
fi

cd "$PROJECT_DIR"
rebar3 compile

# Harness beams, certificate and served files live outside the repo
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/wt-chrome.XXXXXX")"
SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo "Stopping server..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

mkdir -p "$WORK_DIR/ebin"
erlc -o "$WORK_DIR/ebin" -pa _build/default/lib/webtransport/ebin \
    "$SCRIPT_DIR/main.erl" "$SCRIPT_DIR/chrome_test_handler.erl"

echo "Starting WebTransport test server..."
erl -noshell \
    -pa _build/default/lib/*/ebin \
    -pa "$WORK_DIR/ebin" \
    -eval "main:start(#{wt_port => $WT_PORT, http_port => $HTTP_PORT,
                        work_dir => \"$WORK_DIR\", page_dir => \"$SCRIPT_DIR\"})" &
SERVER_PID=$!

echo "Running Chrome interop tests..."
cd "$SCRIPT_DIR"
"$VENV/bin/python" interop.py --wait-for-server \
    --server-url "https://127.0.0.1:$WT_PORT/test" \
    --page-url "http://127.0.0.1:$HTTP_PORT/index.html" \
    ${ARGS[@]+"${ARGS[@]}"}
