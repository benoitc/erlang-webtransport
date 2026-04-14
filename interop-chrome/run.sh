#!/bin/bash
#
# Run WebTransport Chrome interop tests
#
# Usage:
#   ./run.sh           # Run with browser window
#   ./run.sh --headless  # Run headless
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Check for Python virtual environment
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$SCRIPT_DIR/venv"
    source "$SCRIPT_DIR/venv/bin/activate"
    pip install -r "$SCRIPT_DIR/requirements.txt"
else
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Start the Erlang server in the background
echo "Starting WebTransport test server..."
cd "$PROJECT_DIR"

# Compile if needed
rebar3 compile

# Start the server (in background)
erl -pa _build/default/lib/*/ebin \
    -pa interop-chrome \
    -noshell \
    -s main start &
SERVER_PID=$!

# Wait for server to start
sleep 3

# Run Python tests
echo "Running Chrome interop tests..."
cd "$SCRIPT_DIR"
python3 interop.py --wait-for-server "$@"
TEST_EXIT_CODE=$?

# Cleanup
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true

exit $TEST_EXIT_CODE
