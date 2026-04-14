#!/bin/sh
set -e

cd /app

# Start the Erlang node
case "$ROLE" in
    server)
        echo "Starting WebTransport interop server on port $PORT"
        erl -pa lib/*/ebin \
            -noshell \
            -s interop_server start \
            -port "$PORT" \
            -certfile "$CERTFILE" \
            -keyfile "$KEYFILE" \
            -www "/app/www"
        ;;
    client)
        echo "Running WebTransport interop client, testcase: $TESTCASE"
        erl -pa lib/*/ebin \
            -noshell \
            -s interop_client run \
            -host "$SERVER_HOST" \
            -port "$PORT" \
            -testcase "$TESTCASE"
        ;;
    *)
        echo "Unknown role: $ROLE"
        echo "Use ROLE=server or ROLE=client"
        exit 1
        ;;
esac
