#!/bin/sh
set -e

case "$ROLE" in
    server)
        exec /app/server -port "$PORT" -cert "$CERTFILE" -key "$KEYFILE" -www /app/www
        ;;
    client)
        exec /app/client -host "$SERVER_HOST" -port "$PORT" -testcase "$TESTCASE" -www /app/www
        ;;
    *)
        echo "Unknown role: $ROLE"
        exit 1
        ;;
esac
