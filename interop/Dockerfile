FROM erlang:26-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git openssl-dev gcc musl-dev make

# Copy application
COPY rebar.config rebar.lock ./
COPY src ./src
COPY include ./include
COPY interop ./interop

# Fetch dependencies and compile
RUN rebar3 compile

# Compile interop modules
RUN mkdir -p /build/_build/default/lib/webtransport/ebin && \
    ERL_LIBS=/build/_build/default/lib erlc -I include \
        -o _build/default/lib/webtransport/ebin \
        interop/interop.erl \
        interop/interop_server.erl \
        interop/interop_client.erl

# Runtime image
FROM erlang:26-alpine

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache openssl iproute2

# Copy built application
COPY --from=builder /build/_build/default/lib /app/lib

# Copy test data
COPY interop/www /app/www

# Generate certificates
RUN mkdir -p /app/certs && \
    openssl genrsa -out /app/certs/key.pem 2048 && \
    openssl req -new -x509 -key /app/certs/key.pem \
        -out /app/certs/cert.pem -days 365 \
        -subj "/CN=localhost"

# Entry point
COPY interop/run_endpoint.sh /app/run_endpoint.sh
RUN chmod +x /app/run_endpoint.sh

# Environment variables
ENV ROLE=server
ENV PORT=443
ENV TESTCASE=handshake
ENV SERVER_HOST=server
ENV CERTFILE=/app/certs/cert.pem
ENV KEYFILE=/app/certs/key.pem

EXPOSE 443/udp

ENTRYPOINT ["/app/run_endpoint.sh"]
