FROM golang:1.23-alpine AS builder

WORKDIR /src

COPY interop/go/go.mod ./
RUN go mod download || true

COPY interop/go/server ./server
COPY interop/go/client ./client

RUN go mod tidy
RUN go build -o /out/server ./server
RUN go build -o /out/client ./client

FROM alpine:3.19

RUN apk add --no-cache openssl ca-certificates

WORKDIR /app

COPY --from=builder /out/server /app/server
COPY --from=builder /out/client /app/client
COPY interop/www /app/www

RUN mkdir -p /app/certs && \
    openssl genrsa -out /app/certs/key.pem 2048 && \
    openssl req -new -x509 -key /app/certs/key.pem \
        -out /app/certs/cert.pem -days 365 \
        -subj "/CN=localhost"

COPY interop/go/run_endpoint.sh /app/run_endpoint.sh
RUN chmod +x /app/run_endpoint.sh

ENV ROLE=server
ENV PORT=443
ENV TESTCASE=handshake
ENV SERVER_HOST=server-go
ENV CERTFILE=/app/certs/cert.pem
ENV KEYFILE=/app/certs/key.pem

EXPOSE 443/udp

ENTRYPOINT ["/app/run_endpoint.sh"]
