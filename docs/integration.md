# Embedding in an HTTP Server

The `webtransport:accept/4` function lets you add WebTransport to any
HTTP/3 or HTTP/2 server. Instead of `start_listener/2` (which creates its
own server), you call `accept/4` from your existing request handler --
the same pattern as upgrading a request to WebSocket.

## HTTP/3 (quic_h3)

### 1. Merge WT settings into your server opts

```erlang
H3Opts = maps:merge(webtransport:h3_settings(), #{
    cert => CertDer,
    key => PrivateKey,
    handler => fun my_handler/5
}),
{ok, _} = quic_h3:start_server(my_server, 443, H3Opts).
```

`h3_settings/0` returns:

| Key | Value |
|-----|-------|
| `settings` | H3 SETTINGS (SETTINGS_WT_ENABLED for draft-17 and draft-15, flow-control limits, draft-02 and draft-07 compat) |
| `stream_type_handler` | Claims WT extension streams (0x41 bidi, 0x54 uni) |
| `h3_datagram_enabled` | `true` |
| `defer_requests_until_settings` | `true`; requests wait for the client's SETTINGS, which select the draft. Without it a request that arrives ahead of them is refused |
| `quic_opts` | `#{max_datagram_frame_size => 65535, reset_stream_at => true}` |
| `connection_handler` | Creates a per-connection WT stream router |

Pass options to customize:

```erlang
webtransport:h3_settings(#{
    compat_mode => latest,          %% default: auto
    max_data => 4194304,            %% default: 1 MB
    max_streams_bidi => 200         %% default: 100
}).
```

### 2. Route requests and accept WebTransport

```erlang
my_handler(H3Conn, StreamId, <<"CONNECT">>, <<"/chat">>, Headers) ->
    {ok, _Session} = webtransport:accept(H3Conn, StreamId, Headers, #{
        transport => h3,
        handler => chat_handler,
        handler_opts => #{room => lobby}
    });

my_handler(H3Conn, StreamId, <<"CONNECT">>, <<"/game">>, Headers) ->
    {ok, _Session} = webtransport:accept(H3Conn, StreamId, Headers, #{
        transport => h3,
        handler => game_handler
    });

my_handler(H3Conn, StreamId, <<"GET">>, <<"/health">>, _Headers) ->
    quic_h3:send_response(H3Conn, StreamId, 200, []),
    quic_h3:send_data(H3Conn, StreamId, <<"ok">>, true);

my_handler(H3Conn, StreamId, _Method, _Path, _Headers) ->
    quic_h3:send_response(H3Conn, StreamId, 404, []),
    quic_h3:send_data(H3Conn, StreamId, <<"not found">>, true).
```

`accept/4` may be called from any process, not only the one running your
handler. The connection_handler installed by `h3_settings/0` registers the
per-connection stream router keyed by the QUIC connection pid, and `accept/4`
resolves the same router from its `H3Conn`. So if your server dispatches each
request to a per-request worker, calling `accept/4` from that worker still
routes the session's incoming streams and datagrams correctly.

## HTTP/2 (erlang_h2)

### 1. Merge WT settings into your server opts

```erlang
H2Opts = maps:merge(webtransport:h2_settings(), #{
    cert => "cert.pem",
    key => "key.pem",
    handler => fun my_h2_handler/5
}),
{ok, _} = h2:start_server(443, H2Opts).
```

`h2_settings/0` returns:

| Key | Value |
|-----|-------|
| `enable_connect_protocol` | `true` |
| `settings` | H2 SETTINGS (enable_connect_protocol, SETTINGS_WT_ENABLED, wt_initial_max_*) |

### 2. Route requests

```erlang
my_h2_handler(Conn, StreamId, <<"CONNECT">>, <<"/wt">> = Path, Headers) ->
    %% h2 passes the method and path as arguments, not as headers.
    {ok, _Session} = webtransport:accept(Conn, StreamId, Headers, #{
        transport => h2,
        handler => my_wt_handler,
        path => Path
    });

my_h2_handler(Conn, StreamId, <<"GET">>, Path, Headers) ->
    serve_static(Conn, StreamId, Path, Headers).
```

## What `accept/4` does

`accept(Conn, StreamId, Headers, Opts)` performs these steps:

1. Validates the CONNECT request (`:protocol`, compat mode detection for h3)
2. Runs `origin_check/2` on the handler (if exported)
3. h3: checks the client's SETTINGS and transport parameters (HTTP/3
   datagrams, QUIC datagrams, RESET_STREAM_AT) and picks draft-17 or
   draft-15. h2: parses `WebTransport-Init`; an invalid value gets 400
4. Calls `select_protocol/2` on the handler (if exported)
5. Starts a `webtransport_session` gen_statem
6. For h3: registers with the per-connection router so extension streams
   (bidirectional 0x41, unidirectional 0x54) are routed to the session
7. Sends 200 OK, with `WT-Protocol` when a subprotocol was picked
8. Registers the session as the stream handler (`quic_h3:set_stream_handler/4`
   or `h2:set_stream_handler/3`), so capsules the client sent with the request
   are handled only after the 200
9. Returns `{ok, Session}` or `{error, Reason}`

### Options

| Key | Default | Description |
|-----|---------|-------------|
| `transport` | `h3` | `h3` or `h2` |
| `handler` | required | Module implementing `webtransport_handler` |
| `handler_opts` | `#{}` | Passed to `handler:init/3` |
| `compat_mode` | `auto` | h3 draft selection |
| `max_data` | 1048576 | Session flow-control window |
| `max_stream_data` | 262144 | Per-stream window (h2) |
| `max_streams_bidi` | 100 | Max bidi streams |
| `max_streams_uni` | 100 | Max uni streams |
| `path` | from `:path` | Request path. h2 passes it to your handler as an argument, not as a header; pass it here |

On h2 the connection must use TLS 1.3 (http2-16 §7; OTP's `ssl` has no
extended master secret for TLS 1.2). `accept/4` resets the CONNECT stream
with `PROTOCOL_ERROR` otherwise, so offer TLS 1.3 on servers that carry
WebTransport.

## Relationship to `start_listener/2`

`start_listener/2` is a convenience wrapper that:

1. Creates a quic_h3 or h2 server
2. Installs a catch-all handler that calls `accept/4` for every CONNECT
3. Returns 405 for non-CONNECT methods

Use `start_listener/2` for simple WT-only servers. Use `h3_settings/0` +
`accept/4` when you need to:

- Serve regular HTTP alongside WebTransport on the same port
- Route different paths to different handlers
- Integrate with an existing HTTP server framework
