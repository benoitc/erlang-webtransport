# Configuration

## Server Options

```erlang
webtransport:start_listener(Name, Opts).
```

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `transport` | yes | -- | `h2` (HTTP/2) or `h3` (HTTP/3) |
| `port` | yes | -- | TCP/UDP port to listen on |
| `certfile` | yes | -- | Path to TLS certificate (PEM) |
| `keyfile` | yes | -- | Path to TLS private key (PEM) |
| `handler` | yes | -- | Module implementing `webtransport_handler` |
| `handler_opts` | no | `#{}` | Map passed to `handler:init/3` |
| `max_data` | no | 1048576 (1 MB) | Session data the peer may send before we grant more (bytes) |
| `max_stream_data` | no | 262144 (256 KB) | Per-stream data window granted to the peer (HTTP/2 only) |
| `max_streams_bidi` | no | 100 | Bidirectional streams the peer may have open at once |
| `max_streams_uni` | no | 100 | Unidirectional streams the peer may have open at once |
| `max_send_buffer` | no | 4194304 (4 MB) | Bytes a session may hold waiting for the peer's flow-control credit; a send beyond it fails with `{error, send_buffer_full}` |
| `ip` | no | wildcard | Bind address (`inet:ip_address()`; IPv4 4-tuple or IPv6 8-tuple) |
| `family` | no | `inet` | `inet` or `inet6`; forces the family when no `ip` is given (e.g. the IPv6 wildcard) |
| `socket_opts` | no | `[]` | Extra options for the underlying listener socket (h3 only) |
| `sni_callback` | no | -- | Per-SNI certificate selection (see below) |
| `compat_mode` | no | `auto` | HTTP/3 draft selection (see below) |

### Per-SNI certificates

`sni_callback` picks the server certificate per connection from the ClientHello
SNI (RFC 6066 §3), so one listener can present different certs per hostname:

```erlang
SniFun = fun(ServerName) ->
    case lookup_cert(ServerName) of
        {ok, Cert, Key} -> {ok, #{cert => Cert, key => Key}};
        error -> {error, unknown_host}
    end
end,
webtransport:start_listener(srv, Opts#{sni_callback => SniFun}).
```

`ServerName` is a binary hostname (or `undefined` on h3 when the client sends no
SNI). The result `cert` is a DER binary, `key` a decoded private-key term, and
optional `cert_chain` the DER intermediates. For h3 the callback is forwarded to
`quic`; for h2 it is adapted to an `ssl` `sni_fun`. A callback returning
`{error, _}` (or raising) fails the handshake on both transports. When a client
sends no SNI, h2 serves the static `certfile`/`keyfile`. The static cert remains
the default when no callback is set.

### IPv6 binding

```erlang
%% IPv6 wildcard
webtransport:start_listener(srv, Opts#{transport => h3, family => inet6}).

%% A specific IPv6 address
webtransport:start_listener(srv, Opts#{transport => h3,
                                       ip => {0,0,0,0,0,0,0,1}}).
```

Works for both `h3` and `h2`. (h2 IPv6 binding requires the `h2` library
0.7.0 or later, which this release depends on.)

### Bound address

`webtransport:listener_sockname/1` returns the bound `{Ip, Port}`; it is also
included as `sockname` in `listener_info/1`. For h3 the address is resolved
live from the QUIC socket (correct even with `port => 0` or `inet6`). For h2 it
is best-effort: the requested bind address paired with the actual bound port,
because the h2 library exposes only the port.

## Client Options

```erlang
webtransport:connect(Host, Port, Path, Opts).
```

| Option | Default | Description |
|--------|---------|-------------|
| `transport` | `h3` | `h2` or `h3` |
| `verify` | `verify_peer` | `verify_peer` or `verify_none` |
| `cacertfile` | -- | Path to CA certificate bundle. A file that cannot be read or holds no certificate fails the connect with `{error, {cacertfile, File, Reason}}` |
| `certfile` | -- | Client certificate (mutual TLS) |
| `keyfile` | -- | Client private key (mutual TLS) |
| `headers` | `[]` | Extra headers on the CONNECT request |
| `timeout` | 30000 | Connection timeout (ms) |
| `handler_opts` | `#{}` | Map passed to handler's `init/3` |
| `family` | `any` | `inet`, `inet6`, or `any` (h3 only) |
| `happy_eyeballs` | `true` | RFC 8305 v6/v4 racing for multi-address hosts (h3 only) |
| `connection_attempt_delay` | 250 | Happy Eyeballs stagger between attempts, ms (h3 only) |
| `session_ticket` | -- | Stored 0-RTT resumption ticket (h3 only; see below) |
| `socket_opts` | `[]` | gen_tcp/inet options for the client socket (see below) |
| `protocols` | `[]` | Subprotocols to offer, in preference order |
| `require_protocol` | `false` | Fail with `{error, {alpn_error, _}}` when the server picks none |
| `max_data`, `max_stream_data`, `max_streams_bidi`, `max_streams_uni`, `max_send_buffer` | as for listeners | Limits we grant the server, and our send queue |
| `compat_mode` | `latest` | HTTP/3 draft selection (see below) |

### Client socket options

`socket_opts` sets options on the client's socket: the source address to
bind, buffer sizes, TOS and so on. You need it when the client must leave
through a given interface or address, or when you pass the same option list
you use for plain TCP connections.

```erlang
{ok, Session} = webtransport:connect("example.com", 443, <<"/wt">>, #{
    transport => h3,
    socket_opts => [{ip, {192, 168, 1, 10}}, {recbuf, 262144}]
}).
```

- On h3 the options that apply to UDP are set on the QUIC socket. TCP-only
  options (`nodelay`, `keepalive`, `linger`, `send_timeout`,
  `send_timeout_close`, `packet`, `delay_send`, ...) are ignored, so one list
  works on both transports.
- On h2 the options are set on the TCP socket under TLS and win over the
  library's defaults.
- Options the library controls (`active`, `mode`, `packet`, `header`,
  `binary`, `inet`, `inet6`) and TLS options such as `verify` are ignored on
  both. Use `family` to pick the address family.

## Subprotocols

Offer subprotocols when you connect; the server picks one with its
handler's `select_protocol/2` (see [handlers](handlers.md)):

```erlang
{ok, Session} = webtransport:connect("example.com", 443, <<"/wt">>, #{
    protocols => [<<"chat/2">>, <<"chat/1">>],
    require_protocol => true
}),
{ok, #{protocol := <<"chat/2">>}} = webtransport:session_info(Session).
```

The server's choice must be one you offered, or the session is closed
with `WT_ALPN_ERROR` and `connect` returns `{error, {alpn_error, _}}`.

`Host` accepts a hostname (string/binary), an IP-literal string, or an
`inet:ip_address()` tuple. IPv6 literals are bracketed in the `:authority`
header automatically.

## 0-RTT / Session Tickets

After a connection completes, the connecting process receives the connection's
resumption ticket as:

```erlang
{webtransport, session_ticket, Ticket}
```

`Ticket` is an opaque term; store it and pass it back as `session_ticket` on a
later `connect/4` to the same server. `webtransport:early_data_accepted/1`
reports whether the connection negotiated 0-RTT (`true` | `false` | `unknown`
for h3, `not_supported` for h2). If early data is rejected, the connecting
process receives `{webtransport, early_data_rejected, StreamIds}`.

This release implements session-ticket capture and connection-level acceptance
reporting. Full 0-RTT resumption (sending the WebTransport CONNECT as early
data) is not supported through the current synchronous connect path.

## Compatibility Mode

The HTTP/3 WebTransport spec has evolved through multiple drafts. The IETF is
on draft-17 while Chrome and Firefox still use draft-02 and Safari 26 uses
draft-07. This library keeps the latest and the legacy paths separate:

| Mode | `:protocol` | SETTINGS | Use when |
|------|-------------|----------|----------|
| `latest` | `webtransport-h3` | `SETTINGS_WT_ENABLED=1` + initial flow-control | Draft-17 and draft-15 peers |
| `legacy_browser_compat` | `webtransport` | `SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02=1`, `SETTINGS_WEBTRANSPORT_MAX_SESSIONS=1` (server) | Draft-02 peers (Chrome, Firefox, quic-go v0.9) and draft-07 peers (Safari 26) |
| `auto` (server only) | accepts both | advertises both | Accept either draft per request |

### Server detection

In `auto` mode, the server inspects each CONNECT request:

- `:protocol = webtransport-h3` with no draft-02 header -> latest
- `:protocol = webtransport` -> legacy_browser_compat: draft-07 when the
  client sent `SETTINGS_WEBTRANSPORT_MAX_SESSIONS` and no draft-02 header,
  draft-02 otherwise. A connection carries one draft-07 session; further
  CONNECT requests are reset.
- Conflicting signals (e.g. `webtransport-h3` plus the draft-02 header) -> 400

In `latest` mode the server advertises SETTINGS_WT_ENABLED under the
draft-17 and the draft-15 codepoints and picks the version from the client's
SETTINGS; the two drafts share a wire format.

The decision is frozen at session init. Pin to `latest` or
`legacy_browser_compat` to refuse the other:

```erlang
%% Accept only draft-17 and draft-15 clients
webtransport:start_listener(strict, #{
    transport => h3,
    port => 4433,
    certfile => "cert.pem",
    keyfile => "key.pem",
    handler => my_handler,
    compat_mode => latest
}).
```

### Client selection

Clients must choose explicitly. Default is `latest`:

```erlang
{ok, Session} = webtransport:connect("example.com", 443, <<"/wt">>, #{
    transport => h3,
    compat_mode => legacy_browser_compat
}).
```

`legacy_browser_compat` speaks draft-02; use `compat_mode => draft07` for a
draft-07 server.

HTTP/2 has no draft-02 or draft-07 variant; `compat_mode` applies only to
HTTP/3.

## Flow Control

WebTransport provides session-level and per-stream flow control. These
options set what you grant the peer; what the peer grants you comes from its
SETTINGS:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_data` | 1048576 (1 MB) | Session-level byte limit |
| `max_stream_data` | 262144 (256 KB) | Per-stream byte limit (HTTP/2) |
| `max_streams_bidi` | 100 | Bidirectional streams the peer may have open |
| `max_streams_uni` | 100 | Unidirectional streams the peer may have open |

Override at listener or connect time:

```erlang
webtransport:start_listener(my_server, #{
    transport => h3,
    port => 4433,
    certfile => "cert.pem",
    keyfile => "key.pem",
    handler => my_handler,
    max_data => 4194304,        %% 4 MB
    max_streams_bidi => 200,
    max_streams_uni => 50
}).
```

### Enforcement

The library enforces these rules from
[draft-17](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17)
and [draft-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-16):

- **When it applies** -- always on HTTP/2; on HTTP/3 only when both sides
  sent a non-zero initial limit. Otherwise limits are not enforced and
  flow-control capsules are ignored.
- **Strictly increasing limits** -- a limit that does not rise above the
  previous one, or a stream limit above 2^60, closes the session with
  `WT_FLOW_CONTROL_ERROR`.
- **Overruns** -- data beyond what we granted, or one stream too many,
  closes the session with `WT_FLOW_CONTROL_ERROR`.
- **Cumulative stream limits** -- every stream opened in the session counts.
  As the peer's streams finish, `WT_MAX_STREAMS` gives the credit back. When
  your own open fails with `stream_limit_reached`, the handler's
  `handle_stream_credit/2` runs once the peer grants more.
- **HTTP/3 prohibition** -- `WT_MAX_STREAM_DATA` and `WT_STREAM_DATA_BLOCKED`
  capsules are session errors on HTTP/3 (per-stream flow control uses native
  QUIC).
- **HTTP/2 WebTransport-Init** -- the `WebTransport-Init` header
  ([draft-16 section 4.3.2](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-16#section-4.3.2))
  carries initial per-stream data limits. When both SETTINGS and the header
  are present, the greater value is used. A header that does not parse is
  refused with 400.
- **Large sends** -- `send/4` accepts a payload of any size and returns `ok`
  once it is queued. Bytes beyond the session window (both transports) or the
  stream window (HTTP/2) wait in order on the stream, FIN is held until they
  are out, and the queue drains as the peer raises its limits.
  `WT_DATA_BLOCKED` tells the peer we are waiting.
- **Credit on receive** -- the library sends `WT_MAX_DATA` (both transports)
  and `WT_MAX_STREAM_DATA` (HTTP/2) once half of a window has been consumed,
  so a peer that honours our windows is never left waiting.

## Datagram Limits

Datagrams are bounded by the transport:

| Transport | Max payload | Reason |
|-----------|-------------|--------|
| HTTP/3 | 65527 bytes | `max_datagram_frame_size` (65535) minus session-id varint (up to 8 bytes) |
| HTTP/2 | 65471 bytes | HTTP/2 initial stream window (65535) minus capsule framing overhead (64 bytes) |

Sending a datagram larger than the limit returns `{error, datagram_too_large}`.

## Error Codes

The library uses the error codes defined in the
[spec](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17#section-9.5):

| Constant | Value | Meaning |
|----------|-------|---------|
| `WT_BUFFERED_STREAM_REJECTED` | `0x3994bd84` | Peer exceeded buffered stream limit |
| `WT_SESSION_GONE` | `0x170d7b68` | Session terminated |
| `WT_FLOW_CONTROL_ERROR` | `0x045d4487` | Flow-control violation |
| `WT_REQUIREMENTS_NOT_MET` | `0x212c0d48` | Protocol requirements not satisfied |
| `WT_ALPN_ERROR` | `0x0817b3dd` | Subprotocol negotiation failed |

On HTTP/3, application error codes (32 bits) are mapped into the
`WT_APPLICATION_ERROR` range per
[draft-17 section 4.4](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17#section-4.4);
protocol codes such as `WT_SESSION_GONE` are sent as is, and a reset with a
code outside that range reaches the handler as `{reset, undefined}`.

On HTTP/2 the error codes for session errors are not assigned yet. A session
error is sent as `WT_CLOSE_SESSION` followed by `RST_STREAM(PROTOCOL_ERROR)`,
or `RST_STREAM(FLOW_CONTROL_ERROR)` for flow-control violations.

## Session Termination

When a session closes (locally or by the peer):

1. All live streams are reset with `WT_SESSION_GONE`; on HTTP/3 the streams
   we read also get `STOP_SENDING(WT_SESSION_GONE)`.
2. A `CLOSE_SESSION` capsule is sent (or received) with a 32-bit error code
   and a reason of at most 1024 bytes of UTF-8. `close_session/3` returns
   `{error, reason_too_long}`, `{error, invalid_reason}` or
   `{error, invalid_error_code}` otherwise.
3. The CONNECT stream is closed (FIN on HTTP/3, END_STREAM on HTTP/2).
4. The handler's `terminate/2` receives `{closed, ErrorCode, Reason}`.
5. On HTTP/3 a client closes its QUIC connection once its last session has
   ended and the server closed the CONNECT stream; a server leaves it open.

If the peer closes the CONNECT stream without sending `CLOSE_SESSION`, the
session terminates with `{closed, 0, <<"peer closed CONNECT">>}`; if it
resets it, with `{closed, 0, <<"CONNECT stream reset">>}`.
