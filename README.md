# WebTransport for Erlang

An Erlang implementation of the [WebTransport](https://www.w3.org/TR/webtransport/) protocol over:

- **HTTP/3** ([draft-ietf-webtrans-http3-17](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17)) using native QUIC streams; draft-15 clients are accepted too
- **HTTP/2** ([draft-ietf-webtrans-http2-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-16)) using [RFC 9297](https://www.rfc-editor.org/rfc/rfc9297) capsules

[docs/compliance.md](docs/compliance.md) lists what is implemented and the known gaps.

WebTransport provides bidirectional communication between a client and server using reliable streams and unreliable datagrams over HTTP/3 or HTTP/2.

Listeners and clients support IPv4 and IPv6 (bind via `ip`/`family`; the client
accepts IP-literal hosts and address tuples with Happy Eyeballs racing).
`webtransport:listener_sockname/1` reports the bound address. 0-RTT support
covers session-ticket capture and connection-level acceptance reporting
(`webtransport:early_data_accepted/1`); see [docs/configuration.md](docs/configuration.md).

## Requirements

- Erlang/OTP 27.0 or later
- [rebar3](https://rebar3.org/)
- OpenSSL (for certificate generation)

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {webtransport, {git, "https://github.com/benoitc/erlang-webtransport.git", {branch, "main"}}}
]}.
```

Fetch and compile:

```sh
rebar3 get-deps
rebar3 compile
```

## TLS certificates

WebTransport requires TLS. For local development, generate a self-signed certificate:

```sh
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=localhost'
```

This produces two files in the current directory:

- `cert.pem` -- the X.509 certificate
- `key.pem` -- the unencrypted private key

For production, use certificates from a trusted CA (e.g. [Let's Encrypt](https://letsencrypt.org/)). The `certfile` and `keyfile` options accept absolute or relative file paths.

## Quick start

Compile the bundled examples and start a shell with `examples/` on the code path:

```sh
erlc -o examples examples/echo_server.erl examples/echo_client.erl
ERL_FLAGS="-pa examples" rebar3 shell --apps webtransport
```

The `-pa examples` flag is required so that `echo_server` (or any other handler module you keep outside `src/`) is reachable from the session process; otherwise session init fails with `{handler_not_loaded, echo_server, nofile}`.

### 1. Start the server

```erlang
{ok, _} = webtransport:start_listener(my_server, #{
    transport => h3,
    port => 4433,
    certfile => "cert.pem",
    keyfile => "key.pem",
    handler => echo_server
}).
```

### 2. Connect a client

```erlang
{ok, Session} = webtransport:connect("localhost", 4433, <<"/echo">>, #{
    transport => h3,
    verify => verify_none
}).
```

### 3. Send and receive

```erlang
%% Open a bidirectional stream
{ok, Stream} = webtransport:open_stream(Session, bidi).

%% Send data
ok = webtransport:send(Session, Stream, <<"hello">>).

%% Receive the echo
receive
    {webtransport, Session, {stream, Stream, bidi, Data}} ->
        io:format("Got: ~s~n", [Data])  %% prints "Got: echo: hello"
after 3000 ->
    io:format("timeout~n")
end.

%% Send a datagram (unreliable)
ok = webtransport:send_datagram(Session, <<"ping">>).

receive
    {webtransport, Session, {datagram, DgData}} ->
        io:format("Got: ~s~n", [DgData])  %% prints "Got: echo: ping"
after 3000 ->
    io:format("timeout~n")
end.

%% Clean up
webtransport:close_session(Session).
webtransport:stop_listener(my_server).
```

## Writing a handler

Handlers implement the `webtransport_handler` behaviour. The session process calls your handler's callbacks when events occur.

### Minimal handler

```erlang
-module(my_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_datagram/2,
         handle_stream_closed/3, terminate/2]).

init(_Session, _Req, _Opts) ->
    {ok, #{}}.

handle_stream(Stream, Type, Data, State) ->
    %% Echo bidi streams
    Actions = case Type of
        bidi -> [{send, Stream, <<"echo: ", Data/binary>>}];
        uni  -> []
    end,
    {ok, State, Actions}.

handle_datagram(Data, State) ->
    {ok, State, [{send_datagram, <<"echo: ", Data/binary>>}]}.

handle_stream_closed(_Stream, _Reason, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
```

### Callback reference

All callbacks receive the handler state and return `{ok, NewState}`, `{ok, NewState, Actions}`, or `{stop, Reason, NewState}`.

#### `init/3` (required)

Called when a session is established.

```erlang
init(Session, Request, Opts) -> {ok, State} | {ok, State, Actions} | {error, Reason}
```

- `Session` -- the session pid (use for `webtransport:open_stream/2` etc.)
- `Request` -- `#{path := binary(), authority := binary(), headers => [{binary(), binary()}]}`
- `Opts` -- the `handler_opts` map from the listener or connect call

`init/2` is a back-compat shim called only when `init/3` is not exported; it loses `Opts`.

#### `handle_stream/4` (required)

Called when data arrives on a stream.

```erlang
handle_stream(Stream, Type, Data, State) -> {ok, State} | {ok, State, Actions} | {stop, Reason, State}
```

- `Stream` -- stream ID (integer)
- `Type` -- `bidi` or `uni`
- `Data` -- binary payload

#### `handle_stream_fin/4` (optional)

Called when data arrives with the FIN flag (last data on the stream). If not exported, `handle_stream/4` is called instead.

```erlang
handle_stream_fin(Stream, Type, Data, State) -> {ok, State} | {ok, State, Actions} | {stop, Reason, State}
```

#### `handle_datagram/2` (required)

Called when an unreliable datagram arrives.

```erlang
handle_datagram(Data, State) -> {ok, State} | {ok, State, Actions} | {stop, Reason, State}
```

#### `handle_stream_closed/3` (required)

Called when a stream closes or is reset by the peer.

```erlang
handle_stream_closed(Stream, Reason, State) -> {ok, State} | {stop, Reason, State}
```

- `Reason` -- `normal | {reset, ErrorCode} | {error, Term} | {stop_sending, ErrorCode}`

#### `handle_info/2` (optional)

Called for any Erlang message not handled by the session state machine.

```erlang
handle_info(Info, State) -> {ok, State} | {ok, State, Actions} | {stop, Reason, State}
```

#### `handle_action_failed/3` (optional)

Called when an action returned by a callback fails to dispatch (e.g. sending to an unknown stream). Default behaviour: log and continue.

```erlang
handle_action_failed(Action, Reason, State) -> {ok, State} | {stop, Reason, State}
```

#### `origin_check/2` (optional)

Called before `init/3` on server-side CONNECT requests. Return `accept` or `{reject, Status, Reason}` to refuse a session.

```erlang
origin_check(Headers, Opts) -> accept | {reject, 400..599, binary()}
```

When not exported, the default behaviour rejects requests that carry an `origin` header (browser clients) with 403. Requests without an `origin` header (non-browser clients) are accepted. Implement this callback to allow browser origins:

```erlang
origin_check(Headers, _Opts) ->
    case proplists:get_value(<<"origin">>, Headers) of
        <<"https://myapp.example.com">> -> accept;
        _ -> {reject, 403, <<"origin not allowed">>}
    end.
```

#### `terminate/2` (required)

Called when the session ends.

```erlang
terminate(Reason, State) -> term()
```

- `Reason` -- `normal | {closed, ErrorCode, Message} | {error, Term} | Term`

When the peer sends `CLOSE_SESSION`, `Reason` is `{closed, ErrorCode, Message}`.

### Actions

Callbacks can return a list of actions as the third element of the return tuple:

```erlang
handle_stream(Stream, bidi, Data, State) ->
    {ok, State, [
        {send, Stream, <<"echo: ", Data/binary>>},
        {send_datagram, <<"got data on stream">>}
    ]}.
```

| Action | Description |
|--------|-------------|
| `{send, Stream, Data}` | Send data on a stream |
| `{send, Stream, Data, fin}` | Send data and half-close the stream |
| `{send_datagram, Data}` | Send an unreliable datagram |
| `{open_stream, bidi \| uni}` | Open a new stream |
| `{close_stream, Stream}` | Half-close a stream (send FIN) |
| `{reset_stream, Stream, ErrorCode}` | Abort a stream with an error code |
| `{stop_sending, Stream, ErrorCode}` | Ask the peer to stop sending on a stream |
| `drain_session` | Signal that no new streams will be opened |
| `{close_session, ErrorCode, Reason}` | Close the session |

## Server API

### Starting a listener

```erlang
{ok, Pid} = webtransport:start_listener(Name, Opts).
```

`Name` is an atom used to identify the listener. `Opts` is a map:

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `transport` | yes | -- | `h2` (HTTP/2) or `h3` (HTTP/3) |
| `port` | yes | -- | TCP/UDP port to listen on |
| `certfile` | yes | -- | Path to TLS certificate (PEM) |
| `keyfile` | yes | -- | Path to TLS private key (PEM) |
| `handler` | yes | -- | Module implementing `webtransport_handler` |
| `handler_opts` | no | `#{}` | Map passed to `handler:init/3` as the third argument |
| `max_data` | no | 1048576 (1 MB) | Session data the peer may send before we grant more (bytes) |
| `max_stream_data` | no | 262144 (256 KB) | Per-stream data window granted to the peer (HTTP/2 only) |
| `max_streams_bidi` | no | 100 | Bidirectional streams the peer may have open at once |
| `max_streams_uni` | no | 100 | Unidirectional streams the peer may have open at once |
| `compat_mode` | no | `auto` | HTTP/3 draft selection (see [Compatibility](#compatibility-mode)) |

### Managing listeners

```erlang
%% Stop a listener
ok = webtransport:stop_listener(Name).

%% List active listeners
[Name] = webtransport:listeners().

%% Get listener info
{ok, #{transport := h3, port := 4433, handler := my_handler}} =
    webtransport:listener_info(Name).
```

## Embedding in an HTTP server

Use `accept/4` to add WebTransport to an existing HTTP/3 or HTTP/2 server.
Your server owns the listener and routing; `accept/4` upgrades a specific
CONNECT request into a WebTransport session -- the same pattern as WebSocket
upgrade.

### HTTP/3 example

```erlang
%% 1. Merge WT config into your quic_h3 server
H3Opts = maps:merge(webtransport:h3_settings(), #{
    cert => CertDer, key => PrivateKey,
    handler => fun my_handler/5
}),
{ok, _} = quic_h3:start_server(my_server, 443, H3Opts).

%% 2. In your request handler, route and upgrade
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
my_handler(H3Conn, StreamId, <<"GET">>, _Path, _Headers) ->
    quic_h3:send_response(H3Conn, StreamId, 200, []),
    quic_h3:send_data(H3Conn, StreamId, <<"hello">>, true).
```

### HTTP/2 example

```erlang
H2Opts = maps:merge(webtransport:h2_settings(), #{
    cert => "cert.pem", key => "key.pem",
    handler => fun my_h2_handler/5
}),
{ok, _} = h2:start_server(443, H2Opts).

my_h2_handler(Conn, StreamId, <<"CONNECT">>, <<"/wt">>, Headers) ->
    {ok, _Session} = webtransport:accept(Conn, StreamId, Headers, #{
        transport => h2,
        handler => my_wt_handler
    });
my_h2_handler(Conn, StreamId, <<"GET">>, Path, Headers) ->
    serve_static(Conn, StreamId, Path, Headers).
```

### accept/4 options

| Option | Default | Description |
|--------|---------|-------------|
| `transport` | `h3` | `h3` or `h2` |
| `handler` | required | Module implementing `webtransport_handler` |
| `handler_opts` | `#{}` | Passed to `handler:init/3` |
| `compat_mode` | `auto` | HTTP/3 draft selection |
| `max_data` | 1048576 | Session flow-control window |
| `max_stream_data` | 262144 | Per-stream window (HTTP/2) |
| `max_streams_bidi` | 100 | Max bidi streams |
| `max_streams_uni` | 100 | Max uni streams |

`accept/4` validates the CONNECT headers and the client's SETTINGS,
negotiates the subprotocol, starts a session, sends 200, then registers the
session as the stream handler so capsules sent with the request are handled
after the 200, and returns `{ok, Session}`. Merge `h3_settings/0` into your
server options: it sets `defer_requests_until_settings`, without which a
request that arrives ahead of the client's SETTINGS is refused. The session pid works with all session API
functions (`send/3`, `open_stream/2`, etc.).

See the [Integration guide](docs/integration.md) for details.

## Client API

### Connecting

```erlang
{ok, Session} = webtransport:connect(Host, Port, Path, Opts).
```

| Option | Default | Description |
|--------|---------|-------------|
| `transport` | `h3` | `h2` or `h3` |
| `verify` | `verify_peer` | `verify_peer` or `verify_none` |
| `cacertfile` | -- | Path to CA certificate bundle for peer verification |
| `certfile` | -- | Client certificate (mutual TLS) |
| `keyfile` | -- | Client private key (mutual TLS) |
| `headers` | `[]` | Extra headers on the CONNECT request |
| `timeout` | 30000 | Connection timeout in milliseconds |
| `handler_opts` | `#{}` | Map passed to the handler's `init/3` |
| `protocols` | `[]` | Subprotocols to offer, in preference order (`WT-Available-Protocols`) |
| `require_protocol` | `false` | Fail with `{error, {alpn_error, _}}` when the server picks none |
| `max_data`, `max_stream_data`, `max_streams_bidi`, `max_streams_uni` | as for listeners | Limits we grant the server |
| `compat_mode` | `latest` | HTTP/3 draft selection (see [Compatibility](#compatibility-mode)) |

### Connecting with a custom handler

```erlang
{ok, Session} = webtransport:connect(Host, Port, Path, Opts, MyHandler).
```

When no handler is given, `webtransport_client_handler` is used. It forwards all events to the calling process as messages.

### Default client messages

When using the default handler, the process that called `connect/4` receives:

| Message | Description |
|---------|-------------|
| `{webtransport, Session, {stream, Stream, Type, Data}}` | Stream data received |
| `{webtransport, Session, {stream_fin, Stream, Type, Data}}` | Stream data with FIN |
| `{webtransport, Session, {datagram, Data}}` | Datagram received |
| `{webtransport, Session, {stream_closed, Stream, Reason}}` | Stream closed or reset by the peer |
| `{webtransport, Session, {stream_credit, Type}}` | The server raised its stream limit after an open failed with `stream_limit_reached` |
| `{webtransport, Session, closed}` | Session terminated |

## Session API

Once connected, use these functions on the session pid:

```erlang
%% Streams
{ok, Stream} = webtransport:open_stream(Session, bidi).  %% or uni
ok = webtransport:send(Session, Stream, Data).
ok = webtransport:send(Session, Stream, Data, fin).
ok = webtransport:close_stream(Session, Stream).
ok = webtransport:reset_stream(Session, Stream, ErrorCode).
ok = webtransport:stop_sending(Session, Stream, ErrorCode).

%% Datagrams
ok = webtransport:send_datagram(Session, Data).

%% Session lifecycle
ok = webtransport:drain_session(Session).
ok = webtransport:close_session(Session).
ok = webtransport:close_session(Session, ErrorCode).
ok = webtransport:close_session(Session, ErrorCode, Reason).

%% Introspection
{ok, Info} = webtransport:session_info(Session).
%% Info :: #{transport, stream_count, local_max_data, remote_max_data,
%%           local_max_streams_bidi, local_max_streams_uni,
%%           remote_max_streams_bidi, remote_max_streams_uni,
%%           bytes_sent, bytes_received, wt_draft,
%%           protocol => binary(), close_info => {Code, Msg}}
```

## Compatibility mode

The HTTP/3 WebTransport spec has evolved through multiple drafts. The IETF is on draft-17; Chrome and Firefox still use draft-02. This library keeps the two paths disjoint:

| Mode | `:protocol` | SETTINGS | Use when |
|------|-------------|----------|----------|
| `latest` | `webtransport-h3` | `SETTINGS_WT_ENABLED=1` + initial flow-control | Talking to draft-17 or draft-15 peers |
| `legacy_browser_compat` | `webtransport` | `SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02=1` | Talking to draft-02 peers (Chrome, Firefox, quic-go v0.9) |
| `auto` (server only) | accepts both | advertises both | Let the server accept either draft based on the client's request |

**Server default:** `auto` -- the server inspects `:protocol` and the `Sec-Webtransport-Http3-Draft02` header on each CONNECT request and dispatches to the matching code path. In `latest` mode the server advertises SETTINGS_WT_ENABLED under both the draft-17 and the draft-15 codepoints and picks the version from the client's SETTINGS (draft-17 section 7.1); the client sends the draft-17 codepoint only. Pin to `latest` or `legacy_browser_compat` to refuse the other:

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

**Client default:** `latest`. To connect to a draft-02 server:

```erlang
{ok, Session} = webtransport:connect("example.com", 443, <<"/wt">>, #{
    transport => h3,
    compat_mode => legacy_browser_compat,
    verify => verify_none
}).
```

HTTP/2 WebTransport (`transport => h2`) has no draft-02 variant; `compat_mode` applies only to HTTP/3.

## Flow control

WebTransport provides session-level and per-stream flow control. These are
the limits you grant the peer; the limits the peer grants you come from its
SETTINGS. Defaults:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_data` | 1 MB | Session-level byte limit |
| `max_stream_data` | 256 KB | Per-stream byte limit (HTTP/2) |
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

The library enforces:

- **When flow control applies** -- always on HTTP/2. On HTTP/3 only when both sides sent a non-zero initial limit in their SETTINGS (draft-17 section 5.1); otherwise limits are not enforced and flow-control capsules are ignored.
- **Strictly increasing limits** -- a `WT_MAX_DATA`, `WT_MAX_STREAM_DATA` or `WT_MAX_STREAMS` that does not raise the previous value, or a stream limit above 2^60, closes the session with `WT_FLOW_CONTROL_ERROR`.
- **Overruns** -- data beyond what we granted, or a stream beyond our stream limit, closes the session with `WT_FLOW_CONTROL_ERROR`.
- **Cumulative stream limits** -- stream limits count every stream opened in the session. As the peer's streams finish, the library sends `WT_MAX_STREAMS` so the peer can always have `max_streams_*` open. When your own open fails with `stream_limit_reached`, `WT_STREAMS_BLOCKED` is sent and the handler's `handle_stream_credit/2` runs once the peer grants more.
- **HTTP/3 prohibition** -- `WT_MAX_STREAM_DATA` and `WT_STREAM_DATA_BLOCKED` capsules are session errors on HTTP/3 (per-stream flow control uses native QUIC).
- **HTTP/2 WebTransport-Init** -- the `WebTransport-Init` header ([draft-16 section 4.3.2](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-16#section-4.3.2)) carries initial per-stream data limits. When both SETTINGS and the header are present, the greater value is used. A header that does not parse is refused with 400.
- **Large sends** -- `send/4` accepts a payload of any size and returns `ok` once it is queued. Bytes beyond the session window (both transports) or the stream window (HTTP/2) wait in order on the stream, FIN is held until they are out, and the queue drains as the peer raises its limits. `WT_DATA_BLOCKED` tells the peer we are waiting.
- **Credit on receive** -- the library sends `WT_MAX_DATA` (both transports) and `WT_MAX_STREAM_DATA` (HTTP/2) once half of a window has been consumed, so a peer that honours our windows is never left waiting.

## Datagram limits

Datagrams are bounded by the transport:

| Transport | Max payload | Reason |
|-----------|-------------|--------|
| HTTP/3 | 65527 bytes | `max_datagram_frame_size` (65535) minus session-id varint (up to 8 bytes) |
| HTTP/2 | 65471 bytes | HTTP/2 initial stream window (65535) minus capsule framing overhead (64 bytes) |

Sending a datagram larger than the limit returns `{error, datagram_too_large}`.

## Error codes

The library uses draft-defined error codes:

| Constant | Value | Meaning |
|----------|-------|---------|
| `WT_BUFFERED_STREAM_REJECTED` | `0x3994bd84` | Peer exceeded buffered stream limit |
| `WT_SESSION_GONE` | `0x170d7b68` | Session terminated; stream belongs to closed session |
| `WT_FLOW_CONTROL_ERROR` | `0x045d4487` | Flow-control violation (e.g. decreased limit) |
| `WT_REQUIREMENTS_NOT_MET` | `0x212c0d48` | Protocol requirements not satisfied |
| `WT_ALPN_ERROR` | `0x0817b3dd` | Subprotocol negotiation failed |

On HTTP/3, application error codes (32 bits) are mapped into the
`WT_APPLICATION_ERROR` range of QUIC error codes per [draft-17 section 4.4](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17#section-4.4); protocol codes such as `WT_SESSION_GONE` are sent as is, and a reset whose code is outside the range reaches the handler as `{reset, undefined}`. On HTTP/2 the codes for session errors are not assigned yet: a session error is `WT_CLOSE_SESSION` followed by `RST_STREAM(PROTOCOL_ERROR)`, or `FLOW_CONTROL_ERROR` for flow-control violations.

## Session termination

When a session closes (locally or by the peer):

1. All live streams are reset with `WT_SESSION_GONE`; on HTTP/3 the streams we read also get `STOP_SENDING(WT_SESSION_GONE)`.
2. A `CLOSE_SESSION` capsule is sent (or received) with a 32-bit error code and a reason of at most 1024 bytes of UTF-8.
3. The CONNECT stream is closed (FIN on HTTP/3, END_STREAM on HTTP/2). The peer closing or resetting the CONNECT stream without `CLOSE_SESSION` also ends the session.
4. The handler's `terminate/2` receives `{closed, ErrorCode, Reason}` as the reason.
5. On HTTP/3 the QUIC connection is closed once its last session has ended.

If the peer FINs the CONNECT stream without sending `CLOSE_SESSION`, the session terminates with `{closed, 0, <<"peer closed CONNECT">>}`.

## Architecture

```
webtransport            Public API (connect, send, open_stream, ...)
    |
webtransport_session    gen_statem per session (flow control, handler dispatch)
    |
    +-- webtransport_h3     HTTP/3 transport (QUIC streams + datagrams)
    |     +-- wt_h3              Settings, headers, peer validation
    |     +-- wt_h3_capsule      Session and flow-control capsules, stream headers
    |     +-- webtransport_h3_router   Per-connection stream demux
    |
    +-- webtransport_h2     HTTP/2 transport (capsules over CONNECT stream)
    |     +-- wt_h2_capsule      HTTP/2 capsule encode/decode
    |     +-- wt_h2_init         WebTransport-Init header parse/encode
    |
    +-- wt_sf                   Structured field parsing (RFC 9651)
    |
    +-- webtransport_stream     Per-stream state (flow control, buffers)
    +-- wt_error                App error code mapping (draft-17 section 4.4)
    +-- webtransport_handler    Behaviour definition
```

## Examples

The `examples/` directory contains a working echo server and client.

Compile and run them:

```sh
# Generate certs (if not done already)
openssl req -x509 -newkey rsa:2048 \
  -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj '/CN=localhost'

# Compile examples
erlc -o examples \
  -pa _build/default/lib/webtransport/ebin \
  -pa _build/default/lib/quic/ebin \
  -pa _build/default/lib/erlang_h2/ebin \
  -I include \
  examples/echo_server.erl examples/echo_client.erl

# Start a shell
ERL_FLAGS="-pa examples" rebar3 shell --apps webtransport
```

```erlang
%% Start the echo server
echo_server:start(4433).

%% Run the echo client tests
echo_client:test("localhost", 4433).
```

## Testing

```sh
# Unit tests (301 tests)
rebar3 eunit

# Integration tests (67 tests, both h2 and h3)
rebar3 ct --suite=test/webtransport_SUITE

# IPv6, sockname and 0-RTT integration tests
rebar3 ct --suite=test/webtransport_ipv6_SUITE

# Docker interop (erlang vs erlang)
cd interop && docker compose up --abort-on-container-exit --build

# Cross-implementation interop (erlang vs webtransport-go)
./scripts/interop_cross.sh
```

## Specifications

- [draft-ietf-webtrans-http3-17](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3-17) -- WebTransport over HTTP/3
- [draft-ietf-webtrans-http2-16](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-16) -- WebTransport over HTTP/2
- [draft-ietf-webtrans-overview-14](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-overview-14) -- The WebTransport Protocol Framework
- [RFC 9297](https://www.rfc-editor.org/rfc/rfc9297) -- HTTP Datagrams and the Capsule Protocol
- [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000) -- QUIC: A UDP-Based Multiplexed and Secure Transport
- [RFC 9114](https://www.rfc-editor.org/rfc/rfc9114) -- HTTP/3
- [RFC 8441](https://www.rfc-editor.org/rfc/rfc8441) -- Bootstrapping WebSockets with HTTP/2 (Extended CONNECT)
- [W3C WebTransport API](https://www.w3.org/TR/webtransport/) -- Browser API specification

## Sponsors

<a href="https://enki-multimedia.eu"><img src="docs/images/enki-multimedia.svg" alt="Enki Multimedia" height="50" /></a>

## License

Apache-2.0
