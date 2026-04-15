# WebTransport for Erlang

An Erlang implementation of WebTransport over HTTP/2 ([RFC 9297](https://www.rfc-editor.org/rfc/rfc9297)) and HTTP/3 ([draft-ietf-webtrans-http3](https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http3)).

WebTransport is a protocol providing bidirectional communication between a client and server using reliable streams and unreliable datagrams over HTTP.

## Features

- HTTP/2 transport via capsules (WebTransport over HTTP/2)
- HTTP/3 transport via native QUIC streams
- Bidirectional and unidirectional streams
- Unreliable datagrams
- Flow control
- Handler behaviour for custom session logic

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {webtransport, {git, "https://github.com/benoitc/erlang-webtransport.git", {branch, "main"}}}
]}.
```

## Quick Start

### Server

```erlang
%% Start a listener with your handler
{ok, _} = webtransport:start_listener(my_listener, #{
    transport => h3,
    port => 8443,
    certfile => "cert.pem",
    keyfile => "key.pem",
    handler => my_wt_handler
}).
```

### Client

```erlang
%% Connect to a server
{ok, Session} = webtransport:connect("example.com", 8443, <<"/wt">>, #{
    transport => h3,
    verify => verify_none
}).

%% Open a stream and send data
{ok, Stream} = webtransport:open_stream(Session, bidi),
ok = webtransport:send(Session, Stream, <<"Hello">>).
```

## Handler Behaviour

Implement the `webtransport_handler` behaviour to handle WebTransport sessions:

```erlang
-module(my_wt_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_datagram/2,
         handle_stream_closed/3, terminate/2]).

init(Session, Req, Opts) ->
    %% Session established. `Opts' is the `handler_opts' map supplied at
    %% listener start or at `webtransport:connect/4,5' — use it to receive
    %% an owner pid, configuration, etc.
    {ok, #{}}.

handle_stream(Stream, Type, Data, State) ->
    %% Data received on stream (Type = bidi | uni)
    {ok, State}.

handle_datagram(Data, State) ->
    %% Unreliable datagram received
    {ok, State}.

handle_stream_closed(Stream, Reason, State) ->
    %% Stream closed (Reason = normal | {reset, Code} | {error, _})
    {ok, State}.

terminate(Reason, State) ->
    ok.
```

### Callbacks

| Callback | Required | Description |
|----------|----------|-------------|
| `init/3` | Yes (preferred) | Session initialization; receives `handler_opts` as the 3rd argument |
| `init/2` | Back-compat | Shim called only when `init/3` is not exported; loses `handler_opts` |
| `handle_stream/4` | Yes | Stream data received |
| `handle_stream_fin/4` | No | Stream data with FIN flag |
| `handle_datagram/2` | Yes | Datagram received |
| `handle_stream_closed/3` | Yes | Stream closed |
| `handle_info/2` | No | Generic message |
| `terminate/2` | Yes | Cleanup |

### Actions

Callbacks can return actions to be executed:

```erlang
handle_stream(Stream, bidi, Data, State) ->
    {ok, State, [
        {send, Stream, <<"echo: ", Data/binary>>},
        {send_datagram, <<"ack">>}
    ]}.
```

Available actions:

| Action | Description |
|--------|-------------|
| `{send, Stream, Data}` | Send data on stream |
| `{send, Stream, Data, fin}` | Send data and close stream |
| `{send_datagram, Data}` | Send unreliable datagram |
| `{open_stream, bidi \| uni}` | Open new stream |
| `{close_stream, Stream}` | Close stream gracefully |
| `{reset_stream, Stream, Code}` | Abort stream with error |
| `{stop_sending, Stream, Code}` | Request peer stop sending |
| `drain_session` | Signal no new streams |
| `{close_session, Code, Reason}` | Close session |

## API Reference

### Server API

```erlang
%% Start a listener
webtransport:start_listener(Name, Opts) -> {ok, Pid} | {error, Reason}

%% Stop a listener
webtransport:stop_listener(Name) -> ok | {error, not_found}

%% List active listeners
webtransport:listeners() -> [Name]

%% Get listener info
webtransport:listener_info(Name) -> {ok, Info} | {error, not_found}
```

**Listener options:**

| Option | Required | Description |
|--------|----------|-------------|
| `transport` | Yes | `h2` or `h3` |
| `port` | Yes | Listen port |
| `certfile` | Yes | TLS certificate path |
| `keyfile` | Yes | TLS private key path |
| `handler` | Yes | Handler module |
| `handler_opts` | No | Map passed to `init/3` as the 3rd argument |
| `max_data` | No | Session data limit (default: 1MB) |
| `max_streams_bidi` | No | Max bidi streams (default: 100) |
| `max_streams_uni` | No | Max uni streams (default: 100) |

### Client API

```erlang
%% Connect to a server
webtransport:connect(Host, Port, Path, Opts) -> {ok, Session} | {error, Reason}

%% Connect with custom handler
webtransport:connect(Host, Port, Path, Opts, Handler) -> {ok, Session} | {error, Reason}
```

**Connect options:**

| Option | Description |
|--------|-------------|
| `transport` | `h2` or `h3` (default: `h3`) |
| `cacertfile` | CA certificate for verification |
| `verify` | `verify_peer` or `verify_none` (default: `verify_peer`) |
| `headers` | Extra headers for CONNECT request |
| `timeout` | Connection timeout in ms (default: 30000) |

### Session API

```erlang
%% Open a stream
webtransport:open_stream(Session, bidi | uni) -> {ok, Stream} | {error, Reason}

%% Send data
webtransport:send(Session, Stream, Data) -> ok | {error, Reason}
webtransport:send(Session, Stream, Data, fin | nofin) -> ok | {error, Reason}

%% Send datagram
webtransport:send_datagram(Session, Data) -> ok | {error, Reason}

%% Close stream
webtransport:close_stream(Session, Stream) -> ok | {error, Reason}

%% Reset stream with error code
webtransport:reset_stream(Session, Stream, ErrorCode) -> ok | {error, Reason}

%% Request peer stop sending
webtransport:stop_sending(Session, Stream, ErrorCode) -> ok | {error, Reason}

%% Signal draining (no new streams)
webtransport:drain_session(Session) -> ok

%% Close session
webtransport:close_session(Session) -> ok
webtransport:close_session(Session, ErrorCode) -> ok
webtransport:close_session(Session, ErrorCode, Reason) -> ok

%% Get session info
webtransport:session_info(Session) -> {ok, Info} | {error, Reason}
```

## Default Client Handler

When connecting without a handler, `webtransport_client_handler` is used. It forwards events to the calling process:

```erlang
{ok, Session} = webtransport:connect("localhost", 8443, <<"/wt">>, #{}),
{ok, Stream} = webtransport:open_stream(Session, bidi),
ok = webtransport:send(Session, Stream, <<"Hello">>),

receive
    {webtransport, Session, {stream, Stream, bidi, Data}} ->
        io:format("Received: ~p~n", [Data])
end.
```

Messages sent to the owner process:

| Message | Description |
|---------|-------------|
| `{webtransport, Session, {stream, Stream, Type, Data}}` | Stream data |
| `{webtransport, Session, {stream_fin, Stream, Type, Data}}` | Stream data with FIN |
| `{webtransport, Session, {datagram, Data}}` | Datagram received |
| `{webtransport, Session, {stream_closed, Stream, Reason}}` | Stream closed |
| `{webtransport, Session, closed}` | Session closed |

## Configuration

Flow control and stream limits can be set per listener:

```erlang
webtransport:start_listener(my_listener, #{
    transport => h3,
    port => 8443,
    certfile => "cert.pem",
    keyfile => "key.pem",
    handler => my_handler,
    max_data => 2097152,          %% 2 MB session data
    max_streams_bidi => 50,       %% 50 bidirectional streams
    max_streams_uni => 50         %% 50 unidirectional streams
}).
```

## Examples

See the `examples/` directory for:

- `echo_server.erl` - Simple echo server
- `echo_client.erl` - Client that tests the echo server

## License

Apache-2.0
