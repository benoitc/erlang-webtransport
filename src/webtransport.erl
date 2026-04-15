%% @doc WebTransport Public API
%%
%% This module provides the public API for WebTransport over HTTP/2 and HTTP/3.
%%
%% == Server Usage ==
%%
%% ```
%% %% Start a WebTransport listener
%% {ok, Listener} = webtransport:start_listener(my_listener, #{
%%     transport => h3,
%%     port => 8443,
%%     certfile => "cert.pem",
%%     keyfile => "key.pem",
%%     handler => my_wt_handler
%% }).
%%
%% %% Stop the listener
%% ok = webtransport:stop_listener(my_listener).
%% '''
%%
%% == Client Usage ==
%%
%% ```
%% %% Connect to a WebTransport server
%% {ok, Session} = webtransport:connect("example.com", 443, "/wt", #{
%%     transport => h3
%% }).
%%
%% %% Open a bidirectional stream
%% {ok, Stream} = webtransport:open_stream(Session, bidi).
%%
%% %% Send data
%% ok = webtransport:send(Session, Stream, <<"Hello">>).
%%
%% %% Send with FIN flag
%% ok = webtransport:send(Session, Stream, <<"World">>, fin).
%%
%% %% Send unreliable datagram
%% ok = webtransport:send_datagram(Session, <<"ping">>).
%%
%% %% Close stream
%% ok = webtransport:close_stream(Session, Stream).
%%
%% %% Close session
%% ok = webtransport:close_session(Session).
%% '''
%%
%% == Handler Behaviour ==
%%
%% See {@link webtransport_handler} for the callback behaviour.
%%
-module(webtransport).

%% Server API
-export([start_listener/2, stop_listener/1]).
-export([listeners/0, listener_info/1]).

%% Client API
-export([connect/4, connect/5]).

%% Session API
-export([open_stream/2]).
-export([send/3, send/4]).
-export([send_datagram/2]).
-export([close_stream/2]).
-export([reset_stream/3, stop_sending/3]).
-export([drain_session/1]).
-export([close_session/1, close_session/2, close_session/3]).
-export([session_info/1]).

%% Types
-type session() :: pid().
-type stream() :: non_neg_integer().
-type listener_name() :: atom().
-type request() :: #{
    path := binary(),
    authority := binary(),
    headers => [{binary(), binary()}]
}.

-type listener_opts() :: #{
    transport := h2 | h3,
    port := inet:port_number(),
    certfile := file:filename(),
    keyfile := file:filename(),
    handler := module(),
    handler_opts => term(),
    max_data => non_neg_integer(),
    max_streams_bidi => non_neg_integer(),
    max_streams_uni => non_neg_integer()
}.

-type connect_opts() :: #{
    transport => h2 | h3,
    certfile => file:filename(),
    keyfile => file:filename(),
    cacertfile => file:filename(),
    verify => verify_none | verify_peer,
    headers => [{binary(), binary()}],
    timeout => timeout()
}.

-export_type([session/0, stream/0, request/0]).
-export_type([listener_name/0, listener_opts/0, connect_opts/0]).

-include("webtransport.hrl").

%% ============================================================================
%% Server API
%% ============================================================================

%% @doc Start a WebTransport listener.
%%
%% Options:
%% - `transport' - Required. Either `h2' (HTTP/2) or `h3' (HTTP/3)
%% - `port' - Required. Port to listen on
%% - `certfile' - Required. Path to TLS certificate
%% - `keyfile' - Required. Path to TLS private key
%% - `handler' - Required. Module implementing webtransport_handler behaviour
%% - `handler_opts' - Optional. Initial options passed to handler:init/2
%% - `max_data' - Optional. Initial max session data (default 1MB)
%% - `max_streams_bidi' - Optional. Initial max bidi streams (default 100)
%% - `max_streams_uni' - Optional. Initial max uni streams (default 100)
%%
-spec start_listener(listener_name(), listener_opts()) ->
    {ok, pid()} | {error, term()}.
start_listener(Name, #{transport := Transport} = Opts) when is_atom(Name) ->
    case validate_listener_opts(Opts) of
        ok ->
            case Transport of
                h2 -> start_h2_listener(Name, Opts);
                h3 -> start_h3_listener(Name, Opts)
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Stop a WebTransport listener.
-spec stop_listener(listener_name()) -> ok | {error, term()}.
stop_listener(Name) when is_atom(Name) ->
    case whereis(Name) of
        undefined ->
            {error, not_found};
        Pid ->
            case persistent_term:get({webtransport_listener, Name}, undefined) of
                undefined ->
                    {error, not_found};
                #{transport := h2, server_ref := ServerRef} ->
                    h2:stop_server(ServerRef),
                    persistent_term:erase({webtransport_listener, Name}),
                    exit(Pid, shutdown),
                    ok;
                #{transport := h3, server_ref := ServerRef} ->
                    quic_h3:stop_server(ServerRef),
                    persistent_term:erase({webtransport_listener, Name}),
                    exit(Pid, shutdown),
                    ok
            end
    end.

%% @doc List all active listeners.
-spec listeners() -> [listener_name()].
listeners() ->
    %% Get all persistent terms that match our pattern
    [Name || {{webtransport_listener, Name}, _} <- persistent_term:get()].

%% @doc Get information about a listener.
-spec listener_info(listener_name()) -> {ok, map()} | {error, not_found}.
listener_info(Name) ->
    case persistent_term:get({webtransport_listener, Name}, undefined) of
        undefined -> {error, not_found};
        Info -> {ok, maps:without([server_ref], Info)}
    end.

%% ============================================================================
%% Client API
%% ============================================================================

%% @doc Connect to a WebTransport server.
%%
%% Options:
%% - `transport' - Optional. Either `h2' or `h3' (default: h3)
%% - `cacertfile' - Optional. CA certificate file for verification
%% - `verify' - Optional. `verify_none' or `verify_peer' (default: verify_peer)
%% - `headers' - Optional. Extra headers to send in CONNECT request
%% - `timeout' - Optional. Connection timeout in ms (default: 30000)
%%
-spec connect(Host, Port, Path, Opts) -> {ok, session()} | {error, term()} when
    Host :: string() | binary(),
    Port :: inet:port_number(),
    Path :: binary(),
    Opts :: connect_opts().
connect(Host, Port, Path, Opts) ->
    connect(Host, Port, Path, Opts, undefined).

%% @doc Connect to a WebTransport server with a custom handler.
-spec connect(Host, Port, Path, Opts, Handler) -> {ok, session()} | {error, term()} when
    Host :: string() | binary(),
    Port :: inet:port_number(),
    Path :: binary(),
    Opts :: connect_opts(),
    Handler :: module() | undefined.
connect(Host, Port, Path, Opts, Handler) ->
    Transport = maps:get(transport, Opts, h3),
    case Transport of
        h2 -> connect_h2(Host, Port, Path, Opts, Handler);
        h3 -> connect_h3(Host, Port, Path, Opts, Handler)
    end.

%% ============================================================================
%% Session API
%% ============================================================================

%% @doc Open a new stream on the session.
-spec open_stream(session(), bidi | uni) -> {ok, stream()} | {error, term()}.
open_stream(Session, Type) when Type =:= bidi; Type =:= uni ->
    webtransport_session:open_stream(Session, Type).

%% @doc Send data on a stream.
-spec send(session(), stream(), iodata()) -> ok | {error, term()}.
send(Session, Stream, Data) ->
    webtransport_session:send(Session, Stream, Data, false).

%% @doc Send data on a stream with optional FIN flag.
-spec send(session(), stream(), iodata(), fin | nofin) -> ok | {error, term()}.
send(Session, Stream, Data, fin) ->
    webtransport_session:send(Session, Stream, Data, true);
send(Session, Stream, Data, nofin) ->
    webtransport_session:send(Session, Stream, Data, false).

%% @doc Send an unreliable datagram.
-spec send_datagram(session(), iodata()) -> ok | {error, term()}.
send_datagram(Session, Data) ->
    webtransport_session:send_datagram(Session, Data).

%% @doc Close a stream gracefully (send FIN).
-spec close_stream(session(), stream()) -> ok | {error, term()}.
close_stream(Session, Stream) ->
    webtransport_session:close_stream(Session, Stream).

%% @doc Abruptly terminate a stream with an error code.
-spec reset_stream(session(), stream(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(Session, Stream, ErrorCode) ->
    webtransport_session:reset_stream(Session, Stream, ErrorCode).

%% @doc Request that the peer stop sending on a stream.
-spec stop_sending(session(), stream(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(Session, Stream, ErrorCode) ->
    webtransport_session:stop_sending(Session, Stream, ErrorCode).

%% @doc Signal that no new streams will be created.
-spec drain_session(session()) -> ok.
drain_session(Session) ->
    webtransport_session:drain(Session).

%% @doc Close the session gracefully.
-spec close_session(session()) -> ok.
close_session(Session) ->
    close_session(Session, 0, <<>>).

%% @doc Close the session with an error code.
-spec close_session(session(), non_neg_integer()) -> ok.
close_session(Session, ErrorCode) ->
    close_session(Session, ErrorCode, <<>>).

%% @doc Close the session with an error code and reason.
-spec close_session(session(), non_neg_integer(), binary()) -> ok.
close_session(Session, ErrorCode, Reason) ->
    webtransport_session:close(Session, ErrorCode, Reason).

%% @doc Get session information.
-spec session_info(session()) -> {ok, map()} | {error, term()}.
session_info(Session) ->
    webtransport_session:get_info(Session).

%% ============================================================================
%% Internal Functions - Listener Setup
%% ============================================================================

validate_listener_opts(Opts) ->
    Required = [transport, port, certfile, keyfile, handler],
    Missing = [K || K <- Required, not maps:is_key(K, Opts)],
    case Missing of
        [] -> ok;
        _ -> {error, {missing_options, Missing}}
    end.

start_h2_listener(Name, Opts) ->
    #{
        port := Port,
        certfile := CertFile,
        keyfile := KeyFile,
        handler := Handler
    } = Opts,

    HandlerOpts = maps:get(handler_opts, Opts, #{}),

    %% Create wrapper handler for H2
    H2Handler = make_h2_handler(Handler, HandlerOpts, Opts),

    ServerOpts = #{
        cert => CertFile,
        key => KeyFile,
        handler => H2Handler,
        settings => #{enable_connect_protocol => 1}
    },

    case h2:start_server(Port, ServerOpts) of
        {ok, ServerRef} ->
            %% Register the listener
            Pid = spawn_link(fun() -> listener_loop(Name) end),
            register(Name, Pid),
            persistent_term:put({webtransport_listener, Name}, #{
                transport => h2,
                port => Port,
                handler => Handler,
                server_ref => ServerRef
            }),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

start_h3_listener(Name, Opts) ->
    #{
        port := Port,
        certfile := CertFile,
        keyfile := KeyFile,
        handler := Handler
    } = Opts,

    HandlerOpts = maps:get(handler_opts, Opts, #{}),

    %% Read and decode certificate and key
    case read_cert_and_key(CertFile, KeyFile) of
        {ok, CertDer, PrivateKey} ->
            Claim = wt_stream_type_handler(),

            ConnectionHandler = fun(_QuicConnPid) ->
                {ok, Router} = webtransport_h3_router:start(undefined),
                #{
                    owner => Router,
                    handler => make_h3_handler(Handler, HandlerOpts, Opts, Router),
                    stream_type_handler => Claim,
                    h3_datagram_enabled => true
                }
            end,

            ServerOpts = #{
                cert => CertDer,
                key => PrivateKey,
                handler => make_h3_handler(Handler, HandlerOpts, Opts, undefined),
                settings => wt_h3:default_settings(),
                quic_opts => #{max_datagram_frame_size => 65535},
                stream_type_handler => Claim,
                h3_datagram_enabled => true,
                connection_handler => ConnectionHandler
            },

            case quic_h3:start_server(Name, Port, ServerOpts) of
                {ok, ServerRef} ->
                    Pid = spawn_link(fun() -> listener_loop(Name) end),
                    register(Name, Pid),
                    persistent_term:put({webtransport_listener, Name}, #{
                        transport => h3,
                        port => Port,
                        handler => Handler,
                        server_ref => ServerRef
                    }),
                    {ok, Pid};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

listener_loop(Name) ->
    receive
        {stop, _From} ->
            persistent_term:erase({webtransport_listener, Name}),
            ok;
        _ ->
            listener_loop(Name)
    end.

%% ============================================================================
%% Internal Functions - Request Handling
%% ============================================================================

handle_h2_request(Conn, StreamId, <<"CONNECT">>, Path, Headers, Handler, HandlerOpts, Opts) ->
    case is_webtransport_request(Headers) of
        true ->
            %% Accept the WebTransport session
            h2:send_response(Conn, StreamId, 200, []),

            %% Create transport state
            TransportState = webtransport_h2:new(Conn, StreamId),

            %% Build request info
            Authority = proplists:get_value(<<":authority">>, Headers, <<>>),
            Request = #{
                path => Path,
                authority => Authority,
                headers => Headers
            },

            %% Start session
            SessionOpts = maps:merge(HandlerOpts, #{
                request => Request,
                is_server => true,
                max_data => maps:get(max_data, Opts, ?DEFAULT_MAX_DATA),
                max_streams_bidi => maps:get(max_streams_bidi, Opts, ?DEFAULT_MAX_STREAMS_BIDI),
                max_streams_uni => maps:get(max_streams_uni, Opts, ?DEFAULT_MAX_STREAMS_UNI)
            }),

            case webtransport_session:start_link(h2, TransportState, Handler, SessionOpts) of
                {ok, Session} ->
                    %% Start receiving data
                    spawn(fun() -> h2_data_loop(Conn, StreamId, Session) end);
                {error, Reason} ->
                    h2:send_response(Conn, StreamId, 500, []),
                    h2:send_data(Conn, StreamId, iolist_to_binary(io_lib:format("~p", [Reason])), true)
            end;
        false ->
            %% Not a WebTransport request
            h2:send_response(Conn, StreamId, 400, []),
            h2:send_data(Conn, StreamId, <<"Bad Request">>, true)
    end;

handle_h2_request(Conn, StreamId, _Method, _Path, _Headers, _Handler, _HandlerOpts, _Opts) ->
    h2:send_response(Conn, StreamId, 405, []),
    h2:send_data(Conn, StreamId, <<"Method Not Allowed">>, true).

handle_h3_request(H3Conn, StreamId, <<"CONNECT">>, Path, Headers,
                  Handler, HandlerOpts, Opts, Router) ->
    case is_webtransport_h3_request(Headers) of
        true ->
            %% Accept the WebTransport session
            %% Note: Don't include :protocol in response - quic_h3 rejects it as
            %% a request pseudo-header in responses. A 2xx response is sufficient
            %% to indicate acceptance.
            quic_h3:send_response(H3Conn, StreamId, 200, []),

            %% Create transport state (H3Conn handles underlying QUIC)
            TransportState = webtransport_h3:new(H3Conn, StreamId),

            %% Build request info
            Authority = proplists:get_value(<<":authority">>, Headers, <<>>),
            Request = #{
                path => Path,
                authority => Authority,
                headers => Headers
            },

            %% Start session
            SessionOpts = maps:merge(HandlerOpts, #{
                request => Request,
                is_server => true,
                max_data => maps:get(max_data, Opts, ?DEFAULT_MAX_DATA),
                max_streams_bidi => maps:get(max_streams_bidi, Opts, ?DEFAULT_MAX_STREAMS_BIDI),
                max_streams_uni => maps:get(max_streams_uni, Opts, ?DEFAULT_MAX_STREAMS_UNI)
            }),

            case webtransport_session:start_link(h3, TransportState, Handler, SessionOpts) of
                {ok, Session} ->
                    %% Register to receive stream data for this session
                    quic_h3:set_stream_handler(H3Conn, StreamId, Session),
                    case Router of
                        undefined -> ok;
                        _ -> webtransport_h3_router:register_session(Router, StreamId, Session)
                    end,
                    ok;
                {error, Reason} ->
                    quic_h3:send_response(H3Conn, StreamId, 500, []),
                    quic_h3:send_data(H3Conn, StreamId, iolist_to_binary(io_lib:format("~p", [Reason])), true)
            end;
        false ->
            quic_h3:send_response(H3Conn, StreamId, 400, []),
            quic_h3:send_data(H3Conn, StreamId, <<"Bad Request">>, true)
    end;

handle_h3_request(H3Conn, StreamId, _Method, _Path, _Headers,
                  _Handler, _HandlerOpts, _Opts, _Router) ->
    quic_h3:send_response(H3Conn, StreamId, 405, []),
    quic_h3:send_data(H3Conn, StreamId, <<"Method Not Allowed">>, true).

is_webtransport_request(Headers) ->
    case proplists:get_value(<<":protocol">>, Headers) of
        <<"webtransport">> -> true;
        _ -> false
    end.

is_webtransport_h3_request(Headers) ->
    case proplists:get_value(<<":protocol">>, Headers) of
        <<"webtransport-h3">> -> true;
        <<"webtransport">> -> true;
        _ -> false
    end.

%% ============================================================================
%% Internal Functions - Data Loops
%% ============================================================================

h2_data_loop(Conn, StreamId, Session) ->
    receive
        {h2, Conn, {data, StreamId, Data, _IsFin}} ->
            %% Decode capsules and dispatch
            case webtransport_h2:decode_capsules(Data) of
                {ok, Capsules, _Rest} ->
                    lists:foreach(fun(Capsule) ->
                        dispatch_h2_capsule(Session, Capsule)
                    end, Capsules);
                {error, _Reason} ->
                    ok
            end,
            h2_data_loop(Conn, StreamId, Session);
        {h2, Conn, {stream_reset, StreamId, _ErrorCode}} ->
            webtransport_session:close(Session, 0, <<"stream reset">>);
        {h2, Conn, closed} ->
            webtransport_session:close(Session, 0, <<"connection closed">>);
        _ ->
            h2_data_loop(Conn, StreamId, Session)
    end.

dispatch_h2_capsule(Session, {wt_stream, WtStreamId, Data}) ->
    webtransport_session:handle_stream_data(Session, WtStreamId, Data, false);
dispatch_h2_capsule(Session, {wt_stream_fin, WtStreamId, Data}) ->
    webtransport_session:handle_stream_data(Session, WtStreamId, Data, true);
dispatch_h2_capsule(Session, {datagram, Data}) ->
    webtransport_session:handle_datagram_data(Session, Data);
dispatch_h2_capsule(Session, {reset_stream, WtStreamId, ErrorCode}) ->
    webtransport_session:handle_stream_closed(Session, WtStreamId, {reset, ErrorCode});
dispatch_h2_capsule(Session, Capsule) ->
    webtransport_session:handle_capsule(Session, Capsule).

%% ============================================================================
%% Internal Functions - Client Connection
%% ============================================================================

connect_h2(Host, Port, Path, Opts, Handler) ->
    case webtransport_h2:connect(Host, Port, Path, Opts) of
        {ok, TransportState} ->
            %% Default handler for client
            ActualHandler = case Handler of
                undefined -> webtransport_client_handler;
                _ -> Handler
            end,
            Request = #{
                path => Path,
                authority => iolist_to_binary([Host, ":", integer_to_list(Port)]),
                headers => maps:get(headers, Opts, [])
            },
            SessionOpts = #{
                request => Request,
                is_server => false
            },
            webtransport_session:start_link(h2, TransportState, ActualHandler, SessionOpts);
        {error, Reason} ->
            {error, Reason}
    end.

connect_h3(Host, Port, Path, Opts, Handler) ->
    HostBin = if is_list(Host) -> list_to_binary(Host); true -> Host end,
    {ok, Router} = webtransport_h3_router:start_link(self()),
    H3ConnOpts = build_h3_connect_opts(Opts),
    case webtransport_h3_router:client_connect(Router, Host, Port, H3ConnOpts) of
        {ok, H3Conn} ->
            h3_validate_and_request(H3Conn, HostBin, Port, Path, Opts, Handler, Router);
        {error, Reason} ->
            {error, Reason}
    end.

build_h3_connect_opts(Opts) ->
    %% Use WebTransport settings
    WTSettings = wt_h3:default_settings(),
    BaseOpts = #{
        settings => WTSettings,
        sync => true,
        connect_timeout => maps:get(timeout, Opts, 30000),
        verify => maps:get(verify, Opts, verify_peer),
        quic_opts => #{max_datagram_frame_size => 65535},
        stream_type_handler => fun
            (uni,  _, 16#54) -> claim;
            (bidi, _, 16#41) -> claim;
            (_, _, _) -> ignore
        end
    },
    %% Handle client certificate if provided
    WithCert = case {maps:find(certfile, Opts), maps:find(cert, Opts)} of
        {{ok, CertFile}, _} ->
            case read_cert_file(CertFile) of
                {ok, CertDer} -> BaseOpts#{cert => CertDer};
                _ -> BaseOpts
            end;
        {_, {ok, CertDer}} ->
            BaseOpts#{cert => CertDer};
        _ ->
            BaseOpts
    end,
    %% Handle client key if provided
    WithKey = case {maps:find(keyfile, Opts), maps:find(key, Opts)} of
        {{ok, KeyFile}, _} ->
            case read_key_file(KeyFile) of
                {ok, KeyTerm} -> WithCert#{key => KeyTerm};
                _ -> WithCert
            end;
        {_, {ok, KeyTerm}} ->
            WithCert#{key => KeyTerm};
        _ ->
            WithCert
    end,
    %% Handle CA certificates if provided
    case {maps:find(cacertfile, Opts), maps:find(cacerts, Opts)} of
        {{ok, CAFile}, _} ->
            case read_cacerts_file(CAFile) of
                {ok, CACerts} -> WithKey#{cacerts => CACerts};
                _ -> WithKey
            end;
        {_, {ok, CACerts}} ->
            WithKey#{cacerts => CACerts};
        _ ->
            WithKey
    end.

h3_validate_and_request(H3Conn, HostBin, Port, Path, Opts, Handler, Router) ->
    QuicConn = quic_h3:get_quic_conn(H3Conn),
    case wt_h3:validate_wt_support(H3Conn, QuicConn) of
        ok ->
            Authority = <<HostBin/binary, ":", (integer_to_binary(Port))/binary>>,
            h3_send_connect(H3Conn, Authority, Path, Opts, Handler, Router);
        {error, Reason} ->
            quic_h3:close(H3Conn),
            {error, Reason}
    end.

h3_send_connect(H3Conn, Authority, Path, Opts, Handler, Router) ->
    case wt_h3:request_session(H3Conn, Authority, Path, maps:get(headers, Opts, [])) of
        {ok, SessionId} ->
            h3_await_response(H3Conn, SessionId, Authority, Path, Opts, Handler, Router);
        {error, Reason} ->
            quic_h3:close(H3Conn),
            {error, Reason}
    end.

h3_await_response(H3Conn, SessionId, Authority, Path, Opts, Handler, Router) ->
    Timeout = maps:get(timeout, Opts, 30000),
    receive
        {quic_h3, H3Conn, {response, SessionId, Status, Headers}} when Status >= 200, Status < 300 ->
            h3_start_session(H3Conn, SessionId, Authority, Path, Headers, Handler, Router);
        {quic_h3, H3Conn, {response, SessionId, Status, _Headers}} ->
            quic_h3:close(H3Conn),
            {error, {http_error, Status}};
        {quic_h3, H3Conn, closed} ->
            {error, connection_closed}
    after Timeout ->
        quic_h3:close(H3Conn),
        {error, timeout}
    end.

h3_start_session(H3Conn, SessionId, Authority, Path, Headers, Handler, Router) ->
    TransportState = webtransport_h3:new(H3Conn, SessionId),
    ActualHandler = case Handler of
        undefined -> webtransport_client_handler;
        _ -> Handler
    end,
    Request = #{
        path => Path,
        authority => Authority,
        headers => Headers
    },
    SessionOpts = #{
        request => Request,
        is_server => false
    },
    case webtransport_session:start_link(h3, TransportState, ActualHandler, SessionOpts) of
        {ok, Session} ->
            webtransport_h3_router:register_session(Router, SessionId, Session),
            {ok, Session};
        {error, _} = Err ->
            Err
    end.

%% ============================================================================
%% Handler Factories
%% ============================================================================

make_h2_handler(Handler, HandlerOpts, Opts) ->
    fun(Conn, StreamId, Method, Path, Headers) ->
        handle_h2_request(Conn, StreamId, Method, Path, Headers, Handler, HandlerOpts, Opts)
    end.

make_h3_handler(Handler, HandlerOpts, Opts, Router) ->
    fun(H3Conn, StreamId, Method, Path, Headers) ->
        handle_h3_request(H3Conn, StreamId, Method, Path, Headers,
                          Handler, HandlerOpts, Opts, Router)
    end.

wt_stream_type_handler() ->
    fun
        (uni,  _StreamId, 16#54) -> claim;
        (bidi, _StreamId, 16#41) -> claim;
        (_, _, _) -> ignore
    end.

%% ============================================================================
%% Certificate/Key Helpers
%% ============================================================================

%% @private Read and decode certificate and private key files.
read_cert_and_key(CertFile, KeyFile) ->
    case {file:read_file(CertFile), file:read_file(KeyFile)} of
        {{ok, CertPem}, {ok, KeyPem}} ->
            case public_key:pem_decode(CertPem) of
                [{_, CertDer, _} | _] ->
                    case decode_private_key(KeyPem) of
                        {ok, PrivateKey} ->
                            {ok, CertDer, PrivateKey};
                        {error, Reason} ->
                            {error, {key_decode_failed, Reason}}
                    end;
                [] ->
                    {error, invalid_certificate}
            end;
        {{error, CertErr}, _} ->
            {error, {cert_read_failed, CertErr}};
        {_, {error, KeyErr}} ->
            {error, {key_read_failed, KeyErr}}
    end.

%% @private Decode a PEM-encoded private key.
decode_private_key(PemData) ->
    case public_key:pem_decode(PemData) of
        [{Type, Der, not_encrypted}] ->
            decode_key_entry(Type, Der);
        [{Type, Der, _Cipher}] ->
            decode_key_entry(Type, Der);
        _ ->
            {error, invalid_private_key}
    end.

decode_key_entry('RSAPrivateKey', Der) ->
    {ok, public_key:der_decode('RSAPrivateKey', Der)};
decode_key_entry('ECPrivateKey', Der) ->
    {ok, public_key:der_decode('ECPrivateKey', Der)};
decode_key_entry('PrivateKeyInfo', Der) ->
    {ok, public_key:der_decode('PrivateKeyInfo', Der)};
decode_key_entry(Type, _Der) ->
    {error, {unsupported_key_type, Type}}.

%% @private Read a single certificate from a PEM file.
read_cert_file(CertFile) ->
    case file:read_file(CertFile) of
        {ok, PemData} ->
            case public_key:pem_decode(PemData) of
                [{_, CertDer, _} | _] -> {ok, CertDer};
                [] -> {error, invalid_certificate}
            end;
        {error, Reason} ->
            {error, {cert_read_failed, Reason}}
    end.

%% @private Read a private key from a PEM file.
read_key_file(KeyFile) ->
    case file:read_file(KeyFile) of
        {ok, PemData} ->
            decode_private_key(PemData);
        {error, Reason} ->
            {error, {key_read_failed, Reason}}
    end.

%% @private Read CA certificates from a PEM file.
read_cacerts_file(CAFile) ->
    case file:read_file(CAFile) of
        {ok, PemData} ->
            Certs = [Der || {_, Der, _} <- public_key:pem_decode(PemData)],
            case Certs of
                [] -> {error, no_certificates};
                _ -> {ok, Certs}
            end;
        {error, Reason} ->
            {error, {cacerts_read_failed, Reason}}
    end.
