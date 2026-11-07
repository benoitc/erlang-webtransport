%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc HTTP/2 WebTransport runtime wrapper.
%%
%% WebTransport over HTTP/2 uses a single CONNECT stream to multiplex all
%% WebTransport data through capsules. This module manages the CONNECT stream
%% and handles capsule encoding/decoding.
%%
%% Key differences from HTTP/3 transport:
%% <ul>
%% <li>All streams are multiplexed over one HTTP/2 stream using capsules</li>
%% <li>Flow control is managed via capsules, not native QUIC flow control</li>
%% <li>Datagrams use the DATAGRAM capsule type</li>
%% </ul>
%%
%% @see wt_h2_capsule
%%
-module(webtransport_h2).

-export([new/2, with_peer_settings/2]).
-export([h2_conn/1, connect_stream_id/1, peer_settings/1]).
-export([response_headers/1, peer_stream_data/1]).
-export([open_stream/3]).
-export([send/4, send_datagram/2]).
-export([close_session/3, drain_session/1]).
-export([send_capsule/2, session_error/4]).
-export([reset_stream/3, reset_stream/4, stop_sending/3]).
-export([decode_capsule/1, decode_capsules/1]).

%% Client connection helpers
-export([connect/4, request_headers/2, request_headers/3]).
-export([is_success_response/1]).

-include("webtransport.hrl").

-record(state, {
    h2_conn :: pid(),
    connect_stream_id :: non_neg_integer(),
    %% The peer's HTTP/2 SETTINGS when the session was set up.
    peer_settings = #{} :: map(),
    %% Per-stream data limits the peer granted us, as WebTransport-Init
    %% keys (u, bl, br): its SETTINGS and WebTransport-Init combined.
    peer_stream_data = #{} :: wt_h2_init:init_params(),
    %% Client only: headers of the 2xx response.
    response_headers = [] :: [{binary(), binary()}],
    recv_buffer = <<>> :: binary()
}).

-opaque state() :: #state{}.

-export_type([state/0]).

%% ============================================================================
%% Lifecycle
%% ============================================================================

%% @doc Create a new H2 transport state for the given CONNECT stream.
-spec new(pid(), non_neg_integer()) -> state().
new(H2Conn, ConnectStreamId) ->
    #state{
        h2_conn = H2Conn,
        connect_stream_id = ConnectStreamId
    }.

%% @doc Return a new state with updated peer settings.
-spec with_peer_settings(state(), map()) -> state().
with_peer_settings(State, PeerSettings) ->
    State#state{peer_settings = PeerSettings}.

%% @doc Return the HTTP/2 connection pid.
-spec h2_conn(state()) -> pid().
h2_conn(#state{h2_conn = H2Conn}) ->
    H2Conn.

%% @doc Return the HTTP/2 CONNECT stream ID used for this session.
-spec connect_stream_id(state()) -> non_neg_integer().
connect_stream_id(#state{connect_stream_id = StreamId}) ->
    StreamId.

%% @doc Return the peer's advertised WebTransport settings.
-spec peer_settings(state()) -> map().
peer_settings(#state{peer_settings = PeerSettings}) ->
    PeerSettings.

-spec peer_stream_data(state()) -> wt_h2_init:init_params().
peer_stream_data(#state{peer_stream_data = Data}) ->
    Data.

-spec response_headers(state()) -> [{binary(), binary()}].
response_headers(#state{response_headers = Headers}) ->
    Headers.

%% ============================================================================
%% Client Connection
%% ============================================================================

%% @doc Establish an HTTP/2 WebTransport session to the given host, port, and path.
-spec connect(string() | binary() | inet:ip_address(), inet:port_number(), binary(), map()) ->
    {ok, state()} | {error, term()}.
connect(Host, Port, Path, Opts) ->
    %% h2:connect hands the host to ssl:connect, which accepts an
    %% inet:ip_address() tuple directly and infers the family from it. h2's
    %% client opts have no family control and its ssl-opt merge rejects bare
    %% socket atoms, so for IPv6 we resolve to an address tuple here.
    H2Host = h2_host(Host),
    SSLOpts = maps:get(ssl_opts, Opts, []),
    H2Opts = #{
        ssl_opts => [{alpn_advertised_protocols, [<<"h2">>]} | SSLOpts],
        settings => maps:get(settings, Opts, #{})
    },
    case h2:connect(H2Host, Port, H2Opts) of
        {ok, H2Conn} ->
            %% h2:connect returns once the SETTINGS exchange is done.
            PeerSettings = h2:get_peer_settings(H2Conn),
            case check_wt_enabled(PeerSettings) of
                ok ->
                    send_connect(H2Conn, Host, Port, Path, Opts, PeerSettings);
                {error, {settings_error, _}} = Err ->
                    %% http2-16 §3.1: a value above 1 is a connection error
                    %% of type PROTOCOL_ERROR.
                    _ = h2:goaway(H2Conn, protocol_error),
                    h2:close(H2Conn),
                    Err;
                {error, _} = Err ->
                    h2:close(H2Conn),
                    Err
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% http2-16 §3.1: a client MUST NOT open a session until the server sent
%% SETTINGS_WT_ENABLED = 1.
-spec check_wt_enabled(map()) -> ok | {error, term()}.
check_wt_enabled(PeerSettings) ->
    case maps:get(?SETTINGS_WT_ENABLED_H2, PeerSettings, 0) of
        1 -> ok;
        V when is_integer(V), V > 1 -> {error, {settings_error, wt_enabled}};
        _ -> {error, wt_not_enabled}
    end.

send_connect(H2Conn, Host, Port, Path, Opts, PeerSettings) ->
    Authority = authority(Host, Port),
    Headers = request_headers(Authority, Path, maps:get(headers, Opts, []), Opts),
    case h2:request(H2Conn, <<"CONNECT">>, Path, Headers) of
        {ok, StreamId} ->
            receive
                {h2, H2Conn, {response, StreamId, Status, RespHeaders}}
                  when Status >= 200, Status < 300 ->
                    State = new(H2Conn, StreamId),
                    {ok, State#state{
                        peer_settings = PeerSettings,
                        peer_stream_data = response_stream_data(PeerSettings, RespHeaders),
                        response_headers = RespHeaders
                    }};
                {h2, H2Conn, {response, StreamId, Status, _RespHeaders}} ->
                    h2:close(H2Conn),
                    {error, {http_error, Status}};
                {h2, H2Conn, {stream_reset, StreamId, ErrorCode}} ->
                    h2:close(H2Conn),
                    {error, {stream_reset, ErrorCode}};
                {h2, H2Conn, {closed, _Reason}} ->
                    {error, connection_closed};
                {h2, H2Conn, closed} ->
                    {error, connection_closed}
            after
                maps:get(timeout, Opts, 30000) ->
                    h2:close(H2Conn),
                    {error, timeout}
            end;
        {error, Reason} ->
            h2:close(H2Conn),
            {error, Reason}
    end.

%% Per-stream limits the server granted: its SETTINGS, raised by a
%% WebTransport-Init in the response if it sent one.
response_stream_data(PeerSettings, RespHeaders) ->
    FromSettings = wt_h2_init:from_settings(PeerSettings),
    case lists:keyfind(<<"webtransport-init">>, 1, RespHeaders) of
        {_, InitBin} ->
            case wt_h2_init:parse(InitBin) of
                {ok, Init} -> wt_h2_init:apply_greater_of(Init, FromSettings);
                {error, _} -> FromSettings
            end;
        false ->
            FromSettings
    end.

authority(Host, Port) when is_tuple(Host) ->
    authority(inet:ntoa(Host), Port);
authority(Host, Port) when is_list(Host) ->
    authority(list_to_binary(Host), Port);
authority(Host, Port) when is_binary(Host) ->
    PortBin = integer_to_binary(Port),
    case is_ipv6_literal(Host) of
        true -> <<"[", Host/binary, "]:", PortBin/binary>>;
        false -> <<Host/binary, ":", PortBin/binary>>
    end.

is_ipv6_literal(<<"[", _/binary>>) -> false;
is_ipv6_literal(Host) -> binary:match(Host, <<":">>) =/= nomatch.

%% Resolve the host into a form ssl:connect can reach. An IP tuple passes
%% through; an IPv6 literal string/binary is parsed to a tuple so ssl picks
%% the v6 family (a bare literal string would otherwise resolve as a v4
%% hostname and fail). Plain hostnames and IPv4 literals are returned as-is.
h2_host(Host) when is_tuple(Host) ->
    Host;
h2_host(Host) ->
    HostBin = if is_list(Host) -> list_to_binary(Host); true -> Host end,
    case is_ipv6_literal(HostBin) of
        true ->
            case inet:parse_address(binary_to_list(HostBin)) of
                {ok, Addr} -> Addr;
                {error, _} -> Host
            end;
        false ->
            Host
    end.

%% @doc Build CONNECT request headers for an H2 WebTransport session.
-spec request_headers(binary(), binary()) -> [{binary(), binary()}].
request_headers(Authority, Path) ->
    request_headers(Authority, Path, []).

%% @doc Build CONNECT request headers with additional custom headers.
-spec request_headers(binary(), binary(), [{binary(), binary()}]) -> [{binary(), binary()}].
request_headers(Authority, Path, ExtraHeaders) ->
    request_headers(Authority, Path, ExtraHeaders, #{}).

%% @doc Build CONNECT request headers with custom headers and initial flow-control options.
-spec request_headers(binary(), binary(), [{binary(), binary()}], map()) ->
    [{binary(), binary()}].
request_headers(Authority, Path, ExtraHeaders, ConnectOpts) ->
    %% h2_connection derives :authority from the "host" header and rejects
    %% duplicate :authority pseudo-headers, so we only send "host" here.
    BaseHeaders = [
        {<<":protocol">>, <<"webtransport">>},
        {<<":scheme">>, <<"https">>},
        {<<":path">>, Path},
        {<<"host">>, Authority}
    ],
    %% WebTransport-Init carries per-stream data limits only (http2-16
    %% §4.3.2). Send it when the caller set `max_stream_data'.
    InitHeader = case maps:find(max_stream_data, ConnectOpts) of
        {ok, V} ->
            [{<<"webtransport-init">>,
              wt_h2_init:encode(#{u => V, bl => V, br => V})}];
        error ->
            []
    end,
    BaseHeaders ++ InitHeader ++ strip_reserved_headers(ExtraHeaders).

%% @doc Return true if the response headers contain a 2xx status code.
-spec is_success_response([{binary(), binary()}]) -> boolean().
is_success_response(Headers) ->
    case lists:keyfind(<<":status">>, 1, Headers) of
        {_, Status} ->
            try binary_to_integer(Status) of
                N when N >= 200, N < 300 -> true;
                _ -> false
            catch
                _:_ -> false
            end;
        false ->
            false
    end.

%% ============================================================================
%% WebTransport Streams (multiplexed over capsules)
%% ============================================================================

%% @doc No-op: H2 streams are implicitly opened when first used via capsules.
-spec open_stream(state(), non_neg_integer(), bidi | uni) -> ok | {error, term()}.
open_stream(_State, _StreamId, _Type) ->
    %% In HTTP/2, streams are implicitly opened when first used
    %% No explicit open operation needed
    ok.

%% @doc Send data on a multiplexed stream via a WT_STREAM capsule.
-spec send(state(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, StreamId, Data, Fin) ->
    DataBin = iolist_to_binary(Data),
    Capsule = case Fin of
        true -> wt_h2_capsule:wt_stream_fin(StreamId, DataBin);
        false -> wt_h2_capsule:wt_stream(StreamId, DataBin)
    end,
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% @doc Send an unreliable datagram via the DATAGRAM capsule type.
-spec send_datagram(state(), binary()) -> ok | {error, term()}.
send_datagram(_State, Data) when byte_size(Data) > ?WT_H2_DATAGRAM_MAX ->
    {error, datagram_too_large};
send_datagram(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, Data) ->
    Capsule = wt_h2_capsule:datagram(Data),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Session Control
%% ============================================================================

%% @doc Send a CLOSE_WEBTRANSPORT_SESSION capsule and FIN the CONNECT stream.
-spec close_session(state(), non_neg_integer(), binary()) -> ok | {error, term()}.
close_session(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, ErrorCode, Reason) ->
    case wt_h2_capsule:close_session(ErrorCode, Reason) of
        {error, _} = Err ->
            Err;
        Capsule ->
            h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), true)
    end.

%% @doc End the session after a protocol violation (http2-16 §3.4):
%% WT_CLOSE_SESSION with `Code' and `Reason', then RST_STREAM on the CONNECT
%% stream with the HTTP/2 error `H2Code'.
-spec session_error(state(), non_neg_integer(), binary(), h2_error:error_code()) ->
    ok | {error, term()}.
session_error(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId},
              Code, Reason, H2Code) ->
    Reason1 = case wt_h3_capsule:valid_close_reason(Reason) of
        true -> Reason;
        false -> <<>>
    end,
    Capsule = wt_h2_capsule:close_session(Code, Reason1),
    _ = h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false),
    h2:cancel(H2Conn, ConnectStreamId, H2Code).

%% @doc Send a DRAIN_WEBTRANSPORT_SESSION capsule to signal graceful shutdown.
-spec drain_session(state()) -> ok | {error, term()}.
drain_session(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}) ->
    Capsule = wt_h2_capsule:drain_session(),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Stream Control (via capsules)
%% ============================================================================

%% @doc Reset a multiplexed stream by sending a WT_RESET_STREAM capsule.
-spec reset_stream(state(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(State, StreamId, ErrorCode) ->
    reset_stream(State, StreamId, ErrorCode, 0).

%% @doc Send WT_RESET_STREAM. `ReliableSize' is the number of bytes sent on
%% the stream in WT_STREAM capsules (http2-16 §6.2).
-spec reset_stream(state(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | {error, term()}.
reset_stream(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId},
             StreamId, ErrorCode, ReliableSize) ->
    Capsule = wt_h2_capsule:reset_stream(StreamId, ErrorCode, ReliableSize),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% @doc Request the peer stop sending on a stream via a WT_STOP_SENDING capsule.
-spec stop_sending(state(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, StreamId, ErrorCode) ->
    Capsule = wt_h2_capsule:stop_sending(StreamId, ErrorCode),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% @doc Write a pre-built capsule on the CONNECT stream (e.g. for flow-control signalling).
-spec send_capsule(state(), wt_h2_capsule:capsule()) -> ok | {error, term()}.
send_capsule(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, Capsule) ->
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Capsule Decoding
%% ============================================================================

%% @doc Decode a single capsule from the binary buffer, returning any remainder.
-spec decode_capsule(binary()) ->
    {ok, wt_h2_capsule:capsule(), binary()} | {more, pos_integer()} | {error, term()}.
decode_capsule(Bin) ->
    wt_h2_capsule:decode(Bin).

%% @doc Decode all complete capsules from the binary buffer, returning the unparsed tail.
-spec decode_capsules(binary()) ->
    {ok, [wt_h2_capsule:capsule()], binary()} | {error, term()}.
decode_capsules(Bin) ->
    wt_h2_capsule:decode_all(Bin).

%% ============================================================================
%% Internal Functions
%% ============================================================================

strip_reserved_headers(Headers) ->
    Reserved = #{
        <<":method">> => true,
        <<":protocol">> => true,
        <<":scheme">> => true,
        <<":authority">> => true,
        <<":path">> => true
    },
    [{Name, Value} || {Name, Value} <- Headers, not maps:is_key(Name, Reserved)].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

request_headers_test() ->
    Headers = request_headers(<<"example.com:443">>, <<"/wt">>),
    ?assertEqual({<<":protocol">>, <<"webtransport">>}, lists:nth(1, Headers)),
    ?assertEqual({<<"host">>, <<"example.com:443">>},
                 lists:keyfind(<<"host">>, 1, Headers)).

request_headers_with_init_test() ->
    Headers = request_headers(<<"example.com:443">>, <<"/wt">>, [],
                              #{max_stream_data => 4096, max_streams_uni => 50}),
    {_, InitBin} = lists:keyfind(<<"webtransport-init">>, 1, Headers),
    ?assertEqual({ok, #{u => 4096, bl => 4096, br => 4096}}, wt_h2_init:parse(InitBin)),
    ?assertNot(lists:keymember(<<"webtransport-init">>, 1,
                               request_headers(<<"h">>, <<"/">>, [],
                                               #{max_streams_uni => 50}))).

check_wt_enabled_test_() ->
    [
        ?_assertEqual(ok, check_wt_enabled(#{16#2b67 => 1})),
        ?_assertEqual({error, wt_not_enabled}, check_wt_enabled(#{})),
        ?_assertEqual({error, {settings_error, wt_enabled}},
                      check_wt_enabled(#{16#2b67 => 2}))
    ].

strip_reserved_headers_test() ->
    Input = [
        {<<":method">>, <<"CONNECT">>},
        {<<"origin">>, <<"https://example.com">>},
        {<<":path">>, <<"/wt">>},
        {<<"x-custom">>, <<"value">>}
    ],
    Output = strip_reserved_headers(Input),
    ?assertEqual(2, length(Output)),
    ?assert(lists:member({<<"origin">>, <<"https://example.com">>}, Output)),
    ?assert(lists:member({<<"x-custom">>, <<"value">>}, Output)).

-endif.
