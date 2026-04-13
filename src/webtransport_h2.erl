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
-export([open_stream/3]).
-export([send/4, send_datagram/2]).
-export([close_session/3, drain_session/1]).
-export([reset_stream/3, stop_sending/3]).
-export([decode_capsule/1, decode_capsules/1]).

%% Client connection helpers
-export([connect/4, request_headers/2, request_headers/3]).
-export([is_success_response/1]).

-include("webtransport.hrl").

-record(state, {
    h2_conn :: pid(),
    connect_stream_id :: non_neg_integer(),
    peer_settings = #{} :: map(),
    recv_buffer = <<>> :: binary()
}).

-opaque state() :: #state{}.

-export_type([state/0]).

%% ============================================================================
%% Lifecycle
%% ============================================================================

-spec new(pid(), non_neg_integer()) -> state().
new(H2Conn, ConnectStreamId) ->
    #state{
        h2_conn = H2Conn,
        connect_stream_id = ConnectStreamId
    }.

-spec with_peer_settings(state(), map()) -> state().
with_peer_settings(State, PeerSettings) ->
    State#state{peer_settings = PeerSettings}.

-spec h2_conn(state()) -> pid().
h2_conn(#state{h2_conn = H2Conn}) ->
    H2Conn.

-spec connect_stream_id(state()) -> non_neg_integer().
connect_stream_id(#state{connect_stream_id = StreamId}) ->
    StreamId.

-spec peer_settings(state()) -> map().
peer_settings(#state{peer_settings = PeerSettings}) ->
    PeerSettings.

%% ============================================================================
%% Client Connection
%% ============================================================================

-spec connect(string() | binary(), inet:port_number(), binary(), map()) ->
    {ok, state()} | {error, term()}.
connect(Host, Port, Path, Opts) ->
    HostBin = if is_list(Host) -> list_to_binary(Host); true -> Host end,
    SSLOpts = maps:get(ssl_opts, Opts, []),
    H2Opts = #{
        ssl_opts => [{alpn_advertised_protocols, [<<"h2">>]} | SSLOpts],
        settings => #{enable_connect_protocol => 1}
    },
    case h2:connect(Host, Port, H2Opts) of
        {ok, H2Conn} ->
            Authority = <<HostBin/binary, ":", (integer_to_binary(Port))/binary>>,
            Headers = request_headers(Authority, Path, maps:get(headers, Opts, [])),
            case h2:request(H2Conn, <<"CONNECT">>, Path, Headers) of
                {ok, StreamId} ->
                    %% Wait for response
                    receive
                        {h2, H2Conn, {response, StreamId, Status, RespHeaders}} when Status >= 200, Status < 300 ->
                            State = new(H2Conn, StreamId),
                            {ok, extract_peer_settings(State, RespHeaders)};
                        {h2, H2Conn, {response, StreamId, Status, _RespHeaders}} ->
                            h2:close(H2Conn),
                            {error, {http_error, Status}};
                        {h2, H2Conn, {stream_reset, StreamId, ErrorCode}} ->
                            h2:close(H2Conn),
                            {error, {stream_reset, ErrorCode}};
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
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec request_headers(binary(), binary()) -> [{binary(), binary()}].
request_headers(Authority, Path) ->
    request_headers(Authority, Path, []).

-spec request_headers(binary(), binary(), [{binary(), binary()}]) -> [{binary(), binary()}].
request_headers(Authority, Path, ExtraHeaders) ->
    BaseHeaders = [
        {<<":protocol">>, <<"webtransport">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, Authority},
        {<<":path">>, Path}
    ],
    BaseHeaders ++ strip_reserved_headers(ExtraHeaders).

-spec is_success_response([{binary(), binary()}]) -> boolean().
is_success_response(Headers) ->
    case lists:keyfind(<<":status">>, 1, Headers) of
        {_, Status} ->
            case catch binary_to_integer(Status) of
                N when is_integer(N), N >= 200, N < 300 -> true;
                _ -> false
            end;
        false ->
            false
    end.

%% ============================================================================
%% WebTransport Streams (multiplexed over capsules)
%% ============================================================================

-spec open_stream(state(), non_neg_integer(), bidi | uni) -> ok | {error, term()}.
open_stream(_State, _StreamId, _Type) ->
    %% In HTTP/2, streams are implicitly opened when first used
    %% No explicit open operation needed
    ok.

-spec send(state(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, StreamId, Data, Fin) ->
    DataBin = iolist_to_binary(Data),
    Capsule = case Fin of
        true -> wt_h2_capsule:wt_stream_fin(StreamId, DataBin);
        false -> wt_h2_capsule:wt_stream(StreamId, DataBin)
    end,
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

-spec send_datagram(state(), binary()) -> ok | {error, term()}.
send_datagram(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, Data) ->
    Capsule = wt_h2_capsule:datagram(Data),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Session Control
%% ============================================================================

-spec close_session(state(), non_neg_integer(), binary()) -> ok | {error, term()}.
close_session(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, ErrorCode, Reason) ->
    Capsule = wt_h2_capsule:close_session(ErrorCode, Reason),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), true).

-spec drain_session(state()) -> ok | {error, term()}.
drain_session(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}) ->
    Capsule = wt_h2_capsule:drain_session(),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Stream Control (via capsules)
%% ============================================================================

-spec reset_stream(state(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, StreamId, ErrorCode) ->
    Capsule = wt_h2_capsule:reset_stream(StreamId, ErrorCode),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

-spec stop_sending(state(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(#state{h2_conn = H2Conn, connect_stream_id = ConnectStreamId}, StreamId, ErrorCode) ->
    Capsule = wt_h2_capsule:stop_sending(StreamId, ErrorCode),
    h2:send_data(H2Conn, ConnectStreamId, wt_h2_capsule:encode(Capsule), false).

%% ============================================================================
%% Capsule Decoding
%% ============================================================================

-spec decode_capsule(binary()) ->
    {ok, wt_h2_capsule:capsule(), binary()} | {more, pos_integer()} | {error, term()}.
decode_capsule(Bin) ->
    wt_h2_capsule:decode(Bin).

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

extract_peer_settings(State, Headers) ->
    %% Extract any WebTransport settings from response headers
    Settings = lists:foldl(fun
        ({<<"wt-max-streams-bidi">>, V}, Acc) ->
            Acc#{max_streams_bidi => binary_to_integer(V)};
        ({<<"wt-max-streams-uni">>, V}, Acc) ->
            Acc#{max_streams_uni => binary_to_integer(V)};
        ({<<"wt-max-data">>, V}, Acc) ->
            Acc#{max_data => binary_to_integer(V)};
        (_, Acc) ->
            Acc
    end, #{}, Headers),
    State#state{peer_settings = Settings}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

request_headers_test() ->
    Headers = request_headers(<<"example.com:443">>, <<"/wt">>),
    ?assertEqual({<<":protocol">>, <<"webtransport">>}, lists:nth(1, Headers)),
    ?assertEqual({<<":authority">>, <<"example.com:443">>}, lists:nth(3, Headers)).

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
