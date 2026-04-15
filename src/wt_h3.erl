%% @doc HTTP/3 WebTransport protocol helpers.
%%
%% This module bridges the generic HTTP/3 connection layer from quic_h3
%% with WebTransport-specific session negotiation and capsule handling.
%%
-module(wt_h3).

-export([default_settings/0]).
-export([request_headers/2, request_headers/3]).
-export([request_session/4]).
-export([response_status/1, is_success_response/1]).
-export([validate_wt_support/2]).
-export([send_capsule/3, close_session/4, drain_session/2]).

-include("webtransport.hrl").

-type headers() :: [{binary(), binary()}].

-export_type([headers/0]).

%% ============================================================================
%% Settings
%% ============================================================================

-spec default_settings() -> map().
default_settings() ->
    %% SETTINGS_H3_DATAGRAM (0x33) is emitted by quic_h3 when
    %% `h3_datagram_enabled => true' is passed to connect/3 or
    %% start_server/3; don't duplicate it here (duplicate setting ids are
    %% an H3_SETTINGS_ERROR per RFC 9114 §7.2.4.1).
    #{
        enable_connect_protocol => 1,
        ?SETTINGS_WT_ENABLED => 1,
        ?SETTINGS_WT_INITIAL_MAX_DATA => ?DEFAULT_MAX_DATA,
        ?SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI => ?DEFAULT_MAX_STREAMS_BIDI,
        ?SETTINGS_WT_INITIAL_MAX_STREAMS_UNI => ?DEFAULT_MAX_STREAMS_UNI
    }.

%% ============================================================================
%% Session Establishment
%% ============================================================================

-spec request_headers(binary(), binary()) -> headers().
request_headers(Authority, Path) ->
    request_headers(Authority, Path, []).

-spec request_headers(binary(), binary(), headers()) -> headers().
request_headers(Authority, Path, ExtraHeaders) ->
    BaseHeaders = [
        {<<":method">>, <<"CONNECT">>},
        {<<":protocol">>, <<"webtransport-h3">>},
        {<<":scheme">>, <<"https">>},
        {<<":authority">>, Authority},
        {<<":path">>, Path}
    ],
    BaseHeaders ++ strip_reserved_headers(ExtraHeaders).

-spec request_session(pid(), binary(), binary(), headers()) ->
    {ok, non_neg_integer()} | {error, term()}.
request_session(H3Conn, Authority, Path, ExtraHeaders) ->
    Headers = request_headers(Authority, Path, ExtraHeaders),
    quic_h3:request(H3Conn, Headers, #{end_stream => false}).

-spec response_status(headers()) -> {ok, non_neg_integer()} | {error, term()}.
response_status(Headers) ->
    case lists:keyfind(<<":status">>, 1, Headers) of
        {_, StatusBin} when is_binary(StatusBin) ->
            try
                {ok, binary_to_integer(StatusBin)}
            catch
                error:badarg -> {error, {invalid_status, StatusBin}}
            end;
        false ->
            {error, missing_status}
    end.

-spec is_success_response(headers()) -> boolean().
is_success_response(Headers) ->
    case response_status(Headers) of
        {ok, Status} when Status >= 200, Status < 300 -> true;
        _ -> false
    end.

%% ============================================================================
%% Peer Validation
%% ============================================================================

-spec validate_wt_support(pid(), pid()) -> ok | {error, term()}.
validate_wt_support(H3Conn, _QuicConn) ->
    %% Basic validation: check that peer supports extended CONNECT protocol
    %% (RFC 9220). Full WebTransport support is validated by the server's
    %% 200 response to a CONNECT request with :protocol=webtransport.
    %%
    %% Note: quic_h3 drops unknown settings from peer_settings, so we can't
    %% check for SETTINGS_WT_ENABLED directly. The server accepting the
    %% extended CONNECT request is sufficient validation.
    case quic_h3:get_peer_settings(H3Conn) of
        undefined ->
            {error, settings_not_received};
        PeerSettings ->
            case setting_enabled(PeerSettings, enable_connect_protocol) of
                true -> ok;
                false -> {error, connect_protocol_not_enabled}
            end
    end.

setting_enabled(Settings, Key) ->
    Value = maps:get(Key, Settings, 0),
    Value =:= 1 orelse Value =:= true.

%% ============================================================================
%% CONNECT Stream Capsules
%% ============================================================================

-spec send_capsule(pid(), non_neg_integer(), wt_h3_capsule:capsule()) -> ok | {error, term()}.
send_capsule(H3Conn, SessionId, Capsule) ->
    quic_h3:send_data(H3Conn, SessionId, wt_h3_capsule:encode(Capsule), false).

-spec close_session(pid(), non_neg_integer(), non_neg_integer(), binary()) -> ok | {error, term()}.
close_session(H3Conn, SessionId, ErrorCode, Reason) ->
    send_capsule(H3Conn, SessionId, wt_h3_capsule:close_session(ErrorCode, Reason)).

-spec drain_session(pid(), non_neg_integer()) -> ok | {error, term()}.
drain_session(H3Conn, SessionId) ->
    send_capsule(H3Conn, SessionId, wt_h3_capsule:drain_session()).

strip_reserved_headers(Headers) ->
    Reserved = #{
        <<":method">> => true,
        <<":protocol">> => true,
        <<":scheme">> => true,
        <<":authority">> => true,
        <<":path">> => true
    },
    [
        {Name, Value}
     || {Name, Value} <- Headers,
        not maps:is_key(Name, Reserved)
    ].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

request_headers_test() ->
    Headers = request_headers(<<"example.com">>, <<"/wt">>, [{<<"origin">>, <<"https://example.com">>}]),
    ?assertEqual({<<":method">>, <<"CONNECT">>}, lists:nth(1, Headers)),
    ?assert(lists:member({<<":protocol">>, <<"webtransport-h3">>}, Headers)),
    ?assert(lists:member({<<"origin">>, <<"https://example.com">>}, Headers)).

response_status_test_() ->
    [
        ?_assertEqual({ok, 200}, response_status([{<<":status">>, <<"200">>}])),
        ?_assert(is_success_response([{<<":status">>, <<"204">>}])),
        ?_assertNot(is_success_response([{<<":status">>, <<"404">>}]))
    ].

-endif.
