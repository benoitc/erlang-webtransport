%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc Minimal runtime wrapper for an HTTP/3 WebTransport session.
%%
%% The HTTP/3 CONNECT stream is managed through quic_h3. Native WebTransport
%% streams and datagrams use the underlying QUIC connection.
%%
-module(webtransport_h3).

-include("webtransport.hrl").

-export([new/2, new/3, with_peer_settings/2, with_router/2]).
-export([session_id/1, h3_conn/1, quic_conn/1, peer_settings/1, router/1]).
-export([open_bidi_stream/1, open_uni_stream/1]).
-export([send/4, send_datagram/2]).
-export([close_session/3, drain_session/1, send_capsule/2]).
-export([reset_stream/4, stop_sending/3, stream_header_size/2]).
-export([decode_stream_header/1, decode_datagram/1]).

-record(state, {
    h3_conn :: pid(),
    quic_conn :: pid(),
    session_id :: non_neg_integer(),
    peer_settings = #{} :: map(),
    router :: undefined | pid()
}).

-opaque state() :: #state{}.
-type code() :: {app, non_neg_integer()} | {wt, non_neg_integer()}.

-export_type([state/0, code/0]).

%% ============================================================================
%% Lifecycle
%% ============================================================================

%% @doc Create a new H3 transport state with no router.
-spec new(pid(), non_neg_integer()) -> state().
new(H3Conn, SessionId) ->
    new(H3Conn, SessionId, undefined).

%% @doc Create a new H3 transport state, optionally attaching a stream router.
-spec new(pid(), non_neg_integer(), undefined | pid()) -> state().
new(H3Conn, SessionId, Router) ->
    QuicConn = quic_h3:get_quic_conn(H3Conn),
    PeerSettings =
        case quic_h3:get_peer_settings(H3Conn) of
            undefined -> #{};
            Settings -> Settings
        end,
    #state{
        h3_conn = H3Conn,
        quic_conn = QuicConn,
        session_id = SessionId,
        peer_settings = PeerSettings,
        router = Router
    }.

%% @doc Return a new state with updated peer settings.
-spec with_peer_settings(state(), map()) -> state().
with_peer_settings(State, PeerSettings) ->
    State#state{peer_settings = PeerSettings}.

%% @doc Return a new state with the given stream router pid.
-spec with_router(state(), undefined | pid()) -> state().
with_router(State, Router) ->
    State#state{router = Router}.

%% @doc Return the stream router pid, or `undefined' if none is set.
-spec router(state()) -> undefined | pid().
router(#state{router = Router}) ->
    Router.

%% @doc Return the WebTransport session ID (the CONNECT stream ID).
-spec session_id(state()) -> non_neg_integer().
session_id(#state{session_id = SessionId}) ->
    SessionId.

%% @doc Return the HTTP/3 connection pid.
-spec h3_conn(state()) -> pid().
h3_conn(#state{h3_conn = H3Conn}) ->
    H3Conn.

%% @doc Return the underlying QUIC connection pid.
-spec quic_conn(state()) -> pid().
quic_conn(#state{quic_conn = QuicConn}) ->
    QuicConn.

%% @doc Return the peer's advertised WebTransport settings.
-spec peer_settings(state()) -> map().
peer_settings(#state{peer_settings = PeerSettings}) ->
    PeerSettings.

%% ============================================================================
%% Native WebTransport Streams
%% ============================================================================

%% Native WebTransport streams use the underlying QUIC connection directly.
%% Each stream starts with a header identifying the WebTransport session:
%% - Bidirectional streams: Session ID (varint)
%% - Unidirectional streams: Stream Type 0x54 (varint) + Session ID (varint)

%% @doc Open a new bidirectional WebTransport stream on the QUIC connection.
-spec open_bidi_stream(state()) -> {ok, non_neg_integer(), state()} | {error, term()}.
open_bidi_stream(#state{router = Router, session_id = SessionId} = State) when is_pid(Router) ->
    %% Route through the H3 router so stream_type_open for our own local open
    %% is ignored (no varint-decode misparse on echoed data).
    case webtransport_h3_router:open_bidi_stream(Router, SessionId) of
        {ok, StreamId} -> {ok, StreamId, State};
        {error, _} = Error -> Error
    end;
open_bidi_stream(#state{h3_conn = H3Conn, quic_conn = QuicConn, session_id = SessionId} = State) ->
    %% No router (e.g., tests): fall back to direct quic_h3 + send.
    case quic_h3:open_bidi_stream(H3Conn, ?WT_BIDI_SIGNAL) of
        {ok, StreamId} ->
            Header = wt_h3_capsule:encode_bidi_stream_header(SessionId),
            case quic:send_data(QuicConn, StreamId, Header, false) of
                ok -> {ok, StreamId, State};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Open a new unidirectional WebTransport stream on the QUIC connection.
-spec open_uni_stream(state()) -> {ok, non_neg_integer(), state()} | {error, term()}.
open_uni_stream(#state{router = Router, session_id = SessionId} = State) when is_pid(Router) ->
    %% Through the router, so events on the stream reach the session.
    case webtransport_h3_router:open_uni_stream(Router, SessionId) of
        {ok, StreamId} -> {ok, StreamId, State};
        {error, _} = Error -> Error
    end;
open_uni_stream(#state{quic_conn = QuicConn, session_id = SessionId} = State) ->
    case quic:open_unidirectional_stream(QuicConn) of
        {ok, StreamId} ->
            Header = wt_h3_capsule:encode_uni_stream_header(SessionId),
            case quic:send_data(QuicConn, StreamId, Header, false) of
                ok -> {ok, StreamId, State};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Send data on a stream. Routes through H3 for the CONNECT stream, QUIC for native streams.
-spec send(state(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send(#state{quic_conn = QuicConn, h3_conn = H3Conn, session_id = SessionId}, StreamId, Data, Fin) ->
    case StreamId =:= SessionId of
        true ->
            %% CONNECT stream - use H3 connection
            quic_h3:send_data(H3Conn, StreamId, iolist_to_binary(Data), Fin);
        false ->
            %% Native WebTransport stream - use QUIC connection directly
            quic:send_data(QuicConn, StreamId, iolist_to_binary(Data), Fin)
    end.

%% ============================================================================
%% HTTP Datagrams
%% ============================================================================

%% @doc Send an unreliable datagram, prefixed with the quarter stream ID.
-spec send_datagram(state(), binary()) -> ok | {error, term()}.
send_datagram(_State, Data) when byte_size(Data) > ?WT_H3_DATAGRAM_MAX ->
    {error, datagram_too_large};
send_datagram(#state{quic_conn = QuicConn, session_id = SessionId}, Data) ->
    %% Encode datagram with quarter stream ID prefix
    Datagram = wt_h3_capsule:encode_datagram(SessionId, Data),
    quic:send_datagram(QuicConn, Datagram).

%% @doc Decode an incoming QUIC datagram into session ID and payload.
-spec decode_datagram(binary()) ->
    {ok, non_neg_integer(), binary()} | {more, pos_integer()} | {error, term()}.
decode_datagram(Bin) ->
    wt_h3_capsule:decode_datagram(Bin).

%% ============================================================================
%% CONNECT Stream Capsules
%% ============================================================================

%% @doc Send a CLOSE_WEBTRANSPORT_SESSION capsule and FIN the CONNECT stream.
-spec close_session(state(), non_neg_integer(), binary()) -> ok | {error, term()}.
close_session(#state{h3_conn = H3Conn, session_id = SessionId}, ErrorCode, Reason) ->
    %% WT_CLOSE_SESSION then FIN on the CONNECT stream (http3-17 §6).
    case wt_h3_capsule:close_session(ErrorCode, Reason) of
        {error, _} = Err ->
            Err;
        Capsule ->
            quic_h3:send_data(H3Conn, SessionId, wt_h3_capsule:encode(Capsule), true)
    end.

%% @doc Send a session-level capsule (flow control) on the CONNECT stream.
-spec send_capsule(state(), wt_h3_capsule:capsule()) -> ok | {error, term()}.
send_capsule(#state{h3_conn = H3Conn, session_id = SessionId}, Capsule) ->
    quic_h3:send_data(H3Conn, SessionId, wt_h3_capsule:encode(Capsule), false).

%% @doc Send a DRAIN_WEBTRANSPORT_SESSION capsule to signal graceful shutdown.
-spec drain_session(state()) -> ok | {error, term()}.
drain_session(#state{h3_conn = H3Conn, session_id = SessionId}) ->
    %% Send DRAIN_WEBTRANSPORT_SESSION capsule
    Capsule = wt_h3_capsule:encode(wt_h3_capsule:drain_session()),
    quic_h3:send_data(H3Conn, SessionId, Capsule, false).

%% ============================================================================
%% Stream Control
%% ============================================================================

%% @doc Reset the send side of a stream.
%%
%% On a WebTransport data stream this is RESET_STREAM_AT with `ReliableSize'
%% covering the stream header we wrote plus any application offset
%% (http3-17 §4.4), so the peer still learns which session the stream
%% belonged to. With a `ReliableSize' of 0 (a peer-initiated stream, where we
%% wrote no header) a plain RESET_STREAM is sent.
-spec reset_stream(state(), non_neg_integer(), code(), non_neg_integer()) ->
    ok | {error, term()}.
reset_stream(#state{quic_conn = QuicConn, h3_conn = H3Conn, session_id = SessionId},
             StreamId, Code, ReliableSize) ->
    QuicCode = quic_code(Code),
    case StreamId =:= SessionId of
        true ->
            quic_h3:cancel(H3Conn, StreamId, QuicCode);
        false when ReliableSize > 0 ->
            quic:reset_stream_at(QuicConn, StreamId, QuicCode, ReliableSize);
        false ->
            quic:reset_stream(QuicConn, StreamId, QuicCode)
    end.

%% @doc Ask the peer to stop sending on a stream.
-spec stop_sending(state(), non_neg_integer(), code()) -> ok | {error, term()}.
stop_sending(#state{quic_conn = QuicConn, h3_conn = H3Conn, session_id = SessionId},
             StreamId, Code) ->
    QuicCode = quic_code(Code),
    case StreamId =:= SessionId of
        true ->
            quic_h3:cancel(H3Conn, StreamId, QuicCode);
        false ->
            quic:stop_sending(QuicConn, StreamId, QuicCode)
    end.

%% `{app, Code}' is a WebTransport application code, mapped into the
%% WT_APPLICATION_ERROR range (http3-17 §4.4). `{wt, Code}' is a protocol
%% code such as WT_SESSION_GONE, sent unchanged (http3-17 §6).
quic_code({app, Code}) -> wt_error:to_quic(Code);
quic_code({wt, Code}) -> Code.

%% @doc Size of the header we write on a stream we open for this session:
%% the stream type or signal, then the session id.
-spec stream_header_size(state(), bidi | uni) -> pos_integer().
stream_header_size(#state{session_id = SessionId}, bidi) ->
    byte_size(wt_h3_capsule:encode_bidi_stream_header(SessionId));
stream_header_size(#state{session_id = SessionId}, uni) ->
    byte_size(wt_h3_capsule:encode_uni_stream_header(SessionId)).

%% ============================================================================
%% Incoming Data Helpers
%% ============================================================================

%% @doc Decode the session ID and stream kind from the start of an incoming stream.
-spec decode_stream_header(binary()) ->
    {ok, non_neg_integer(), wt_h3_capsule:stream_kind(), binary()} |
    {more, pos_integer()} |
    {error, term()}.
decode_stream_header(Bin) ->
    wt_h3_capsule:decode_stream_header(Bin).

%% ============================================================================
%% Eunit
%% ============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% http3-17 §6: WT_SESSION_GONE is a protocol code and goes on the wire as
%% is; application codes are mapped into the WT_APPLICATION_ERROR range.
quic_code_test_() ->
    [
        ?_assertEqual(16#170d7b68, quic_code({wt, ?WT_SESSION_GONE})),
        ?_assertEqual(16#3994bd84, quic_code({wt, ?WT_BUFFERED_STREAM_REJECTED})),
        ?_assertEqual(wt_error:to_quic(7), quic_code({app, 7})),
        ?_assertNotEqual(?WT_SESSION_GONE, quic_code({app, ?WT_SESSION_GONE}))
    ].

stream_header_size_test_() ->
    State = #state{h3_conn = self(), quic_conn = self(), session_id = 0},
    Big = State#state{session_id = 1024},
    [
        %% 0x54 and 0x41 are two-byte varints; session 0 is one byte.
        ?_assertEqual(3, stream_header_size(State, uni)),
        ?_assertEqual(3, stream_header_size(State, bidi)),
        ?_assertEqual(4, stream_header_size(Big, bidi))
    ].

-endif.
