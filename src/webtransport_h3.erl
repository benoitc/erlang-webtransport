%% @doc Minimal runtime wrapper for an HTTP/3 WebTransport session.
%%
%% The HTTP/3 CONNECT stream is managed through quic_h3, while native
%% WebTransport streams and datagrams use the underlying QUIC connection.
%%
-module(webtransport_h3).

-export([new/3, with_peer_settings/2]).
-export([session_id/1, h3_conn/1, quic_conn/1, peer_settings/1]).
-export([open_bidi_stream/1, open_uni_stream/1]).
-export([send/4, send_datagram/2]).
-export([close_session/3, drain_session/1]).
-export([reset_stream/4, stop_sending/3]).
-export([decode_stream_header/1, decode_datagram/1]).

-record(state, {
    h3_conn :: pid(),
    quic_conn :: pid(),
    session_id :: non_neg_integer(),
    peer_settings = #{} :: map()
}).

-opaque state() :: #state{}.

-export_type([state/0]).

%% ============================================================================
%% Lifecycle
%% ============================================================================

-spec new(pid(), pid(), non_neg_integer()) -> state().
new(H3Conn, QuicConn, SessionId) ->
    PeerSettings =
        case quic_h3:get_peer_settings(H3Conn) of
            undefined -> #{};
            Settings -> Settings
        end,
    #state{
        h3_conn = H3Conn,
        quic_conn = QuicConn,
        session_id = SessionId,
        peer_settings = PeerSettings
    }.

-spec with_peer_settings(state(), map()) -> state().
with_peer_settings(State, PeerSettings) ->
    State#state{peer_settings = PeerSettings}.

-spec session_id(state()) -> non_neg_integer().
session_id(#state{session_id = SessionId}) ->
    SessionId.

-spec h3_conn(state()) -> pid().
h3_conn(#state{h3_conn = H3Conn}) ->
    H3Conn.

-spec quic_conn(state()) -> pid().
quic_conn(#state{quic_conn = QuicConn}) ->
    QuicConn.

-spec peer_settings(state()) -> map().
peer_settings(#state{peer_settings = PeerSettings}) ->
    PeerSettings.

%% ============================================================================
%% Native WebTransport Streams
%% ============================================================================

-spec open_bidi_stream(state()) -> {ok, non_neg_integer(), state()} | {error, term()}.
open_bidi_stream(#state{quic_conn = QuicConn, session_id = SessionId} = State) ->
    case quic:open_stream(QuicConn) of
        {ok, StreamId} ->
            Header = wt_h3_capsule:encode_bidi_stream_header(SessionId),
            case quic:send_data(QuicConn, StreamId, Header, false) of
                ok -> {ok, StreamId, State};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec open_uni_stream(state()) -> {ok, non_neg_integer(), state()} | {error, term()}.
open_uni_stream(#state{quic_conn = QuicConn, session_id = SessionId} = State) ->
    case quic:open_unidirectional_stream(QuicConn) of
        {ok, StreamId} ->
            Header = wt_h3_capsule:encode_uni_stream_header(SessionId),
            case quic:send_data(QuicConn, StreamId, Header, false) of
                ok -> {ok, StreamId, State};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec send(state(), non_neg_integer(), iodata(), boolean()) -> ok | {error, term()}.
send(#state{quic_conn = QuicConn}, StreamId, Data, Fin) ->
    quic:send_data(QuicConn, StreamId, Data, Fin).

%% ============================================================================
%% HTTP Datagrams
%% ============================================================================

-spec send_datagram(state(), binary()) -> ok | {error, term()}.
send_datagram(#state{quic_conn = QuicConn, session_id = SessionId}, Data) ->
    quic:send_datagram(QuicConn, wt_h3_capsule:encode_datagram(SessionId, Data)).

-spec decode_datagram(binary()) ->
    {ok, non_neg_integer(), binary()} | {more, pos_integer()} | {error, term()}.
decode_datagram(Bin) ->
    wt_h3_capsule:decode_datagram(Bin).

%% ============================================================================
%% CONNECT Stream Capsules
%% ============================================================================

-spec close_session(state(), non_neg_integer(), binary()) -> ok | {error, term()}.
close_session(#state{h3_conn = H3Conn, session_id = SessionId}, ErrorCode, Reason) ->
    wt_h3:close_session(H3Conn, SessionId, ErrorCode, Reason).

-spec drain_session(state()) -> ok | {error, term()}.
drain_session(#state{h3_conn = H3Conn, session_id = SessionId}) ->
    wt_h3:drain_session(H3Conn, SessionId).

%% ============================================================================
%% Stream Control
%% ============================================================================

-spec reset_stream(state(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | {error, term()}.
reset_stream(#state{quic_conn = QuicConn}, StreamId, ErrorCode, ReliableSize) ->
    quic:reset_stream_at(QuicConn, StreamId, ErrorCode, ReliableSize).

-spec stop_sending(state(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(#state{quic_conn = QuicConn}, StreamId, ErrorCode) ->
    quic:stop_sending(QuicConn, StreamId, ErrorCode).

%% ============================================================================
%% Incoming Data Helpers
%% ============================================================================

-spec decode_stream_header(binary()) ->
    {ok, non_neg_integer(), wt_h3_capsule:stream_kind(), binary()} |
    {more, pos_integer()} |
    {error, term()}.
decode_stream_header(Bin) ->
    wt_h3_capsule:decode_stream_header(Bin).
