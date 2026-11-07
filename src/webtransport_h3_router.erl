%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc Per-H3-connection router for WebTransport extension events.
%%
%% Owns claimed-stream and HTTP-datagram events emitted by quic_h3 for one
%% H3 connection, and demultiplexes them to the right WebTransport session
%% by the session-id varint that prefixes each stream / quarter-stream-id
%% that prefixes each datagram.
-module(webtransport_h3_router).
-behaviour(gen_server).

-include("webtransport.hrl").

-export([start_link/0, start_link/1, start/1]).
-export([register_session/3, unregister_session/2, expect_session/2]).
-export([session_count/2]).
-export([client_connect/4]).
-export([set_passthrough/2]).
-export([open_bidi_stream/2]).
-export([get_h3_conn/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Datagrams held for sessions that are not registered yet.
-define(MAX_PARKED_DATAGRAMS, 64).

-record(state, {
    sessions = #{} :: #{non_neg_integer() => pid()},
    monitors = #{} :: #{reference() => non_neg_integer()},
    pending = #{} :: #{non_neg_integer() => {bidi | uni, binary()}},
    %% Max pending (un-dispatched) streams before we cancel with
    %% WT_BUFFERED_STREAM_REJECTED. Default 64.
    max_pending_streams = 64 :: non_neg_integer(),
    %% Streams opened locally (by us) — StreamId => SessionPid.
    %% Data on these streams is delivered straight to the session, skipping
    %% the session-id varint decode that peer-initiated streams go through.
    local_streams = #{} :: #{non_neg_integer() => pid()},
    %% Peer streams by the session they belong to, set once their header
    %% is read.
    stream_sessions = #{} :: #{non_neg_integer() => non_neg_integer()},
    %% http3-17 §4.6: events for a session that is not registered yet, held
    %% newest first and replayed when it registers. Counted against
    %% max_pending_streams (streams) and ?MAX_PARKED_DATAGRAMS.
    parked = #{} :: #{non_neg_integer() => [term()]},
    parked_datagrams = 0 :: non_neg_integer(),
    %% Sessions that were registered and have ended: their streams are
    %% reset with WT_SESSION_GONE instead of held.
    gone = #{} :: #{non_neg_integer() => true},
    %% Client side: CONNECT streams whose session is not registered yet.
    %% Their data is held here, in arrival order, and replayed to the session
    %% when it registers.
    expected = #{} :: #{non_neg_integer() => [term()]},
    passthrough :: undefined | pid(),
    h3_conn :: undefined | pid(),
    h3_monitor :: undefined | reference()
}).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a router with no passthrough target.
-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(undefined).

%% @doc Start a linked router, forwarding unhandled H3 events to Passthrough.
-spec start_link(undefined | pid()) -> {ok, pid()}.
start_link(Passthrough) ->
    gen_server:start_link(?MODULE, [Passthrough], []).

%% @doc Start an unlinked router with the given passthrough target.
-spec start(undefined | pid()) -> {ok, pid()}.
start(Passthrough) ->
    gen_server:start(?MODULE, [Passthrough], []).

%% @doc Set or clear the passthrough pid for unhandled H3 events.
-spec set_passthrough(pid(), undefined | pid()) -> ok.
set_passthrough(Router, Pid) ->
    gen_server:call(Router, {set_passthrough, Pid}).

%% @doc Register a session pid for the given session ID.
-spec register_session(pid(), non_neg_integer(), pid()) -> ok.
register_session(Router, SessionId, SessionPid) ->
    gen_server:call(Router, {register, SessionId, SessionPid}).

%% @doc Remove the session registration for the given session ID.
%% @doc Client side: the CONNECT request for `SessionId' was sent. Hold the
%% data the server sends on that stream until the session registers, so
%% capsules that follow the 2xx response (flow control, close, drain) reach
%% the session and not the connecting process.
-spec expect_session(pid(), non_neg_integer()) -> ok.
expect_session(Router, SessionId) ->
    gen_server:call(Router, {expect, SessionId}).

%% @doc Number of sessions registered on this connection other than
%% `SessionId'.
-spec session_count(pid(), non_neg_integer()) -> non_neg_integer().
session_count(Router, SessionId) ->
    gen_server:call(Router, {session_count, SessionId}, 5000).

-spec unregister_session(pid(), non_neg_integer()) -> ok.
unregister_session(Router, SessionId) ->
    gen_server:call(Router, {unregister, SessionId}).

%% @doc Connect to an H3 server. The router becomes the H3 connection owner.
-spec client_connect(pid(), string() | binary() | inet:ip_address(), inet:port_number(), map()) ->
    {ok, pid()} | {error, term()}.
client_connect(Router, Host, Port, H3Opts) ->
    gen_server:call(Router, {client_connect, Host, Port, H3Opts}, infinity).

%% @doc Open a client-initiated WT bidi stream, sending the session-id header atomically.
-spec open_bidi_stream(pid(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
open_bidi_stream(Router, SessionId) ->
    %% Bounded: this runs inside the session gen_statem's call handler, so an
    %% unresponsive H3 connection must not stall the session indefinitely.
    gen_server:call(Router, {open_bidi_stream, SessionId}, 30000).

%% @doc Return the H3 connection pid, or undefined if not yet connected.
-spec get_h3_conn(pid()) -> undefined | pid().
get_h3_conn(Router) ->
    gen_server:call(Router, get_h3_conn).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([Passthrough]) ->
    process_flag(trap_exit, true),
    {ok, #state{passthrough = Passthrough}}.

handle_call({register, SessionId, SessionPid}, _From, State) ->
    Ref = erlang:monitor(process, SessionPid),
    Sessions = (State#state.sessions)#{SessionId => SessionPid},
    Monitors = (State#state.monitors)#{Ref => SessionId},
    {Held, Expected} = case maps:take(SessionId, State#state.expected) of
        {Msgs, Rest} -> {lists:reverse(Msgs), Rest};
        error -> {[], State#state.expected}
    end,
    [SessionPid ! Msg || Msg <- Held],
    State1 = replay_parked(SessionId, SessionPid,
                           State#state{sessions = Sessions, monitors = Monitors,
                                       expected = Expected}),
    {reply, ok, State1};
handle_call({session_count, SessionId}, _From, #state{sessions = Sessions} = State) ->
    {reply, map_size(maps:remove(SessionId, Sessions)), State};
handle_call({expect, SessionId}, _From, #state{expected = Expected} = State) ->
    {reply, ok, State#state{expected = Expected#{SessionId => []}}};
handle_call({unregister, SessionId}, _From, State) ->
    {reply, ok, drop_session(SessionId, State)};
handle_call({client_connect, Host, Port, H3Opts}, _From, State) ->
    case quic_h3:connect(Host, Port, H3Opts) of
        {ok, H3Conn} = OK ->
            Ref = erlang:monitor(process, H3Conn),
            {reply, OK, State#state{h3_conn = H3Conn, h3_monitor = Ref}};
        Err ->
            {reply, Err, State}
    end;
handle_call({open_bidi_stream, SessionId}, _From, State) ->
    do_open_local_bidi(SessionId, State);
handle_call(get_h3_conn, _From, State) ->
    {reply, State#state.h3_conn, State};
handle_call({set_passthrough, Pid}, _From, State) ->
    {reply, ok, State#state{passthrough = Pid}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({quic_h3, _, _} = Msg, State) ->
    handle_quic_h3(Msg, State);
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{h3_monitor = Ref} = State) ->
    {stop, normal, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{monitors = Monitors} = State) ->
    case maps:take(Ref, Monitors) of
        {SessionId, Monitors1} ->
            {noreply, drop_session(SessionId, State#state{monitors = Monitors1})};
        error ->
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

handle_quic_h3({quic_h3, Conn, {stream_type_open, Direction, StreamId, _Type}}, State0) ->
    State = ensure_h3_monitor(Conn, State0),
    case maps:is_key(StreamId, State#state.local_streams) of
        true ->
            {noreply, State};
        false ->
            case map_size(State#state.pending) >= State#state.max_pending_streams of
                true ->
                    %% Buffering cap reached; reject per draft-15 §4.6.
                    _ = cancel_stream(Conn, StreamId, ?WT_BUFFERED_STREAM_REJECTED),
                    {noreply, State};
                false ->
                    Pending = (State#state.pending)#{StreamId => {Direction, <<>>}},
                    {noreply, State#state{pending = Pending}}
            end
    end;
handle_quic_h3({quic_h3, Conn, {stream_type_data, _Direction, StreamId, Data, Fin}}, State0) ->
    State = ensure_h3_monitor(Conn, State0),
    {noreply, route_stream_data(StreamId, Data, Fin, State)};
handle_quic_h3({quic_h3, _Conn, {stream_type_closed, _Direction, StreamId}}, State) ->
    {noreply, route_stream_closed(StreamId, normal, State)};
handle_quic_h3({quic_h3, _Conn, {stream_type_reset, _Direction, StreamId, ErrorCode}}, State) ->
    {noreply, route_stream_closed(StreamId, {reset, map_from_quic(ErrorCode)}, State)};
handle_quic_h3({quic_h3, _Conn, {stream_type_stop_sending, _Direction, StreamId, ErrorCode}}, State) ->
    {noreply, route_stream_closed(StreamId, {stop_sending, map_from_quic(ErrorCode)}, State)};
handle_quic_h3({quic_h3, Conn, {datagram, StreamId, Payload}}, State0) ->
    State = ensure_h3_monitor(Conn, State0),
    {noreply, route_datagram(StreamId, Payload, State)};
%% A reset of a session's CONNECT stream ends that session.
handle_quic_h3({quic_h3, _Conn, {stream_reset, StreamId, _Code}} = Msg,
               #state{sessions = Sessions} = State)
  when is_map_key(StreamId, Sessions) ->
    maps:get(StreamId, Sessions) ! Msg,
    {noreply, State};
%% Client side: data on a CONNECT stream belongs to its session.
handle_quic_h3({quic_h3, _Conn, {data, StreamId, _Data, _Fin}} = Msg,
               #state{sessions = Sessions, expected = Expected} = State)
  when is_map_key(StreamId, Sessions); is_map_key(StreamId, Expected) ->
    case maps:find(StreamId, Sessions) of
        {ok, Session} ->
            Session ! Msg,
            {noreply, State};
        error ->
            Held = maps:get(StreamId, Expected),
            {noreply, State#state{expected = Expected#{StreamId => [Msg | Held]}}}
    end;
handle_quic_h3({quic_h3, _Conn, {session_ticket, Ticket}}, State) ->
    %% Connection-scoped 0-RTT resumption ticket. Surface it to the connecting
    %% process (the passthrough on the client side); it can store the ticket
    %% and pass it back as `session_ticket' on a later connect.
    notify_passthrough({webtransport, session_ticket, Ticket}, State),
    {noreply, State};
handle_quic_h3({quic_h3, _Conn, {early_data_rejected, StreamIds}}, State) ->
    notify_passthrough({webtransport, early_data_rejected, StreamIds}, State),
    {noreply, State};
handle_quic_h3({quic_h3, _, _} = Msg, #state{passthrough = Pid} = State) when is_pid(Pid) ->
    Pid ! Msg,
    {noreply, State};
handle_quic_h3(_Msg, State) ->
    {noreply, State}.

notify_passthrough(Msg, #state{passthrough = Pid}) when is_pid(Pid) ->
    Pid ! Msg,
    ok;
notify_passthrough(_Msg, _State) ->
    ok.

terminate(_Reason, _State) ->
    ok.

%% ============================================================================
%% Internal
%% ============================================================================

ensure_h3_monitor(Conn, #state{h3_conn = undefined} = State) ->
    Ref = erlang:monitor(process, Conn),
    State#state{h3_conn = Conn, h3_monitor = Ref};
ensure_h3_monitor(_Conn, State) ->
    State.

do_open_local_bidi(SessionId, State) ->
    case maps:find(SessionId, State#state.sessions) of
        {ok, SessionPid} ->
            case State#state.h3_conn of
                undefined ->
                    {reply, {error, no_h3_conn}, State};
                H3Conn ->
                    case quic_h3:open_bidi_stream(H3Conn, ?WT_BIDI_SIGNAL) of
                        {ok, StreamId} ->
                            Header = wt_h3_capsule:encode_bidi_stream_header(SessionId),
                            QuicConn = quic_h3:get_quic_conn(H3Conn),
                            _ = quic:send_data(QuicConn, StreamId, Header, false),
                            Local = (State#state.local_streams)#{StreamId => SessionPid},
                            State1 = ensure_h3_monitor(H3Conn, State),
                            {reply, {ok, StreamId},
                             State1#state{local_streams = Local}};
                        {error, _} = Err ->
                            {reply, Err, State}
                    end
            end;
        error ->
            {reply, {error, unknown_session}, State}
    end.

route_stream_data(StreamId, Data, Fin, #state{local_streams = Local} = State) ->
    case maps:find(StreamId, Local) of
        {ok, Session} ->
            webtransport_session:handle_stream_data(Session, StreamId, Data, Fin),
            State;
        error ->
            route_peer_stream_data(StreamId, Data, Fin, State)
    end.

route_peer_stream_data(StreamId, Data, Fin, #state{pending = Pending} = State) ->
    case maps:find(StreamId, Pending) of
        {ok, {Direction, Buffered}} ->
            Combined = <<Buffered/binary, Data/binary>>,
            case h2_varint:decode(Combined) of
                {ok, SessionId, _Rest} when SessionId band 3 =/= 0 ->
                    %% http3-17 §4: a session id must be a client-initiated
                    %% bidirectional stream id.
                    close_connection(?H3_ID_ERROR, State),
                    State#state{pending = maps:remove(StreamId, Pending)};
                {ok, SessionId, Rest} ->
                    State1 = State#state{pending = maps:remove(StreamId, Pending)},
                    deliver_open_and_data(SessionId, StreamId, Direction, Rest, Fin, State1);
                {error, incomplete} ->
                    Pending1 = Pending#{StreamId => {Direction, Combined}},
                    State#state{pending = Pending1}
            end;
        error ->
            forward_to_session_by_stream(StreamId, Data, Fin, State)
    end.

deliver_open_and_data(SessionId, StreamId, Direction, Rest, Fin, State0) ->
    State = State0#state{stream_sessions = (State0#state.stream_sessions)#{StreamId => SessionId}},
    case maps:find(SessionId, State#state.sessions) of
        {ok, Session} ->
            webtransport_session:handle_stream_opened(Session, StreamId, Direction),
            case Rest of
                <<>> when not Fin -> ok;
                _ -> webtransport_session:handle_stream_data(Session, StreamId, Rest, Fin)
            end,
            State;
        error ->
            case maps:is_key(SessionId, State#state.gone) of
                true ->
                    %% The session has ended: reset with WT_SESSION_GONE so
                    %% the peer sees why instead of stalling (http3-17 §6).
                    reset_unknown_session_stream(StreamId, State);
                false ->
                    park_stream(SessionId, StreamId, Direction, Rest, Fin, State)
            end
    end.

%% http3-17 §4.6: a stream may arrive before its session is established
%% (the client may open streams before it sees the 2xx, the server before
%% the client registered the session). Hold it, within the stream cap.
park_stream(SessionId, StreamId, Direction, Rest, Fin, #state{parked = Parked} = State) ->
    case parked_streams(Parked) >= State#state.max_pending_streams of
        true ->
            _ = cancel_parked(StreamId, State),
            State#state{stream_sessions = maps:remove(StreamId, State#state.stream_sessions)};
        false ->
            park(SessionId, {opened, StreamId, Direction, Rest, Fin}, State)
    end.

cancel_parked(_StreamId, #state{h3_conn = undefined}) ->
    ok;
cancel_parked(StreamId, #state{h3_conn = H3Conn}) ->
    cancel_stream(H3Conn, StreamId, ?WT_BUFFERED_STREAM_REJECTED).

park(SessionId, Event, #state{parked = Parked} = State) ->
    State#state{parked = Parked#{SessionId => [Event | maps:get(SessionId, Parked, [])]}}.

parked_streams(Parked) ->
    maps:fold(fun(_, Events, Acc) ->
        Acc + length([E || E <- Events, element(1, E) =:= opened])
    end, 0, Parked).

replay_parked(SessionId, Session, #state{parked = Parked} = State) ->
    case maps:take(SessionId, Parked) of
        {Events, Parked1} ->
            Datagrams = length([E || E <- Events, element(1, E) =:= datagram]),
            lists:foreach(fun(E) -> replay(Session, E) end, lists:reverse(Events)),
            State#state{parked = Parked1,
                        parked_datagrams = State#state.parked_datagrams - Datagrams};
        error ->
            State
    end.

replay(Session, {opened, StreamId, Direction, Data, Fin}) ->
    webtransport_session:handle_stream_opened(Session, StreamId, Direction),
    case Data of
        <<>> when not Fin -> ok;
        _ -> webtransport_session:handle_stream_data(Session, StreamId, Data, Fin)
    end;
replay(Session, {data, StreamId, Data, Fin}) ->
    webtransport_session:handle_stream_data(Session, StreamId, Data, Fin);
replay(Session, {closed, StreamId, Reason}) ->
    webtransport_session:handle_stream_closed(Session, StreamId, Reason);
replay(Session, {datagram, Payload}) ->
    webtransport_session:handle_datagram_data(Session, Payload).

reset_unknown_session_stream(_StreamId, #state{h3_conn = undefined} = State) ->
    State;
reset_unknown_session_stream(StreamId, #state{h3_conn = H3Conn} = State) ->
    _ = cancel_stream(H3Conn, StreamId, ?WT_SESSION_GONE),
    State.

close_connection(_Code, #state{h3_conn = undefined}) ->
    ok;
close_connection(Code, #state{h3_conn = H3Conn}) ->
    try quic_h3:close(H3Conn, Code) catch _:_ -> ok end.

cancel_stream(H3Conn, StreamId, ErrorCode) ->
    case is_process_alive(H3Conn) of
        false -> ok;
        true ->
            try quic_h3:cancel(H3Conn, StreamId, ErrorCode)
            catch exit:_ -> ok; error:_ -> ok
            end
    end.

forward_to_session_by_stream(StreamId, Data, Fin, State) ->
    case find_session_for_stream(StreamId, State) of
        {ok, Session} ->
            webtransport_session:handle_stream_data(Session, StreamId, Data, Fin),
            State;
        {parked, SessionId} ->
            park(SessionId, {data, StreamId, Data, Fin}, State);
        error ->
            State
    end.

route_stream_closed(StreamId, Reason, State0) ->
    State = case find_session_for_stream(StreamId, State0) of
        {ok, Session} ->
            webtransport_session:handle_stream_closed(Session, StreamId, Reason),
            State0;
        {parked, SessionId} ->
            park(SessionId, {closed, StreamId, Reason}, State0);
        error ->
            State0
    end,
    State#state{
        stream_sessions = maps:remove(StreamId, State#state.stream_sessions),
        pending = maps:remove(StreamId, State#state.pending),
        local_streams = maps:remove(StreamId, State#state.local_streams)
    }.

route_datagram(QuarterOrStreamId, Payload, State) ->
    %% quic_h3 already strips the quarter-stream-id varint and gives us the
    %% (decoded) stream id plus payload. The stream id IS the session id for
    %% WebTransport's CONNECT stream.
    case maps:find(QuarterOrStreamId, State#state.sessions) of
        {ok, Session} ->
            webtransport_session:handle_datagram_data(Session, Payload),
            State;
        error ->
            %% http3-17 §4.6: hold a few datagrams for a session that is not
            %% registered yet; drop the rest.
            case maps:is_key(QuarterOrStreamId, State#state.gone) orelse
                 State#state.parked_datagrams >= ?MAX_PARKED_DATAGRAMS of
                true ->
                    State;
                false ->
                    State1 = park(QuarterOrStreamId, {datagram, Payload}, State),
                    State1#state{parked_datagrams = State1#state.parked_datagrams + 1}
            end
    end.

%% `{ok, Pid}' for a stream of a registered session, `{parked, SessionId}'
%% for one held until its session registers.
find_session_for_stream(StreamId, #state{local_streams = Local} = State) ->
    case maps:find(StreamId, Local) of
        {ok, _} = Found ->
            Found;
        error ->
            case maps:find(StreamId, State#state.stream_sessions) of
                {ok, SessionId} ->
                    case maps:find(SessionId, State#state.sessions) of
                        {ok, _} = Found -> Found;
                        error -> {parked, SessionId}
                    end;
                error ->
                    find_session_for_peer_stream(StreamId, State)
            end
    end.

find_session_for_peer_stream(_StreamId, #state{sessions = Sessions}) when map_size(Sessions) =:= 0 ->
    error;
find_session_for_peer_stream(StreamId, #state{sessions = Sessions}) ->
    %% A native WT stream id is always > the session-id of the CONNECT
    %% stream that opened it, and below the next session's CONNECT id.
    %% With one session per H3 connection (the common case) this is a
    %% single map lookup.
    case maps:size(Sessions) of
        1 ->
            [{_, Session}] = maps:to_list(Sessions),
            {ok, Session};
        _ ->
            %% Pick the session whose id is the largest one <= StreamId.
            Candidates = [{Sid, Pid} || {Sid, Pid} <- maps:to_list(Sessions), Sid =< StreamId],
            case Candidates of
                [] -> error;
                _ ->
                    {_, Pid} = lists:last(lists:keysort(1, Candidates)),
                    {ok, Pid}
            end
    end.

drop_session(SessionId, #state{sessions = Sessions, monitors = Monitors} = State) ->
    Sessions1 = maps:remove(SessionId, Sessions),
    Monitors1 = maps:filter(fun(_Ref, Sid) -> Sid =/= SessionId end, Monitors),
    StreamSessions = maps:filter(fun(_S, Sid) -> Sid =/= SessionId end,
                                 State#state.stream_sessions),
    State#state{sessions = Sessions1, monitors = Monitors1,
                stream_sessions = StreamSessions,
                gone = (State#state.gone)#{SessionId => true}}.

%% draft-15 §3.3: QUIC error codes inside the reserved WebTransport range
%% map back to app-level codes. Anything outside stays opaque for the
%% handler (e.g. native HTTP/3 or QUIC-level errors).
%% http3-17 §4.4: a reset code outside the WT_APPLICATION_ERROR range
%% carries no application code; report it as `undefined'.
map_from_quic(ErrorCode) ->
    case wt_error:from_quic(ErrorCode) of
        {ok, App} -> App;
        error -> undefined
    end.
