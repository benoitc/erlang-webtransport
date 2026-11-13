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

-export([start_link/0, start_link/1, start/1, start/2]).
-export([register_session/3, unregister_session/2, expect_session/2]).
-export([session_count/2, reserve_session/3, session_gone/2, session_ended/3]).
-export([client_connect/4]).
-export([set_passthrough/2]).
-export([open_bidi_stream/2, open_uni_stream/2]).
-export([get_h3_conn/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Datagrams held for sessions that are not registered yet.
-define(MAX_PARKED_DATAGRAMS, 64).
%% Ended or refused sessions remembered per connection; the oldest is
%% forgotten first.
-define(MAX_GONE_SESSIONS, 256).
%% Bytes of stream data held for sessions that are not registered yet.
-define(MAX_PARKED_BYTES, 1048576).
%% How long a client waits for the server to close its side of a CONNECT
%% stream before closing the connection (http3-17 §6).
-define(CLOSE_WAIT_MS, 1000).

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
    parked_bytes = 0 :: non_neg_integer(),
    %% CONNECT streams being accepted: counted as sessions so two requests
    %% racing on a connection without flow control cannot both pass.
    reserved = #{} :: #{non_neg_integer() => true},
    %% Sessions that ended or were refused: their streams are reset with
    %% WT_SESSION_GONE instead of held. `peer_closed' when the peer sent
    %% WT_CLOSE_SESSION, so later data on the CONNECT stream is an error.
    gone = #{} :: #{non_neg_integer() => ended | peer_closed},
    %% Ids in `gone', oldest first, so it stays within ?MAX_GONE_SESSIONS.
    gone_order = queue:new() :: queue:queue(non_neg_integer()),
    %% Client side: sessions that ended, waiting for the server to close the
    %% CONNECT stream before the connection is closed.
    closing = #{} :: #{non_neg_integer() => reference()},
    role = server :: server | client,
    %% Client side: CONNECT streams whose session is not registered yet.
    %% Their data is held here, in arrival order, and replayed to the session
    %% when it registers.
    expected = #{} :: #{non_neg_integer() => [term()]},
    passthrough :: undefined | pid(),
    h3_conn :: undefined | pid(),
    h3_monitor :: undefined | reference(),
    %% Server: monitor on the QUIC connection this router serves. Its end
    %% stops the router.
    conn_monitor :: undefined | reference()
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
    gen_server:start_link(?MODULE, [Passthrough, undefined], []).

%% @doc Start an unlinked router with the given passthrough target.
-spec start(undefined | pid()) -> {ok, pid()}.
start(Passthrough) ->
    start(Passthrough, undefined).

%% @doc Start an unlinked router that stops when `Conn', the QUIC
%% connection it serves, ends.
-spec start(undefined | pid(), undefined | pid()) -> {ok, pid()}.
start(Passthrough, Conn) ->
    gen_server:start(?MODULE, [Passthrough, Conn], []).

%% @doc Set or clear the passthrough pid for unhandled H3 events.
-spec set_passthrough(pid(), undefined | pid()) -> ok.
set_passthrough(Router, Pid) ->
    gen_server:call(Router, {set_passthrough, Pid}).

%% @doc Register a session pid for the given session ID.
-spec register_session(pid(), non_neg_integer(), pid()) -> ok.
register_session(Router, SessionId, SessionPid) ->
    gen_server:call(Router, {register, SessionId, SessionPid}).

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

%% @doc Hold a slot for a CONNECT stream being accepted. With `Alone' the
%% session must be the only one on the connection (http3-17 §5.1, no flow
%% control) and the call fails if another session or slot exists.
-spec reserve_session(pid(), non_neg_integer(), boolean()) -> ok | {error, busy}.
reserve_session(Router, SessionId, Alone) ->
    gen_server:call(Router, {reserve, SessionId, Alone}).

%% @doc The CONNECT stream was refused or its session failed to start: free
%% its slot and reset the streams held for it with WT_SESSION_GONE.
-spec session_gone(pid(), non_neg_integer()) -> ok.
session_gone(Router, SessionId) ->
    gen_server:call(Router, {gone, SessionId}).

%% @doc A session has ended. `PeerClosed' when the peer sent
%% WT_CLOSE_SESSION: later data on its CONNECT stream is then reset with
%% H3_MESSAGE_ERROR (http3-17 §6). On a client the connection is closed once
%% no session is left, after the server closed the CONNECT stream or a short
%% wait.
-spec session_ended(pid(), non_neg_integer(), boolean()) -> ok.
session_ended(Router, SessionId, PeerClosed) ->
    gen_server:call(Router, {ended, SessionId, PeerClosed}, 5000).

%% @doc Remove the session registration for the given session ID.
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

%% @doc Open a unidirectional stream for a session, write its header and
%% remember it, so events on it reach the session.
-spec open_uni_stream(pid(), non_neg_integer()) -> {ok, non_neg_integer()} | {error, term()}.
open_uni_stream(Router, SessionId) ->
    gen_server:call(Router, {open_uni_stream, SessionId}, 30000).

%% @doc Return the H3 connection pid, or undefined if not yet connected.
-spec get_h3_conn(pid()) -> undefined | pid().
get_h3_conn(Router) ->
    gen_server:call(Router, get_h3_conn).

%% ============================================================================
%% gen_server callbacks
%% ============================================================================

init([Passthrough, Conn]) ->
    process_flag(trap_exit, true),
    ConnMonitor = case Conn of
        undefined -> undefined;
        _ -> erlang:monitor(process, Conn)
    end,
    {ok, #state{passthrough = Passthrough, conn_monitor = ConnMonitor}}.

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
                                       expected = Expected,
                                       reserved = maps:remove(SessionId, State#state.reserved)}),
    {reply, ok, State1};
handle_call({session_count, SessionId}, _From, State) ->
    {reply, others(SessionId, State), State};
handle_call({reserve, SessionId, Alone}, _From, #state{reserved = Reserved} = State) ->
    case Alone andalso others(SessionId, State) > 0 of
        true -> {reply, {error, busy}, State};
        false -> {reply, ok, State#state{reserved = Reserved#{SessionId => true}}}
    end;
handle_call({gone, SessionId}, _From, State) ->
    {reply, ok, refuse_session(SessionId, State)};
handle_call({ended, SessionId, PeerClosed}, _From, State) ->
    State1 = drop_session(SessionId, State),
    Gone = case PeerClosed of true -> peer_closed; false -> ended end,
    State2 = mark_gone(SessionId, Gone, State1),
    {reply, ok, start_closing(SessionId, PeerClosed, State2)};
handle_call({open_uni_stream, SessionId}, _From, State) ->
    do_open_local_uni(SessionId, State);
handle_call({expect, SessionId}, _From, #state{expected = Expected} = State) ->
    {reply, ok, State#state{expected = Expected#{SessionId => []}}};
handle_call({unregister, SessionId}, _From, State) ->
    {reply, ok, drop_session(SessionId, State)};
handle_call({client_connect, Host, Port, H3Opts}, _From, State) ->
    case quic_h3:connect(Host, Port, H3Opts) of
        {ok, H3Conn} = OK ->
            Ref = erlang:monitor(process, H3Conn),
            {reply, OK, State#state{h3_conn = H3Conn, h3_monitor = Ref, role = client}};
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
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{conn_monitor = Ref} = State) ->
    {stop, normal, State};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #state{monitors = Monitors} = State) ->
    case maps:take(Ref, Monitors) of
        {SessionId, Monitors1} ->
            {noreply, drop_session(SessionId, State#state{monitors = Monitors1})};
        error ->
            {noreply, State}
    end;
handle_info({close_wait, SessionId}, State) ->
    {noreply, finish_closing(SessionId, State)};
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
%% With `stream_reset_final_size' the reset carries the stream's final size
%% (http3-17 §5.4); the session counts the bytes it never received.
handle_quic_h3({quic_h3, _Conn, {stream_type_reset, _Direction, StreamId, ErrorCode, FinalSize}},
               State) ->
    {noreply, route_stream_closed(StreamId, {reset, map_from_quic(ErrorCode), FinalSize}, State)};
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
%% A CONNECT stream whose session has ended. After the peer's
%% WT_CLOSE_SESSION, more data on it is an error (http3-17 §6). Its end
%% lets a closing client close the connection.
handle_quic_h3({quic_h3, Conn, {data, StreamId, Data, Fin}}, #state{gone = Gone} = State)
  when is_map_key(StreamId, Gone) ->
    case {maps:get(StreamId, Gone), Data} of
        {peer_closed, D} when D =/= <<>> ->
            _ = cancel_stream(Conn, StreamId, ?H3_MESSAGE_ERROR);
        _ ->
            ok
    end,
    case Fin of
        true -> {noreply, finish_closing(StreamId, State)};
        false -> {noreply, State}
    end;
handle_quic_h3({quic_h3, _Conn, {stream_reset, StreamId, _Code}}, #state{gone = Gone} = State)
  when is_map_key(StreamId, Gone) ->
    {noreply, finish_closing(StreamId, State)};
%% http3-17 §4.7: GOAWAY asks every session to shut down.
handle_quic_h3({quic_h3, _Conn, {goaway, Id}} = Msg, #state{sessions = Sessions} = State) ->
    maps:foreach(fun(_Sid, Pid) -> webtransport_session:goaway(Pid, Id) end, Sessions),
    notify_passthrough(Msg, State),
    {noreply, State};
%% quic_h3 can open a stream again after one was refused at the peer's
%% limit: every session that was refused gets to retry. The client's
%% connect may be waiting on it for its CONNECT.
handle_quic_h3({quic_h3, _Conn, {streams_available, Type}} = Msg,
               #state{sessions = Sessions} = State) ->
    maps:foreach(fun(_Sid, Pid) ->
        webtransport_session:transport_streams_available(Pid, Type)
    end, Sessions),
    notify_passthrough(Msg, State),
    {noreply, State};
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

%% quic_h3 opens the stream, writes its type (0x54) and claims it, so the
%% peer's STOP_SENDING and resets on it reach us; the session id follows.
do_open_local_uni(SessionId, #state{h3_conn = H3Conn} = State) when H3Conn =/= undefined ->
    case maps:find(SessionId, State#state.sessions) of
        {ok, SessionPid} ->
            QuicConn = quic_h3:get_quic_conn(H3Conn),
            case quic_h3:open_uni_stream(H3Conn, ?WT_UNI_STREAM_TYPE) of
                {ok, StreamId} ->
                    case quic:send_data(QuicConn, StreamId, h2_varint:encode(SessionId), false) of
                        ok ->
                            keep_header_on_reset(
                              QuicConn, StreamId,
                              byte_size(wt_h3_capsule:encode_uni_stream_header(SessionId))),
                            Local = (State#state.local_streams)#{StreamId => SessionPid},
                            {reply, {ok, StreamId}, State#state{local_streams = Local}};
                        {error, _} = Err ->
                            {reply, Err, State}
                    end;
                {error, _} = Err ->
                    {reply, Err, State}
            end;
        error ->
            {reply, {error, unknown_session}, State}
    end;
do_open_local_uni(_SessionId, State) ->
    {reply, {error, no_h3_conn}, State}.

%% http3-17 §4.4: any reset quic sends on its own for one of our streams
%% (answering STOP_SENDING) is RESET_STREAM_AT keeping the stream header,
%% so the peer still learns which session the stream belonged to.
keep_header_on_reset(QuicConn, StreamId, HeaderSize) ->
    _ = quic:set_stream_reliable_size(QuicConn, StreamId, HeaderSize),
    ok.

%% Sessions and slots on this connection other than `SessionId'.
others(SessionId, #state{sessions = Sessions, reserved = Reserved}) ->
    map_size(maps:remove(SessionId, maps:merge(Sessions, Reserved))).

%% A refused CONNECT: no session will come, so held streams are reset.
refuse_session(SessionId, #state{parked = Parked} = State) ->
    State1 = mark_gone(SessionId, ended,
                       State#state{reserved = maps:remove(SessionId, State#state.reserved)}),
    case maps:take(SessionId, Parked) of
        {Events, Parked1} ->
            [reset_unknown_session_stream(Id, State1) || {opened, Id, _, _, _} <- Events],
            forget_parked(Events, State1#state{parked = Parked1});
        error ->
            State1
    end.

forget_parked(Events, State) ->
    Datagrams = length([E || E <- Events, element(1, E) =:= datagram]),
    State#state{parked_datagrams = State#state.parked_datagrams - Datagrams,
                parked_bytes = max(0, State#state.parked_bytes - parked_size(Events))}.

parked_size(Events) ->
    lists:sum([byte_size(D) || E <- Events,
                               D <- case E of
                                        {opened, _, _, Bin, _} -> [Bin];
                                        {data, _, Bin, _} -> [Bin];
                                        _ -> []
                                    end]).

%% Client: once its last session ended, close the connection when the
%% server has closed that session's CONNECT stream, or after a short wait.
%% The server closed it already when it sent WT_CLOSE_SESSION.
start_closing(_SessionId, _PeerClosed, #state{role = server} = State) ->
    State;
start_closing(SessionId, true, State) ->
    maybe_close_connection(State#state{closing = maps:remove(SessionId, State#state.closing)});
start_closing(SessionId, false, #state{closing = Closing} = State) ->
    Ref = erlang:send_after(?CLOSE_WAIT_MS, self(), {close_wait, SessionId}),
    State#state{closing = Closing#{SessionId => Ref}}.

finish_closing(SessionId, #state{closing = Closing} = State) ->
    case maps:take(SessionId, Closing) of
        {Ref, Closing1} ->
            _ = erlang:cancel_timer(Ref),
            maybe_close_connection(State#state{closing = Closing1});
        error ->
            State
    end.

maybe_close_connection(#state{role = client, h3_conn = H3Conn, closing = Closing} = State)
  when H3Conn =/= undefined, map_size(Closing) =:= 0 ->
    case map_size(State#state.sessions) + map_size(State#state.reserved) of
        0 -> try quic_h3:close(H3Conn) catch _:_ -> ok end;
        _ -> ok
    end,
    State;
maybe_close_connection(State) ->
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
                            keep_header_on_reset(QuicConn, StreamId, byte_size(Header)),
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
    Size = parked_size([Event]),
    case State#state.parked_bytes + Size > ?MAX_PARKED_BYTES of
        true ->
            %% Too much held: give up on this stream.
            drop_parked_stream(SessionId, element(2, Event), State);
        false ->
            State#state{parked = Parked#{SessionId => [Event | maps:get(SessionId, Parked, [])]},
                        parked_bytes = State#state.parked_bytes + Size}
    end.

drop_parked_stream(SessionId, StreamId, #state{parked = Parked} = State) ->
    _ = cancel_parked(StreamId, State),
    Events = maps:get(SessionId, Parked, []),
    {Mine, Rest} = lists:partition(fun(E) -> element(2, E) =:= StreamId end, Events),
    State1 = forget_parked(Mine, State),
    State1#state{parked = Parked#{SessionId => Rest},
                 stream_sessions = maps:remove(StreamId, State#state.stream_sessions)}.

parked_streams(Parked) ->
    maps:fold(fun(_, Events, Acc) ->
        Acc + length([E || E <- Events, element(1, E) =:= opened])
    end, 0, Parked).

replay_parked(SessionId, Session, #state{parked = Parked} = State) ->
    case maps:take(SessionId, Parked) of
        {Events, Parked1} ->
            lists:foreach(fun(E) -> replay(Session, E) end, lists:reverse(Events)),
            forget_parked(Events, State#state{parked = Parked1});
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
    %% A reset or STOP_SENDING ends one direction; the stream stays mapped
    %% until it is fully closed, so later events still find their session.
    case Reason of
        normal ->
            State#state{
                stream_sessions = maps:remove(StreamId, State#state.stream_sessions),
                pending = maps:remove(StreamId, State#state.pending),
                local_streams = maps:remove(StreamId, State#state.local_streams)
            };
        _ ->
            State#state{pending = maps:remove(StreamId, State#state.pending)}
    end.

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
find_session_for_peer_stream(_StreamId, #state{sessions = Sessions}) ->
    %% Every stream is mapped when its header is read or when we open it.
    %% An unmapped one can only belong to the single session, if there is
    %% one; with several, guessing would deliver to the wrong session.
    case maps:to_list(Sessions) of
        [{_, Session}] -> {ok, Session};
        _ -> error
    end.

drop_session(SessionId, #state{sessions = Sessions, monitors = Monitors} = State) ->
    Sessions1 = maps:remove(SessionId, Sessions),
    Monitors1 = maps:filter(fun(_Ref, Sid) -> Sid =/= SessionId end, Monitors),
    StreamSessions = maps:filter(fun(_S, Sid) -> Sid =/= SessionId end,
                                 State#state.stream_sessions),
    State1 = State#state{sessions = Sessions1, monitors = Monitors1,
                         stream_sessions = StreamSessions},
    mark_gone(SessionId, maps:get(SessionId, State#state.gone, ended), State1).

%% Remember that a session ended or was refused, so later streams and
%% datagrams for it are reset or dropped (http3-17 §6) instead of held.
%% Only the latest ?MAX_GONE_SESSIONS are kept: a stream for an older one
%% is held like any early stream, within the parking limits.
mark_gone(SessionId, Kind, #state{gone = Gone} = State) when is_map_key(SessionId, Gone) ->
    State#state{gone = Gone#{SessionId => Kind}};
mark_gone(SessionId, Kind, #state{gone = Gone, gone_order = Order} = State) ->
    Gone1 = Gone#{SessionId => Kind},
    Order1 = queue:in(SessionId, Order),
    case map_size(Gone1) > ?MAX_GONE_SESSIONS of
        true ->
            {{value, Oldest}, Order2} = queue:out(Order1),
            State#state{gone = maps:remove(Oldest, Gone1), gone_order = Order2};
        false ->
            State#state{gone = Gone1, gone_order = Order1}
    end.

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

%% ============================================================================
%% Eunit
%% ============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Only the latest ?MAX_GONE_SESSIONS ended sessions are kept.
mark_gone_keeps_latest_test() ->
    State = lists:foldl(fun(Id, S) -> mark_gone(Id, ended, S) end,
                        #state{}, lists:seq(1, ?MAX_GONE_SESSIONS + 44)),
    ?assertEqual(?MAX_GONE_SESSIONS, map_size(State#state.gone)),
    ?assertNot(is_map_key(44, State#state.gone)),
    ?assert(is_map_key(45, State#state.gone)),
    ?assert(is_map_key(?MAX_GONE_SESSIONS + 44, State#state.gone)),
    %% Marking a known id again updates it in place.
    State1 = mark_gone(100, peer_closed, State),
    ?assertEqual(peer_closed, maps:get(100, State1#state.gone)),
    ?assertEqual(?MAX_GONE_SESSIONS, map_size(State1#state.gone)).

-endif.
