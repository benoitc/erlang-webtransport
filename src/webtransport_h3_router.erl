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
-export([register_session/3, unregister_session/2]).
-export([client_connect/4]).
-export([set_passthrough/2]).
-export([open_bidi_stream/2]).
-export([get_h3_conn/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    sessions = #{} :: #{non_neg_integer() => pid()},
    monitors = #{} :: #{reference() => non_neg_integer()},
    pending = #{} :: #{non_neg_integer() => {bidi | uni, binary()}},
    %% Streams opened locally (by us) — StreamId => SessionPid.
    %% Data on these streams is delivered straight to the session, skipping
    %% the session-id varint decode that peer-initiated streams go through.
    local_streams = #{} :: #{non_neg_integer() => pid()},
    passthrough :: undefined | pid(),
    h3_conn :: undefined | pid(),
    h3_monitor :: undefined | reference()
}).

%% ============================================================================
%% API
%% ============================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(undefined).

-spec start_link(undefined | pid()) -> {ok, pid()}.
start_link(Passthrough) ->
    gen_server:start_link(?MODULE, [Passthrough], []).

-spec start(undefined | pid()) -> {ok, pid()}.
start(Passthrough) ->
    gen_server:start(?MODULE, [Passthrough], []).

-spec set_passthrough(pid(), undefined | pid()) -> ok.
set_passthrough(Router, Pid) ->
    gen_server:call(Router, {set_passthrough, Pid}).

-spec register_session(pid(), non_neg_integer(), pid()) -> ok.
register_session(Router, SessionId, SessionPid) ->
    gen_server:call(Router, {register, SessionId, SessionPid}).

-spec unregister_session(pid(), non_neg_integer()) -> ok.
unregister_session(Router, SessionId) ->
    gen_server:call(Router, {unregister, SessionId}).

%% Run quic_h3:connect/3 from inside the router so the router becomes the
%% H3 connection owner.
-spec client_connect(pid(), string() | binary(), inet:port_number(), map()) ->
    {ok, pid()} | {error, term()}.
client_connect(Router, Host, Port, H3Opts) ->
    gen_server:call(Router, {client_connect, Host, Port, H3Opts}, infinity).

%% Atomically open a client-initiated WT bidi stream: pre-claim on quic_h3
%% with the WT bidi signal, register the new StreamId against the session,
%% send the session-id header, all before any inbound stream_type_* events
%% on that stream can be processed from our mailbox.
-spec open_bidi_stream(pid(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
open_bidi_stream(Router, SessionId) ->
    gen_server:call(Router, {open_bidi_stream, SessionId}, infinity).

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
    {reply, ok, State#state{sessions = Sessions, monitors = Monitors}};
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
            Pending = (State#state.pending)#{StreamId => {Direction, <<>>}},
            {noreply, State#state{pending = Pending}}
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
handle_quic_h3({quic_h3, _, _} = Msg, #state{passthrough = Pid} = State) when is_pid(Pid) ->
    Pid ! Msg,
    {noreply, State};
handle_quic_h3(_Msg, State) ->
    {noreply, State}.

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
                {ok, SessionId, Rest} ->
                    State1 = State#state{pending = maps:remove(StreamId, Pending)},
                    deliver_open_and_data(SessionId, StreamId, Direction, Rest, Fin, State1);
                {error, incomplete} ->
                    Pending1 = Pending#{StreamId => {Direction, Combined}},
                    State#state{pending = Pending1};
                {error, _} ->
                    State#state{pending = maps:remove(StreamId, Pending)}
            end;
        error ->
            forward_to_session_by_stream(StreamId, Data, Fin, State)
    end.

deliver_open_and_data(SessionId, StreamId, Direction, Rest, Fin, State) ->
    case maps:find(SessionId, State#state.sessions) of
        {ok, Session} ->
            webtransport_session:handle_stream_opened(Session, StreamId, Direction),
            case Rest of
                <<>> when not Fin -> ok;
                _ -> webtransport_session:handle_stream_data(Session, StreamId, Rest, Fin)
            end,
            State;
        error ->
            State
    end.

forward_to_session_by_stream(StreamId, Data, Fin, State) ->
    %% After header parsing the stream stays bound to its session; we look it
    %% up by walking sessions. For the (rare) hot path, a stream-id index
    %% could be added later.
    case find_session_for_stream(StreamId, State) of
        {ok, Session} ->
            webtransport_session:handle_stream_data(Session, StreamId, Data, Fin),
            State;
        error ->
            State
    end.

route_stream_closed(StreamId, Reason, State) ->
    case find_session_for_stream(StreamId, State) of
        {ok, Session} ->
            webtransport_session:handle_stream_closed(Session, StreamId, Reason);
        error ->
            ok
    end,
    State#state{
        pending = maps:remove(StreamId, State#state.pending),
        local_streams = maps:remove(StreamId, State#state.local_streams)
    }.

route_datagram(QuarterOrStreamId, Payload, State) ->
    %% quic_h3 already strips the quarter-stream-id varint and gives us the
    %% (decoded) stream id plus payload. The stream id IS the session id for
    %% WebTransport's CONNECT stream.
    case maps:find(QuarterOrStreamId, State#state.sessions) of
        {ok, Session} ->
            webtransport_session:handle_datagram_data(Session, Payload);
        error ->
            ok
    end,
    State.

find_session_for_stream(StreamId, #state{local_streams = Local} = State) ->
    case maps:find(StreamId, Local) of
        {ok, _} = Found -> Found;
        error -> find_session_for_peer_stream(StreamId, State)
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
    State#state{sessions = Sessions1, monitors = Monitors1}.

%% draft-15 §3.3: QUIC error codes inside the reserved WebTransport range
%% map back to app-level codes. Anything outside stays opaque for the
%% handler (e.g. native HTTP/3 or QUIC-level errors).
map_from_quic(ErrorCode) ->
    case wt_error:from_quic(ErrorCode) of
        {ok, App} -> App;
        error -> ErrorCode
    end.
