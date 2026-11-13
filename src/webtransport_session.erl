%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc WebTransport session state machine.
%%
%% This module implements a gen_statem that manages a WebTransport session's
%% lifecycle, including stream management, flow control, and handler callbacks.
%%
%% The session supports both HTTP/2 and HTTP/3 transports, using the
%% appropriate transport module for capsule/stream handling.
%%
-module(webtransport_session).
-behaviour(gen_statem).

-include("webtransport.hrl").

%% A limit that is never reached: flow control off, or per-stream windows
%% on h3, where QUIC does them.
-define(UNLIMITED, 1 bsl 62).
%% Largest capsule we buffer on the h3 CONNECT stream.
-define(CONNECT_BUFFER_MAX, 64 * 1024 * 1024).
%% Largest Maximum Streams value (http3-17 §5.6.2, http2-16 §6.7).
-define(MAX_STREAMS, 1 bsl 60).

%% API
-export([start_link/4, start_link/5]).
-export([send/4, send_datagram/2]).
-export([open_stream/2, close_stream/2]).
-export([reset_stream/3, stop_sending/3]).
-export([drain/1, close/3]).
-export([get_info/1]).
-export([early_data_accepted/1]).

%% Internal API (called by transport layers)
-export([handle_capsule/2, handle_stream_data/4, handle_datagram_data/2]).
-export([established/1, established/2, goaway/2, transport_streams_available/2]).
-export([handle_stream_opened/3, handle_stream_closed/3]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3]).
-export([connecting/3, open/3, draining/3]).

-record(data, {
    transport :: h2 | h3,
    transport_state :: term(),
    handler :: module(),
    handler_state :: term(),
    request :: map(),
    %% Stream management
    streams = #{} :: #{non_neg_integer() => webtransport_stream:stream()},
    next_bidi_id :: non_neg_integer(),
    next_uni_id :: non_neg_integer(),
    %% Flow control
    local_max_data = 0 :: non_neg_integer(),
    remote_max_data = 0 :: non_neg_integer(),
    local_max_streams_bidi = 0 :: non_neg_integer(),
    local_max_streams_uni = 0 :: non_neg_integer(),
    remote_max_streams_bidi = 0 :: non_neg_integer(),
    remote_max_streams_uni = 0 :: non_neg_integer(),
    bytes_sent = 0 :: non_neg_integer(),
    bytes_received = 0 :: non_neg_integer(),
    %% Size of the session receive window we advertise and replenish.
    session_window = 0 :: non_neg_integer(),
    %% WebTransport flow control in force (http3-17 §5.1). Always on for
    %% h2; on h3 only when both endpoints sent a non-zero initial limit.
    %% When off, limits are not enforced and flow-control capsules are
    %% ignored.
    flow_control = true :: boolean(),
    %% Per-stream receive window we grant on h2 streams.
    local_max_stream_data = 0 :: non_neg_integer(),
    %% Per-stream send windows the peer granted on h2, as WebTransport-Init
    %% keys: u (our uni), br (our bidi), bl (its bidi).
    peer_stream_data = #{} :: #{u | bl | br => non_neg_integer()},
    %% Streams opened over the session's life, by us and by the peer. Stream
    %% limits are cumulative (http3-17 §5.6.2).
    opened_bidi = 0 :: non_neg_integer(),
    opened_uni = 0 :: non_neg_integer(),
    peer_opened_bidi = 0 :: non_neg_integer(),
    peer_opened_uni = 0 :: non_neg_integer(),
    %% h2: the client's WebTransport-Init, kept to combine with the SETTINGS
    %% in effect when the session is accepted.
    webtransport_init = #{} :: map(),
    %% The peer sent WT_CLOSE_SESSION.
    peer_closed = false :: boolean(),
    %% Actions returned by the handler's init, run once the session is
    %% established: after the 2xx on a server, after registration on a
    %% client. Before that nothing may go out on the CONNECT stream.
    pending_actions = [] :: [webtransport_handler:action()],
    %% h3: CONNECT stream bytes that do not end on a capsule boundary yet.
    connect_buf = <<>> :: binary(),
    %% Stream types we failed to open for lack of credit; the handler is told
    %% when credit comes back (overview-14 §4.3).
    blocked = #{} :: #{bidi | uni => true},
    %% Types refused by the QUIC connection's stream limit (h3), waiting
    %% for quic's streams_available notice.
    transport_blocked = #{} :: #{bidi | uni => true},
    %% h3: monitor on the H3 connection. Its end ends the session.
    conn_monitor :: undefined | reference(),
    %% Flags
    is_server :: boolean(),
    close_info :: undefined | {non_neg_integer(), binary()}
}).

-type session() :: pid().
-type stream_ref() :: non_neg_integer().
-type request() :: #{
    path := binary(),
    authority := binary(),
    headers => [{binary(), binary()}]
}.

-export_type([session/0, stream_ref/0, request/0]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Start a session with default gen_statem options.
-spec start_link(h2 | h3, term(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(Transport, TransportState, Handler, Opts) ->
    start_link(Transport, TransportState, Handler, Opts, []).

%% @doc Start a session linked to the caller, with explicit gen_statem start options.
-spec start_link(h2 | h3, term(), module(), map(), list()) -> {ok, pid()} | {error, term()}.
start_link(Transport, TransportState, Handler, Opts, StartOpts) ->
    gen_statem:start_link(?MODULE, {Transport, TransportState, Handler, Opts}, StartOpts).

%% @doc Send data on a stream. Set Fin to true to half-close the local side.
-spec send(session(), stream_ref(), iodata(), boolean()) -> ok | {error, term()}.
send(Session, StreamId, Data, Fin) ->
    gen_statem:call(Session, {send, StreamId, iolist_to_binary(Data), Fin}).

%% @doc Send an unreliable datagram on the session.
-spec send_datagram(session(), iodata()) -> ok | {error, term()}.
send_datagram(Session, Data) ->
    gen_statem:call(Session, {send_datagram, iolist_to_binary(Data)}).

%% @doc Open a new locally-initiated stream of the given type.
-spec open_stream(session(), bidi | uni) -> {ok, stream_ref()} | {error, term()}.
open_stream(Session, Type) ->
    gen_statem:call(Session, {open_stream, Type}).

%% @doc Gracefully close a stream by sending FIN.
-spec close_stream(session(), stream_ref()) -> ok | {error, term()}.
close_stream(Session, StreamId) ->
    gen_statem:call(Session, {close_stream, StreamId}).

%% @doc Abruptly reset a stream with the given application error code.
-spec reset_stream(session(), stream_ref(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(Session, StreamId, ErrorCode) ->
    gen_statem:call(Session, {reset_stream, StreamId, ErrorCode}).

%% @doc Ask the peer to stop sending on a stream with the given error code.
-spec stop_sending(session(), stream_ref(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(Session, StreamId, ErrorCode) ->
    gen_statem:call(Session, {stop_sending, StreamId, ErrorCode}).

%% @doc Initiate a graceful drain: no new streams, existing ones finish.
-spec drain(session()) -> ok.
drain(Session) ->
    gen_statem:cast(Session, drain).

%% @doc Close the session immediately with the given error code and reason.
-spec close(session(), non_neg_integer(), binary()) -> ok.
close(Session, ErrorCode, Reason) ->
    gen_statem:cast(Session, {close, ErrorCode, Reason}).

%% @doc Return a map of session metrics: stream count, flow control limits, bytes sent/received.
-spec get_info(session()) -> {ok, map()} | {error, term()}.
get_info(Session) ->
    gen_statem:call(Session, get_info).

%% @doc Report whether the underlying connection negotiated 0-RTT early data.
%% Returns `true'/`false'/`unknown' for h3 (per RFC 9001 §4.6), or
%% `not_supported' for h2 which has no QUIC 0-RTT.
-spec early_data_accepted(session()) -> boolean() | unknown | not_supported.
early_data_accepted(Session) ->
    gen_statem:call(Session, early_data_accepted).

%% Internal API

%% @doc The session is established: the server sent its 2xx, or the client
%% registered the session. Runs the actions the handler's init returned.
-spec established(session()) -> ok.
established(Session) ->
    gen_statem:cast(Session, established).

%% @doc As established/1, with the peer SETTINGS in effect when the session
%% was accepted (h2, http2-16 §4.3.1). They replace the ones read earlier.
-spec established(session(), map()) -> ok.
established(Session, Info) ->
    gen_statem:cast(Session, {established, Info}).

%% @doc The QUIC connection can open a stream of this type again, after an
%% open was refused with `stream_limit' (h3, sent by the router).
-spec transport_streams_available(session(), bidi | uni) -> ok.
transport_streams_available(Session, Type) ->
    gen_statem:cast(Session, {streams_available, Type}).

%% @doc The connection received GOAWAY: no new streams (http3-17 §4.7).
-spec goaway(session(), non_neg_integer()) -> ok.
goaway(Session, Id) ->
    gen_statem:cast(Session, {goaway, Id}).

%% @doc Deliver a decoded capsule to the session (called by transport layers).
-spec handle_capsule(session(), term()) -> ok.
handle_capsule(Session, Capsule) ->
    gen_statem:cast(Session, {capsule, Capsule}).

%% @doc Deliver inbound stream data to the session (called by transport layers).
-spec handle_stream_data(session(), stream_ref(), binary(), boolean()) -> ok.
handle_stream_data(Session, StreamId, Data, Fin) ->
    gen_statem:cast(Session, {stream_data, StreamId, Data, Fin}).

%% @doc Deliver an inbound datagram payload to the session.
-spec handle_datagram_data(session(), binary()) -> ok.
handle_datagram_data(Session, Data) ->
    gen_statem:cast(Session, {datagram_data, Data}).

%% @doc Notify the session that a peer-initiated stream was opened.
-spec handle_stream_opened(session(), stream_ref(), bidi | uni) -> ok.
handle_stream_opened(Session, StreamId, Type) ->
    gen_statem:cast(Session, {stream_opened, StreamId, Type}).

%% @doc Notify the session that a stream was closed or reset by the peer.
-spec handle_stream_closed(session(), stream_ref(), term()) -> ok.
handle_stream_closed(Session, StreamId, Reason) ->
    gen_statem:cast(Session, {stream_closed, StreamId, Reason}).

%% ============================================================================
%% gen_statem callbacks
%% ============================================================================

callback_mode() -> state_functions.

init({Transport, TransportState, Handler, Opts}) ->
    IsServer = maps:get(is_server, Opts, true),
    Request = maps:get(request, Opts, #{}),
    HandlerOpts = maps:get(handler_opts, Opts, #{}),

    %% Stream ID assignment depends on role
    {NextBidi, NextUni} = case IsServer of
        true -> {1, 3};   %% Server: odd IDs
        false -> {0, 2}   %% Client: even IDs
    end,

    Data = init_limits(Transport, TransportState, Opts, #data{
        transport = Transport,
        transport_state = TransportState,
        handler = Handler,
        request = Request,
        is_server = IsServer,
        next_bidi_id = NextBidi,
        next_uni_id = NextUni,
        conn_monitor = monitor_connection(Transport, TransportState)
    }),

    %% Initialize handler. `code:ensure_loaded/1' is required because
    %% `erlang:function_exported/3' returns false for modules that haven't
    %% been loaded yet — which would silently demote init/3 callers to
    %% init/2 and throw handler_opts away.
    %%
    %% Preference order: `init/3' > `init/2'. Handlers that export neither
    %% are a configuration error and we stop immediately with a clear
    %% reason so the user sees it in the crash log.
    InitResult =
        case code:ensure_loaded(Handler) of
            {error, LoadReason} ->
                {error, {handler_not_loaded, Handler, LoadReason}};
            {module, Handler} ->
                case erlang:function_exported(Handler, init, 3) of
                    true ->
                        Handler:init(self(), Request, HandlerOpts);
                    false ->
                        case erlang:function_exported(Handler, init, 2) of
                            true -> Handler:init(self(), Request);
                            false -> {error, {no_init_callback, Handler}}
                        end
                end
        end,
    case InitResult of
        {ok, HandlerState} ->
            {ok, open, Data#data{handler_state = HandlerState}};
        {ok, HandlerState, Actions} ->
            {ok, open, Data#data{handler_state = HandlerState, pending_actions = Actions}};
        {error, Reason} ->
            {stop, Reason}
    end.

%% h3: watch the H3 connection, so the session ends with it (http3-17 §6).
%% On h2 the CONNECT stream's data loop watches the connection.
monitor_connection(h3, TransportState) ->
    erlang:monitor(process, webtransport_h3:h3_conn(TransportState));
monitor_connection(h2, _TransportState) ->
    undefined.

%% Flow-control limits at session start.
%%
%% Local limits are what we grant the peer: our own options. Remote limits
%% are what the peer granted us: its SETTINGS (http3-17 §5.5, http2-16
%% §4.3.1), plus WebTransport-Init for h2 per-stream limits. A limit the
%% peer did not send is 0.
init_limits(Transport, TransportState, Opts, Data) ->
    MaxData = maps:get(max_data, Opts, ?DEFAULT_MAX_DATA),
    MaxBidi = maps:get(max_streams_bidi, Opts, ?DEFAULT_MAX_STREAMS_BIDI),
    MaxUni = maps:get(max_streams_uni, Opts, ?DEFAULT_MAX_STREAMS_UNI),
    Peer = peer_limits(Transport, TransportState, Opts),
    %% http3-17 §5.1: flow control is on when both sides sent a non-zero
    %% initial limit in SETTINGS, whatever the draft: Safari 26 negotiates
    %% http3-07 and still enforces the limits. Legacy clients and a listener
    %% pinned to legacy mode send none (`declared_flow_control => false').
    WeDeclare = maps:get(declared_flow_control, Opts, true) andalso
                lists:any(fun(V) -> V > 0 end, [MaxData, MaxBidi, MaxUni]),
    FlowControl = case Transport of
        h2 -> true;
        h3 -> WeDeclare andalso lists:any(fun(V) -> V > 0 end, maps:values(Peer))
    end,
    Remote = fun(Key) ->
        case FlowControl of
            true -> maps:get(Key, Peer, 0);
            false -> ?UNLIMITED
        end
    end,
    Data#data{
        flow_control = FlowControl,
        local_max_data = MaxData,
        session_window = MaxData,
        local_max_streams_bidi = MaxBidi,
        local_max_streams_uni = MaxUni,
        local_max_stream_data = maps:get(max_stream_data, Opts, ?DEFAULT_MAX_STREAM_DATA),
        remote_max_data = Remote(max_data),
        remote_max_streams_bidi = Remote(max_streams_bidi),
        remote_max_streams_uni = Remote(max_streams_uni),
        peer_stream_data = maps:get(peer_stream_data, Opts, #{}),
        webtransport_init = maps:get(webtransport_init, Opts, #{})
    }.

%% h2: limits from the peer SETTINGS in effect when the session was
%% accepted, replacing the ones read before (no stream exists yet).
apply_peer_limits(PeerSettings, Init, StateData) ->
    Peer = peer_limits(h2, undefined, #{peer_settings => PeerSettings}),
    StateData#data{
        remote_max_data = maps:get(max_data, Peer),
        remote_max_streams_bidi = maps:get(max_streams_bidi, Peer),
        remote_max_streams_uni = maps:get(max_streams_uni, Peer),
        peer_stream_data = wt_h2_init:apply_greater_of(Init,
                                                       wt_h2_init:from_settings(PeerSettings))
    }.

%% The peer's initial session-level limits, from its SETTINGS.
peer_limits(h3, TransportState, Opts) ->
    Settings = case maps:get(peer_settings, Opts, #{}) of
        S when map_size(S) > 0 -> S;
        _ -> webtransport_h3:peer_settings(TransportState)
    end,
    maps:filter(fun(_, V) -> is_integer(V) end, #{
        max_data => wt_h3:peer_setting(Settings, 16#2b61),
        max_streams_bidi => wt_h3:peer_setting(Settings, 16#2b65),
        max_streams_uni => wt_h3:peer_setting(Settings, 16#2b64)
    });
peer_limits(h2, _TransportState, Opts) ->
    Settings = maps:get(peer_settings, Opts, #{}),
    #{
        max_data => maps:get(wt_initial_max_data, Settings, 0),
        max_streams_bidi => maps:get(wt_initial_max_streams_bidi, Settings, 0),
        max_streams_uni => maps:get(wt_initial_max_streams_uni, Settings, 0)
    }.


%% Open state - normal operation
open({call, From}, {send, StreamId, Data, Fin}, #data{} = StateData) ->
    case do_send(StreamId, Data, Fin, StateData) of
        {ok, StateData1} ->
            {keep_state, StateData1, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, {send_datagram, Data}, #data{} = StateData) ->
    case do_send_datagram(Data, StateData) of
        {ok, StateData1} ->
            {keep_state, StateData1, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, {open_stream, Type}, #data{} = StateData) ->
    case do_open_stream(Type, StateData) of
        {ok, StreamId, StateData1} ->
            {keep_state, StateData1, [{reply, From, {ok, StreamId}}]};
        {error, stream_limit_reached = Reason} ->
            {keep_state, mark_blocked(Type, StateData), [{reply, From, {error, Reason}}]};
        {error, stream_limit = Reason} ->
            {keep_state, mark_transport_blocked(Type, StateData),
             [{reply, From, {error, Reason}}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, {close_stream, StreamId}, #data{} = StateData) ->
    case do_close_stream(StreamId, StateData) of
        {ok, StateData1} ->
            {keep_state, StateData1, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, {reset_stream, StreamId, ErrorCode}, #data{} = StateData) ->
    case do_reset_stream(StreamId, ErrorCode, StateData) of
        {ok, StateData1} ->
            {keep_state, StateData1, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, {stop_sending, StreamId, ErrorCode}, #data{} = StateData) ->
    case do_stop_sending(StreamId, ErrorCode, StateData) of
        {ok, StateData1} ->
            {keep_state, StateData1, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

open({call, From}, get_info, StateData) ->
    Info = build_info(StateData),
    {keep_state, StateData, [{reply, From, {ok, Info}}]};

open({call, From}, early_data_accepted, StateData) ->
    {keep_state, StateData, [{reply, From, do_early_data_accepted(StateData)}]};

open(cast, {established, #{peer_settings := PeerSettings}},
     #data{transport = h2, webtransport_init = Init} = StateData) ->
    StateData1 = apply_peer_limits(PeerSettings, Init, StateData),
    open(cast, established, StateData1);
open(cast, {established, _}, StateData) ->
    open(cast, established, StateData);
open(cast, established, #data{pending_actions = Actions} = StateData) ->
    {StateData1, Transition} = handle_actions(Actions, StateData#data{pending_actions = []}),
    open_apply_transition(Transition, StateData1);
open(cast, {streams_available, Type}, StateData) ->
    transition_result(on_transport_streams_available(Type, StateData));
open(cast, {goaway, Id}, StateData) ->
    %% http3-17 §4.7: GOAWAY asks every session to shut down; stop opening
    %% streams and let the handler know.
    {StateData1, Transition} = notify_info({goaway, Id}, StateData),
    case Transition of
        continue -> maybe_stop_if_drained(StateData1, [], draining);
        _ -> open_apply_transition(Transition, StateData1)
    end;
open(cast, {capsule, {datagram, Data}}, StateData) ->
    {StateData1, Transition} = handle_incoming_datagram(Data, StateData),
    open_apply_transition(Transition, StateData1);
open(cast, {capsule, Capsule}, StateData) ->
    apply_capsule(Capsule, StateData);

open(cast, {stream_data, StreamId, Data, Fin}, StateData) ->
    {StateData1, Transition} = handle_incoming_stream_data(StreamId, Data, Fin, StateData),
    open_apply_transition(Transition, StateData1);

open(cast, {datagram_data, Data}, StateData) ->
    {StateData1, Transition} = handle_incoming_datagram(Data, StateData),
    open_apply_transition(Transition, StateData1);

open(cast, {stream_opened, StreamId, Type}, StateData) ->
    {StateData1, Transition} = handle_remote_stream_opened(StreamId, Type, StateData),
    open_apply_transition(Transition, StateData1);

open(cast, {stream_closed, StreamId, Reason}, StateData) ->
    {StateData1, Transition} = handle_remote_stream_closed(StreamId, Reason, StateData),
    open_apply_transition(Transition, StateData1);

open(cast, drain, StateData) ->
    do_drain(StateData),
    enter_draining(StateData);

open(cast, {close, ErrorCode, Reason}, StateData) ->
    do_close(ErrorCode, Reason, StateData),
    {stop, normal, StateData#data{close_info = {ErrorCode, Reason}}};

%% h3 CONNECT stream: capsules (http3-17 §6, RFC 9297) and its end. The
%% server receives it as the stream handler; the client through the router.
open(info, {quic_h3, _Conn, {data, StreamId, Data, Fin}},
     #data{transport = h3, transport_state = H3State} = StateData) ->
    case webtransport_h3:session_id(H3State) of
        StreamId -> handle_connect_data(Data, Fin, StateData);
        _ -> {keep_state, StateData}
    end;
open(info, {quic_h3, _Conn, {stream_reset, StreamId, _Code}},
     #data{transport = h3, transport_state = H3State} = StateData) ->
    case webtransport_h3:session_id(H3State) of
        StreamId -> connect_stream_gone(<<"CONNECT stream reset">>, StateData);
        _ -> {keep_state, StateData}
    end;
%% http3-17 §6: the connection closing ends the CONNECT stream, and so the
%% session. Nothing can be sent any more.
open(info, {'DOWN', Ref, process, _, _}, #data{conn_monitor = Ref} = StateData) ->
    {stop, normal, StateData#data{close_info = {0, <<"connection closed">>}}};

open(info, Msg, #data{handler = Handler, handler_state = HState} = StateData) ->
    case erlang:function_exported(Handler, handle_info, 2) of
        true ->
            case Handler:handle_info(Msg, HState) of
                {ok, HState1} ->
                    {keep_state, StateData#data{handler_state = HState1}};
                {ok, HState1, Actions} ->
                    {StateData1, Transition} = handle_actions(Actions, StateData#data{handler_state = HState1}),
                    open_apply_transition(Transition, StateData1);
                {stop, Reason, HState1} ->
                    {stop, Reason, StateData#data{handler_state = HState1}}
            end;
        false ->
            {keep_state, StateData}
    end.

%% Bytes on the h3 CONNECT stream. Capsules may span DATA frames, so the
%% tail that does not end on a capsule boundary waits for the next chunk.
%% A malformed capsule, a bad WT_CLOSE_SESSION reason, or anything after
%% WT_CLOSE_SESSION resets the stream with H3_MESSAGE_ERROR (http3-17 §6).
%% The stream ending without WT_CLOSE_SESSION ends the session too.
handle_connect_data(Data, Fin, #data{connect_buf = Buf} = StateData) ->
    case wt_h3_capsule:decode_all(<<Buf/binary, Data/binary>>) of
        {ok, _Capsules, Rest} when byte_size(Rest) > ?CONNECT_BUFFER_MAX ->
            connect_message_error(<<"capsule too large">>, StateData);
        {ok, Capsules, Rest} ->
            case lists:splitwith(fun(C) -> element(1, C) =/= close_session end, Capsules) of
                {_, [_Close, _ | _]} ->
                    connect_message_error(<<"data after WT_CLOSE_SESSION">>, StateData);
                {_, [_Close]} when Rest =/= <<>> ->
                    connect_message_error(<<"data after WT_CLOSE_SESSION">>, StateData);
                _ when Fin, Rest =/= <<>> ->
                    connect_message_error(<<"CONNECT stream ended inside a capsule">>, StateData);
                _ ->
                    Result = apply_capsule_list(Capsules, StateData#data{connect_buf = Rest}),
                    case {Fin, Result} of
                        {true, {keep_state, SD}} ->
                            connect_stream_gone(<<"peer closed CONNECT">>, SD);
                        {true, {next_state, draining, SD}} ->
                            connect_stream_gone(<<"peer closed CONNECT">>, SD);
                        {true, {stop, _, _} = Stop} ->
                            Stop;
                        _ ->
                            Result
                    end
            end;
        {error, Reason} ->
            logger:warning("h3 CONNECT capsule decode error: ~p", [Reason]),
            connect_message_error(<<"malformed capsule">>, StateData)
    end.

connect_message_error(Reason, #data{transport_state = H3State} = StateData) ->
    reset_all_streams(StateData),
    _ = transport_call(fun() ->
            webtransport_h3:reset_stream(H3State, webtransport_h3:session_id(H3State),
                                         {wt, ?H3_MESSAGE_ERROR}, 0)
        end),
    {stop, {shutdown, {session_error, ?H3_MESSAGE_ERROR}},
     StateData#data{close_info = {?H3_MESSAGE_ERROR, Reason}}}.

%% The session's stream ended without WT_CLOSE_SESSION: the session is over
%% (http3-17 §6, http2-16 §3.4).
connect_stream_gone(Reason, StateData) ->
    reset_all_streams(StateData),
    finish_connect(StateData),
    {stop, normal, StateData#data{close_info = {0, Reason}}}.

apply_capsule_list([], StateData) ->
    {keep_state, StateData};
apply_capsule_list([{datagram, Data} | Rest], StateData) ->
    %% RFC 9297 DATAGRAM capsule on the CONNECT stream.
    case handle_incoming_datagram(Data, StateData) of
        {StateData1, continue} -> apply_capsule_list(Rest, StateData1);
        {StateData1, Transition} -> open_apply_transition(Transition, StateData1)
    end;
apply_capsule_list([Capsule | Rest], StateData) ->
    case apply_capsule(Capsule, StateData) of
        {keep_state, StateData1} -> apply_capsule_list(Rest, StateData1);
        Other -> Other
    end.

%% Apply one capsule from the peer and return the gen_statem result.
%% h2: the CONNECT stream ended (FIN) or was reset by the peer.
apply_capsule(connect_fin, StateData) ->
    connect_stream_gone(<<"peer closed CONNECT">>, StateData);
apply_capsule({malformed, Reason}, StateData) ->
    %% RFC 9297 §3.3: a malformed capsule stream is a session error.
    stop_with_session_error(?WT_ERROR_GENERIC, Reason, StateData);
%% The connection failed under the session (h2 SETTINGS error): nothing can
%% be sent any more.
apply_capsule({connection_error, Reason}, StateData) ->
    {stop, {shutdown, {connection_error, Reason}},
     StateData#data{close_info = {0, Reason}}};
apply_capsule(connect_reset, StateData) ->
    reset_all_streams(StateData),
    {stop, normal, StateData#data{close_info = {0, <<"CONNECT stream reset">>}}};
apply_capsule({datagram, Data}, StateData) ->
    {StateData1, Transition} = handle_incoming_datagram(Data, StateData),
    open_apply_transition(Transition, StateData1);
%% http2-16 §5.2: streams follow QUIC stream states, where RESET_STREAM,
%% STOP_SENDING, MAX_STREAM_DATA and STREAM_DATA_BLOCKED open a stream the
%% peer has not used yet just as data does.
apply_capsule(Capsule, #data{transport = h2, streams = Streams} = StateData)
  when (element(1, Capsule) =:= reset_stream orelse element(1, Capsule) =:= stop_sending
        orelse element(1, Capsule) =:= max_stream_data
        orelse element(1, Capsule) =:= stream_data_blocked)
       andalso not is_map_key(element(2, Capsule), Streams) ->
    StreamId = element(2, Capsule),
    case is_local_stream(StreamId, StateData) orelse is_retired(StreamId, StateData) of
        true ->
            %% Ours and never opened, or finished: the handlers below
            %% report it as a stream-state error.
            apply_stream_capsule(Capsule, StateData);
        false ->
            case open_peer_stream(StreamId, webtransport_stream:stream_type(StreamId),
                                  StateData) of
                {StateData1, continue} -> apply_stream_capsule(Capsule, StateData1);
                {StateData1, Transition} -> open_apply_transition(Transition, StateData1)
            end
    end;
apply_capsule(Capsule, StateData) ->
    apply_stream_capsule(Capsule, StateData).

apply_stream_capsule({reset_stream, StreamId, Code, ReliableSize}, StateData) ->
    transition_result(handle_peer_reset(StreamId, Code, ReliableSize, StateData));
apply_stream_capsule({stop_sending, StreamId, Code}, StateData) ->
    transition_result(handle_peer_stop_sending(StreamId, Code, StateData));
apply_stream_capsule(Capsule, StateData) ->
    case handle_incoming_capsule(Capsule, StateData) of
        {session_error, Code, Reason} ->
            stop_with_session_error(Code, Reason, StateData);
        StateData1 ->
            case notify_stream_credit(StateData1) of
                {StateData2, continue} -> apply_capsule_transition(Capsule, StateData2);
                {StateData2, Transition} -> open_apply_transition(Transition, StateData2)
            end
    end.

transition_result({StateData, Transition}) ->
    open_apply_transition(Transition, StateData).

%% WT_RESET_STREAM (h2): the peer ended its send side. http2-16 §6.2: the
%% stream must have a send side on the peer's end that is still open, and
%% the Reliable Size must equal what we received; otherwise
%% WT_STREAM_STATE_ERROR. Error codes are 32 bits (WT_ERROR otherwise).
handle_peer_reset(_StreamId, Code, _ReliableSize, StateData) when Code > 16#ffffffff ->
    fail_session(?WT_ERROR_GENERIC, <<"reset error code above 32 bits">>, StateData);
handle_peer_reset(StreamId, Code, ReliableSize, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case {has_peer_send_side(StreamId, StateData),
                  webtransport_stream:bytes_received(Stream) =:= ReliableSize,
                  webtransport_stream:receive_reset(Stream, Code)} of
                {true, true, {ok, Stream1}} ->
                    StateData1 = retire_stream(
                                   StreamId, StateData#data{streams = Streams#{StreamId => Stream1}}),
                    notify_stream_closed(StreamId, {reset, Code}, StateData1);
                {_, false, _} ->
                    fail_session(?WT_STREAM_STATE_ERROR, <<"reset reliable size mismatch">>,
                                 StateData);
                _ ->
                    fail_session(?WT_STREAM_STATE_ERROR, <<"reset on a finished stream">>,
                                 StateData)
            end;
        error ->
            fail_session(?WT_STREAM_STATE_ERROR, <<"reset on an unknown stream">>, StateData)
    end.

%% WT_STOP_SENDING (h2): the peer no longer reads. http2-16 §6.3: answer
%% with WT_RESET_STREAM if our send side is still open (RFC 9000 §3.5); a
%% second STOP_SENDING, or one for a stream we never send on, is a
%% WT_STREAM_STATE_ERROR.
handle_peer_stop_sending(_StreamId, Code, StateData) when Code > 16#ffffffff ->
    fail_session(?WT_ERROR_GENERIC, <<"stop_sending error code above 32 bits">>, StateData);
handle_peer_stop_sending(StreamId, Code, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case {has_local_send_side(StreamId, StateData),
                  webtransport_stream:peer_stop_sending(Stream, Code)} of
                {true, {ok, Stream1}} ->
                    Stream2 = case webtransport_stream:local_fin(Stream1) orelse
                                   webtransport_stream:reset_code(Stream1) =/= undefined of
                        true ->
                            Stream1;
                        false ->
                            transport_reset_stream(StreamId, {app, Code}, Stream1, StateData),
                            webtransport_stream:reset(Stream1, Code)
                    end,
                    StateData1 = retire_stream(
                                   StreamId, StateData#data{streams = Streams#{StreamId => Stream2}}),
                    notify_stream_closed(StreamId, {stop_sending, Code}, StateData1);
                _ ->
                    fail_session(?WT_STREAM_STATE_ERROR, <<"invalid stop_sending">>, StateData)
            end;
        error ->
            fail_session(?WT_STREAM_STATE_ERROR, <<"stop_sending on an unknown stream">>,
                         StateData)
    end.

%% The peer sends on bidi streams and on uni streams it opened.
has_peer_send_side(StreamId, StateData) ->
    webtransport_stream:stream_type(StreamId) =:= bidi orelse
        not is_local_stream(StreamId, StateData).

%% We send on bidi streams and on uni streams we opened.
has_local_send_side(StreamId, StateData) ->
    webtransport_stream:stream_type(StreamId) =:= bidi orelse
        is_local_stream(StreamId, StateData).

notify_stream_closed(StreamId, Reason, #data{handler = Handler, handler_state = HState} = StateData) ->
    case Handler:handle_stream_closed(StreamId, Reason, HState) of
        {ok, HState1} -> {StateData#data{handler_state = HState1}, continue};
        {stop, StopReason, HState1} -> {StateData#data{handler_state = HState1}, {stop, StopReason}}
    end.

%% A session error (a flow-control or stream-state violation by the peer):
%% send CLOSE_SESSION and stop.
stop_with_session_error(Code, Reason, StateData) ->
    {StateData1, Stop} = fail_session(Code, Reason, StateData),
    {stop, element(2, Stop), StateData1}.

%% On h3 the session closes with WT_CLOSE_SESSION carrying `Code'. On h2
%% (http2-16 §3.4) WT_CLOSE_SESSION is followed by RST_STREAM on the CONNECT
%% stream. The HTTP/2 WebTransport error codes are not assigned yet, so
%% stream-state and generic errors carry code 0 in WT_CLOSE_SESSION and
%% PROTOCOL_ERROR on RST_STREAM, and flow-control errors carry
%% WT_FLOW_CONTROL_ERROR and FLOW_CONTROL_ERROR.
fail_session(Code, Reason, #data{transport = h2, transport_state = H2State} = StateData) ->
    logger:info("webtransport: h2 session error ~p: ~s", [Code, Reason]),
    {WireCode, H2Code} = case Code of
        ?WT_FLOW_CONTROL_ERROR -> {?WT_FLOW_CONTROL_ERROR, flow_control_error};
        _ -> {0, protocol_error}
    end,
    _ = transport_call(fun() ->
            webtransport_h2:session_error(H2State, WireCode, Reason, H2Code)
        end),
    {StateData#data{close_info = {WireCode, Reason}},
     {stop, {shutdown, {session_error, Code}}}};
fail_session(Code, Reason, StateData) ->
    logger:info("webtransport: session error ~.16#: ~s", [Code, Reason]),
    do_close(Code, Reason, StateData),
    {StateData#data{close_info = {Code, Reason}},
     {stop, {shutdown, {session_error, Code}}}}.

mark_blocked(Type, #data{blocked = Blocked} = StateData) ->
    StateData#data{blocked = Blocked#{Type => true}}.

mark_transport_blocked(Type, #data{transport_blocked = Blocked} = StateData) ->
    StateData#data{transport_blocked = Blocked#{Type => true}}.

%% quic says a stream of this type can open again. Tell the handler if an
%% open was refused for it, unless the session's own limit still holds; that
%% one is told when the peer raises it.
on_transport_streams_available(Type, #data{transport_blocked = Blocked} = StateData) ->
    case maps:take(Type, Blocked) of
        error ->
            {StateData, continue};
        {_, Blocked1} ->
            StateData1 = StateData#data{transport_blocked = Blocked1},
            case streams_available(Type, StateData1) > 0 of
                true -> notify_stream_credit([Type], StateData1, continue);
                false -> {mark_blocked(Type, StateData1), continue}
            end
    end.

%% overview-14 §4.3: tell the handler when stream credit comes back for a
%% type it failed to open, through the optional handle_stream_credit/2.
notify_stream_credit(#data{blocked = Blocked} = StateData) when map_size(Blocked) =:= 0 ->
    {StateData, continue};
notify_stream_credit(#data{blocked = Blocked} = StateData) ->
    Ready = [T || T <- maps:keys(Blocked), streams_available(T, StateData) > 0],
    notify_stream_credit(Ready, StateData#data{blocked = maps:without(Ready, Blocked)},
                         continue).

notify_stream_credit([], StateData, Transition) ->
    {StateData, Transition};
notify_stream_credit([Type | Rest], #data{handler = Handler, handler_state = HState} = StateData,
                     Transition) ->
    case erlang:function_exported(Handler, handle_stream_credit, 2) of
        false ->
            notify_stream_credit(Rest, StateData, Transition);
        true ->
            {StateData1, T1} =
                case Handler:handle_stream_credit(Type, HState) of
                    {ok, HState1} ->
                        {StateData#data{handler_state = HState1}, continue};
                    {ok, HState1, Actions} ->
                        handle_actions(Actions, StateData#data{handler_state = HState1});
                    {stop, Reason, HState1} ->
                        {StateData#data{handler_state = HState1}, {stop, Reason}}
                end,
            notify_stream_credit(Rest, StateData1, strongest_transition(Transition, T1))
    end.

streams_available(bidi, #data{remote_max_streams_bidi = L, opened_bidi = O}) -> max(L - O, 0);
streams_available(uni, #data{remote_max_streams_uni = L, opened_uni = O}) -> max(L - O, 0).

%% Translate a `handle_actions' transition into a gen_statem return while
%% in the `open' state.
open_apply_transition(continue, StateData) ->
    {keep_state, StateData};
open_apply_transition(drain, StateData) ->
    enter_draining(StateData);
open_apply_transition({stop, Reason}, StateData) ->
    {stop, Reason, StateData}.

%% Peer-sent control capsules influence the state machine.
%% draft-14 §4.6 / draft-15 §5: CLOSE_SESSION is terminal.
%% draft-14 §4.7 / draft-15 §5.1: DRAIN_SESSION moves us to draining.
apply_capsule_transition({close_session, _Code, _Reason}, StateData) ->
    %% Reset all live streams with WT_SESSION_GONE, close our side of the
    %% CONNECT stream (http2-16 §6.12 requires END_STREAM), then stop.
    reset_all_streams(StateData),
    finish_connect(StateData),
    {stop, normal, StateData};
apply_capsule_transition({drain_session}, StateData) ->
    enter_draining(StateData);
apply_capsule_transition(_Capsule, StateData) ->
    {keep_state, StateData}.

%% Connecting state (for client sessions)
connecting({call, From}, _, StateData) ->
    {keep_state, StateData, [{reply, From, {error, not_connected}}]};

connecting(cast, connected, StateData) ->
    {next_state, open, StateData};

connecting(cast, {connection_error, Reason}, StateData) ->
    {stop, {connection_error, Reason}, StateData}.

%% Draining state - no new streams, waiting for existing to finish
draining({call, From}, {send, StreamId, Data, Fin}, StateData) ->
    %% Allow sending on existing streams during drain
    case do_send(StreamId, Data, Fin, StateData) of
        {ok, StateData1} ->
            maybe_stop_if_drained(StateData1, [{reply, From, ok}]);
        {error, Reason} ->
            {keep_state, StateData, [{reply, From, {error, Reason}}]}
    end;

draining({call, From}, {open_stream, _Type}, StateData) ->
    {keep_state, StateData, [{reply, From, {error, session_draining}}]};

draining({call, From}, get_info, StateData) ->
    Info = build_info(StateData),
    {keep_state, StateData, [{reply, From, {ok, Info}}]};

draining({call, From}, early_data_accepted, StateData) ->
    {keep_state, StateData, [{reply, From, do_early_data_accepted(StateData)}]};

%% Existing streams can still be finished, reset or stopped while
%% draining, and datagrams still flow (http3-17 §4.7).
draining({call, _From} = Call, {Op, _, _} = Req, StateData)
  when Op =:= reset_stream; Op =:= stop_sending ->
    after_draining(open(Call, Req, StateData));
draining({call, _From} = Call, {close_stream, _} = Req, StateData) ->
    after_draining(open(Call, Req, StateData));
draining({call, _From} = Call, {send_datagram, _} = Req, StateData) ->
    after_draining(open(Call, Req, StateData));
draining({call, From}, _, StateData) ->
    {keep_state, StateData, [{reply, From, {error, session_draining}}]};

draining(cast, {stream_data, StreamId, Data, Fin}, StateData) ->
    {StateData1, Transition} = handle_incoming_stream_data(StreamId, Data, Fin, StateData),
    draining_apply_transition(Transition, StateData1);

draining(cast, {stream_closed, StreamId, Reason}, StateData) ->
    {StateData1, Transition} = handle_remote_stream_closed(StreamId, Reason, StateData),
    draining_apply_transition(Transition, StateData1);

draining(cast, {capsule, Capsule}, StateData) ->
    after_draining(apply_capsule(Capsule, StateData));
draining(cast, {close, ErrorCode, Reason}, StateData) ->
    do_close(ErrorCode, Reason, StateData),
    {stop, normal, StateData#data{close_info = {ErrorCode, Reason}}};
draining(cast, {stream_opened, StreamId, Type}, StateData) ->
    {StateData1, Transition} = handle_remote_stream_opened(StreamId, Type, StateData),
    draining_apply_transition(Transition, StateData1);
draining(cast, {datagram_data, Data}, StateData) ->
    {StateData1, Transition} = handle_incoming_datagram(Data, StateData),
    draining_apply_transition(Transition, StateData1);
draining(cast, {established, #{peer_settings := PeerSettings}},
         #data{transport = h2, webtransport_init = Init} = StateData) ->
    draining(cast, established, apply_peer_limits(PeerSettings, Init, StateData));
draining(cast, {established, _}, StateData) ->
    draining(cast, established, StateData);
draining(cast, established, #data{pending_actions = Actions} = StateData) ->
    {StateData1, Transition} = handle_actions(Actions, StateData#data{pending_actions = []}),
    draining_apply_transition(Transition, StateData1);
draining(cast, _, StateData) ->
    maybe_stop_if_drained(StateData, []);
draining(info, Msg, StateData) ->
    %% Same handling as `open' (CONNECT stream, handler messages); a result
    %% that asks to go back to open stays draining.
    after_draining(open(info, Msg, StateData)).

after_draining({keep_state, StateData}) ->
    maybe_stop_if_drained(StateData, []);
after_draining({keep_state, StateData, Actions}) ->
    maybe_stop_if_drained(StateData, Actions);
after_draining({next_state, _, StateData}) ->
    maybe_stop_if_drained(StateData, []);
after_draining(Other) ->
    Other.

%% In the draining state we never go backwards: drain stays drain,
%% a `stop' transition takes immediate effect, and a `continue' just
%% re-evaluates whether all streams are closed.
draining_apply_transition({stop, Reason}, StateData) ->
    {stop, Reason, StateData};
draining_apply_transition(_Transition, StateData) ->
    maybe_stop_if_drained(StateData, []).

terminate(Reason, _State, #data{close_info = undefined} = StateData) ->
    %% The session ends without a close having been sent or received (a
    %% handler stop, a crash, a finished drain): tell the peer, so its side
    %% of the session does not hang (http3-17 §6, http2-16 §3.4).
    try do_close(0, <<>>, StateData) catch _:_ -> ok end,
    terminate_session(Reason, StateData);
terminate(Reason, _State, StateData) ->
    terminate_session(Reason, StateData).

terminate_session(Reason, #data{handler = Handler, handler_state = HState,
                                close_info = CloseInfo,
                                transport = Transport,
                                transport_state = TransportState,
                                is_server = IsServer,
                                peer_closed = PeerClosed}) ->
    %% Close the underlying connection gracefully so quic_h3_connection
    %% enters its `closing' state and exits with `normal' instead of
    %% `quic_closed'. Without this, the QUIC connection drop races with
    %% our process exit and produces noisy crash reports.
    case Transport of
        h3 -> close_transport(h3, TransportState, PeerClosed);
        h2 -> close_transport(h2, TransportState, IsServer)
    end,
    %% Surface close_info to the handler so it can distinguish a clean
    %% local/remote close (with error code + reason) from an abnormal exit.
    Handler:terminate(augment_reason(Reason, CloseInfo), HState),
    ok.

%% Close our side of the CONNECT stream once the peer ended the session.
%% On h3 our WT_CLOSE_SESSION is not sent in that case and the stream is
%% finished with an empty FIN; on h2 END_STREAM is required (http2-16
%% §6.12).
finish_connect(#data{transport = h2, transport_state = H2State}) ->
    try webtransport_h2:finish(H2State) catch _:_ -> ok end;
finish_connect(#data{transport = h3, transport_state = H3State}) ->
    try quic_h3:send_data(webtransport_h3:h3_conn(H3State),
                          webtransport_h3:session_id(H3State), <<>>, true)
    catch _:_ -> ok
    end.

%% h3: the router learns the session ended. A client closes its connection
%% there once no session is left and the server closed the CONNECT stream
%% (http3-17 §6); a server leaves the connection open, as it can carry other
%% requests.
close_transport(h3, H3State, PeerClosed) ->
    case webtransport_h3:router(H3State) of
        undefined ->
            ok;
        Router ->
            try webtransport_h3_router:session_ended(Router, webtransport_h3:session_id(H3State),
                                                     PeerClosed)
            catch _:_ -> ok
            end
    end;
%% A client owns its h2 connection, one per session: close it. A server's
%% h2 connection carries other requests and is left to the client.
close_transport(h2, H2State, false) ->
    try h2:close(webtransport_h2:h2_conn(H2State)) catch _:_ -> ok end;
close_transport(h2, _H2State, true) ->
    ok.

augment_reason(Reason, undefined) -> Reason;
augment_reason(normal, {Code, Msg}) -> {closed, Code, Msg};
augment_reason(Reason, {Code, Msg}) -> {Reason, {closed, Code, Msg}}.

%% ============================================================================
%% Internal functions
%% ============================================================================

%% Per-stream WebTransport windows only exist on h2 (http2-16 §6). On h3
%% the QUIC transport does per-stream flow control.

%% Send and receive windows of a new stream. h3 leaves per-stream flow
%% control to QUIC. On h2 the send window is what the peer granted for this
%% kind of stream (http2-16 §4.3) and the receive window is ours.
-spec stream_windows(local | peer, bidi | uni, #data{}) ->
    {non_neg_integer(), non_neg_integer()}.
stream_windows(_Who, _Type, #data{transport = h3}) ->
    {?UNLIMITED, ?UNLIMITED};
stream_windows(local, uni, #data{peer_stream_data = P}) ->
    {maps:get(u, P, 0), 0};
stream_windows(local, bidi, #data{peer_stream_data = P, local_max_stream_data = L}) ->
    {maps:get(br, P, 0), L};
stream_windows(peer, uni, #data{local_max_stream_data = L}) ->
    {0, L};
stream_windows(peer, bidi, #data{peer_stream_data = P, local_max_stream_data = L}) ->
    {maps:get(bl, P, 0), L}.

new_stream(StreamId, Who, Type, StateData) ->
    {Send, Recv} = stream_windows(Who, Type, StateData),
    webtransport_stream:new(StreamId, Type, Send, Recv).

do_send(StreamId, Data, Fin, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} -> send_limited(StreamId, Data, Fin, Stream, StateData);
        error -> {error, unknown_stream}
    end.

%% Honour the session window (both transports) and the per-stream window
%% (h2; unbounded on h3, where QUIC does it). Bytes that do not fit are
%% queued on the stream, in order, and go out from flush_streams/1 when the
%% peer raises a limit. A FIN is held back until the queue drains, so the
%% peer never sees FIN before the last byte.
send_limited(StreamId, Data, Fin, Stream, #data{streams = Streams} = StateData) ->
    case webtransport_stream:send_buffer(Stream) of
        <<>> ->
            push_limited(StreamId, Data, Fin, Stream, StateData);
        _ ->
            %% Earlier bytes are still waiting for credit: queue behind them.
            case webtransport_stream:is_writable(Stream) andalso
                 not webtransport_stream:fin_pending(Stream) of
                true ->
                    Stream1 = webtransport_stream:buffer_send(Stream, Data),
                    Stream2 = maybe_set_fin_pending(Fin, Stream1),
                    {ok, StateData#data{streams = Streams#{StreamId => Stream2}}};
                false ->
                    {error, stream_fin_sent}
            end
    end.

push_limited(StreamId, Data, Fin, Stream,
             #data{streams = Streams, bytes_sent = SessSent,
                   remote_max_data = SessMax} = StateData) ->
    SessAvail = max(SessMax - SessSent, 0),
    {Now, Later} = split_binary_at(Data, SessAvail),
    case webtransport_stream:send(Stream, Now) of
        {ok, ToSend, Stream1} ->
            Stream2 = webtransport_stream:buffer_send(Stream1, Later),
            Drained = webtransport_stream:send_buffer(Stream2) =:= <<>>,
            FinNow = Fin andalso Drained,
            Stream3 = maybe_set_fin_pending(Fin andalso not Drained, Stream2),
            case ToSend =:= <<>> andalso not FinNow of
                true ->
                    %% Nothing fits right now; tell the peer we are blocked.
                    StateData1 = StateData#data{streams = Streams#{StreamId => Stream3}},
                    emit_blocked(StreamId, Stream3, SessAvail, StateData1),
                    {ok, StateData1};
                false ->
                    case transport_send(StreamId, ToSend, FinNow, StateData) of
                        ok ->
                            Stream4 = maybe_close_local(FinNow, Stream3),
                            StateData1 = StateData#data{
                                streams = Streams#{StreamId => Stream4},
                                bytes_sent = SessSent + byte_size(ToSend)},
                            case Drained of
                                true -> ok;
                                false -> emit_blocked(StreamId, Stream4,
                                                      SessAvail - byte_size(ToSend),
                                                      StateData1)
                            end,
                            {ok, StateData1};
                        {error, _} = Err ->
                            Err
                    end
            end;
        {error, _} = Err ->
            Err
    end.

%% Retry queued bytes on every stream after the peer raised a limit.
-spec flush_streams(#data{}) -> #data{}.
flush_streams(#data{streams = Streams} = StateData) ->
    maps:fold(fun flush_stream/3, StateData, Streams).

flush_stream(StreamId, Stream, #data{streams = Streams} = StateData) ->
    case webtransport_stream:send_buffer(Stream) of
        <<>> ->
            StateData;
        Buf ->
            {_, Stream0} = webtransport_stream:flush_send_buffer(Stream),
            Fin = webtransport_stream:fin_pending(Stream0),
            Stream1 = webtransport_stream:clear_fin_pending(Stream0),
            StateData1 = StateData#data{streams = Streams#{StreamId => Stream1}},
            case push_limited(StreamId, Buf, Fin, Stream1, StateData1) of
                {ok, StateData2} ->
                    StateData2;
                {error, Reason} ->
                    logger:warning("webtransport: flush of stream ~p failed: ~p",
                                   [StreamId, Reason]),
                    StateData
            end
    end.

maybe_close_local(true, Stream) ->
    {ok, S} = webtransport_stream:close_local(Stream),
    S;
maybe_close_local(false, Stream) ->
    Stream.

maybe_set_fin_pending(true, Stream) -> webtransport_stream:set_fin_pending(Stream);
maybe_set_fin_pending(false, Stream) -> Stream.

split_binary_at(Bin, At) when At >= byte_size(Bin) ->
    {Bin, <<>>};
split_binary_at(Bin, At) ->
    <<Now:At/binary, Later/binary>> = Bin,
    {Now, Later}.

%% Tell the peer we are blocked: WT_DATA_BLOCKED on both transports
%% (http3-17 §5.6.5, http2-16 §6.8), WT_STREAM_DATA_BLOCKED on h2 only.
emit_blocked(StreamId, Stream, SessAvail, StateData) ->
    case SessAvail =< 0 of
        true -> send_flow_capsule({data_blocked, StateData#data.remote_max_data}, StateData);
        false -> ok
    end,
    maybe_emit_stream_data_blocked(StreamId, Stream, StateData).

maybe_emit_stream_data_blocked(StreamId, Stream, #data{transport = h2} = StateData) ->
    Window = webtransport_stream:send_window(Stream),
    case Window - webtransport_stream:bytes_sent(Stream) of
        0 -> send_flow_capsule({stream_data_blocked, StreamId, Window}, StateData);
        _ -> ok
    end;
maybe_emit_stream_data_blocked(_StreamId, _Stream, _StateData) ->
    ok.

%% Send a flow-control capsule on the CONNECT stream, unless flow control
%% is off for this session.
send_flow_capsule(_Capsule, #data{flow_control = false}) ->
    ok;
send_flow_capsule(Capsule, #data{transport = h2, transport_state = H2State}) ->
    _ = webtransport_h2:send_capsule(H2State, Capsule),
    ok;
send_flow_capsule(Capsule, #data{transport = h3, transport_state = H3State}) ->
    _ = transport_call(fun() -> webtransport_h3:send_capsule(H3State, Capsule) end),
    ok.

do_send_datagram(Data, StateData) ->
    case transport_send_datagram(Data, StateData) of
        ok -> {ok, StateData};
        {error, _} = Err -> Err
    end.

%% Stream limits count every stream opened in the session, open or not
%% (http3-17 §5.6.2, http2-16 §6.7). At the limit we send
%% WT_STREAMS_BLOCKED and remember it, so the handler can be told when the
%% peer grants more.
do_open_stream(Type, #data{streams = Streams} = StateData) ->
    {Opened, Limit, NextId} = open_counters(Type, StateData),
    case Opened < Limit of
        true ->
            case transport_open_stream(NextId, Type, StateData) of
                {ok, StreamId} ->
                    Stream = new_stream(StreamId, local, Type, StateData),
                    StateData1 = bump_opened(Type, StateData#data{
                        streams = Streams#{StreamId => Stream}}),
                    {ok, StreamId, StateData1};
                {error, _} = Err ->
                    Err
            end;
        false ->
            send_flow_capsule(streams_blocked(Type, Limit), StateData),
            {error, stream_limit_reached}
    end.

open_counters(bidi, #data{opened_bidi = O, remote_max_streams_bidi = L, next_bidi_id = N}) ->
    {O, L, N};
open_counters(uni, #data{opened_uni = O, remote_max_streams_uni = L, next_uni_id = N}) ->
    {O, L, N}.

bump_opened(bidi, #data{opened_bidi = O, next_bidi_id = N} = D) ->
    D#data{opened_bidi = O + 1, next_bidi_id = N + 4};
bump_opened(uni, #data{opened_uni = O, next_uni_id = N} = D) ->
    D#data{opened_uni = O + 1, next_uni_id = N + 4}.

streams_blocked(bidi, Limit) -> {streams_blocked_bidi, Limit};
streams_blocked(uni, Limit) -> {streams_blocked_uni, Limit}.

do_close_stream(StreamId, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case webtransport_stream:send_buffer(Stream) of
                <<>> -> close_stream_now(StreamId, Stream, StateData);
                _ -> close_stream_after_queue(StreamId, Stream, StateData)
            end;
        error ->
            {error, unknown_stream}
    end.

%% Bytes are still waiting for credit: the FIN goes out after them, from
%% push_limited/5, so the peer gets the whole stream.
close_stream_after_queue(StreamId, Stream, #data{streams = Streams} = StateData) ->
    case webtransport_stream:fin_pending(Stream) of
        true ->
            {error, stream_fin_sent};
        false ->
            Stream1 = webtransport_stream:set_fin_pending(Stream),
            {ok, StateData#data{streams = Streams#{StreamId => Stream1}}}
    end.

close_stream_now(StreamId, Stream, #data{streams = Streams} = StateData) ->
    case webtransport_stream:close_local(Stream) of
        {ok, Stream1} ->
            transport_send(StreamId, <<>>, true, StateData),
            {ok, retire_stream(StreamId, StateData#data{streams = Streams#{StreamId => Stream1}})};
        {error, _} = Err ->
            Err
    end.

%% http2-16 §6.2, §6.3: no reset once our send side has ended, and at most
%% one STOP_SENDING per stream.
do_reset_stream(_StreamId, ErrorCode, _StateData) when ErrorCode > 16#ffffffff ->
    %% WebTransport application error codes are 32 bits (http3-17 §4.4).
    {error, invalid_error_code};
do_reset_stream(StreamId, ErrorCode, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case webtransport_stream:local_fin(Stream) orelse
                 webtransport_stream:reset_code(Stream) =/= undefined orelse
                 not has_local_send_side(StreamId, StateData) of
                true ->
                    {error, stream_send_side_closed};
                false ->
                    Stream1 = webtransport_stream:reset(Stream, ErrorCode),
                    transport_reset_stream(StreamId, {app, ErrorCode}, Stream, StateData),
                    {ok, retire_stream(
                           StreamId, StateData#data{streams = Streams#{StreamId => Stream1}})}
            end;
        error ->
            {error, unknown_stream}
    end.

do_stop_sending(_StreamId, ErrorCode, _StateData) when ErrorCode > 16#ffffffff ->
    {error, invalid_error_code};
do_stop_sending(StreamId, ErrorCode, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case webtransport_stream:is_readable(Stream) andalso
                 has_peer_send_side(StreamId, StateData) of
                false ->
                    {error, stream_receive_side_closed};
                true ->
                    Stream1 = webtransport_stream:stop_sending(Stream, ErrorCode),
                    transport_stop_sending(StreamId, {app, ErrorCode}, StateData),
                    {ok, StateData#data{streams = Streams#{StreamId => Stream1}}}
            end;
        error ->
            {error, unknown_stream}
    end.

do_drain(#data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:drain_session(H2State);
do_drain(#data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:drain_session(H3State).

do_close(ErrorCode0, Reason0, StateData) ->
    %% Reset all live streams with WT_SESSION_GONE, then send CLOSE_SESSION.
    %% The transport may already be gone (connection lost, peer closed
    %% first); closing is then a no-op, not a crash of the session. A code or
    %% reason the capsule cannot carry is replaced, so a close always goes out.
    ErrorCode = case is_integer(ErrorCode0) andalso ErrorCode0 =< 16#ffffffff of
        true -> ErrorCode0;
        false -> 0
    end,
    Reason = case wt_h3_capsule:valid_close_reason(Reason0) of
        true -> Reason0;
        false -> <<>>
    end,
    reset_all_streams(StateData),
    transport_call(fun() ->
        case StateData of
            #data{transport = h2, transport_state = H2State} ->
                webtransport_h2:close_session(H2State, ErrorCode, Reason);
            #data{transport = h3, transport_state = H3State} ->
                webtransport_h3:close_session(H3State, ErrorCode, Reason)
        end
    end).

%% http3-17 §6: on termination every stream of the session has its send side
%% reset and its receive side aborted with WT_SESSION_GONE, a protocol code
%% sent as is. On h2 the streams end with the CONNECT stream.
reset_all_streams(#data{streams = Streams} = StateData) ->
    maps:foreach(fun(StreamId, Stream) ->
        case webtransport_stream:is_writable(Stream) andalso
             has_local_send_side(StreamId, StateData) of
            true ->
                transport_call(fun() ->
                    transport_reset_stream(StreamId, {wt, ?WT_SESSION_GONE},
                                           Stream, StateData)
                end);
            false ->
                ok
        end,
        case StateData#data.transport =:= h3 andalso
             webtransport_stream:is_readable(Stream) andalso
             has_peer_send_side(StreamId, StateData) of
            true ->
                transport_call(fun() ->
                    transport_stop_sending(StreamId, {wt, ?WT_SESSION_GONE}, StateData)
                end);
            false ->
                ok
        end
    end, Streams).

%% A call into a transport process that has already exited.
transport_call(Fun) ->
    try
        Fun()
    catch
        exit:{noproc, _} -> {error, closed};
        exit:{normal, _} -> {error, closed};
        exit:{shutdown, _} -> {error, closed};
        exit:{{shutdown, _}, _} -> {error, closed}
    end.

%% http3-17 §5.1: without flow control, flow-control capsules are ignored.
%% WT_MAX_STREAM_DATA and WT_STREAM_DATA_BLOCKED are not among them on h3
%% (§5.6): receiving one stays an error (§5.4).
handle_incoming_capsule(Capsule, #data{flow_control = false} = StateData)
  when element(1, Capsule) =:= max_data;
       element(1, Capsule) =:= max_streams_bidi; element(1, Capsule) =:= max_streams_uni;
       element(1, Capsule) =:= data_blocked;
       element(1, Capsule) =:= streams_blocked_bidi;
       element(1, Capsule) =:= streams_blocked_uni ->
    StateData;
%% Every limit must strictly increase over the previous or initial value
%% (http3-17 §5.6.2, §5.6.4; http2-16 §6.5-§6.7).
handle_incoming_capsule({max_data, Limit}, #data{remote_max_data = Prev} = StateData) ->
    case Limit =< Prev of
        true ->
            {session_error, ?WT_FLOW_CONTROL_ERROR, <<"max_data did not increase">>};
        false ->
            flush_streams(StateData#data{remote_max_data = Limit})
    end;
handle_incoming_capsule({max_stream_data, _StreamId, _Limit},
                        #data{transport = h3} = _StateData) ->
    %% http3-17 §5.4: no per-stream flow-control capsules on h3.
    {session_error, ?WT_FLOW_CONTROL_ERROR,
     <<"h3 prohibits per-stream flow control capsules">>};
handle_incoming_capsule({max_stream_data, StreamId, Limit},
                        #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            ValidState = has_local_send_side(StreamId, StateData) andalso
                webtransport_stream:peer_stop_sending_code(Stream) =:= undefined,
            case Limit =< webtransport_stream:send_window(Stream) of
                _ when not ValidState ->
                    %% http2-16 §6.6: not after the peer's STOP_SENDING, and
                    %% only for a stream we send on.
                    {session_error, ?WT_STREAM_STATE_ERROR,
                     <<"max_stream_data on a stream we do not send on">>};
                true ->
                    {session_error, ?WT_FLOW_CONTROL_ERROR,
                     <<"max_stream_data did not increase">>};
                false ->
                    Stream1 = webtransport_stream:update_send_window(Stream, Limit),
                    flush_streams(StateData#data{streams = Streams#{StreamId => Stream1}})
            end;
        error ->
            {session_error, ?WT_STREAM_STATE_ERROR, <<"max_stream_data on an unknown stream">>}
    end;
handle_incoming_capsule({max_streams_bidi, Limit}, _StateData) when Limit > ?MAX_STREAMS ->
    {session_error, ?WT_FLOW_CONTROL_ERROR, <<"max_streams above 2^60">>};
handle_incoming_capsule({max_streams_uni, Limit}, _StateData) when Limit > ?MAX_STREAMS ->
    {session_error, ?WT_FLOW_CONTROL_ERROR, <<"max_streams above 2^60">>};
handle_incoming_capsule({max_streams_bidi, Limit}, #data{remote_max_streams_bidi = Prev} = StateData) ->
    case Limit =< Prev of
        true -> {session_error, ?WT_FLOW_CONTROL_ERROR, <<"max_streams_bidi did not increase">>};
        false -> StateData#data{remote_max_streams_bidi = Limit}
    end;
handle_incoming_capsule({max_streams_uni, Limit}, #data{remote_max_streams_uni = Prev} = StateData) ->
    case Limit =< Prev of
        true -> {session_error, ?WT_FLOW_CONTROL_ERROR, <<"max_streams_uni did not increase">>};
        false -> StateData#data{remote_max_streams_uni = Limit}
    end;
handle_incoming_capsule({data_blocked, Limit}, StateData) ->
    logger:debug("peer data_blocked at ~p", [Limit]),
    StateData;
handle_incoming_capsule({stream_data_blocked, _StreamId, _Limit},
                        #data{transport = h3} = _StateData) ->
    %% http3-17 §5.4: no per-stream flow-control capsules on h3.
    {session_error, ?WT_FLOW_CONTROL_ERROR,
     <<"h3 prohibits per-stream flow control capsules">>};
handle_incoming_capsule({stream_data_blocked, StreamId, Limit},
                        #data{streams = Streams} = StateData) ->
    %% http2-16 §6.9: only for a stream the peer is still sending on.
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case has_peer_send_side(StreamId, StateData) andalso
                 not webtransport_stream:remote_fin(Stream) andalso
                 webtransport_stream:remote_reset_code(Stream) =:= undefined of
                true ->
                    logger:debug("peer stream_data_blocked stream=~p limit=~p",
                                 [StreamId, Limit]),
                    StateData;
                false ->
                    {session_error, ?WT_STREAM_STATE_ERROR,
                     <<"stream_data_blocked on a finished stream">>}
            end;
        error ->
            {session_error, ?WT_STREAM_STATE_ERROR, <<"stream_data_blocked on an unknown stream">>}
    end;
handle_incoming_capsule({streams_blocked_bidi, Limit}, _StateData) when Limit > ?MAX_STREAMS ->
    {session_error, ?WT_FLOW_CONTROL_ERROR, <<"streams_blocked above 2^60">>};
handle_incoming_capsule({streams_blocked_uni, Limit}, _StateData) when Limit > ?MAX_STREAMS ->
    {session_error, ?WT_FLOW_CONTROL_ERROR, <<"streams_blocked above 2^60">>};
handle_incoming_capsule({streams_blocked_bidi, Limit}, StateData) ->
    logger:debug("peer streams_blocked_bidi at ~p", [Limit]),
    StateData;
handle_incoming_capsule({streams_blocked_uni, Limit}, StateData) ->
    logger:debug("peer streams_blocked_uni at ~p", [Limit]),
    StateData;
handle_incoming_capsule({padding, _}, StateData) ->
    StateData;
handle_incoming_capsule({close_session, ErrorCode, Reason}, #data{} = StateData) ->
    StateData#data{close_info = {ErrorCode, Reason}, peer_closed = true};
handle_incoming_capsule({drain_session}, #data{} = StateData) ->
    StateData;
handle_incoming_capsule({unknown, Type, _Payload}, StateData) ->
    %% RFC 9297 §3.2: an unknown capsule type MUST be silently dropped.
    logger:debug("webtransport: dropping unknown capsule type ~p", [Type]),
    StateData.

handle_incoming_stream_data(StreamId, Data, Fin, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, _Stream} when StateData#data.transport =:= h2 ->
            case has_peer_send_side(StreamId, StateData) of
                true -> receive_stream_data(StreamId, Data, Fin, StateData);
                false ->
                    %% http2-16 §6.4: data on a uni stream only we send on.
                    fail_session(?WT_STREAM_STATE_ERROR, <<"data on a stream the peer cannot send on">>,
                                 StateData)
            end;
        {ok, _Stream} ->
            receive_stream_data(StreamId, Data, Fin, StateData);
        error when StateData#data.transport =:= h2 ->
            case is_retired(StreamId, StateData) of
                true ->
                    %% http2-16 §6.4: data on a finished stream.
                    fail_session(?WT_STREAM_STATE_ERROR, <<"data on a finished stream">>, StateData);
                false ->
                    {StateData, continue}
            end;
        error ->
            {StateData, continue}
    end.

receive_stream_data(StreamId, Data, Fin,
                    #data{streams = Streams, handler = Handler,
                          handler_state = HState,
                          bytes_received = SessRecv} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            Type = webtransport_stream:type(Stream),
            SessRecv1 = SessRecv + byte_size(Data),
            case receive_into(Stream, Data, SessRecv1, StateData) of
                {ok, Stream1} ->
                    Stream2 = case Fin of
                        true ->
                            {ok, S} = webtransport_stream:receive_fin(Stream1),
                            S;
                        false ->
                            Stream1
                    end,
                    Callback = case Fin andalso erlang:function_exported(Handler, handle_stream_fin, 4) of
                        true -> fun() -> Handler:handle_stream_fin(StreamId, Type, Data, HState) end;
                        false -> fun() -> Handler:handle_stream(StreamId, Type, Data, HState) end
                    end,
                    StateData1 = StateData#data{bytes_received = SessRecv1},
                    %% The handler consumes the bytes synchronously, so credit
                    %% can be returned to the peer right away.
                    {Stream3, StateData2} = grant_credit(StreamId, Stream2, StateData1),
                    {StateData3, Transition} =
                        apply_stream_callback(Callback(), StreamId, Stream3, Streams, StateData2),
                    {retire_stream(StreamId, StateData3), Transition};
                {error, flow_control} ->
                    %% http3-17 §5.4, http2-16 §6.5/§6.6: data beyond what we
                    %% granted closes the session.
                    fail_session(?WT_FLOW_CONTROL_ERROR, <<"flow control limit exceeded">>,
                                 StateData);
                {error, Reason} when StateData#data.transport =:= h2 ->
                    %% http2-16 §6.4: WT_STREAM on a stream whose peer side
                    %% already ended is a WT_STREAM_STATE_ERROR.
                    fail_session(?WT_STREAM_STATE_ERROR,
                                 iolist_to_binary(io_lib:format("data on stream ~p: ~p",
                                                                [StreamId, Reason])),
                                 StateData);
                {error, Reason} ->
                    logger:warning("webtransport: dropping ~p bytes on stream ~p: ~p",
                                   [byte_size(Data), StreamId, Reason]),
                    {StateData, continue}
            end;
        error ->
            {StateData, continue}
    end.

%% Account received bytes against the session limit and, on h2, the
%% stream window.
receive_into(_Stream, _Data, SessRecv, #data{flow_control = true, local_max_data = Max})
  when SessRecv > Max ->
    {error, flow_control};
receive_into(Stream, Data, _SessRecv, #data{flow_control = FlowControl}) ->
    case webtransport_stream:remote_reset_code(Stream) of
        undefined -> receive_data(Stream, Data, FlowControl);
        _ -> {error, stream_reset}
    end.

receive_data(Stream, Data, FlowControl) ->
    case webtransport_stream:receive_data(Stream, Data) of
        {error, flow_control_error} when FlowControl -> {error, flow_control};
        Other -> Other
    end.

%% Receive-side credit (http3-17 §5.6, http2-16 §4.4): once half of a window
%% has been consumed, advertise a new limit so the peer can keep sending.
%% WT_MAX_DATA on both transports; WT_MAX_STREAM_DATA on h2 only.
-spec grant_credit(non_neg_integer(), webtransport_stream:stream(), #data{}) ->
    {webtransport_stream:stream(), #data{}}.
grant_credit(_StreamId, Stream, #data{flow_control = false} = StateData) ->
    {Stream, StateData};
grant_credit(StreamId, Stream,
             #data{bytes_received = SessRecv, local_max_data = LocalMax,
                   session_window = SessWindow} = StateData) ->
    Stream1 = grant_stream_credit(StreamId, Stream, StateData),
    StateData1 =
        case SessRecv >= LocalMax - (SessWindow div 2) of
            true ->
                NewMax = SessRecv + SessWindow,
                send_flow_capsule({max_data, NewMax}, StateData),
                StateData#data{local_max_data = NewMax};
            false ->
                StateData
        end,
    {Stream1, StateData1}.

grant_stream_credit(StreamId, Stream, #data{transport = h2,
                                            local_max_stream_data = StreamWindow} = StateData) ->
    Window = webtransport_stream:recv_window(Stream),
    Received = webtransport_stream:bytes_received(Stream),
    case webtransport_stream:is_readable(Stream) andalso
         Received >= Window - (StreamWindow div 2) of
        true ->
            NewWindow = Received + StreamWindow,
            send_flow_capsule({max_stream_data, StreamId, NewWindow}, StateData),
            webtransport_stream:update_recv_window(Stream, NewWindow);
        false ->
            Stream
    end;
grant_stream_credit(_StreamId, Stream, _StateData) ->
    Stream.

apply_stream_callback({ok, HState1}, StreamId, Stream2, Streams, StateData) ->
    {StateData#data{streams = Streams#{StreamId => Stream2},
                    handler_state = HState1},
     continue};
apply_stream_callback({ok, HState1, Actions}, StreamId, Stream2, Streams, StateData) ->
    StateData1 = StateData#data{streams = Streams#{StreamId => Stream2},
                                handler_state = HState1},
    handle_actions(Actions, StateData1);
apply_stream_callback({stop, Reason, HState1}, StreamId, Stream2, Streams, StateData) ->
    {StateData#data{streams = Streams#{StreamId => Stream2},
                    handler_state = HState1},
     {stop, Reason}}.

handle_incoming_datagram(Data, #data{handler = Handler, handler_state = HState} = StateData) ->
    case Handler:handle_datagram(Data, HState) of
        {ok, HState1} ->
            {StateData#data{handler_state = HState1}, continue};
        {ok, HState1, Actions} ->
            handle_actions(Actions, StateData#data{handler_state = HState1});
        {stop, Reason, HState1} ->
            {StateData#data{handler_state = HState1}, {stop, Reason}}
    end.

%% A stream opened by the peer. The peer may open as many streams as our
%% cumulative limit; one more is a WT_FLOW_CONTROL_ERROR (http3-17 §5.6.2,
%% http2-16 §6.7). On h2 a stream id implicitly opens every lower id of the
%% same kind, so the count is the id's ordinal; on h3 QUIC stream ids are
%% shared with other uses and each new stream counts one.
handle_remote_stream_opened(StreamId, Type, #data{streams = Streams} = StateData) ->
    case maps:is_key(StreamId, Streams) orelse is_retired(StreamId, StateData) of
        true ->
            {StateData, continue};
        false when StateData#data.transport =:= h2 ->
            case is_local_stream(StreamId, StateData) of
                true ->
                    %% http2-16 §6.4: data on a stream id of ours that we
                    %% never opened.
                    fail_session(?WT_STREAM_STATE_ERROR, <<"data on a stream we never opened">>,
                                 StateData);
                false ->
                    open_peer_stream(StreamId, Type, StateData)
            end;
        false ->
            open_peer_stream(StreamId, Type, StateData)
    end.

open_peer_stream(StreamId, Type, #data{streams = Streams} = StateData) ->
    case peer_stream_allowed(StreamId, Type, StateData) of
        {ok, Opened} ->
            Stream = new_stream(StreamId, peer, Type, StateData),
            StateData1 = set_peer_opened(Type, Opened, StateData),
            {StateData1#data{streams = Streams#{StreamId => Stream}}, continue};
        exceeded ->
            fail_session(?WT_FLOW_CONTROL_ERROR, <<"stream limit exceeded">>, StateData)
    end.

%% How many peer streams of this kind exist once `StreamId' is open, or
%% `exceeded' when that is more than we allow.
peer_stream_allowed(StreamId, Type, StateData) ->
    Opened = peer_opened_after(StreamId, Type, StateData),
    Limit = case Type of
        bidi -> StateData#data.local_max_streams_bidi;
        uni  -> StateData#data.local_max_streams_uni
    end,
    case StateData#data.flow_control andalso Opened > Limit of
        true -> exceeded;
        false -> {ok, Opened}
    end.

peer_opened_after(StreamId, Type, #data{transport = h2} = StateData) ->
    max(StreamId div 4 + 1, peer_opened(Type, StateData));
peer_opened_after(_StreamId, Type, StateData) ->
    peer_opened(Type, StateData) + 1.

peer_opened(bidi, #data{peer_opened_bidi = N}) -> N;
peer_opened(uni, #data{peer_opened_uni = N}) -> N.

set_peer_opened(bidi, N, StateData) -> StateData#data{peer_opened_bidi = N};
set_peer_opened(uni, N, StateData) -> StateData#data{peer_opened_uni = N}.

%% A stream that was opened and has since finished and left the map. On h2
%% stream ids of each kind are used in order, so an id at or below the
%% count opened so far and not in the map is finished. On h3 the router
%% never reports a stream twice.
is_retired(StreamId, #data{transport = h2, streams = Streams} = StateData) ->
    not maps:is_key(StreamId, Streams) andalso
        StreamId div 4 + 1 =< opened_count(StreamId, StateData);
is_retired(_StreamId, _StateData) ->
    false.

opened_count(StreamId, StateData) ->
    case {is_local_stream(StreamId, StateData), webtransport_stream:stream_type(StreamId)} of
        {true, bidi} -> StateData#data.opened_bidi;
        {true, uni} -> StateData#data.opened_uni;
        {false, bidi} -> StateData#data.peer_opened_bidi;
        {false, uni} -> StateData#data.peer_opened_uni
    end.

%% A finished stream leaves the session state, so a peer cannot grow it
%% without bound. A peer-initiated one also gives its credit back: our limit
%% for that kind goes up by one and WT_MAX_STREAMS tells the peer, so it can
%% always have as many streams open as we allow (http3-17 §5.6, http2-16
%% §4.4 SHOULD).
retire_stream(StreamId, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case stream_done(StreamId, Stream, StateData) of
                true ->
                    StateData1 = StateData#data{streams = maps:remove(StreamId, Streams)},
                    case is_local_stream(StreamId, StateData) of
                        true -> StateData1;
                        false -> raise_peer_limit(webtransport_stream:type(Stream), StateData1)
                    end;
                false ->
                    StateData
            end;
        error ->
            StateData
    end.

raise_peer_limit(bidi, #data{local_max_streams_bidi = L} = StateData) ->
    send_flow_capsule({max_streams_bidi, L + 1}, StateData),
    StateData#data{local_max_streams_bidi = L + 1};
raise_peer_limit(uni, #data{local_max_streams_uni = L} = StateData) ->
    send_flow_capsule({max_streams_uni, L + 1}, StateData),
    StateData#data{local_max_streams_uni = L + 1}.

handle_remote_stream_closed(StreamId, Reason,
                             #data{streams = Streams, handler = Handler,
                                   handler_state = HState} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            StateData0 = count_unreceived(StreamId, Stream, Reason, StateData),
            {Stream1, Reason1} = remote_close(StreamId, Stream, strip_final_size(Reason),
                                              StateData0),
            StateData1 = retire_stream(
                           StreamId, StateData0#data{streams = Streams#{StreamId => Stream1}}),
            case Handler:handle_stream_closed(StreamId, Reason1, HState) of
                {ok, HState1} ->
                    {StateData1#data{handler_state = HState1}, continue};
                {stop, StopReason, HState1} ->
                    {StateData1#data{handler_state = HState1}, {stop, StopReason}}
            end;
        error ->
            {StateData, continue}
    end.

%% http3-17 §5.4: bytes the peer sent on a stream before resetting it count
%% against the session data limit even though they never arrived. The reset
%% carries the stream's final size, which includes the stream header the
%% peer wrote on a stream it opened; body bytes are what is left.
count_unreceived(StreamId, Stream, {reset, _Code, FinalSize},
                 #data{transport = h3, transport_state = H3State,
                       bytes_received = SessRecv} = StateData) ->
    Header = case is_local_stream(StreamId, StateData) of
        true -> 0;
        false -> webtransport_h3:stream_header_size(H3State, webtransport_stream:type(Stream))
    end,
    Missing = max(0, FinalSize - Header - webtransport_stream:bytes_received(Stream)),
    StateData1 = StateData#data{bytes_received = SessRecv + Missing},
    {_, StateData2} = grant_credit(StreamId, Stream, StateData1),
    StateData2;
count_unreceived(_StreamId, _Stream, _Reason, StateData) ->
    StateData.

strip_final_size({reset, Code, _FinalSize}) -> {reset, Code};
strip_final_size(Reason) -> Reason.

%% Apply a stream event from the transport to one direction only: a peer
%% reset ends the peer's side, a STOP_SENDING ends ours (the QUIC stack has
%% already answered it with a reset). A stream the peer sends on that
%% closes before its FIN was seen was reset with code 0, which the QUIC
%% stack reports as a close; it reaches the handler as a reset without an
%% application code (http3-17 §4.4).
remote_close(_StreamId, Stream, {reset, Code}, _StateData) ->
    {received_reset(Stream, Code), {reset, Code}};
remote_close(_StreamId, Stream, {stop_sending, Code}, _StateData) ->
    Stream1 = case webtransport_stream:peer_stop_sending(Stream, Code) of
        {ok, S} -> S;
        {error, duplicate} -> Stream
    end,
    {webtransport_stream:reset(Stream1, Code), {stop_sending, Code}};
remote_close(StreamId, Stream, normal, StateData) ->
    case has_peer_send_side(StreamId, StateData) andalso
         webtransport_stream:is_readable(Stream) of
        true -> {webtransport_stream:close(received_reset(Stream, undefined)),
                 {reset, undefined}};
        false -> {webtransport_stream:close(Stream), normal}
    end;
remote_close(_StreamId, Stream, Other, _StateData) ->
    {webtransport_stream:close(Stream), Other}.

received_reset(Stream, Code) ->
    case webtransport_stream:receive_reset(Stream, code_or_zero(Code)) of
        {ok, S} -> S;
        {error, invalid_state} -> Stream
    end.

code_or_zero(undefined) -> 0;
code_or_zero(Code) -> Code.

%% Execute handler-returned actions inline on the session's own state.
%% Runs inside the gen_statem callback so we cannot round-trip through the
%% public API — that would self-call and crash with `calling_self`.
%%
%% Returns `{NewStateData, Transition}' where Transition is:
%%   `continue'          — stay in current state
%%   `drain'             — caller must enter the `draining' state
%%   `{stop, Reason}'    — caller must stop the gen_statem
%% The strongest transition wins (stop > drain > continue) so an action list
%% like `[drain_session, {close_session, 0, <<>>}]' produces `{stop, normal}'.
-spec handle_actions([webtransport_handler:action()], #data{}) ->
    {#data{}, continue | drain | {stop, term()}}.
handle_actions(Actions, StateData) ->
    handle_actions(Actions, StateData, continue).

handle_actions([], StateData, Transition) ->
    {StateData, Transition};
handle_actions([Action | Rest], StateData, Transition) ->
    {StateData1, NewTrans} = dispatch_action(Action, StateData),
    handle_actions(Rest, StateData1, strongest_transition(Transition, NewTrans)).

strongest_transition(continue, New) -> New;
strongest_transition(Old, continue) -> Old;
strongest_transition({stop, _} = Stop, _) -> Stop;
strongest_transition(_, {stop, _} = Stop) -> Stop;
strongest_transition(drain, drain) -> drain.

dispatch_action({send, Stream, Data} = Action, StateData) ->
    apply_do_result(do_send(Stream, iolist_to_binary(Data), false, StateData), StateData, Action);
dispatch_action({send, Stream, Data, fin} = Action, StateData) ->
    apply_do_result(do_send(Stream, iolist_to_binary(Data), true, StateData), StateData, Action);
dispatch_action({send_datagram, Data} = Action, StateData) ->
    apply_do_result(do_send_datagram(iolist_to_binary(Data), StateData), StateData, Action);
dispatch_action({open_stream, Type} = Action, StateData) ->
    case do_open_stream(Type, StateData) of
        {ok, _StreamId, StateData1} ->
            {StateData1, continue};
        {error, stream_limit_reached = Reason} ->
            handle_action_failure(Action, Reason, mark_blocked(Type, StateData));
        {error, stream_limit = Reason} ->
            handle_action_failure(Action, Reason, mark_transport_blocked(Type, StateData));
        {error, Reason} ->
            handle_action_failure(Action, Reason, StateData)
    end;
dispatch_action({close_stream, _Stream} = Action, StateData) ->
    apply_do_result(do_close_stream(element(2, Action), StateData), StateData, Action);
dispatch_action({reset_stream, Stream, Code} = Action, StateData) ->
    apply_do_result(do_reset_stream(Stream, Code, StateData), StateData, Action);
dispatch_action({stop_sending, Stream, Code} = Action, StateData) ->
    apply_do_result(do_stop_sending(Stream, Code, StateData), StateData, Action);
dispatch_action(drain_session, StateData) ->
    do_drain(StateData),
    {StateData, drain};
dispatch_action({close_session, ErrorCode, Reason}, StateData) ->
    do_close(ErrorCode, Reason, StateData),
    {StateData#data{close_info = {ErrorCode, Reason}}, {stop, normal}}.

apply_do_result({ok, StateData1}, _OldState, _Action) ->
    {StateData1, continue};
apply_do_result({error, Reason}, OldState, Action) ->
    handle_action_failure(Action, Reason, OldState).

%% Invoke the handler's optional `handle_action_failed/3' callback with the
%% action and the underlying error reason. If the callback is not exported,
%% log a warning and keep the session running (pre-callback behaviour).
handle_action_failure(Action, Reason,
                      #data{handler = Handler, handler_state = HState} = StateData) ->
    case erlang:function_exported(Handler, handle_action_failed, 3) of
        true ->
            case Handler:handle_action_failed(Action, Reason, HState) of
                {ok, HState1} ->
                    {StateData#data{handler_state = HState1}, continue};
                {stop, StopReason, HState1} ->
                    {StateData#data{handler_state = HState1}, {stop, StopReason}}
            end;
        false ->
            logger:warning("webtransport action ~p failed: ~p", [Action, Reason]),
            {StateData, continue}
    end.

%% Transport abstraction
transport_send(StreamId, Data, Fin, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:send(H2State, StreamId, Data, Fin);
transport_send(StreamId, Data, Fin, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:send(H3State, StreamId, Data, Fin).

transport_send_datagram(Data, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:send_datagram(H2State, Data);
transport_send_datagram(Data, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:send_datagram(H3State, Data).

transport_open_stream(StreamId, Type, #data{transport = h2, transport_state = H2State}) ->
    ok = webtransport_h2:open_stream(H2State, StreamId, Type),
    {ok, StreamId};
transport_open_stream(_StreamId, bidi, #data{transport = h3, transport_state = H3State}) ->
    case webtransport_h3:open_bidi_stream(H3State) of
        {ok, RealId, _} -> {ok, RealId};
        {error, _} = Err -> Err
    end;
transport_open_stream(_StreamId, uni, #data{transport = h3, transport_state = H3State}) ->
    case webtransport_h3:open_uni_stream(H3State) of
        {ok, RealId, _} -> {ok, RealId};
        {error, _} = Err -> Err
    end.

%% `Code' is `{app, C}' for an application code or `{wt, C}' for a protocol
%% code. h2 carries both as the raw 32-bit value; h3 maps application codes
%% into the WT_APPLICATION_ERROR range.
transport_reset_stream(StreamId, {_, Code}, Stream,
                       #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:reset_stream(H2State, StreamId, Code,
                                 webtransport_stream:bytes_sent(Stream));
transport_reset_stream(StreamId, Code, Stream,
                       #data{transport = h3, transport_state = H3State} = StateData) ->
    webtransport_h3:reset_stream(H3State, StreamId, Code,
                                 h3_reliable_size(StreamId, Stream, StateData)).

%% http3-17 §4.4: the reliable size of a reset covers the stream header we
%% wrote, so the peer can still map the stream to its session. We write no
%% header on a peer-initiated stream.
h3_reliable_size(StreamId, Stream, #data{transport_state = H3State} = StateData) ->
    case is_local_stream(StreamId, StateData) of
        true -> webtransport_h3:stream_header_size(H3State, webtransport_stream:type(Stream));
        false -> 0
    end.

is_local_stream(StreamId, #data{is_server = IsServer}) ->
    (webtransport_stream:initiator(StreamId) =:= server) =:= IsServer.

transport_stop_sending(StreamId, {_, Code}, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:stop_sending(H2State, StreamId, Code);
transport_stop_sending(StreamId, Code, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:stop_sending(H3State, StreamId, Code).

%% Move to draining. A session whose streams all finished already (the last
%% one may have ended in the same batch of actions) stops now: no later
%% stream event would end it. One that never used a stream stays, for
%% datagrams, until it is closed.
enter_draining(#data{opened_bidi = OB, opened_uni = OU,
                     peer_opened_bidi = PB, peer_opened_uni = PU} = StateData)
  when OB + OU + PB + PU > 0 ->
    case maybe_stop_if_drained(StateData, [], draining) of
        {next_state, draining, SD, []} -> {next_state, draining, SD};
        Other -> Other
    end;
enter_draining(StateData) ->
    {next_state, draining, StateData}.

%% Stop once every stream is finished; otherwise stay (or move) in draining.
maybe_stop_if_drained(StateData, Actions) ->
    maybe_stop_if_drained(StateData, Actions, keep).

maybe_stop_if_drained(#data{streams = Streams} = StateData, Actions, Next) ->
    case lists:all(fun({Id, S}) -> stream_done(Id, S, StateData) end, maps:to_list(Streams)) of
        true -> {stop, normal, StateData};
        false when Next =:= draining -> {next_state, draining, StateData, Actions};
        false -> {keep_state, StateData, Actions}
    end.

%% A stream is done once every direction it has is finished: our send side
%% on a uni stream we opened, the peer's on one it opened, both on bidi.
stream_done(StreamId, Stream, StateData) ->
    case webtransport_stream:type(Stream) of
        uni ->
            case is_local_stream(StreamId, StateData) of
                true -> not webtransport_stream:is_writable(Stream);
                false -> not webtransport_stream:is_readable(Stream)
            end;
        bidi ->
            webtransport_stream:state(Stream) =:= closed
    end.

%% Give the handler an event through its optional handle_info/2.
notify_info(Msg, #data{handler = Handler, handler_state = HState} = StateData) ->
    case erlang:function_exported(Handler, handle_info, 2) of
        true ->
            case Handler:handle_info(Msg, HState) of
                {ok, HState1} ->
                    {StateData#data{handler_state = HState1}, continue};
                {ok, HState1, Actions} ->
                    handle_actions(Actions, StateData#data{handler_state = HState1});
                {stop, Reason, HState1} ->
                    {StateData#data{handler_state = HState1}, {stop, Reason}}
            end;
        false ->
            {StateData, continue}
    end.

do_early_data_accepted(#data{transport = h3, transport_state = H3State}) ->
    quic_h3:early_data_accepted(webtransport_h3:h3_conn(H3State));
do_early_data_accepted(#data{transport = h2}) ->
    not_supported.

build_info(#data{request = Request} = StateData) ->
    Info = build_info_base(StateData),
    maps:merge(Info#{max_datagram_size => max_datagram_size(StateData)},
               maps:with([protocol, wt_draft], Request)).

%% overview-14 §4.2: the largest datagram payload the application can send.
max_datagram_size(#data{transport = h2}) ->
    ?WT_H2_DATAGRAM_MAX;
max_datagram_size(#data{transport = h3, transport_state = H3State}) ->
    try quic_h3:max_datagram_size(webtransport_h3:h3_conn(H3State),
                                  webtransport_h3:session_id(H3State)) of
        N when is_integer(N) -> min(N, ?WT_H3_DATAGRAM_MAX)
    catch _:_ -> 0
    end.

build_info_base(#data{transport = Transport, streams = Streams,
                  local_max_data = LocalMaxData, remote_max_data = RemoteMaxData,
                  local_max_streams_bidi = LocalMaxBidi, local_max_streams_uni = LocalMaxUni,
                  remote_max_streams_bidi = RemoteMaxBidi, remote_max_streams_uni = RemoteMaxUni,
                  bytes_sent = Sent, bytes_received = Received, close_info = Close}) ->
    Base = #{
        transport => Transport,
        stream_count => maps:size(Streams),
        local_max_data => LocalMaxData,
        remote_max_data => RemoteMaxData,
        local_max_streams_bidi => LocalMaxBidi,
        local_max_streams_uni => LocalMaxUni,
        remote_max_streams_bidi => RemoteMaxBidi,
        remote_max_streams_uni => RemoteMaxUni,
        bytes_sent => Sent,
        bytes_received => Received
    },
    case Close of
        undefined -> Base;
        _ -> Base#{close_info => Close}
    end.

%% ============================================================================
%% Eunit (internal helpers)
%% ============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

handle_incoming_capsule_max_stream_data_test() ->
    Stream = webtransport_stream:new(4, bidi, 1024),
    Data = #data{streams = #{4 => Stream}, transport = h2,
                 remote_max_data = 0, remote_max_streams_bidi = 0,
                 remote_max_streams_uni = 0,
                 local_max_data = 0, local_max_streams_bidi = 0,
                 local_max_streams_uni = 0, is_server = false,
                 next_bidi_id = 0, next_uni_id = 2,
                 request = #{}, handler = undefined,
                 handler_state = undefined},
    Data1 = handle_incoming_capsule({max_stream_data, 4, 65536}, Data),
    #{4 := Stream1} = Data1#data.streams,
    ?assertEqual(65536, webtransport_stream:send_window(Stream1)),
    %% Lower value triggers session error (monotonicity enforcement).
    ?assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                 handle_incoming_capsule({max_stream_data, 4, 100}, Data1)),
    ok.

handle_incoming_capsule_max_stream_data_h3_rejected_test() ->
    Stream = webtransport_stream:new(4, bidi, 1024),
    Data = #data{streams = #{4 => Stream}, transport = h3,
                 remote_max_data = 0, remote_max_streams_bidi = 0,
                 remote_max_streams_uni = 0,
                 local_max_data = 0, local_max_streams_bidi = 0,
                 local_max_streams_uni = 0, is_server = false,
                 next_bidi_id = 0, next_uni_id = 2,
                 request = #{}, handler = undefined,
                 handler_state = undefined},
    ?assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                 handle_incoming_capsule({max_stream_data, 4, 65536}, Data)),
    ok.

handle_incoming_capsule_max_data_monotonic_test() ->
    Data = #data{streams = #{}, transport = h2,
                 remote_max_data = 1000, remote_max_streams_bidi = 0,
                 remote_max_streams_uni = 0, local_max_data = 0,
                 local_max_streams_bidi = 0, local_max_streams_uni = 0,
                 is_server = false, next_bidi_id = 0, next_uni_id = 2,
                 request = #{}, handler = undefined, handler_state = undefined},
    %% Decrease triggers session error.
    ?assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                 handle_incoming_capsule({max_data, 500}, Data)),
    %% Increase is accepted.
    ?assertEqual(2000, (handle_incoming_capsule({max_data, 2000}, Data))#data.remote_max_data),
    %% http3-17 §5.6.4, http2-16 §6.5: a value that does not increase the
    %% limit is an error too.
    ?assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                 handle_incoming_capsule({max_data, 1000}, Data)),
    ok.

test_data() ->
    #data{streams = #{}, transport = h2,
          remote_max_data = 0, remote_max_streams_bidi = 10,
          remote_max_streams_uni = 10, local_max_data = 100,
          local_max_streams_bidi = 3, local_max_streams_uni = 3,
          is_server = true, next_bidi_id = 1, next_uni_id = 3,
          request = #{}, handler = undefined, handler_state = undefined}.

%% quic's streams_available clears a type refused by the QUIC limit; if
%% the session's own limit is also used up, the type waits for that one.
transport_streams_available_test_() ->
    Blocked = (test_data())#data{transport = h3, transport_blocked = #{bidi => true}},
    Exhausted = Blocked#data{opened_bidi = 10},
    {Cleared, continue} = on_transport_streams_available(bidi, Blocked),
    {Waiting, continue} = on_transport_streams_available(bidi, Exhausted),
    [
        ?_assertEqual(#{}, Cleared#data.transport_blocked),
        ?_assertEqual(#{}, Cleared#data.blocked),
        ?_assertEqual(#{}, Waiting#data.transport_blocked),
        ?_assertEqual(#{bidi => true}, Waiting#data.blocked),
        %% A type that was not refused is left alone.
        ?_assertEqual({Blocked, continue}, on_transport_streams_available(uni, Blocked))
    ].

max_streams_rules_test_() ->
    Data = test_data(),
    [
        ?_assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                      handle_incoming_capsule({max_streams_bidi, 10}, Data)),
        ?_assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                      handle_incoming_capsule({max_streams_uni, (1 bsl 60) + 1}, Data)),
        ?_assertMatch({session_error, ?WT_FLOW_CONTROL_ERROR, _},
                      handle_incoming_capsule({streams_blocked_bidi, (1 bsl 60) + 1}, Data)),
        ?_assertEqual(11, (handle_incoming_capsule({max_streams_bidi, 11}, Data))
                          #data.remote_max_streams_bidi)
    ].

%% http3-17 §5.1: without flow control, flow-control capsules are ignored.
flow_control_off_test_() ->
    Data = (test_data())#data{flow_control = false},
    [
        ?_assertEqual(Data, handle_incoming_capsule({max_data, 0}, Data)),
        ?_assertEqual(Data, handle_incoming_capsule({max_streams_bidi, 1 bsl 61}, Data))
    ].

%% Flow control on h3 needs limits sent by both sides, whatever the
%% draft: Safari 26 speaks http3-07 and sends them.
flow_control_declared_test_() ->
    Peer = #{16#2b61 => 8388608, 16#2b64 => 100, 16#2b65 => 100},
    Fc = fun(Opts) ->
        (init_limits(h3, undefined, Opts#{peer_settings => Peer}, #data{}))#data.flow_control
    end,
    [
        ?_assert(Fc(#{request => #{wt_draft => draft07}})),
        ?_assertNot(Fc(#{declared_flow_control => false})),
        ?_assertNot(Fc(#{max_data => 0, max_streams_bidi => 0, max_streams_uni => 0})),
        ?_assertNot((init_limits(h3, undefined, #{peer_settings => #{16#33 => 1}},
                                 #data{}))#data.flow_control)
    ].

%% http2-16 §6.7: a stream id opens every lower id of its kind, so the
%% peer's count is the id's ordinal; above our limit is an error.
peer_stream_limit_test_() ->
    Data = test_data(),
    [
        ?_assertEqual({ok, 1}, peer_stream_allowed(0, bidi, Data)),
        ?_assertEqual({ok, 3}, peer_stream_allowed(8, bidi, Data)),
        ?_assertEqual(exceeded, peer_stream_allowed(12, bidi, Data)),
        ?_assertEqual({ok, 1},
                      peer_stream_allowed(12, bidi, Data#data{transport = h3})),
        ?_assertEqual({ok, 4},
                      peer_stream_allowed(12, bidi, Data#data{flow_control = false}))
    ].

%% Data beyond the session limit is a flow-control error.
receive_limit_test_() ->
    Stream = webtransport_stream:new(0, bidi, 1000, 1000),
    Data = test_data(),
    [
        ?_assertMatch({ok, _}, receive_into(Stream, <<"x">>, 100, Data)),
        ?_assertEqual({error, flow_control}, receive_into(Stream, <<"x">>, 101, Data)),
        ?_assertMatch({ok, _}, receive_into(Stream, <<"x">>, 101,
                                            Data#data{flow_control = false}))
    ].

%% http2-16 §6.6, §6.9: flow-control capsules on a stream in the wrong
%% state are WT_STREAM_STATE_ERROR.
stream_state_capsule_test_() ->
    Peer = webtransport_stream:new(0, bidi, 100, 100),
    {ok, PeerFin} = webtransport_stream:receive_fin(Peer),
    {ok, PeerStopped} = webtransport_stream:peer_stop_sending(Peer, 1),
    Data = (test_data())#data{streams = #{0 => Peer, 4 => PeerFin, 8 => PeerStopped}},
    [
        ?_assertMatch({session_error, ?WT_STREAM_STATE_ERROR, _},
                      handle_incoming_capsule({max_stream_data, 12, 500}, Data)),
        ?_assertMatch({session_error, ?WT_STREAM_STATE_ERROR, _},
                      handle_incoming_capsule({max_stream_data, 8, 500}, Data)),
        ?_assertMatch(#data{}, handle_incoming_capsule({stream_data_blocked, 0, 100}, Data)),
        ?_assertMatch({session_error, ?WT_STREAM_STATE_ERROR, _},
                      handle_incoming_capsule({stream_data_blocked, 4, 100}, Data)),
        ?_assertMatch({session_error, ?WT_STREAM_STATE_ERROR, _},
                      handle_incoming_capsule({stream_data_blocked, 12, 100}, Data))
    ].

%% RFC 9297: a capsule split across two DATA frames is applied once whole.
connect_capsule_split_test() ->
    Data = (test_data())#data{transport = h3},
    Wire = wt_h3_capsule:encode({max_data, 4096}),
    <<A:3/binary, B/binary>> = Wire,
    {keep_state, D1} = handle_connect_data(A, false, Data),
    ?assertEqual(A, D1#data.connect_buf),
    ?assertEqual(0, D1#data.remote_max_data),
    {keep_state, D2} = handle_connect_data(B, false, D1),
    ?assertEqual(<<>>, D2#data.connect_buf),
    ?assertEqual(4096, D2#data.remote_max_data).

%% Finished streams leave the map; a finished peer stream returns credit.
retire_stream_test() ->
    Data0 = (test_data())#data{flow_control = false},
    {ok, PeerUni} = webtransport_stream:receive_fin(webtransport_stream:new(2, uni, 0, 100)),
    Data = Data0#data{streams = #{2 => PeerUni}, peer_opened_uni = 1},
    Data1 = retire_stream(2, Data),
    ?assertNot(maps:is_key(2, Data1#data.streams)),
    ?assertEqual(Data#data.local_max_streams_uni + 1, Data1#data.local_max_streams_uni),
    %% An open stream stays.
    Open = Data#data{streams = #{2 => webtransport_stream:new(2, uni, 0, 100)}},
    ?assert(maps:is_key(2, (retire_stream(2, Open))#data.streams)).

%% h2: an id below the count already used and not in the map is finished.
retired_by_ordinal_test_() ->
    Data = (test_data())#data{peer_opened_bidi = 3, opened_bidi = 1},
    [
        ?_assert(is_retired(4, Data)),          %% client bidi #2 of 3
        ?_assertNot(is_retired(12, Data)),      %% client bidi #4: new
        ?_assert(is_retired(1, Data)),          %% our bidi #1
        ?_assertNot(is_retired(5, Data)),       %% our bidi #2: never opened
        ?_assertNot(is_retired(4, Data#data{transport = h3}))
    ].

stream_done_test_() ->
    Server = test_data(),
    LocalUni = webtransport_stream:new(3, uni, 100, 0),
    {ok, LocalUniFin} = webtransport_stream:close_local(LocalUni),
    PeerUni = webtransport_stream:new(2, uni, 0, 100),
    {ok, PeerUniFin} = webtransport_stream:receive_fin(PeerUni),
    Bidi = webtransport_stream:new(0, bidi, 100, 100),
    {ok, BidiHalf} = webtransport_stream:receive_fin(Bidi),
    {ok, BidiDone} = webtransport_stream:close_local(BidiHalf),
    [
        ?_assertNot(stream_done(3, LocalUni, Server)),
        ?_assert(stream_done(3, LocalUniFin, Server)),
        ?_assertNot(stream_done(2, PeerUni, Server)),
        ?_assert(stream_done(2, PeerUniFin, Server)),
        ?_assertNot(stream_done(0, BidiHalf, Server)),
        ?_assert(stream_done(0, BidiDone, Server))
    ].

%% close_stream with bytes still queued waits for them before the FIN.
close_stream_with_queue_test() ->
    Stream = webtransport_stream:buffer_send(webtransport_stream:new(1, bidi, 0, 100), <<"q">>),
    Data = (test_data())#data{streams = #{1 => Stream}},
    {ok, Data1} = do_close_stream(1, Data),
    ?assert(webtransport_stream:fin_pending(maps:get(1, Data1#data.streams))),
    ?assertEqual({error, stream_fin_sent}, do_close_stream(1, Data1)).

%% WebTransport application error codes are 32 bits.
our_error_codes_test_() ->
    Data = (test_data())#data{streams = #{1 => webtransport_stream:new(1, bidi, 100, 100)}},
    [
        ?_assertEqual({error, invalid_error_code}, do_reset_stream(1, 1 bsl 32, Data)),
        ?_assertEqual({error, invalid_error_code}, do_stop_sending(1, 1 bsl 32, Data))
    ].

%% http3-17 §4.4: a stream the peer sends on that closes before its FIN
%% was reset with code 0, which reaches the handler as no application code.
remote_close_code_zero_test_() ->
    Server = test_data(),
    Open = webtransport_stream:new(2, uni, 0, 100),
    {ok, Fin} = webtransport_stream:receive_fin(Open),
    [
        ?_assertMatch({_, {reset, undefined}}, remote_close(2, Open, normal, Server)),
        ?_assertMatch({_, normal}, remote_close(2, Fin, normal, Server)),
        ?_assertMatch({_, {stop_sending, 5}},
                      remote_close(1, webtransport_stream:new(1, bidi, 100, 100),
                                   {stop_sending, 5}, Server))
    ].

%% http3-17 §5.4: a reset's unreceived bytes are counted against the
%% session data limit; the stream header is not.
count_unreceived_test() ->
    State = webtransport_h3:new_for_test(0),
    Data = (test_data())#data{transport = h3, transport_state = State,
                              flow_control = false, bytes_received = 100},
    {ok, Stream} = webtransport_stream:receive_data(webtransport_stream:new(2, uni, 0, 1000),
                                                     <<0:(10*8)>>),
    %% Peer uni stream: header is 3 bytes (0x54 is a 2-byte varint, session 0
    %% one byte), 10 body bytes arrived, final size 53: 40 never arrived.
    Data1 = count_unreceived(2, Stream, {reset, 1, 53}, Data),
    ?assertEqual(140, Data1#data.bytes_received),
    ?assertEqual(Data, count_unreceived(2, Stream, {reset, 1}, Data)).

%% http2-16 §4.3.1: the SETTINGS in effect when the session was accepted
%% replace the ones read before, combined with WebTransport-Init.
apply_peer_limits_test() ->
    Data = test_data(),
    Settings = #{wt_initial_max_data => 500, wt_initial_max_streams_bidi => 7,
                 wt_initial_max_streams_uni => 3, wt_initial_max_stream_data_uni => 10},
    D1 = apply_peer_limits(Settings, #{u => 20, br => 5}, Data),
    ?assertEqual(500, D1#data.remote_max_data),
    ?assertEqual(7, D1#data.remote_max_streams_bidi),
    ?assertEqual(3, D1#data.remote_max_streams_uni),
    ?assertEqual(#{u => 20, br => 5}, D1#data.peer_stream_data).

%% Which side of a stream sends: both on bidi, the opener on uni.
send_sides_test_() ->
    Server = test_data(),
    [
        ?_assert(has_peer_send_side(2, Server)),        %% client uni
        ?_assertNot(has_local_send_side(2, Server)),
        ?_assert(has_local_send_side(3, Server)),       %% server uni
        ?_assertNot(has_peer_send_side(3, Server)),
        ?_assert(has_peer_send_side(0, Server)),
        ?_assert(has_local_send_side(0, Server))
    ].

augment_reason_test_() ->
    [
        ?_assertEqual(normal, augment_reason(normal, undefined)),
        ?_assertEqual(shutdown, augment_reason(shutdown, undefined)),
        ?_assertEqual({closed, 7, <<"bye">>},
                      augment_reason(normal, {7, <<"bye">>})),
        ?_assertEqual({{shutdown, x}, {closed, 0, <<>>}},
                      augment_reason({shutdown, x}, {0, <<>>}))
    ].

apply_capsule_transition_test_() ->
    Data = #data{streams = #{}, transport = h2,
                 remote_max_data = 0, remote_max_streams_bidi = 0,
                 remote_max_streams_uni = 0, local_max_data = 0,
                 local_max_streams_bidi = 0, local_max_streams_uni = 0,
                 is_server = false, next_bidi_id = 0, next_uni_id = 2,
                 request = #{}, handler = undefined, handler_state = undefined},
    [
        ?_assertMatch({stop, normal, _},
                      apply_capsule_transition({close_session, 1, <<>>}, Data)),
        ?_assertMatch({next_state, draining, _},
                      apply_capsule_transition({drain_session}, Data)),
        ?_assertMatch({keep_state, _},
                      apply_capsule_transition({max_data, 1000}, Data))
    ].

-endif.
