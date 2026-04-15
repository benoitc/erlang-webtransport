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

%% API
-export([start_link/4, start_link/5]).
-export([send/4, send_datagram/2]).
-export([open_stream/2, close_stream/2]).
-export([reset_stream/3, stop_sending/3]).
-export([drain/1, close/3]).
-export([get_info/1]).

%% Internal API (called by transport layers)
-export([handle_capsule/2, handle_stream_data/4, handle_datagram_data/2]).
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
    local_max_data :: non_neg_integer(),
    remote_max_data :: non_neg_integer(),
    local_max_streams_bidi :: non_neg_integer(),
    local_max_streams_uni :: non_neg_integer(),
    remote_max_streams_bidi :: non_neg_integer(),
    remote_max_streams_uni :: non_neg_integer(),
    bytes_sent = 0 :: non_neg_integer(),
    bytes_received = 0 :: non_neg_integer(),
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

-spec start_link(h2 | h3, term(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(Transport, TransportState, Handler, Opts) ->
    start_link(Transport, TransportState, Handler, Opts, []).

-spec start_link(h2 | h3, term(), module(), map(), list()) -> {ok, pid()} | {error, term()}.
start_link(Transport, TransportState, Handler, Opts, StartOpts) ->
    gen_statem:start_link(?MODULE, {Transport, TransportState, Handler, Opts}, StartOpts).

-spec send(session(), stream_ref(), iodata(), boolean()) -> ok | {error, term()}.
send(Session, StreamId, Data, Fin) ->
    gen_statem:call(Session, {send, StreamId, iolist_to_binary(Data), Fin}).

-spec send_datagram(session(), iodata()) -> ok | {error, term()}.
send_datagram(Session, Data) ->
    gen_statem:call(Session, {send_datagram, iolist_to_binary(Data)}).

-spec open_stream(session(), bidi | uni) -> {ok, stream_ref()} | {error, term()}.
open_stream(Session, Type) ->
    gen_statem:call(Session, {open_stream, Type}).

-spec close_stream(session(), stream_ref()) -> ok | {error, term()}.
close_stream(Session, StreamId) ->
    gen_statem:call(Session, {close_stream, StreamId}).

-spec reset_stream(session(), stream_ref(), non_neg_integer()) -> ok | {error, term()}.
reset_stream(Session, StreamId, ErrorCode) ->
    gen_statem:call(Session, {reset_stream, StreamId, ErrorCode}).

-spec stop_sending(session(), stream_ref(), non_neg_integer()) -> ok | {error, term()}.
stop_sending(Session, StreamId, ErrorCode) ->
    gen_statem:call(Session, {stop_sending, StreamId, ErrorCode}).

-spec drain(session()) -> ok.
drain(Session) ->
    gen_statem:cast(Session, drain).

-spec close(session(), non_neg_integer(), binary()) -> ok.
close(Session, ErrorCode, Reason) ->
    gen_statem:cast(Session, {close, ErrorCode, Reason}).

-spec get_info(session()) -> {ok, map()} | {error, term()}.
get_info(Session) ->
    gen_statem:call(Session, get_info).

%% Internal API
-spec handle_capsule(session(), term()) -> ok.
handle_capsule(Session, Capsule) ->
    gen_statem:cast(Session, {capsule, Capsule}).

-spec handle_stream_data(session(), stream_ref(), binary(), boolean()) -> ok.
handle_stream_data(Session, StreamId, Data, Fin) ->
    gen_statem:cast(Session, {stream_data, StreamId, Data, Fin}).

-spec handle_datagram_data(session(), binary()) -> ok.
handle_datagram_data(Session, Data) ->
    gen_statem:cast(Session, {datagram_data, Data}).

-spec handle_stream_opened(session(), stream_ref(), bidi | uni) -> ok.
handle_stream_opened(Session, StreamId, Type) ->
    gen_statem:cast(Session, {stream_opened, StreamId, Type}).

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

    Data = #data{
        transport = Transport,
        transport_state = TransportState,
        handler = Handler,
        request = Request,
        is_server = IsServer,
        next_bidi_id = NextBidi,
        next_uni_id = NextUni,
        local_max_data = maps:get(max_data, Opts, ?DEFAULT_MAX_DATA),
        remote_max_data = maps:get(max_data, Opts, ?DEFAULT_MAX_DATA),
        local_max_streams_bidi = maps:get(max_streams_bidi, Opts, ?DEFAULT_MAX_STREAMS_BIDI),
        local_max_streams_uni = maps:get(max_streams_uni, Opts, ?DEFAULT_MAX_STREAMS_UNI),
        remote_max_streams_bidi = maps:get(max_streams_bidi, Opts, ?DEFAULT_MAX_STREAMS_BIDI),
        remote_max_streams_uni = maps:get(max_streams_uni, Opts, ?DEFAULT_MAX_STREAMS_UNI)
    },

    %% Initialize handler
    InitResult = case erlang:function_exported(Handler, init, 3) of
        true -> Handler:init(self(), Request, HandlerOpts);
        false -> Handler:init(self(), Request)
    end,
    case InitResult of
        {ok, HandlerState} ->
            {ok, open, Data#data{handler_state = HandlerState}};
        {ok, HandlerState, Actions} ->
            Data1 = handle_actions(Actions, Data#data{handler_state = HandlerState}),
            {ok, open, Data1};
        {error, Reason} ->
            {stop, Reason}
    end.

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

open(cast, {capsule, Capsule}, StateData) ->
    StateData1 = handle_incoming_capsule(Capsule, StateData),
    {keep_state, StateData1};

open(cast, {stream_data, StreamId, Data, Fin}, StateData) ->
    StateData1 = handle_incoming_stream_data(StreamId, Data, Fin, StateData),
    {keep_state, StateData1};

open(cast, {datagram_data, Data}, StateData) ->
    StateData1 = handle_incoming_datagram(Data, StateData),
    {keep_state, StateData1};

open(cast, {stream_opened, StreamId, Type}, StateData) ->
    StateData1 = handle_remote_stream_opened(StreamId, Type, StateData),
    {keep_state, StateData1};

open(cast, {stream_closed, StreamId, Reason}, StateData) ->
    StateData1 = handle_remote_stream_closed(StreamId, Reason, StateData),
    {keep_state, StateData1};

open(cast, drain, StateData) ->
    do_drain(StateData),
    {next_state, draining, StateData};

open(cast, {close, ErrorCode, Reason}, StateData) ->
    do_close(ErrorCode, Reason, StateData),
    {stop, normal, StateData#data{close_info = {ErrorCode, Reason}}};

open(info, Msg, #data{handler = Handler, handler_state = HState} = StateData) ->
    case erlang:function_exported(Handler, handle_info, 2) of
        true ->
            case Handler:handle_info(Msg, HState) of
                {ok, HState1} ->
                    {keep_state, StateData#data{handler_state = HState1}};
                {ok, HState1, Actions} ->
                    StateData1 = handle_actions(Actions, StateData#data{handler_state = HState1}),
                    {keep_state, StateData1};
                {stop, _Reason, HState1} ->
                    {stop, normal, StateData#data{handler_state = HState1}}
            end;
        false ->
            {keep_state, StateData}
    end.

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

draining({call, From}, _, StateData) ->
    {keep_state, StateData, [{reply, From, {error, session_draining}}]};

draining(cast, {stream_data, StreamId, Data, Fin}, StateData) ->
    StateData1 = handle_incoming_stream_data(StreamId, Data, Fin, StateData),
    maybe_stop_if_drained(StateData1, []);

draining(cast, {stream_closed, StreamId, Reason}, StateData) ->
    StateData1 = handle_remote_stream_closed(StreamId, Reason, StateData),
    maybe_stop_if_drained(StateData1, []);

draining(cast, _, StateData) ->
    maybe_stop_if_drained(StateData, []).

terminate(Reason, _State, #data{handler = Handler, handler_state = HState}) ->
    Handler:terminate(Reason, HState),
    ok.

%% ============================================================================
%% Internal functions
%% ============================================================================

do_send(StreamId, Data, Fin, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case webtransport_stream:send(Stream, Data) of
                {ok, ToSend, Stream1} ->
                    %% Actually send the data
                    ok = transport_send(StreamId, ToSend, Fin, StateData),
                    Stream2 = case Fin of
                        true ->
                            {ok, S} = webtransport_stream:close_local(Stream1),
                            S;
                        false ->
                            Stream1
                    end,
                    {ok, StateData#data{streams = Streams#{StreamId => Stream2}}};
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, unknown_stream}
    end.

do_send_datagram(Data, StateData) ->
    ok = transport_send_datagram(Data, StateData),
    {ok, StateData}.

do_open_stream(bidi, #data{streams = Streams, next_bidi_id = NextId,
                           remote_max_streams_bidi = MaxStreams} = StateData) ->
    CurrentCount = count_streams(bidi, Streams),
    case CurrentCount < MaxStreams of
        true ->
            case transport_open_stream(NextId, bidi, StateData) of
                {ok, StreamId} ->
                    Stream = webtransport_stream:new(StreamId, bidi, ?DEFAULT_MAX_STREAM_DATA),
                    Streams1 = Streams#{StreamId => Stream},
                    {ok, StreamId, StateData#data{
                        streams = Streams1,
                        next_bidi_id = NextId + 4
                    }};
                {error, _} = Err ->
                    Err
            end;
        false ->
            {error, stream_limit_reached}
    end;

do_open_stream(uni, #data{streams = Streams, next_uni_id = NextId,
                          remote_max_streams_uni = MaxStreams} = StateData) ->
    CurrentCount = count_streams(uni, Streams),
    case CurrentCount < MaxStreams of
        true ->
            case transport_open_stream(NextId, uni, StateData) of
                {ok, StreamId} ->
                    Stream = webtransport_stream:new(StreamId, uni, ?DEFAULT_MAX_STREAM_DATA),
                    Streams1 = Streams#{StreamId => Stream},
                    {ok, StreamId, StateData#data{
                        streams = Streams1,
                        next_uni_id = NextId + 4
                    }};
                {error, _} = Err ->
                    Err
            end;
        false ->
            {error, stream_limit_reached}
    end.

do_close_stream(StreamId, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            case webtransport_stream:close_local(Stream) of
                {ok, Stream1} ->
                    transport_send(StreamId, <<>>, true, StateData),
                    {ok, StateData#data{streams = Streams#{StreamId => Stream1}}};
                {error, _} = Err ->
                    Err
            end;
        error ->
            {error, unknown_stream}
    end.

do_reset_stream(StreamId, ErrorCode, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            Stream1 = webtransport_stream:reset(Stream, ErrorCode),
            transport_reset_stream(StreamId, ErrorCode, StateData),
            {ok, StateData#data{streams = Streams#{StreamId => Stream1}}};
        error ->
            {error, unknown_stream}
    end.

do_stop_sending(StreamId, ErrorCode, #data{streams = Streams} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            Stream1 = webtransport_stream:stop_sending(Stream, ErrorCode),
            transport_stop_sending(StreamId, ErrorCode, StateData),
            {ok, StateData#data{streams = Streams#{StreamId => Stream1}}};
        error ->
            {error, unknown_stream}
    end.

do_drain(#data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:drain_session(H2State);
do_drain(#data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:drain_session(H3State).

do_close(ErrorCode, Reason, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:close_session(H2State, ErrorCode, Reason);
do_close(ErrorCode, Reason, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:close_session(H3State, ErrorCode, Reason).

handle_incoming_capsule({max_data, Limit}, #data{} = StateData) ->
    StateData#data{remote_max_data = Limit};
handle_incoming_capsule({max_streams_bidi, Limit}, #data{} = StateData) ->
    StateData#data{remote_max_streams_bidi = Limit};
handle_incoming_capsule({max_streams_uni, Limit}, #data{} = StateData) ->
    StateData#data{remote_max_streams_uni = Limit};
handle_incoming_capsule({close_session, ErrorCode, Reason}, #data{} = StateData) ->
    StateData#data{close_info = {ErrorCode, Reason}};
handle_incoming_capsule({drain_session}, #data{} = StateData) ->
    StateData;
handle_incoming_capsule(_Capsule, StateData) ->
    StateData.

handle_incoming_stream_data(StreamId, Data, Fin,
                             #data{streams = Streams, handler = Handler,
                                   handler_state = HState} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            Type = webtransport_stream:type(Stream),
            case webtransport_stream:receive_data(Stream, Data) of
                {ok, Stream1} ->
                    Stream2 = case Fin of
                        true ->
                            {ok, S} = webtransport_stream:receive_fin(Stream1),
                            S;
                        false ->
                            Stream1
                    end,
                    %% Callback to handler
                    Callback = case Fin andalso erlang:function_exported(Handler, handle_stream_fin, 4) of
                        true -> fun() -> Handler:handle_stream_fin(StreamId, Type, Data, HState) end;
                        false -> fun() -> Handler:handle_stream(StreamId, Type, Data, HState) end
                    end,
                    case Callback() of
                        {ok, HState1} ->
                            StateData#data{
                                streams = Streams#{StreamId => Stream2},
                                handler_state = HState1
                            };
                        {ok, HState1, Actions} ->
                            StateData1 = StateData#data{
                                streams = Streams#{StreamId => Stream2},
                                handler_state = HState1
                            },
                            handle_actions(Actions, StateData1);
                        {stop, _Reason, HState1} ->
                            StateData#data{
                                streams = Streams#{StreamId => Stream2},
                                handler_state = HState1
                            }
                    end;
                {error, _Reason} ->
                    StateData
            end;
        error ->
            StateData
    end.

handle_incoming_datagram(Data, #data{handler = Handler, handler_state = HState} = StateData) ->
    case Handler:handle_datagram(Data, HState) of
        {ok, HState1} ->
            StateData#data{handler_state = HState1};
        {ok, HState1, Actions} ->
            handle_actions(Actions, StateData#data{handler_state = HState1});
        {stop, _Reason, HState1} ->
            StateData#data{handler_state = HState1}
    end.

handle_remote_stream_opened(StreamId, Type, #data{streams = Streams} = StateData) ->
    case maps:is_key(StreamId, Streams) of
        true ->
            StateData;
        false ->
            Stream = webtransport_stream:new(StreamId, Type, ?DEFAULT_MAX_STREAM_DATA),
            StateData#data{streams = Streams#{StreamId => Stream}}
    end.

handle_remote_stream_closed(StreamId, Reason,
                             #data{streams = Streams, handler = Handler,
                                   handler_state = HState} = StateData) ->
    case maps:find(StreamId, Streams) of
        {ok, Stream} ->
            Stream1 = webtransport_stream:close(Stream),
            case Handler:handle_stream_closed(StreamId, Reason, HState) of
                {ok, HState1} ->
                    StateData#data{
                        streams = Streams#{StreamId => Stream1},
                        handler_state = HState1
                    };
                {stop, _Reason, HState1} ->
                    StateData#data{
                        streams = Streams#{StreamId => Stream1},
                        handler_state = HState1
                    }
            end;
        error ->
            StateData
    end.

%% Execute handler-returned actions inline on the session's own state.
%% Runs inside the gen_statem callback so we cannot round-trip through the
%% public API — that would self-call and crash with `calling_self`.
%% `drain_session` does NOT perform the {next_state, draining, ...} transition
%% here; handlers that need to enter draining should do so via explicit
%% {stop, Reason, _} or by driving webtransport:drain_session/1 off-process.
handle_actions([], StateData) ->
    StateData;
handle_actions([Action | Rest], StateData) ->
    handle_actions(Rest, dispatch_action(Action, StateData)).

dispatch_action({send, Stream, Data}, StateData) ->
    apply_do_result(do_send(Stream, iolist_to_binary(Data), false, StateData), StateData, {send, Stream});
dispatch_action({send, Stream, Data, fin}, StateData) ->
    apply_do_result(do_send(Stream, iolist_to_binary(Data), true, StateData), StateData, {send, Stream, fin});
dispatch_action({send_datagram, Data}, StateData) ->
    apply_do_result(do_send_datagram(iolist_to_binary(Data), StateData), StateData, send_datagram);
dispatch_action({open_stream, Type}, StateData) ->
    case do_open_stream(Type, StateData) of
        {ok, _StreamId, StateData1} -> StateData1;
        {error, Reason} -> warn_action({open_stream, Type}, Reason), StateData
    end;
dispatch_action({close_stream, Stream}, StateData) ->
    apply_do_result(do_close_stream(Stream, StateData), StateData, {close_stream, Stream});
dispatch_action({reset_stream, Stream, Code}, StateData) ->
    apply_do_result(do_reset_stream(Stream, Code, StateData), StateData, {reset_stream, Stream});
dispatch_action({stop_sending, Stream, Code}, StateData) ->
    apply_do_result(do_stop_sending(Stream, Code, StateData), StateData, {stop_sending, Stream});
dispatch_action(drain_session, StateData) ->
    do_drain(StateData),
    StateData;
dispatch_action({close_session, ErrorCode, Reason}, StateData) ->
    do_close(ErrorCode, Reason, StateData),
    StateData#data{close_info = {ErrorCode, Reason}}.

apply_do_result({ok, StateData1}, _OldState, _Action) ->
    StateData1;
apply_do_result({error, Reason}, OldState, Action) ->
    warn_action(Action, Reason),
    OldState.

warn_action(Action, Reason) ->
    logger:warning("webtransport action ~p failed: ~p", [Action, Reason]).

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
    case webtransport_h2:open_stream(H2State, StreamId, Type) of
        ok -> {ok, StreamId};
        {error, _} = Err -> Err
    end;
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

transport_reset_stream(StreamId, ErrorCode, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:reset_stream(H2State, StreamId, ErrorCode);
transport_reset_stream(StreamId, ErrorCode, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:reset_stream(H3State, StreamId, ErrorCode, 0).

transport_stop_sending(StreamId, ErrorCode, #data{transport = h2, transport_state = H2State}) ->
    webtransport_h2:stop_sending(H2State, StreamId, ErrorCode);
transport_stop_sending(StreamId, ErrorCode, #data{transport = h3, transport_state = H3State}) ->
    webtransport_h3:stop_sending(H3State, StreamId, ErrorCode).

count_streams(Type, Streams) ->
    maps:fold(fun(_Id, Stream, Acc) ->
        case webtransport_stream:type(Stream) of
            Type -> Acc + 1;
            _ -> Acc
        end
    end, 0, Streams).

maybe_stop_if_drained(#data{streams = Streams} = StateData, Actions) ->
    OpenCount = maps:fold(fun(_Id, Stream, Acc) ->
        case webtransport_stream:is_open(Stream) of
            true -> Acc + 1;
            false -> Acc
        end
    end, 0, Streams),
    case OpenCount of
        0 -> {stop, normal, StateData};
        _ -> {keep_state, StateData, Actions}
    end.

build_info(#data{transport = Transport, streams = Streams,
                  local_max_data = LocalMaxData, remote_max_data = RemoteMaxData,
                  local_max_streams_bidi = LocalMaxBidi, local_max_streams_uni = LocalMaxUni,
                  remote_max_streams_bidi = RemoteMaxBidi, remote_max_streams_uni = RemoteMaxUni}) ->
    #{
        transport => Transport,
        stream_count => maps:size(Streams),
        local_max_data => LocalMaxData,
        remote_max_data => RemoteMaxData,
        local_max_streams_bidi => LocalMaxBidi,
        local_max_streams_uni => LocalMaxUni,
        remote_max_streams_bidi => RemoteMaxBidi,
        remote_max_streams_uni => RemoteMaxUni
    }.
