%% @doc Test handler that triggers action-driven state transitions via
%% `handle_info/2'. Sending `drain_now' or `close_now' directly to the
%% session pid exercises `drain_session' / `close_session' action
%% dispatch without needing a server-side echo round-trip.
-module(wt_trigger_handler).
-behaviour(webtransport_handler).

-export([init/2, init/3, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3, handle_info/2,
         handle_action_failed/3, terminate/2]).

-record(state, {
    owner :: pid(),
    session :: pid(),
    %% `continue' — log the failure and keep going (default).
    %% `stop' — return {stop, ...} from handle_action_failed/3.
    %% `forward' — relay the failure to `owner' and continue.
    failure_mode = forward :: continue | stop | forward
}).

init(Session, Req) ->
    init(Session, Req, #{}).

init(Session, _Req, Opts) ->
    Owner = maps:get(owner, Opts, self()),
    FailureMode = maps:get(failure_mode, Opts, forward),
    {ok, #state{owner = Owner, session = Session, failure_mode = FailureMode}}.

handle_stream(_StreamId, _Type, _Data, State) ->
    {ok, State}.

handle_stream_fin(StreamId, Type, Data,
                  #state{owner = Owner, session = Session} = State) ->
    Owner ! {webtransport, Session, {stream_fin, StreamId, Type, Data}},
    {ok, State}.

handle_datagram(Data, #state{owner = Owner, session = Session} = State) ->
    Owner ! {webtransport, Session, {datagram, Data}},
    {ok, State}.

handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

handle_info(drain_now, State) ->
    {ok, State, [drain_session]};
handle_info(close_now, State) ->
    {ok, State, [{close_session, 42, <<"bye">>}]};
%% `send_bad_stream' tries to send on a stream id the session does not
%% know about, forcing `{error, unknown_stream}' from `do_send/4'. The
%% ensuing call to `handle_action_failed/3' is what the CT case asserts on.
handle_info(send_bad_stream, State) ->
    {ok, State, [{send, 99999, <<"nope">>, fin}]};
handle_info(_Other, State) ->
    {ok, State}.

handle_action_failed(Action, Reason,
                     #state{failure_mode = continue} = State) ->
    _ = Action, _ = Reason,
    {ok, State};
handle_action_failed(Action, Reason,
                     #state{failure_mode = stop} = State) ->
    {stop, {action_failed, Action, Reason}, State};
handle_action_failed(Action, Reason,
                     #state{failure_mode = forward,
                            owner = Owner,
                            session = Session} = State) ->
    Owner ! {webtransport, Session, {action_failed, Action, Reason}},
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
