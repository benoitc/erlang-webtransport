%% @doc Test handler that triggers action-driven state transitions via
%% `handle_info/2'. Sending `drain_now' or `close_now' directly to the
%% session pid exercises `drain_session' / `close_session' action
%% dispatch without needing a server-side echo round-trip.
-module(wt_trigger_handler).
-behaviour(webtransport_handler).

-export([init/2, init/3, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3, handle_info/2,
         terminate/2]).

-record(state, {
    owner :: pid(),
    session :: pid()
}).

init(Session, Req) ->
    init(Session, Req, #{}).

init(Session, _Req, Opts) ->
    Owner = maps:get(owner, Opts, self()),
    {ok, #state{owner = Owner, session = Session}}.

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
handle_info(_Other, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
