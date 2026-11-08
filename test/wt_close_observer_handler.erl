%% @doc Test handler: on a stream that ends with "close" or "drain" the
%% session closes (code 5) or drains; other data is echoed. Its terminate
%% reason is sent to the owner.
-module(wt_close_observer_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_stream_fin/4, handle_datagram/2,
         handle_stream_closed/3, terminate/2]).

init(Session, _Req, Opts) ->
    {ok, #{session => Session, owner => maps:get(owner, Opts, undefined)}}.

handle_stream(StreamId, uni, <<"stopme">>, State) ->
    {ok, State, [{stop_sending, StreamId, 9}]};
handle_stream(_StreamId, _Type, _Data, State) ->
    {ok, State}.

handle_stream_fin(_StreamId, _Type, <<"close">>, State) ->
    {ok, State, [{close_session, 5, <<"bye">>}]};
handle_stream_fin(StreamId, bidi, <<"drain">>, State) ->
    %% Finish our side of this stream, then drain: the session stops once
    %% every stream is done.
    {ok, State, [{send, StreamId, <<>>, fin}, drain_session]};
handle_stream_fin(_StreamId, _Type, <<"drain">>, State) ->
    {ok, State, [drain_session]};
handle_stream_fin(_StreamId, _Type, <<"stop">>, State) ->
    {stop, normal, State};
handle_stream_fin(StreamId, bidi, Data, State) ->
    {ok, State, [{send, StreamId, Data, fin}]};
handle_stream_fin(_StreamId, _Type, _Data, State) ->
    {ok, State}.

handle_datagram(_Data, State) ->
    {ok, State}.

handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

terminate(Reason, #{owner := Owner, session := Session}) when is_pid(Owner) ->
    Owner ! {terminated, Session, Reason},
    ok;
terminate(_Reason, _State) ->
    ok.
