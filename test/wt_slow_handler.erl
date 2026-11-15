%% @doc Test handler that takes 20 ms per chunk of stream data, as one
%% handing work to a slow consumer would. It tells `owner' its session.
-module(wt_slow_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_datagram/2, handle_stream_closed/3,
         terminate/2]).

init(Session, _Req, #{owner := Owner}) ->
    Owner ! {slow_session, Session},
    {ok, Owner}.

handle_stream(_StreamId, _Type, _Data, Owner) ->
    timer:sleep(20),
    {ok, Owner}.

handle_datagram(_Data, Owner) ->
    {ok, Owner}.

handle_stream_closed(_StreamId, _Reason, Owner) ->
    {ok, Owner}.

terminate(_Reason, _Owner) ->
    ok.
