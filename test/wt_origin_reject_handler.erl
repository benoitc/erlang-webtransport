%% @doc Test handler that rejects every connect via origin_check/2.
-module(wt_origin_reject_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3, terminate/2]).
-export([origin_check/2]).

init(_Session, _Req, _Opts) -> {ok, none}.
handle_stream(_S, _T, _D, State) -> {ok, State}.
handle_stream_fin(_S, _T, _D, State) -> {ok, State}.
handle_datagram(_D, State) -> {ok, State}.
handle_stream_closed(_S, _R, State) -> {ok, State}.
terminate(_Reason, _State) -> ok.

origin_check(_Headers, _Opts) ->
    {reject, 403, <<"origin not allowed">>}.
