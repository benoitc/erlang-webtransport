%% @doc Test handler whose init returns actions: a datagram and a new bidi
%% stream. They must go out after the session is established.
-module(wt_init_actions_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_datagram/2, handle_stream_closed/3,
         terminate/2]).

init(_Session, _Req, _Opts) ->
    {ok, #{}, [{send_datagram, <<"hello from init">>}, {open_stream, bidi}]}.

handle_stream(_StreamId, _Type, _Data, State) ->
    {ok, State}.

handle_datagram(_Data, State) ->
    {ok, State}.

handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
