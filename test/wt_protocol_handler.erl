%% @doc Test handler that picks a WebTransport subprotocol.
-module(wt_protocol_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_datagram/2,
         handle_stream_closed/3, terminate/2]).
-export([select_protocol/2]).

init(_Session, _Req, _Opts) -> {ok, none}.
handle_stream(_S, _T, _D, State) -> {ok, State}.
handle_datagram(_D, State) -> {ok, State}.
handle_stream_closed(_S, _R, State) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%% Pick "chat/2" when offered, refuse "reject-me", else no protocol.
select_protocol(Offered, _Opts) ->
    case {lists:member(<<"chat/2">>, Offered), lists:member(<<"reject-me">>, Offered)} of
        {true, _} -> {ok, <<"chat/2">>};
        {_, true} -> {reject, 400};
        _ -> none
    end.
