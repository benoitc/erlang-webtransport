%% @doc Handler for Chrome WebTransport interop tests.
-module(chrome_test_handler).
-behaviour(webtransport_handler).

-export([init/2, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3,
         terminate/2]).

-record(state, {
    session :: pid(),
    test_results = [] :: list()
}).

%% ============================================================================
%% Handler Callbacks
%% ============================================================================

init(Session, Request) ->
    io:format("Chrome client connected: ~p~n", [maps:get(path, Request, <<"/">>)]),
    {ok, #state{session = Session}}.

handle_stream(StreamId, Type, Data, State) ->
    io:format("Stream ~p (~p) data: ~p~n", [StreamId, Type, Data]),

    %% Echo back for bidirectional streams
    case Type of
        bidi ->
            {ok, State, [{send, StreamId, Data}]};
        uni ->
            {ok, State}
    end.

handle_stream_fin(StreamId, Type, Data, State) ->
    io:format("Stream ~p (~p) FIN with data: ~p~n", [StreamId, Type, Data]),

    case Type of
        bidi ->
            {ok, State, [{send, StreamId, Data, fin}]};
        uni ->
            {ok, State}
    end.

handle_datagram(Data, State) ->
    io:format("Datagram received: ~p~n", [Data]),
    %% Echo datagrams back
    {ok, State, [{send_datagram, Data}]}.

handle_stream_closed(StreamId, Reason, State) ->
    io:format("Stream ~p closed: ~p~n", [StreamId, Reason]),
    {ok, State}.

terminate(Reason, _State) ->
    io:format("Session terminated: ~p~n", [Reason]),
    ok.
