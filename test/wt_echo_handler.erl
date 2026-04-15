%% @doc Echo handler for WebTransport E2E tests.
%%
%% This handler implements the webtransport_handler behaviour and
%% echoes back any data received on streams or datagrams.
%%
-module(wt_echo_handler).
-behaviour(webtransport_handler).

-export([init/2, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3,
         terminate/2]).

-record(state, {
    session :: pid(),
    streams = #{} :: #{non_neg_integer() => binary()}
}).

%% @doc Initialize the handler.
init(Session, _Request) ->
    {ok, #state{session = Session}}.

%% @doc Handle data on a stream - buffer until FIN.
handle_stream(StreamId, Type, Data, #state{streams = Streams} = State) ->
    Existing = maps:get(StreamId, Streams, <<>>),
    NewStreams = Streams#{StreamId => <<Existing/binary, Data/binary>>},
    State1 = State#state{streams = NewStreams},
    case Type of
        bidi when Data =/= <<>> ->
            {ok, State1, [{send, StreamId, Data}]};
        _ ->
            {ok, State1}
    end.

%% @doc Handle FIN on a stream - send accumulated data back.
handle_stream_fin(StreamId, Type, Data, #state{streams = Streams} = State) ->
    Existing = maps:get(StreamId, Streams, <<>>),
    AllData = <<Existing/binary, Data/binary>>,
    State1 = State#state{streams = maps:remove(StreamId, Streams)},
    case Type of
        bidi ->
            {ok, State1, [{send, StreamId, AllData, fin}]};
        uni ->
            {ok, State1}
    end.

%% @doc Handle datagram - echo it back.
handle_datagram(Data, State) ->
    {ok, State, [{send_datagram, Data}]}.

%% @doc Handle stream closed event.
handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

%% @doc Cleanup on terminate.
terminate(_Reason, _State) ->
    ok.
