%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc Simple WebTransport echo server example.
%%
%% Demonstrates implementing the webtransport_handler behaviour.
%% Echoes stream data back to the client and responds to datagrams.
%%
%% Usage:
%%   echo_server:start().
%%   echo_server:start(Port).
%%   echo_server:stop().
%%
-module(echo_server).
-behaviour(webtransport_handler).

%% API
-export([start/0, start/1, stop/0]).

%% webtransport_handler callbacks
-export([init/3, handle_stream/4, handle_stream_fin/4]).
-export([handle_datagram/2, handle_stream_closed/3, terminate/2]).

-record(state, {
    session :: webtransport:session(),
    streams = #{} :: #{webtransport:stream() => stream_info()}
}).

-type stream_info() :: #{
    type := bidi | uni,
    buffer := binary()
}.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the echo server on port 8443.
-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    start(8443).

%% @doc Start the echo server on the specified port.
-spec start(inet:port_number()) -> {ok, pid()} | {error, term()}.
start(Port) ->
    io:format("Starting echo server on port ~p~n", [Port]),
    webtransport:start_listener(echo_server, #{
        transport => h3,
        port => Port,
        certfile => "cert.pem",
        keyfile => "key.pem",
        handler => ?MODULE
    }).

%% @doc Stop the echo server.
-spec stop() -> ok | {error, term()}.
stop() ->
    webtransport:stop_listener(echo_server).

%%====================================================================
%% webtransport_handler callbacks
%%====================================================================

init(Session, Req, _Opts) ->
    Path = maps:get(path, Req),
    Authority = maps:get(authority, Req),
    io:format("[~p] Session started: ~s~s~n", [Session, Authority, Path]),
    {ok, #state{session = Session}}.

handle_stream(Stream, Type, Data, State) ->
    #state{session = Session, streams = Streams} = State,
    io:format("[~p] Stream ~p (~p) data: ~p~n", [Session, Stream, Type, Data]),

    %% Echo the data back on bidirectional streams
    Actions = case Type of
        bidi -> [{send, Stream, <<"echo: ", Data/binary>>}];
        uni -> []  %% Can't send on incoming unidirectional streams
    end,

    %% Track the stream
    StreamInfo = maps:get(Stream, Streams, #{type => Type, buffer => <<>>}),
    NewBuffer = <<(maps:get(buffer, StreamInfo))/binary, Data/binary>>,
    NewStreams = Streams#{Stream => StreamInfo#{buffer => NewBuffer}},

    {ok, State#state{streams = NewStreams}, Actions}.

handle_stream_fin(Stream, Type, Data, State) ->
    #state{session = Session, streams = Streams} = State,
    io:format("[~p] Stream ~p (~p) FIN with data: ~p~n", [Session, Stream, Type, Data]),

    %% Echo final data and close
    Actions = case Type of
        bidi when Data =/= <<>> ->
            [{send, Stream, <<"echo: ", Data/binary>>, fin}];
        bidi ->
            [{close_stream, Stream}];
        uni ->
            []
    end,

    NewStreams = maps:remove(Stream, Streams),
    {ok, State#state{streams = NewStreams}, Actions}.

handle_datagram(Data, State) ->
    #state{session = Session} = State,
    io:format("[~p] Datagram: ~p~n", [Session, Data]),

    %% Echo the datagram back
    Response = <<"echo: ", Data/binary>>,
    {ok, State, [{send_datagram, Response}]}.

handle_stream_closed(Stream, Reason, State) ->
    #state{session = Session, streams = Streams} = State,
    io:format("[~p] Stream ~p closed: ~p~n", [Session, Stream, Reason]),
    NewStreams = maps:remove(Stream, Streams),
    {ok, State#state{streams = NewStreams}}.

terminate(Reason, #state{session = Session}) ->
    io:format("[~p] Session terminated: ~p~n", [Session, Reason]),
    ok.
