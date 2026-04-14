%% @doc WebTransport interoperability test server.
%%
%% This server implements the interop protocol for testing
%% WebTransport implementations.
%%
-module(interop_server).
-behaviour(webtransport_handler).

-export([start/0]).

%% Handler callbacks
-export([init/2, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3,
         terminate/2]).

-record(state, {
    session :: pid(),
    www_dir :: file:filename(),
    buffers = #{} :: #{non_neg_integer() => binary()}
}).

%% ============================================================================
%% Server Entry Point
%% ============================================================================

%% @doc Start the interop server.
%% Called with command line arguments: -port PORT -certfile FILE -keyfile FILE -www DIR
start() ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(quic),
    {ok, _} = application:ensure_all_started(webtransport),

    Args = init:get_arguments(),
    Port = get_arg(port, Args, "443"),
    CertFile = get_arg(certfile, Args, "/app/certs/cert.pem"),
    KeyFile = get_arg(keyfile, Args, "/app/certs/key.pem"),
    WwwDir = get_arg(www, Args, "/app/www"),

    io:format("Starting interop server on port ~s~n", [Port]),
    io:format("  CertFile: ~s~n", [CertFile]),
    io:format("  KeyFile: ~s~n", [KeyFile]),
    io:format("  WWW Dir: ~s~n", [WwwDir]),

    %% Store www_dir in persistent term for handler access
    persistent_term:put(interop_www_dir, WwwDir),

    %% Start the listener
    ListenerOpts = #{
        transport => h3,
        port => list_to_integer(Port),
        certfile => CertFile,
        keyfile => KeyFile,
        handler => ?MODULE
    },

    case webtransport:start_listener(interop_server, ListenerOpts) of
        {ok, _Pid} ->
            io:format("Server started successfully~n"),
            %% Keep the process alive
            receive
                stop -> ok
            end;
        {error, Reason} ->
            io:format("Failed to start server: ~p~n", [Reason]),
            init:stop(1)
    end.

get_arg(Key, Args, Default) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        [Value | _] -> Value
    end.

%% ============================================================================
%% Handler Callbacks
%% ============================================================================

init(Session, _Request) ->
    WwwDir = persistent_term:get(interop_www_dir, "/app/www"),
    {ok, #state{session = Session, www_dir = WwwDir}}.

%% Buffer incoming data until we get the full request
handle_stream(StreamId, Type, Data, #state{buffers = Buffers} = State) ->
    Existing = maps:get(StreamId, Buffers, <<>>),
    NewData = <<Existing/binary, Data/binary>>,

    %% Check if we have a complete request (ends with newline)
    case binary:match(NewData, <<"\n">>) of
        nomatch ->
            %% Not complete yet, buffer it
            {ok, State#state{buffers = Buffers#{StreamId => NewData}}};
        _ ->
            %% We have a complete request, process it
            process_request(StreamId, Type, NewData, State)
    end.

handle_stream_fin(StreamId, Type, Data, #state{buffers = Buffers} = State) ->
    Existing = maps:get(StreamId, Buffers, <<>>),
    FullData = <<Existing/binary, Data/binary>>,
    process_request(StreamId, Type, FullData, State).

handle_datagram(Data, State) ->
    %% Echo datagrams back
    {ok, State, [{send_datagram, Data}]}.

handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ============================================================================
%% Internal Functions
%% ============================================================================

process_request(StreamId, Type, Data, #state{www_dir = WwwDir, buffers = Buffers} = State) ->
    State1 = State#state{buffers = maps:remove(StreamId, Buffers)},

    case interop:parse_request(Data) of
        {ok, Path} ->
            FilePath = filename:join(WwwDir, binary_to_list(Path)),
            case file:read_file(FilePath) of
                {ok, Content} ->
                    Filename = filename:basename(FilePath),
                    Response = interop:format_response(list_to_binary(Filename), Content),
                    case Type of
                        bidi ->
                            {ok, State1, [{send, StreamId, Response, fin}]};
                        uni ->
                            %% For uni streams, we can't reply on the same stream
                            %% Open a new stream to send the response
                            {ok, State1, [{open_stream, uni, Response}]}
                    end;
                {error, _Reason} ->
                    ErrorResponse = <<"ERROR: File not found\n">>,
                    case Type of
                        bidi ->
                            {ok, State1, [{send, StreamId, ErrorResponse, fin}]};
                        uni ->
                            {ok, State1}
                    end
            end;
        {error, _Reason} ->
            ErrorResponse = <<"ERROR: Invalid request\n">>,
            case Type of
                bidi ->
                    {ok, State1, [{send, StreamId, ErrorResponse, fin}]};
                uni ->
                    {ok, State1}
            end
    end.
