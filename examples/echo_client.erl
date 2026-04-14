%% @doc WebTransport echo client example.
%%
%% Demonstrates connecting to a WebTransport server and using streams/datagrams.
%% Uses the default message-based handler.
%%
%% Usage:
%%   echo_client:test().
%%   echo_client:test(Host, Port).
%%
-module(echo_client).

%% API
-export([test/0, test/2]).
-export([connect/2, send_echo/3, send_datagram/2, close/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Run echo test against localhost:8443.
-spec test() -> ok | {error, term()}.
test() ->
    test("localhost", 8443).

%% @doc Run echo test against specified host and port.
-spec test(string() | binary(), inet:port_number()) -> ok | {error, term()}.
test(Host, Port) ->
    io:format("Connecting to ~s:~p...~n", [Host, Port]),

    case connect(Host, Port) of
        {ok, Session} ->
            run_tests(Session);
        {error, Reason} ->
            io:format("Connection failed: ~p~n", [Reason]),
            {error, Reason}
    end.

%% @doc Connect to a WebTransport server.
-spec connect(string() | binary(), inet:port_number()) -> {ok, webtransport:session()} | {error, term()}.
connect(Host, Port) ->
    webtransport:connect(Host, Port, <<"/echo">>, #{
        transport => h3,
        verify => verify_none
    }).

%% @doc Send data on a stream and wait for echo response.
-spec send_echo(webtransport:session(), webtransport:stream(), binary()) -> {ok, binary()} | {error, term()}.
send_echo(Session, Stream, Data) ->
    ok = webtransport:send(Session, Stream, Data),
    receive
        {webtransport, Session, {stream, Stream, _, Response}} ->
            {ok, Response};
        {webtransport, Session, {stream_fin, Stream, _, Response}} ->
            {ok, Response};
        {webtransport, Session, {stream_closed, Stream, Reason}} ->
            {error, {stream_closed, Reason}}
    after 5000 ->
        {error, timeout}
    end.

%% @doc Send a datagram and wait for echo response.
-spec send_datagram(webtransport:session(), binary()) -> {ok, binary()} | {error, term()}.
send_datagram(Session, Data) ->
    ok = webtransport:send_datagram(Session, Data),
    receive
        {webtransport, Session, {datagram, Response}} ->
            {ok, Response}
    after 5000 ->
        {error, timeout}
    end.

%% @doc Close the session.
-spec close(webtransport:session()) -> ok.
close(Session) ->
    webtransport:close_session(Session).

%%====================================================================
%% Internal functions
%%====================================================================

run_tests(Session) ->
    io:format("Connected!~n~n"),

    %% Test bidirectional stream
    io:format("=== Test 1: Bidirectional Stream ===~n"),
    case test_bidi_stream(Session) of
        ok -> io:format("PASSED~n~n");
        {error, Err1} -> io:format("FAILED: ~p~n~n", [Err1])
    end,

    %% Test unidirectional stream
    io:format("=== Test 2: Unidirectional Stream ===~n"),
    case test_uni_stream(Session) of
        ok -> io:format("PASSED~n~n");
        {error, Err2} -> io:format("FAILED: ~p~n~n", [Err2])
    end,

    %% Test datagram
    io:format("=== Test 3: Datagram ===~n"),
    case test_datagram(Session) of
        ok -> io:format("PASSED~n~n");
        {error, Err3} -> io:format("FAILED: ~p~n~n", [Err3])
    end,

    %% Clean up
    close(Session),
    io:format("All tests completed.~n"),
    ok.

test_bidi_stream(Session) ->
    io:format("Opening bidirectional stream...~n"),
    case webtransport:open_stream(Session, bidi) of
        {ok, Stream} ->
            io:format("Sending: Hello, WebTransport!~n"),
            case send_echo(Session, Stream, <<"Hello, WebTransport!">>) of
                {ok, Response} ->
                    io:format("Received: ~s~n", [Response]),
                    webtransport:close_stream(Session, Stream),
                    ok;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

test_uni_stream(Session) ->
    io:format("Opening unidirectional stream...~n"),
    case webtransport:open_stream(Session, uni) of
        {ok, Stream} ->
            io:format("Sending: One-way message~n"),
            ok = webtransport:send(Session, Stream, <<"One-way message">>, fin),
            io:format("Sent (no response expected for uni stream)~n"),
            ok;
        {error, _} = Err ->
            Err
    end.

test_datagram(Session) ->
    io:format("Sending datagram: ping~n"),
    case send_datagram(Session, <<"ping">>) of
        {ok, Response} ->
            io:format("Received: ~s~n", [Response]),
            ok;
        {error, _} = Err ->
            Err
    end.
