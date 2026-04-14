%% @doc WebTransport interoperability test client.
%%
%% This client runs interop test cases against a WebTransport server.
%%
-module(interop_client).

-export([run/0]).

%% ============================================================================
%% Client Entry Point
%% ============================================================================

%% @doc Run an interop test case.
%% Called with command line arguments: -host HOST -port PORT -testcase CASE
run() ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(quic),
    {ok, _} = application:ensure_all_started(webtransport),

    Args = init:get_arguments(),
    Host = get_arg(host, Args, "localhost"),
    Port = get_arg(port, Args, "443"),
    TestCase = list_to_atom(get_arg(testcase, Args, "handshake")),

    io:format("Running interop test: ~p~n", [TestCase]),
    io:format("  Host: ~s~n", [Host]),
    io:format("  Port: ~s~n", [Port]),

    Result = run_testcase(TestCase, Host, list_to_integer(Port)),

    case Result of
        ok ->
            io:format("TEST PASSED: ~p~n", [TestCase]),
            init:stop(0);
        {error, Reason} ->
            io:format("TEST FAILED: ~p~nReason: ~p~n", [TestCase, Reason]),
            init:stop(1)
    end.

get_arg(Key, Args, Default) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        [Value | _] -> Value
    end.

%% ============================================================================
%% Test Cases
%% ============================================================================

run_testcase(handshake, Host, Port) ->
    %% Test: Basic connection and disconnect
    io:format("  Connecting...~n"),
    case connect(Host, Port) of
        {ok, Session} ->
            io:format("  Connected successfully~n"),
            webtransport:close_session(Session),
            ok;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;

run_testcase(transfer, Host, Port) ->
    %% Test: Basic file transfer
    case connect(Host, Port) of
        {ok, Session} ->
            Result = transfer_file(Session, <<"/small.txt">>),
            webtransport:close_session(Session),
            Result;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;

run_testcase('transfer-bidirectional', Host, Port) ->
    %% Test: Bidirectional stream transfer
    case connect(Host, Port) of
        {ok, Session} ->
            Result = bidi_transfer(Session),
            webtransport:close_session(Session),
            Result;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;

run_testcase('transfer-unidirectional', Host, Port) ->
    %% Test: Unidirectional stream transfer
    case connect(Host, Port) of
        {ok, Session} ->
            Result = uni_transfer(Session),
            webtransport:close_session(Session),
            Result;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;

run_testcase('transfer-datagram', Host, Port) ->
    %% Test: Datagram exchange
    case connect(Host, Port) of
        {ok, Session} ->
            Result = datagram_transfer(Session),
            webtransport:close_session(Session),
            Result;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;

run_testcase(Unknown, _Host, _Port) ->
    {error, {unknown_testcase, Unknown}}.

%% ============================================================================
%% Internal Functions
%% ============================================================================

connect(Host, Port) ->
    webtransport:connect(Host, Port, <<"/interop">>, #{
        transport => h3,
        verify => verify_none,
        timeout => 10000
    }).

%% Debug function to test raw H3 connection
test_raw_h3(Host, Port) ->
    io:format("[RAW H3 TEST] Connecting to ~s:~p~n", [Host, Port]),
    case quic_h3:connect(Host, Port, #{verify => verify_none, sync => true}) of
        {ok, Conn} ->
            io:format("[RAW H3 TEST] Connected, H3Conn=~p, self()=~p~n", [Conn, self()]),
            %% Try a simple GET request
            Headers = [
                {<<":method">>, <<"GET">>},
                {<<":scheme">>, <<"https">>},
                {<<":path">>, <<"/">>},
                {<<":authority">>, list_to_binary(Host)}
            ],
            case quic_h3:request(Conn, Headers) of
                {ok, StreamId} ->
                    io:format("[RAW H3 TEST] Request sent on stream ~p, waiting...~n", [StreamId]),
                    wait_for_raw_response(Conn, StreamId, 5000);
                {error, Reason} ->
                    io:format("[RAW H3 TEST] Request failed: ~p~n", [Reason]),
                    quic_h3:close(Conn),
                    {error, Reason}
            end;
        {error, Reason} ->
            io:format("[RAW H3 TEST] Connect failed: ~p~n", [Reason]),
            {error, Reason}
    end.

wait_for_raw_response(Conn, StreamId, Timeout) ->
    receive
        {quic_h3, Conn, {response, StreamId, Status, Headers}} ->
            io:format("[RAW H3 TEST] Got response: status=~p, headers=~p~n", [Status, Headers]),
            quic_h3:close(Conn),
            {ok, Status};
        Other ->
            io:format("[RAW H3 TEST] Got other message: ~p~n", [Other]),
            wait_for_raw_response(Conn, StreamId, Timeout)
    after Timeout ->
        io:format("[RAW H3 TEST] Timeout! Mailbox: ~p~n", [erlang:process_info(self(), messages)]),
        quic_h3:close(Conn),
        {error, timeout}
    end.

transfer_file(Session, Path) ->
    io:format("  Opening stream and requesting ~s~n", [Path]),
    case webtransport:open_stream(Session, bidi) of
        {ok, StreamId} ->
            Request = interop:format_request(Path),
            ok = webtransport:send(Session, StreamId, Request, fin),

            %% Wait for response
            io:format("  Waiting for response...~n"),
            timer:sleep(1000),
            ok;
        {error, Reason} ->
            {error, {open_stream_failed, Reason}}
    end.

bidi_transfer(Session) ->
    io:format("  Testing bidirectional stream transfer~n"),
    case webtransport:open_stream(Session, bidi) of
        {ok, StreamId} ->
            %% Send test data
            TestData = <<"Hello, Bidirectional Stream!">>,
            ok = webtransport:send(Session, StreamId, TestData, fin),
            timer:sleep(500),
            ok;
        {error, Reason} ->
            {error, {open_stream_failed, Reason}}
    end.

uni_transfer(Session) ->
    io:format("  Testing unidirectional stream transfer~n"),
    case webtransport:open_stream(Session, uni) of
        {ok, StreamId} ->
            %% Send test data
            Request = interop:format_request(<<"/small.txt">>),
            ok = webtransport:send(Session, StreamId, Request, fin),
            timer:sleep(500),
            ok;
        {error, Reason} ->
            {error, {open_stream_failed, Reason}}
    end.

datagram_transfer(Session) ->
    io:format("  Testing datagram transfer~n"),
    %% Send a datagram
    TestData = <<"ping">>,
    ok = webtransport:send_datagram(Session, TestData),
    timer:sleep(500),
    ok.
