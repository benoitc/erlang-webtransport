%% @doc WebTransport interoperability test client.
%%
%% This client runs interop test cases against a WebTransport server.
%% Each test case asserts the echoed / served payload against the expected
%% on-disk content; sleep-only passes are not accepted.
%%
-module(interop_client).

-export([run/0]).

-define(WWW_DIR, "/app/www").
-define(STREAM_TIMEOUT_MS, 5000).
-define(DATAGRAM_TIMEOUT_MS, 3000).

%% ============================================================================
%% Client Entry Point
%% ============================================================================

run() ->
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
    with_session(Host, Port, fun(Session) ->
        transfer_file(Session, <<"/small.txt">>)
    end);

run_testcase('transfer-bidirectional', Host, Port) ->
    with_session(Host, Port, fun(Session) ->
        bidi_transfer(Session, <<"/small.txt">>)
    end);

run_testcase('transfer-unidirectional', Host, Port) ->
    with_session(Host, Port, fun(Session) ->
        uni_transfer(Session, <<"/small.txt">>)
    end);

run_testcase('transfer-datagram', Host, Port) ->
    with_session(Host, Port, fun(Session) ->
        datagram_transfer(Session)
    end);

run_testcase(Unknown, _Host, _Port) ->
    {error, {unknown_testcase, Unknown}}.

%% ============================================================================
%% Internal: Session Lifecycle
%% ============================================================================

connect(Host, Port) ->
    CompatMode = case os:getenv("COMPAT") of
        "legacy" -> legacy_browser_compat;
        "legacy_browser_compat" -> legacy_browser_compat;
        _ -> latest
    end,
    webtransport:connect(Host, Port, <<"/interop">>, #{
        transport => h3,
        verify => verify_none,
        timeout => 10000,
        compat_mode => CompatMode,
        handler_opts => #{owner => self()}
    }).

with_session(Host, Port, Fun) ->
    case connect(Host, Port) of
        {ok, Session} ->
            try
                Fun(Session)
            after
                webtransport:close_session(Session)
            end;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end.

%% ============================================================================
%% Internal: Test Bodies
%% ============================================================================

transfer_file(Session, Path) ->
    io:format("  Opening bidi stream and requesting ~s~n", [Path]),
    case webtransport:open_stream(Session, bidi) of
        {ok, StreamId} ->
            Request = interop:format_request(Path),
            ok = webtransport:send(Session, StreamId, Request, fin),
            io:format("  Waiting for response...~n"),
            collect_and_verify(Session, StreamId, Path, ?STREAM_TIMEOUT_MS);
        {error, Reason} ->
            {error, {open_stream_failed, Reason}}
    end.

bidi_transfer(Session, Path) ->
    io:format("  Testing bidirectional stream transfer~n"),
    transfer_file(Session, Path).

uni_transfer(Session, Path) ->
    io:format("  Testing unidirectional stream transfer~n"),
    case webtransport:open_stream(Session, uni) of
        {ok, StreamId} ->
            Request = interop:format_request(Path),
            ok = webtransport:send(Session, StreamId, Request, fin),
            %% Server replies on a new peer-initiated uni stream (it can't
            %% write on our client-initiated uni). Collect the first such
            %% response and verify it matches the requested file.
            await_peer_uni(Session, Path, ?STREAM_TIMEOUT_MS);
        {error, Reason} ->
            {error, {open_stream_failed, Reason}}
    end.

datagram_transfer(Session) ->
    io:format("  Testing datagram transfer~n"),
    Payload = <<"ping">>,
    ok = webtransport:send_datagram(Session, Payload),
    receive
        {webtransport, Session, {datagram, Payload}} ->
            ok;
        {webtransport, Session, {datagram, Other}} ->
            {error, {datagram_mismatch, Payload, Other}}
    after ?DATAGRAM_TIMEOUT_MS ->
        {error, datagram_timeout}
    end.

%% ============================================================================
%% Internal: Stream Collection + Verification
%% ============================================================================

collect_and_verify(Session, StreamId, Path, Timeout) ->
    case collect_stream(Session, StreamId, <<>>, Timeout) of
        {ok, Raw} ->
            verify_response(Path, Raw);
        {error, _} = Err ->
            Err
    end.

collect_stream(Session, StreamId, Acc, Timeout) ->
    receive
        {webtransport, Session, {stream, StreamId, _Type, Data}} ->
            collect_stream(Session, StreamId, <<Acc/binary, Data/binary>>, Timeout);
        {webtransport, Session, {stream_fin, StreamId, _Type, Data}} ->
            {ok, <<Acc/binary, Data/binary>>};
        %% Peer closed the stream without flagging fin on the last data
        %% frame (quic-go closes uni replies this way). Treat close as
        %% end-of-data for reliable reads.
        {webtransport, Session, {stream_closed, StreamId, _Reason}} ->
            {ok, Acc}
    after Timeout ->
        {error, {stream_timeout, byte_size(Acc)}}
    end.

await_peer_uni(Session, Path, Timeout) ->
    receive
        {webtransport, Session, {stream_fin, StreamId, uni, Data}} ->
            verify_response(Path, Data);
        {webtransport, Session, {stream, StreamId, uni, Data}} ->
            case collect_stream(Session, StreamId, Data, Timeout) of
                {ok, Raw} -> verify_response(Path, Raw);
                {error, _} = Err -> Err
            end
    after Timeout ->
        {error, uni_response_timeout}
    end.

verify_response(Path, Raw) ->
    case interop:parse_response(Raw) of
        {ok, _Filename, Payload} ->
            verify_content(Path, Payload);
        {error, Reason} ->
            {error, {parse_response, Reason, byte_size(Raw)}}
    end.

verify_content(<<"/", Rel/binary>>, Payload) ->
    File = filename:join(?WWW_DIR, binary_to_list(Rel)),
    case file:read_file(File) of
        {ok, Payload} ->
            io:format("  Verified ~p bytes match ~s~n", [byte_size(Payload), File]),
            ok;
        {ok, Expected} ->
            {error, {content_mismatch,
                     byte_size(Expected), byte_size(Payload)}};
        {error, Reason} ->
            {error, {load_expected_failed, File, Reason}}
    end.
