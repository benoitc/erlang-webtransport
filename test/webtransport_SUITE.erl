%% @doc WebTransport E2E test suite.
%%
%% This Common Test suite runs integration tests for WebTransport
%% over HTTP/3.
%%
-module(webtransport_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("webtransport.hrl").

%% CT callbacks
-export([all/0, groups/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% Basic tests
    connect_disconnect_test/1,
    session_info_test/1,

    %% Stream tests
    open_bidi_stream_test/1,
    open_uni_stream_test/1,
    bidi_echo_test/1,
    bidi_large_data_test/1,
    multi_stream_test/1,
    close_stream_test/1,

    %% Datagram tests
    datagram_echo_test/1,
    datagram_large_test/1,

    %% Flow control tests
    stream_limit_test/1,

    %% Error handling tests
    reset_stream_test/1,
    stop_sending_test/1,

    %% Session management tests
    drain_session_test/1,
    close_session_with_error_test/1,

    %% Round-trip assertion tests
    datagram_round_trip_test/1,
    action_drain_test/1,
    action_close_test/1,
    action_failure_continue_test/1,
    action_failure_forward_test/1,
    action_failure_stop_test/1,
    datagram_oversize_test/1,
    datagram_boundary_test/1,
    bidi_round_trip_test/1,
    server_initiated_bidi_test/1,
    close_session_reason_too_long_test/1,
    origin_check_reject_test/1,

    %% Listener lifecycle
    stop_listener_does_not_kill_caller_test/1,
    listener_info_lookup_test/1,
    listeners_lists_active_listener_test/1,
    start_listener_invalid_certfile_test/1,
    start_listener_invalid_keyfile_test/1,
    start_listener_bad_pem_test/1
]).

%% ============================================================================
%% CT Callbacks
%% ============================================================================

all() ->
    [
        {group, h3_tests},
        {group, h2_tests}
    ].

groups() ->
    Cases = [
        close_session_reason_too_long_test,
        origin_check_reject_test,
        connect_disconnect_test,
        session_info_test,
        open_bidi_stream_test,
        open_uni_stream_test,
        bidi_echo_test,
        bidi_large_data_test,
        multi_stream_test,
        close_stream_test,
        datagram_echo_test,
        datagram_large_test,
        stream_limit_test,
        reset_stream_test,
        stop_sending_test,
        drain_session_test,
        close_session_with_error_test,
        datagram_round_trip_test,
        bidi_round_trip_test,
        action_drain_test,
        action_close_test,
        action_failure_continue_test,
        action_failure_forward_test,
        action_failure_stop_test,
        datagram_oversize_test,
        datagram_boundary_test,
        server_initiated_bidi_test,
        stop_listener_does_not_kill_caller_test,
        listener_info_lookup_test,
        listeners_lists_active_listener_test,
        start_listener_invalid_certfile_test,
        start_listener_invalid_keyfile_test,
        start_listener_bad_pem_test
    ],
    [
        {h3_tests, [sequence], Cases},
        {h2_tests, [sequence], Cases}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(h2),
    {ok, _} = application:ensure_all_started(quic),

    %% Generate test certificates
    PrivDir = proplists:get_value(priv_dir, Config),
    case test_helpers:generate_self_signed_cert(PrivDir) of
        {ok, #{certfile := CertFile, keyfile := KeyFile}} ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        {error, Reason} ->
            {skip, {cert_generation_failed, Reason}}
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(h3_tests, Config) ->
    start_listener_group(h3, test_h3_listener, Config);
init_per_group(h2_tests, Config) ->
    start_listener_group(h2, test_h2_listener, Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(h3_tests, _Config) ->
    webtransport:stop_listener(test_h3_listener),
    ok;
end_per_group(h2_tests, _Config) ->
    webtransport:stop_listener(test_h2_listener),
    ok;
end_per_group(_Group, _Config) ->
    ok.

start_listener_group(Transport, Name, Config) ->
    Port = test_helpers:find_free_port(),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    ListenerOpts = #{
        transport => Transport,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => wt_echo_handler
    },
    case webtransport:start_listener(Name, ListenerOpts) of
        {ok, _Pid} ->
            timer:sleep(100),
            [{port, Port}, {transport, Transport} | Config];
        {error, Reason} ->
            {skip, {listener_start_failed, Reason}}
    end.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% ============================================================================
%% Basic Tests
%% ============================================================================

connect_disconnect_test(Config) ->
    Port = proplists:get_value(port, Config),

    %% Connect
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    ?assert(is_pid(Session)),
    ?assert(is_process_alive(Session)),

    %% Close session
    ok = webtransport:close_session(Session),

    %% Give time for cleanup
    timer:sleep(100),
    ?assertNot(is_process_alive(Session)).

session_info_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Get session info
    {ok, Info} = webtransport:session_info(Session),

    ?assert(is_map(Info)),
    ?assertEqual(proplists:get_value(transport, Config), maps:get(transport, Info)),
    ?assert(maps:is_key(stream_count, Info)),
    ?assert(maps:is_key(local_max_data, Info)),
    ?assert(maps:is_key(remote_max_data, Info)),

    webtransport:close_session(Session).

%% ============================================================================
%% Stream Tests
%% ============================================================================

open_bidi_stream_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Open bidirectional stream
    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    ?assert(is_integer(StreamId)),
    ?assert(StreamId >= 0),

    webtransport:close_session(Session).

open_uni_stream_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Open unidirectional stream
    {ok, StreamId} = webtransport:open_stream(Session, uni),

    ?assert(is_integer(StreamId)),
    ?assert(StreamId >= 0),

    webtransport:close_session(Session).

bidi_echo_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Send data
    TestData = <<"Hello, WebTransport!">>,
    ok = webtransport:send(Session, StreamId, TestData, fin),

    %% Wait for echo response
    %% The echo handler should send the data back
    timer:sleep(500),

    webtransport:close_session(Session).

bidi_large_data_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Send 100KB of data
    LargeData = test_helpers:random_data(100 * 1024),
    ok = webtransport:send(Session, StreamId, LargeData, fin),

    %% Wait for processing
    timer:sleep(1000),

    webtransport:close_session(Session).

multi_stream_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Open multiple streams
    {ok, Stream1} = webtransport:open_stream(Session, bidi),
    {ok, Stream2} = webtransport:open_stream(Session, bidi),
    {ok, Stream3} = webtransport:open_stream(Session, uni),

    %% Verify they're different
    ?assertNotEqual(Stream1, Stream2),
    ?assertNotEqual(Stream2, Stream3),
    ?assertNotEqual(Stream1, Stream3),

    %% Send data on each
    ok = webtransport:send(Session, Stream1, <<"stream1">>, nofin),
    ok = webtransport:send(Session, Stream2, <<"stream2">>, nofin),
    ok = webtransport:send(Session, Stream3, <<"stream3">>, nofin),

    webtransport:close_session(Session).

close_stream_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Send some data
    ok = webtransport:send(Session, StreamId, <<"data">>, nofin),

    %% Close the stream
    ok = webtransport:close_stream(Session, StreamId),

    %% Sending more data should fail
    ?assertMatch({error, _}, webtransport:send(Session, StreamId, <<"more">>, nofin)),

    webtransport:close_session(Session).

%% ============================================================================
%% Datagram Tests
%% ============================================================================

datagram_echo_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Send datagram
    TestData = <<"ping">>,
    ok = webtransport:send_datagram(Session, TestData),

    %% Datagrams are unreliable, just verify send succeeds
    timer:sleep(200),

    webtransport:close_session(Session).

datagram_large_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Send larger datagram (within MTU limits)
    TestData = test_helpers:random_data(1000),
    ok = webtransport:send_datagram(Session, TestData),

    timer:sleep(200),

    webtransport:close_session(Session).

%% ============================================================================
%% Flow Control Tests
%% ============================================================================

stream_limit_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Open many streams up to the limit
    %% Default limit is 100 per type
    Streams = [webtransport:open_stream(Session, bidi) || _ <- lists:seq(1, 50)],

    %% Verify all succeeded
    lists:foreach(fun({ok, _}) -> ok end, Streams),

    webtransport:close_session(Session).

%% ============================================================================
%% Error Handling Tests
%% ============================================================================

reset_stream_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Send some data
    ok = webtransport:send(Session, StreamId, <<"data">>, nofin),

    %% Reset the stream
    ok = webtransport:reset_stream(Session, StreamId, 1),

    timer:sleep(200),

    webtransport:close_session(Session).

stop_sending_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Request peer to stop sending
    ok = webtransport:stop_sending(Session, StreamId, 1),

    timer:sleep(200),

    webtransport:close_session(Session).

%% ============================================================================
%% Session Management Tests
%% ============================================================================

drain_session_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),

    %% Drain the session
    ok = webtransport:drain_session(Session),

    %% Opening new streams should fail
    ?assertMatch({error, session_draining}, webtransport:open_stream(Session, bidi)),

    %% But existing streams should still work
    ok = webtransport:send(Session, StreamId, <<"final data">>, fin),

    timer:sleep(200).

close_session_with_error_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),

    %% Close with error code and reason
    ok = webtransport:close_session(Session, 42, <<"test error">>),

    timer:sleep(100),
    ?assertNot(is_process_alive(Session)).

%% ============================================================================
%% Round-Trip Assertion Tests
%% ============================================================================

datagram_round_trip_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self()}
    }),

    Payload = <<"datagram-ping">>,
    ok = webtransport:send_datagram(Session, Payload),

    receive
        {webtransport, Session, {datagram, Payload}} -> ok
    after 2000 ->
        error(no_datagram_echo)
    end,

    webtransport:close_session(Session).

bidi_round_trip_test(Config) ->
    Port = proplists:get_value(port, Config),

    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self()}
    }),

    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    Payload = <<"ping">>,
    ok = webtransport:send(Session, StreamId, Payload, fin),

    Echoed = collect_stream_echo(Session, StreamId, <<>>, 2000),
    ?assertEqual(Payload, Echoed),

    webtransport:close_session(Session).

%% A handler returning the action `drain_session' should drive the session
%% into the `draining' state. We trigger it via `handle_info' on the
%% client-side session so the transition is locally observable: subsequent
%% attempts to open a stream on the session must fail with
%% `session_draining'.
action_drain_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self()}
    }, wt_trigger_handler),

    Session ! drain_now,
    wait_for_draining(Session, 2000),
    ?assertMatch({error, session_draining},
                 webtransport:open_stream(Session, bidi)),

    webtransport:close_session(Session).

%% A handler returning the action `{close_session, Code, Reason}' should
%% stop the gen_statem. Asserted by waiting for the session pid to exit.
action_close_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self()}
    }, wt_trigger_handler),

    MRef = erlang:monitor(process, Session),
    Session ! close_now,
    receive
        {'DOWN', MRef, process, Session, _Reason} -> ok
    after 2000 ->
        error({session_did_not_stop, Session})
    end.

wait_for_draining(_Session, Timeout) when Timeout =< 0 ->
    ok;
wait_for_draining(Session, Timeout) ->
    case webtransport:open_stream(Session, bidi) of
        {error, session_draining} ->
            ok;
        _ ->
            timer:sleep(50),
            wait_for_draining(Session, Timeout - 50)
    end.

%% A handler exporting `handle_action_failed/3' that returns `{ok, State}'
%% keeps the session running and lets subsequent actions succeed.
action_failure_continue_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self(), failure_mode => continue}
    }, wt_trigger_handler),

    %% Trigger a send on an unknown stream → do_send returns
    %% {error, unknown_stream} → handle_action_failed/3 returns {ok, _}.
    Session ! send_bad_stream,
    timer:sleep(100),
    ?assert(is_process_alive(Session)),

    %% And the session is still usable.
    {ok, _StreamId} = webtransport:open_stream(Session, bidi),
    webtransport:close_session(Session).

%% `forward' mode relays the failure to the owner pid so the test can
%% observe the exact {Action, Reason} that failed.
action_failure_forward_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self(), failure_mode => forward}
    }, wt_trigger_handler),

    Session ! send_bad_stream,
    receive
        {webtransport, Session, {action_failed, {send, 99999, <<"nope">>, fin}, Reason}} ->
            ?assertEqual(unknown_stream, Reason)
    after 1000 ->
        error(action_failure_not_forwarded)
    end,
    webtransport:close_session(Session).

%% `stop' mode returns `{stop, _}' from the callback; the session
%% gen_statem terminates. We trap exits because `webtransport:connect/4,5'
%% uses `start_link' — otherwise the abnormal session exit propagates
%% here and kills the CT runner before the monitor message arrives.
action_failure_stop_test(Config) ->
    process_flag(trap_exit, true),
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        handler_opts => #{owner => self(), failure_mode => stop}
    }, wt_trigger_handler),

    MRef = erlang:monitor(process, Session),
    Session ! send_bad_stream,
    receive
        {'DOWN', MRef, process, Session, _Reason} -> ok
    after 2000 ->
        error({session_did_not_stop, Session})
    end,
    %% Drain the linked-exit signal so it doesn't leak into the next case.
    receive {'EXIT', Session, _} -> ok after 100 -> ok end,
    process_flag(trap_exit, false).

%% Oversize payload must return `{error, datagram_too_large}' synchronously
%% on both transports, not stall on flow control or crash the session.
datagram_oversize_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    Huge = test_helpers:random_data(128 * 1024),
    ?assertEqual({error, datagram_too_large},
                 webtransport:send_datagram(Session, Huge)),
    webtransport:close_session(Session).

%% h2 ceiling is deterministic (65471 bytes fit under HTTP/2 stream window
%% minus capsule framing). h3 is PMTU-bounded and varies per path, so we
%% only exercise the h2 boundary exactly; h3 uses a modest MTU-safe size.
datagram_boundary_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => Transport,
        verify => verify_none
    }),
    Size = case Transport of
               h2 -> ?WT_H2_DATAGRAM_MAX;
               h3 -> 1200
           end,
    Payload = test_helpers:random_data(Size),
    ?assertEqual(ok, webtransport:send_datagram(Session, Payload)),
    webtransport:close_session(Session).

%% A server handler can open a bidi stream back to the client. The client
%% sees it as a peer-initiated bidi and receives the payload through
%% `webtransport_client_handler' → owner messages. We run this against a
%% per-case listener so the rest of the suite keeps using the echo handler.
server_initiated_bidi_test(Config) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    PushPort = test_helpers:find_free_port(),
    PushName = list_to_atom("push_listener_" ++ atom_to_list(Transport)),
    {ok, _} = webtransport:start_listener(PushName, #{
        transport => Transport,
        port => PushPort,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => wt_server_push_handler
    }),
    try
        timer:sleep(100),
        {ok, Session} = webtransport:connect("localhost", PushPort, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),
        {ok, ClientStreamId} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, ClientStreamId, <<"push">>, fin),
        PushedStreamId = wait_for_pushed_bidi(Session, ClientStreamId, 3000),
        ?assertNotEqual(ClientStreamId, PushedStreamId),
        ok = webtransport:close_session(Session)
    after
        webtransport:stop_listener(PushName),
        %% stop_listener races exit signals to linked listener processes;
        %% let them land before we drain, otherwise one slips past the
        %% flush and CT reports the case as failed with `{'EXIT', shutdown}'.
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

flush_exits() ->
    receive {'EXIT', _, _} -> flush_exits() after 0 -> ok end.

%% Accumulate bytes per stream id until we see a FIN on a stream that is
%% not the one the client opened; assert that it carries the expected
%% pushed payload and return that stream id.
wait_for_pushed_bidi(Session, ClientStreamId, Timeout) ->
    wait_for_pushed_bidi(Session, ClientStreamId, #{}, Timeout).

wait_for_pushed_bidi(Session, ClientStreamId, Acc, Timeout) ->
    receive
        {webtransport, Session, {stream, StreamId, _Type, Data}} ->
            Buf = maps:get(StreamId, Acc, <<>>),
            wait_for_pushed_bidi(Session, ClientStreamId,
                                 Acc#{StreamId => <<Buf/binary, Data/binary>>},
                                 Timeout);
        {webtransport, Session, {stream_fin, StreamId, bidi, Data}}
          when StreamId =/= ClientStreamId ->
            Buf = maps:get(StreamId, Acc, <<>>),
            ?assertEqual(<<"pushed-from-server">>, <<Buf/binary, Data/binary>>),
            StreamId;
        {webtransport, Session, {stream_fin, StreamId, _Type, Data}} ->
            Buf = maps:get(StreamId, Acc, <<>>),
            wait_for_pushed_bidi(Session, ClientStreamId,
                                 Acc#{StreamId => <<Buf/binary, Data/binary>>},
                                 Timeout)
    after Timeout ->
        error({no_pushed_bidi, Acc})
    end.

%% Reason strings over 1024 bytes must be refused at the public API,
%% matching draft-14 §4.6 and draft-15 §5.
close_session_reason_too_long_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    Too = binary:copy(<<"x">>, 1025),
    ?assertEqual({error, reason_too_long},
                 webtransport:close_session(Session, 1, Too)),
    ?assert(is_process_alive(Session)),
    ok = webtransport:close_session(Session).

%% The handler's terminate/2 must receive the close code + reason when the
%% session closes with an error instead of a bare `normal'.
%% A handler exporting origin_check/2 can refuse the session before
%% init/3 runs. The client receives the rejection status and must not
%% observe a running session.
origin_check_reject_test(Config) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom("reject_listener_" ++ atom_to_list(Transport)),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => wt_origin_reject_handler
    }),
    try
        timer:sleep(100),
        Res = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none
        }),
        ?assertMatch({error, _}, Res)
    after
        webtransport:stop_listener(Name),
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

collect_stream_echo(Session, StreamId, Acc, Timeout) ->
    receive
        {webtransport, Session, {stream, StreamId, _Type, Data}} ->
            collect_stream_echo(Session, StreamId, <<Acc/binary, Data/binary>>, Timeout);
        {webtransport, Session, {stream_fin, StreamId, _Type, Data}} ->
            <<Acc/binary, Data/binary>>
    after Timeout ->
        error({no_stream_echo, Acc})
    end.

%% ============================================================================
%% Listener Lifecycle Regression
%% ============================================================================

%% Regression: start_listener used to spawn_link the listener loop to the
%% caller, so stop_listener (via exit(Pid, shutdown)) propagated to the
%% caller. Verify the caller survives a full start/stop cycle.
stop_listener_does_not_kill_caller_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    Name = list_to_atom("regression_listener_" ++ atom_to_list(Transport)),
    Port = test_helpers:find_free_port(),
    Parent = self(),

    Child = spawn(fun() ->
        ListenerOpts = #{
            transport => Transport,
            port => Port,
            certfile => CertFile,
            keyfile => KeyFile,
            handler => wt_echo_handler
        },
        case webtransport:start_listener(Name, ListenerOpts) of
            {ok, _Pid} ->
                Parent ! {self(), started},
                receive stop -> ok after 5000 -> ok end,
                StopRes = webtransport:stop_listener(Name),
                Parent ! {self(), {stopped, StopRes}},
                timer:sleep(200);
            {error, Reason} ->
                Parent ! {self(), {start_failed, Reason}}
        end
    end),

    MRef = erlang:monitor(process, Child),

    receive
        {Child, started} -> ok;
        {Child, {start_failed, R}} -> ct:fail({start_failed, R});
        {'DOWN', MRef, process, Child, Reason} ->
            ct:fail({child_died_during_start, Reason})
    after 5000 ->
        ct:fail(start_timeout)
    end,

    Child ! stop,

    receive
        {Child, {stopped, ok}} -> ok;
        {Child, {stopped, Other}} -> ct:fail({stop_listener_returned, Other});
        {'DOWN', MRef, process, Child, KillReason} ->
            ct:fail({caller_killed_by_stop_listener, KillReason})
    after 5000 ->
        ct:fail(stop_timeout)
    end,

    %% Caller must survive the stop_listener call without receiving an
    %% abnormal exit. The child finishes its sleep and exits normally —
    %% that is the success path. Anything else (shutdown, killed) means
    %% stop_listener leaked an exit signal to the caller.
    receive
        {'DOWN', MRef, process, Child, normal} ->
            ok;
        {'DOWN', MRef, process, Child, KillReason2} ->
            ct:fail({caller_killed_after_stop, KillReason2})
    after 1000 ->
        erlang:demonitor(MRef, [flush]),
        exit(Child, kill),
        ok
    end.

listener_info_lookup_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    Port = proplists:get_value(port, Config),
    Name = listener_name_for(Transport),

    {ok, Info} = webtransport:listener_info(Name),
    ?assertEqual(Transport, maps:get(transport, Info)),
    ?assertEqual(Port, maps:get(port, Info)),
    ?assert(maps:is_key(handler, Info)),
    %% server_ref must be hidden from the public view.
    ?assertNot(maps:is_key(server_ref, Info)),

    ?assertEqual({error, not_found}, webtransport:listener_info(no_such_listener)).

listeners_lists_active_listener_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    Name = listener_name_for(Transport),
    Active = webtransport:listeners(),
    ?assert(lists:member(Name, Active)).

start_listener_invalid_certfile_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    Result = webtransport:start_listener(bad_cert_listener, #{
        transport => Transport,
        port => test_helpers:find_free_port(),
        certfile => "/nonexistent/cert.pem",
        keyfile => KeyFile,
        handler => wt_echo_handler
    }),
    ?assertMatch({error, _}, Result).

start_listener_invalid_keyfile_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    CertFile = proplists:get_value(certfile, Config),
    Result = webtransport:start_listener(bad_key_listener, #{
        transport => Transport,
        port => test_helpers:find_free_port(),
        certfile => CertFile,
        keyfile => "/nonexistent/key.pem",
        handler => wt_echo_handler
    }),
    ?assertMatch({error, _}, Result).

start_listener_bad_pem_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    PrivDir = proplists:get_value(priv_dir, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    BadCert = filename:join(PrivDir, "bad-cert.pem"),
    ok = file:write_file(BadCert, <<"not a pem file">>),
    Result = webtransport:start_listener(bad_pem_listener, #{
        transport => Transport,
        port => test_helpers:find_free_port(),
        certfile => BadCert,
        keyfile => KeyFile,
        handler => wt_echo_handler
    }),
    ?assertMatch({error, _}, Result).

listener_name_for(h3) -> test_h3_listener;
listener_name_for(h2) -> test_h2_listener.
