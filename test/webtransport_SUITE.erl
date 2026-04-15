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
    bidi_round_trip_test/1
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
        datagram_boundary_test
    ],
    [
        {h3_tests, [sequence], Cases},
        {h2_tests, [sequence], Cases}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(ssl),
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

collect_stream_echo(Session, StreamId, Acc, Timeout) ->
    receive
        {webtransport, Session, {stream, StreamId, _Type, Data}} ->
            collect_stream_echo(Session, StreamId, <<Acc/binary, Data/binary>>, Timeout);
        {webtransport, Session, {stream_fin, StreamId, _Type, Data}} ->
            <<Acc/binary, Data/binary>>
    after Timeout ->
        error({no_stream_echo, Acc})
    end.
