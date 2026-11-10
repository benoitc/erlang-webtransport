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
    %% Client socket options
    client_socket_opts_bind_ip_test/1,
    client_socket_opts_tcp_only_ignored_test/1,
    client_socket_opts_h2_test/1,

    %% Basic tests
    connect_disconnect_test/1,
    session_info_test/1,

    %% Stream tests
    open_bidi_stream_test/1,
    open_uni_stream_test/1,
    bidi_echo_test/1,
    bidi_large_data_test/1,
    bidi_1mb_echo_test/1,
    large_window_echo_test/1,
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
    protocol_negotiation_test/1,
    flow_control_credit_test/1,
    server_close_reaches_client_test/1,
    init_actions_after_accept_test/1,
    stop_sending_on_own_uni_test/1,
    handler_stop_closes_session_test/1,
    drain_completes_test/1,
    reset_unseen_stream_test/1,
    close_stream_with_queue_test/1,
    server_drain_reaches_client_test/1,
    stream_credit_replenish_test/1,
    server_without_wt_enabled_test/1,
    invalid_webtransport_init_test/1,
    client_refuses_tls12_server_test/1,
    h2_embedded_accept_round_trip_test/1,
    listener_refuses_tls12_client_test/1,
    accept_refuses_tls12_test/1,

    %% Embedding (accept/4 from a non-connection process)
    embedded_accept_round_trip_test/1,
    quic_stream_limit_credit_test/1,
    quic_uni_stream_limit_credit_test/1,

    %% Listener lifecycle
    stop_listener_does_not_kill_caller_test/1,
    listener_info_lookup_test/1,
    listeners_lists_active_listener_test/1,
    start_listener_invalid_certfile_test/1,
    start_listener_invalid_keyfile_test/1,
    start_listener_bad_pem_test/1,
    sni_callback_round_trip_test/1,

    %% Legacy drafts (draft-02, http3-07) against the default listener
    legacy_draft02_round_trip_test/1,
    legacy_draft07_round_trip_test/1
]).

%% ============================================================================
%% CT Callbacks
%% ============================================================================

all() ->
    [
        {group, h3_tests},
        {group, h2_tests},
        {group, h3_legacy_tests}
    ].

groups() ->
    Cases = [
        close_session_reason_too_long_test,
        origin_check_reject_test,
        protocol_negotiation_test,
        server_without_wt_enabled_test,
        flow_control_credit_test,
        server_close_reaches_client_test,
        init_actions_after_accept_test,
        stop_sending_on_own_uni_test,
        handler_stop_closes_session_test,
        drain_completes_test,
        reset_unseen_stream_test,
        close_stream_with_queue_test,
        server_drain_reaches_client_test,
        stream_credit_replenish_test,
        connect_disconnect_test,
        session_info_test,
        open_bidi_stream_test,
        open_uni_stream_test,
        bidi_echo_test,
        bidi_large_data_test,
        bidi_1mb_echo_test,
        large_window_echo_test,
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
        start_listener_bad_pem_test,
        sni_callback_round_trip_test
    ],
    %% Embedding via h3_settings/0 + accept/4 is HTTP/3-only.
    H3Only = [embedded_accept_round_trip_test, quic_stream_limit_credit_test,
              quic_uni_stream_limit_credit_test,
              client_socket_opts_bind_ip_test,
              client_socket_opts_tcp_only_ignored_test],
    %% WebTransport-Init is an HTTP/2 header.
    H2Only = [invalid_webtransport_init_test, client_refuses_tls12_server_test,
              h2_embedded_accept_round_trip_test,
              listener_refuses_tls12_client_test, accept_refuses_tls12_test,
              client_socket_opts_h2_test],
    [
        {h3_tests, [sequence], Cases ++ H3Only},
        {h2_tests, [sequence], Cases ++ H2Only},
        {h3_legacy_tests, [sequence], [legacy_draft02_round_trip_test,
                                       legacy_draft07_round_trip_test]}
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
init_per_group(h3_legacy_tests, Config) ->
    start_listener_group(h3, test_h3_legacy_listener, Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(h3_tests, _Config) ->
    webtransport:stop_listener(test_h3_listener),
    ok;
end_per_group(h2_tests, _Config) ->
    webtransport:stop_listener(test_h2_listener),
    ok;
end_per_group(h3_legacy_tests, _Config) ->
    webtransport:stop_listener(test_h3_legacy_listener),
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

%% A 1 MiB payload must come back byte for byte on both transports. On h2
%% the payload is one WT_STREAM capsule split over ~64 DATA frames, which
%% the CONNECT-stream reader has to reassemble (see h2_data_loop/6).
bidi_1mb_echo_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    Payload = test_helpers:random_data(1024 * 1024),
    ok = webtransport:send(Session, StreamId, Payload, fin),
    Echoed = collect_stream_echo(Session, StreamId, <<>>, 10000),
    %% wt_echo_handler echoes each chunk as it arrives and then the whole
    %% payload again at FIN, so the echo is <prefix of Payload> ++ Payload.
    Size = byte_size(Payload),
    ?assert(byte_size(Echoed) >= Size),
    PrefixLen = byte_size(Echoed) - Size,
    ?assertEqual(Payload, binary:part(Echoed, PrefixLen, Size)),
    ?assertEqual(binary:part(Payload, 0, PrefixLen), binary:part(Echoed, 0, PrefixLen)),
    webtransport:close_session(Session).

%% A peer granting large windows (Safari grants 8 MiB) lets the server
%% write a 2 MiB echo in one send; on h2 that is more than the HTTP/2
%% per-stream send buffer and must still arrive whole.
large_window_echo_test(Config) ->
    Port = proplists:get_value(port, Config),
    Big = 8 * 1024 * 1024,
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        max_data => Big,
        max_stream_data => Big
    }),
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    Payload = test_helpers:random_data(2 * 1024 * 1024),
    ok = webtransport:send(Session, StreamId, Payload, fin),
    Echoed = collect_stream_echo(Session, StreamId, <<>>, 10000),
    Size = byte_size(Payload),
    ?assert(byte_size(Echoed) >= Size),
    ?assertEqual(Payload, binary:part(Echoed, byte_size(Echoed) - Size, Size)),
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

%% After a reset the session stays up: on h2 the WT_RESET_STREAM carries a
%% Reliable Size equal to what we sent (http2-16 §6.2), so the server
%% accepts it. A second reset of the same send side is refused locally.
reset_stream_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    ok = webtransport:send(Session, StreamId, <<"data">>, nofin),
    ok = webtransport:reset_stream(Session, StreamId, 1),
    ?assertEqual({error, stream_send_side_closed},
                 webtransport:reset_stream(Session, StreamId, 1)),
    assert_session_usable(Session),
    webtransport:close_session(Session).

%% STOP_SENDING makes the peer reset its send side with our code
%% (http2-16 §6.3, RFC 9000 §3.5). A second STOP_SENDING for the same
%% stream is refused locally.
stop_sending_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    ok = webtransport:send(Session, StreamId, <<"hello">>, nofin),
    <<"hello">> = collect_until(Session, StreamId, 5),
    ok = webtransport:stop_sending(Session, StreamId, 7),
    receive
        {webtransport, Session, {stream_closed, StreamId, {reset, Code}}} ->
            ?assertEqual(7, Code)
    after 5000 ->
        error(no_reset_after_stop_sending)
    end,
    ?assertEqual({error, stream_receive_side_closed},
                 webtransport:stop_sending(Session, StreamId, 7)),
    assert_session_usable(Session),
    webtransport:close_session(Session).

%% A fresh stream still echoes.
assert_session_usable(Session) ->
    {ok, S} = webtransport:open_stream(Session, bidi),
    ok = webtransport:send(Session, S, <<"ping">>, fin),
    Echoed = collect_stream_echo(Session, S, <<>>, 5000),
    ?assertEqual(<<"ping">>, binary:part(Echoed, byte_size(Echoed) - 4, 4)).

collect_until(Session, StreamId, Size) ->
    collect_until(Session, StreamId, Size, <<>>).

collect_until(_Session, _StreamId, Size, Acc) when byte_size(Acc) >= Size ->
    Acc;
collect_until(Session, StreamId, Size, Acc) ->
    receive
        {webtransport, Session, {stream, StreamId, _, Data}} ->
            collect_until(Session, StreamId, Size, <<Acc/binary, Data/binary>>)
    after 5000 ->
        error({no_data, Acc})
    end.

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

%% Bad close arguments must be refused at the public API (http3-17 §6,
%% http2-16 §6.12).
close_session_reason_too_long_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    Too = binary:copy(<<"x">>, 1025),
    ?assertEqual({error, reason_too_long},
                 webtransport:close_session(Session, 1, Too)),
    %% http3-17 §6: the reason must be UTF-8 and the code 32 bits.
    ?assertEqual({error, invalid_reason},
                 webtransport:close_session(Session, 1, <<16#ff>>)),
    ?assertEqual({error, invalid_error_code},
                 webtransport:close_session(Session, 1 bsl 32, <<>>)),
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

%% http3-17 §3.3, http2-16 §3.3: the client offers protocols, the server
%% picks one of them, and the client refuses a missing pick when it
%% requires one.
protocol_negotiation_test(Config) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom("proto_listener_" ++ atom_to_list(Transport)),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => proplists:get_value(certfile, Config),
        keyfile => proplists:get_value(keyfile, Config),
        handler => wt_protocol_handler
    }),
    Connect = fun(Extra) ->
        webtransport:connect("localhost", Port, <<"/test">>,
                             maps:merge(#{transport => Transport,
                                          verify => verify_none}, Extra))
    end,
    try
        timer:sleep(100),
        {ok, S1} = Connect(#{protocols => [<<"chat/1">>, <<"chat/2">>]}),
        {ok, Info} = webtransport:session_info(S1),
        ?assertEqual(<<"chat/2">>, maps:get(protocol, Info)),
        ok = webtransport:close_session(S1),
        {ok, S2} = Connect(#{protocols => [<<"other">>]}),
        {ok, Info2} = webtransport:session_info(S2),
        ?assertNot(maps:is_key(protocol, Info2)),
        ok = webtransport:close_session(S2),
        ?assertEqual({error, {alpn_error, no_protocol}},
                     Connect(#{protocols => [<<"other">>], require_protocol => true})),
        ?assertMatch({error, {http_error, 400}},
                     Connect(#{protocols => [<<"reject-me">>]}))
    after
        webtransport:stop_listener(Name),
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

%% http3-17 §5.4, http2-16 §6.5/§6.6: with windows far smaller than the
%% payload, the echo only completes if each side keeps granting credit as
%% it consumes data (WT_MAX_DATA, and WT_MAX_STREAM_DATA on h2), and the
%% sender queues what does not fit instead of dropping it.
flow_control_credit_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none,
        max_data => 256,
        max_stream_data => 256
    }),
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    Payload = test_helpers:random_data(16 * 1024),
    ok = webtransport:send(Session, StreamId, Payload, fin),
    Echoed = collect_stream_echo(Session, StreamId, <<>>, 10000),
    Size = byte_size(Payload),
    ?assert(byte_size(Echoed) >= Size),
    ?assertEqual(Payload, binary:part(Echoed, byte_size(Echoed) - Size, Size)),
    webtransport:close_session(Session).

%% A WT_CLOSE_SESSION from the server reaches the client session with its
%% code and reason (http3-17 §6, http2-16 §6.12).
server_close_reaches_client_test(Config) ->
    with_observer_listener(close, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }, wt_close_observer_handler),
        {ok, S} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, S, <<"close">>, fin),
        receive
            {terminated, Session, Reason} ->
                ?assertEqual({closed, 5, <<"bye">>}, Reason)
        after 5000 ->
            error(client_session_not_closed)
        end
    end).

%% A WT_DRAIN_SESSION from the server puts the client session in draining:
%% new streams are refused, existing ones keep working (http3-17 §4.7).
server_drain_reaches_client_test(Config) ->
    with_observer_listener(drain, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }, wt_close_observer_handler),
        %% A stream the server knows and that stays open, so the drain has
        %% something to wait for.
        {ok, Keep} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, Keep, <<"keep ">>, nofin),
        {ok, S} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, S, <<"drain">>, fin),
        ok = wait_for_draining(Session, 5000),
        ?assertEqual({error, session_draining}, webtransport:open_stream(Session, bidi)),
        ok = webtransport:send(Session, Keep, <<"still here">>, fin),
        webtransport:close_session(Session)
    end).

%% Actions returned by a server handler's init go out once the session is
%% established, after the 2xx: the client sees the datagram and the stream.
init_actions_after_accept_test(Config) ->
    with_listener(init_actions, wt_init_actions_handler, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none
        }),
        receive
            {webtransport, Session, {datagram, D}} -> ?assertEqual(<<"hello from init">>, D)
        after 5000 ->
            error(no_datagram_from_init)
        end,
        {ok, Info} = webtransport:session_info(Session),
        %% overview-14 §4.2: the largest datagram we can send.
        ?assert(maps:get(max_datagram_size, Info) > 0),
        webtransport:close_session(Session)
    end).

%% The peer's STOP_SENDING on a uni stream we opened reaches the handler,
%% and the stream no longer accepts data.
stop_sending_on_own_uni_test(Config) ->
    with_observer_listener(stopme, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none
        }),
        {ok, U} = webtransport:open_stream(Session, uni),
        ok = webtransport:send(Session, U, <<"stopme">>, nofin),
        receive
            {webtransport, Session, {stream_closed, U, {stop_sending, 9}}} -> ok
        after 5000 ->
            error(no_stop_sending)
        end,
        ?assertMatch({error, _}, webtransport:send(Session, U, <<"more">>, nofin)),
        webtransport:close_session(Session)
    end).

%% A server handler that stops without closing still ends the session for
%% the client (http3-17 §6, http2-16 §3.4).
handler_stop_closes_session_test(Config) ->
    with_observer_listener(stop, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }, wt_close_observer_handler),
        {ok, S} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, S, <<"stop">>, fin),
        receive
            {terminated, Session, {closed, 0, _}} -> ok
        after 5000 ->
            error(client_session_not_closed)
        end
    end).

%% A draining server session stops once its streams are done, uni streams
%% included, and the client learns the session ended.
drain_completes_test(Config) ->
    with_observer_listener(drain_done, Config, fun(Port, Transport) ->
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }, wt_close_observer_handler),
        {ok, U} = webtransport:open_stream(Session, uni),
        ok = webtransport:send(Session, U, <<"uni data">>, fin),
        {ok, S} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, S, <<"drain">>, fin),
        %% The server's WT_DRAIN_SESSION drains the client too; with all its
        %% streams done the client session ends, whichever close comes first.
        receive
            {terminated, Session, _Reason} -> ok
        after 5000 ->
            error(drain_did_not_complete)
        end
    end).

%% A reset on a stream the peer has not seen yet opens it, as in QUIC
%% (http2-16 §5.2): the session stays up.
reset_unseen_stream_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    {ok, S} = webtransport:open_stream(Session, bidi),
    ok = webtransport:reset_stream(Session, S, 3),
    ?assertEqual({error, invalid_error_code},
                 webtransport:reset_stream(Session, S, 1 bsl 32)),
    assert_session_usable(Session),
    webtransport:close_session(Session).

%% close_stream after a send larger than the peer's window: the FIN waits
%% for the queued bytes, so the peer gets the whole stream.
close_stream_with_queue_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => proplists:get_value(transport, Config),
        verify => verify_none
    }),
    {ok, S} = webtransport:open_stream(Session, bidi),
    Payload = test_helpers:random_data(1536 * 1024),
    ok = webtransport:send(Session, S, Payload, nofin),
    ok = webtransport:close_stream(Session, S),
    Echoed = collect_stream_echo(Session, S, <<>>, 15000),
    Size = byte_size(Payload),
    ?assertEqual(Payload, binary:part(Echoed, byte_size(Echoed) - Size, Size)),
    webtransport:close_session(Session).

with_listener(Tag, Handler, Config, Fun) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom(lists:concat(["listener_", Tag, "_", Transport])),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => proplists:get_value(certfile, Config),
        keyfile => proplists:get_value(keyfile, Config),
        handler => Handler
    }),
    try
        timer:sleep(100),
        Fun(Port, Transport)
    after
        webtransport:stop_listener(Name),
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

with_observer_listener(Tag, Config, Fun) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom(lists:concat(["observer_", Tag, "_", Transport])),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => proplists:get_value(certfile, Config),
        keyfile => proplists:get_value(keyfile, Config),
        handler => wt_close_observer_handler
    }),
    try
        timer:sleep(100),
        Fun(Port, Transport)
    after
        webtransport:stop_listener(Name),
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

%% http3-17 §5.6.2, http2-16 §6.7: stream limits are cumulative. The server
%% allows 2 bidi streams and returns credit (WT_MAX_STREAMS) as each one
%% finishes, so the client can open many more over the session's life. A
%% client that hits the limit is told when credit comes back
%% (overview-14 §4.3).
stream_credit_replenish_test(Config) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom("credit_listener_" ++ atom_to_list(Transport)),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => proplists:get_value(certfile, Config),
        keyfile => proplists:get_value(keyfile, Config),
        handler => wt_echo_handler,
        max_streams_bidi => 2
    }),
    try
        timer:sleep(100),
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none
        }),
        {ok, Info} = webtransport:session_info(Session),
        ?assertEqual(2, maps:get(remote_max_streams_bidi, Info)),
        lists:foreach(fun(N) ->
            StreamId = open_bidi_when_allowed(Session),
            Payload = <<"msg ", (integer_to_binary(N))/binary>>,
            ok = webtransport:send(Session, StreamId, Payload, fin),
            Echoed = collect_stream_echo(Session, StreamId, <<>>, 5000),
            ?assertEqual(Payload, binary:part(Echoed, byte_size(Echoed) - byte_size(Payload),
                                              byte_size(Payload)))
        end, lists:seq(1, 10)),
        {ok, Info2} = webtransport:session_info(Session),
        ?assert(maps:get(remote_max_streams_bidi, Info2) >= 10),
        webtransport:close_session(Session)
    after
        webtransport:stop_listener(Name),
        timer:sleep(100),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

open_bidi_when_allowed(Session) ->
    case webtransport:open_stream(Session, bidi) of
        {ok, StreamId} ->
            StreamId;
        {error, stream_limit_reached} ->
            receive
                {webtransport, Session, {stream_credit, bidi}} ->
                    open_bidi_when_allowed(Session)
            after 5000 ->
                error(no_stream_credit)
            end
    end.

%% http3-17 §3.1, http2-16 §3.1: a client MUST NOT open a session until the
%% server sent SETTINGS_WT_ENABLED. Run a bare server that only enables
%% extended CONNECT.
server_without_wt_enabled_test(Config) ->
    process_flag(trap_exit, true),
    Transport = proplists:get_value(transport, Config),
    Port = test_helpers:find_free_port(),
    Stop = start_bare_server(Transport, Port, Config),
    try
        timer:sleep(100),
        ?assertEqual({error, wt_not_enabled},
                     webtransport:connect("localhost", Port, <<"/test">>,
                                          #{transport => Transport,
                                            verify => verify_none}))
    after
        Stop(),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

start_bare_server(h3, Port, Config) ->
    {ok, CertPem} = file:read_file(proplists:get_value(certfile, Config)),
    [{_, CertDer, _} | _] = public_key:pem_decode(CertPem),
    {ok, KeyPem} = file:read_file(proplists:get_value(keyfile, Config)),
    [KeyEntry] = public_key:pem_decode(KeyPem),
    Name = bare_h3_server,
    {ok, _} = quic_h3:start_server(Name, Port, #{
        cert => CertDer,
        key => public_key:pem_entry_decode(KeyEntry),
        settings => #{enable_connect_protocol => 1},
        h3_datagram_enabled => true,
        quic_opts => #{max_datagram_frame_size => 65535, reset_stream_at => true},
        handler => fun(C, S, _, _, _) -> quic_h3:send_response(C, S, 200, []) end
    }),
    fun() -> quic_h3:stop_server(Name) end;
start_bare_server(h2, Port, Config) ->
    {ok, Ref} = h2:start_server(Port, #{
        cert => proplists:get_value(certfile, Config),
        key => proplists:get_value(keyfile, Config),
        enable_connect_protocol => true,
        settings => #{enable_connect_protocol => 1},
        handler => fun(C, S, _, _, _) -> h2:send_response(C, S, 200, []) end
    }),
    fun() -> h2:stop_server(Ref) end.

%% http2-16 §7: WebTransport over HTTP/2 needs TLS 1.3 here (OTP's ssl has
%% no extended master secret for TLS 1.2). A client does not send a
%% WebTransport request over TLS 1.2.
client_refuses_tls12_server_test(Config) ->
    process_flag(trap_exit, true),
    Port = test_helpers:find_free_port(),
    {ok, Ref} = h2:start_server(Port, #{
        cert => proplists:get_value(certfile, Config),
        key => proplists:get_value(keyfile, Config),
        enable_connect_protocol => true,
        ssl_opts => [{versions, ['tlsv1.2']}],
        settings => #{enable_connect_protocol => 1, 16#2b67 => 1},
        handler => fun(C, S, _, _, _) -> h2:send_response(C, S, 200, []) end
    }),
    try
        timer:sleep(100),
        ?assertEqual({error, {tls_version_not_allowed, 'tlsv1.2'}},
                     webtransport:connect("localhost", Port, <<"/test">>,
                                          #{transport => h2, verify => verify_none}))
    after
        h2:stop_server(Ref),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

%% Our h2 listener only offers TLS 1.3.
listener_refuses_tls12_client_test(Config) ->
    Port = proplists:get_value(port, Config),
    ?assertMatch({error, _},
                 h2:connect("localhost", Port, #{
                     ssl_opts => [{verify, verify_none}, {versions, ['tlsv1.2']},
                                  {alpn_advertised_protocols, [<<"h2">>]}]
                 })).

%% accept/4 from an h2 server's request handler: h2 passes method and path
%% as arguments, so the path is given as an option.
h2_embedded_accept_round_trip_test(Config) ->
    process_flag(trap_exit, true),
    Port = test_helpers:find_free_port(),
    Opts = maps:merge(webtransport:h2_settings(), #{
        cert => proplists:get_value(certfile, Config),
        key => proplists:get_value(keyfile, Config),
        handler => fun(C, S, <<"CONNECT">>, Path, Headers) ->
            {ok, _} = webtransport:accept(C, S, Headers, #{transport => h2,
                                                           handler => wt_echo_handler,
                                                           path => Path})
        end
    }),
    {ok, Ref} = h2:start_server(Port, Opts),
    try
        timer:sleep(100),
        {ok, Session} = webtransport:connect("localhost", Port, <<"/embedded">>,
                                             #{transport => h2, verify => verify_none}),
        assert_session_usable(Session),
        webtransport:close_session(Session)
    after
        h2:stop_server(Ref),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

%% A server embedding accept/4 on a TLS 1.2 connection treats the request
%% as malformed: the CONNECT stream is reset with PROTOCOL_ERROR.
accept_refuses_tls12_test(Config) ->
    process_flag(trap_exit, true),
    Port = test_helpers:find_free_port(),
    Opts = maps:merge(webtransport:h2_settings(), #{
        cert => proplists:get_value(certfile, Config),
        key => proplists:get_value(keyfile, Config),
        ssl_opts => [{versions, ['tlsv1.2']}],
        handler => fun(C, S, _M, _P, Headers) ->
            webtransport:accept(C, S, Headers, #{transport => h2, handler => wt_echo_handler})
        end
    }),
    {ok, Ref} = h2:start_server(Port, Opts),
    try
        timer:sleep(100),
        {ok, Conn} = h2:connect("localhost", Port, #{
            ssl_opts => [{verify, verify_none}, {versions, ['tlsv1.2']},
                         {alpn_advertised_protocols, [<<"h2">>]}]
        }),
        Headers = [{<<":protocol">>, <<"webtransport">>},
                   {<<":scheme">>, <<"https">>},
                   {<<":path">>, <<"/test">>},
                   {<<"host">>, <<"localhost">>}],
        {ok, Sid} = h2:request(Conn, <<"CONNECT">>, <<"/test">>, Headers),
        receive
            {h2, Conn, {stream_reset, Sid, Code}} -> ?assertEqual(protocol_error, Code)
        after 5000 ->
            error(no_reset)
        end,
        h2:close(Conn)
    after
        h2:stop_server(Ref),
        flush_exits(),
        process_flag(trap_exit, false)
    end.

%% http2-16 §4.3.2: a WebTransport-Init that does not hold valid values is
%% rejected with a 4xx.
invalid_webtransport_init_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Conn} = h2:connect("localhost", Port, #{
        ssl_opts => [{verify, verify_none}, {alpn_advertised_protocols, [<<"h2">>]}]
    }),
    try
        Headers = [{<<":protocol">>, <<"webtransport">>},
                   {<<":scheme">>, <<"https">>},
                   {<<":path">>, <<"/test">>},
                   {<<"host">>, <<"localhost">>},
                   {<<"webtransport-init">>, <<"u=abc">>}],
        {ok, Sid} = h2:request(Conn, <<"CONNECT">>, <<"/test">>, Headers),
        receive
            {h2, Conn, {response, Sid, Status, _}} -> ?assertEqual(400, Status)
        after 5000 ->
            error(no_response)
        end
    after
        h2:close(Conn)
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
%% Embedding (accept/4 from a non-connection process)
%% ============================================================================

%% A generic quic_h3 server merges webtransport:h3_settings/0 into its opts
%% and calls webtransport:accept/4 from a per-request worker process — not
%% the QUIC connection process — mimicking an embedder (e.g. Livery) that
%% dispatches each request to its own worker. The connection_handler's router
%% (which receives the claimed extension streams) and the router accept/4
%% registers the session in must be the same one, so a connected client can
%% round-trip both stream data and datagrams through the echo handler.
%% The QUIC connection runs out of bidi streams before the session's own
%% limit: open_stream fails with stream_limit, and once a stream finishes
%% the handler is told it can open one again.
quic_stream_limit_credit_test(Config) ->
    with_quic_limited_server(Config, #{max_streams_bidi => 3}, fun(Session) ->
        {ok, S1} = webtransport:open_stream(Session, bidi),
        {ok, _S2} = webtransport:open_stream(Session, bidi),
        ?assertEqual({error, stream_limit}, webtransport:open_stream(Session, bidi)),
        ok = webtransport:send(Session, S1, <<"done">>, fin),
        <<"done">> = collect_stream_echo(Session, S1, <<>>, 3000),
        await_stream_credit(Session, bidi),
        ?assertMatch({ok, _}, webtransport:open_stream(Session, bidi))
    end).

%% Same for uni streams. The client's HTTP/3 control and QPACK streams take
%% three of the five. quic gives streams back in steps of half the limit
%% (two here), so both of ours have to finish.
quic_uni_stream_limit_credit_test(Config) ->
    with_quic_limited_server(Config, #{max_streams_uni => 5}, fun(Session) ->
        {ok, U1} = webtransport:open_stream(Session, uni),
        {ok, U2} = webtransport:open_stream(Session, uni),
        ?assertEqual({error, stream_limit}, webtransport:open_stream(Session, uni)),
        ok = webtransport:send(Session, U1, <<"done">>, fin),
        ok = webtransport:send(Session, U2, <<"done">>, fin),
        await_stream_credit(Session, uni),
        ?assertMatch({ok, _}, webtransport:open_stream(Session, uni))
    end).

await_stream_credit(Session, Type) ->
    receive
        {webtransport, Session, {stream_credit, Type}} -> ok
    after 5000 ->
        error({no_stream_credit, Type})
    end.

%% An embedded server with tight QUIC stream limits, so the connection runs
%% out before the session's own limits.
with_quic_limited_server(Config, Limits, Fun) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, CertDer, PrivateKey} = read_cert_key_der(CertFile, KeyFile),
    Port = test_helpers:find_free_port(),
    Name = list_to_atom("quic_limited_" ++ integer_to_list(erlang:unique_integer([positive]))),
    Dispatch = fun(Conn, StreamId, <<"CONNECT">>, _Path, Headers) ->
                       _ = spawn(fun() ->
                               {ok, _} = webtransport:accept(
                                   Conn, StreamId, Headers,
                                   #{transport => h3, handler => wt_echo_handler})
                           end),
                       ok
               end,
    Settings = webtransport:h3_settings(),
    QuicOpts = maps:merge(maps:get(quic_opts, Settings), Limits),
    {ok, _} = quic_h3:start_server(Name, Port, Settings#{
        cert => CertDer, key => PrivateKey, handler => Dispatch,
        quic_opts => QuicOpts
    }),
    try
        timer:sleep(100),
        {ok, Session} = webtransport:connect("localhost", Port, <<"/wt">>, #{
            transport => h3,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),
        Fun(Session),
        ok = webtransport:close_session(Session)
    after
        quic_h3:stop_server(Name)
    end.

embedded_accept_round_trip_test(Config) ->
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, CertDer, PrivateKey} = read_cert_key_der(CertFile, KeyFile),
    Port = test_helpers:find_free_port(),
    Name = embedded_accept_server,

    Dispatch = fun(Conn, StreamId, <<"CONNECT">>, _Path, Headers) ->
                       %% Dispatch to a per-request worker, like an embedder.
                       %% The worker exits normally after accept/4 returns;
                       %% the session it start_link'd survives that.
                       _ = spawn(fun() ->
                               {ok, _Session} = webtransport:accept(
                                   Conn, StreamId, Headers,
                                   #{transport => h3, handler => wt_echo_handler})
                           end),
                       ok;
                  (Conn, StreamId, _Method, _Path, _Headers) ->
                       quic_h3:send_response(Conn, StreamId, 404, []),
                       quic_h3:send_data(Conn, StreamId, <<"not found">>, true)
               end,

    ServerOpts = maps:merge(webtransport:h3_settings(), #{
        cert => CertDer,
        key => PrivateKey,
        handler => Dispatch
    }),
    {ok, _ServerRef} = quic_h3:start_server(Name, Port, ServerOpts),
    try
        timer:sleep(100),
        {ok, Session} = webtransport:connect("localhost", Port, <<"/wt">>, #{
            transport => h3,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),

        %% Bidi stream: send with FIN, expect the echo back.
        {ok, StreamId} = webtransport:open_stream(Session, bidi),
        Payload = <<"embedded-hello">>,
        ok = webtransport:send(Session, StreamId, Payload, fin),
        Echoed = collect_stream_echo(Session, StreamId, <<>>, 3000),
        ?assertEqual(Payload, Echoed),

        %% Datagram: send, expect the echo back.
        DPayload = <<"embedded-datagram">>,
        ok = webtransport:send_datagram(Session, DPayload),
        receive
            {webtransport, Session, {datagram, DPayload}} -> ok
        after 3000 ->
            error(no_datagram_echo)
        end,

        ok = webtransport:close_session(Session)
    after
        quic_h3:stop_server(Name)
    end.

%% Decode a PEM cert + key into the DER cert binary and decoded private key
%% that quic_h3:start_server/3 expects under `cert' and `key'.
read_cert_key_der(CertFile, KeyFile) ->
    {ok, CertPem} = file:read_file(CertFile),
    {ok, KeyPem} = file:read_file(KeyFile),
    [{'Certificate', CertDer, _} | _] = public_key:pem_decode(CertPem),
    [{KeyType, KeyDer, _} | _] = public_key:pem_decode(KeyPem),
    {ok, CertDer, public_key:der_decode(KeyType, KeyDer)}.

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

%% A listener configured with an `sni_callback' selects its cert per
%% ClientHello SNI (RFC 6066 §3). The callback reports the SNI it saw and
%% returns the test cert/key; the session must still round-trip on both
%% transports, and the callback must have been invoked with `<<"localhost">>'.
sni_callback_round_trip_test(Config) ->
    Transport = proplists:get_value(transport, Config),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),
    {ok, CertKey} = load_cert_key(CertFile, KeyFile),
    Self = self(),
    SniFun = fun(ServerName) ->
        Self ! {sni_seen, ServerName},
        {ok, CertKey}
    end,
    Name = sni_test_listener,
    Port = test_helpers:find_free_port(),
    {ok, _} = webtransport:start_listener(Name, #{
        transport => Transport,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => wt_echo_handler,
        sni_callback => SniFun
    }),
    timer:sleep(100),
    try
        {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),
        {ok, StreamId} = webtransport:open_stream(Session, bidi),
        Payload = <<"sni-ping">>,
        ok = webtransport:send(Session, StreamId, Payload, fin),
        Echoed = collect_stream_echo(Session, StreamId, <<>>, 2000),
        ?assertEqual(Payload, Echoed),
        receive
            {sni_seen, Seen} ->
                ?assertEqual(<<"localhost">>, Seen)
        after 2000 ->
            ct:fail(sni_callback_not_invoked)
        end,
        webtransport:close_session(Session)
    after
        webtransport:stop_listener(Name)
    end.

%% Load a PEM cert/key into the quic-style `sni_callback' result shape:
%% `cert' as a DER binary, `key' as a decoded private-key term.
load_cert_key(CertFile, KeyFile) ->
    {ok, CertPem} = file:read_file(CertFile),
    {ok, KeyPem} = file:read_file(KeyFile),
    [{_, CertDer, _} | _] = public_key:pem_decode(CertPem),
    [{KeyType, KeyDer, _} | _] = public_key:pem_decode(KeyPem),
    KeyTerm = public_key:der_decode(KeyType, KeyDer),
    {ok, #{cert => CertDer, key => KeyTerm}}.

listener_name_for(h3) -> test_h3_listener;
listener_name_for(h2) -> test_h2_listener.

%% ============================================================================
%% Legacy drafts
%% ============================================================================

%% The default listener (`compat_mode => auto') serves draft-02 clients
%% (quic-go v0.9 and older) and http3-07 ones (Safari 26) next to the
%% latest drafts, without WebTransport flow control.
legacy_draft02_round_trip_test(Config) ->
    legacy_round_trip(Config, legacy_browser_compat, draft02).

legacy_draft07_round_trip_test(Config) ->
    legacy_round_trip(Config, draft07, draft07).

legacy_round_trip(Config, Mode, Draft) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => h3,
        compat_mode => Mode,
        verify => verify_none,
        handler_opts => #{owner => self()}
    }),
    {ok, Info} = webtransport:session_info(Session),
    ?assertEqual(Draft, maps:get(wt_draft, Info)),
    %% The server picked the same draft.
    ok = webtransport:send_datagram(Session, <<"wt_draft?">>),
    DraftBin = atom_to_binary(Draft),
    receive
        {webtransport, Session, {datagram, DraftBin}} -> ok
    after 2000 ->
        error(no_server_draft)
    end,
    %% More than the default session limit (1 MiB) in each direction: with
    %% flow control wrongly on, one side would wait for credit.
    {ok, StreamId} = webtransport:open_stream(Session, bidi),
    Payload = test_helpers:random_data(2 * 1024 * 1024),
    ok = webtransport:send(Session, StreamId, Payload, fin),
    Echoed = collect_stream_echo(Session, StreamId, <<>>, 10000),
    Size = byte_size(Payload),
    ?assertEqual(Payload, binary:part(Echoed, byte_size(Echoed) - Size, Size)),
    ok = webtransport:close_session(Session).

%% ============================================================================
%% Client socket options
%% ============================================================================

%% h3: `{ip, _}' binds the client's QUIC socket to that address. Without it
%% the socket is on the wildcard address.
client_socket_opts_bind_ip_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect({127, 0, 0, 1}, Port, <<"/test">>, #{
        transport => h3,
        verify => verify_none,
        socket_opts => [{ip, {127, 0, 0, 1}}]
    }),
    try
        ?assertMatch({ok, {{127, 0, 0, 1}, _}}, h3_client_sockname(Session))
    after
        webtransport:close_session(Session)
    end.

%% h3: TCP-only options are accepted and ignored; the session works.
client_socket_opts_tcp_only_ignored_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => h3,
        verify => verify_none,
        socket_opts => [{nodelay, true}, {keepalive, true}, {linger, {true, 1}},
                        {send_timeout, 5000}, {send_timeout_close, true},
                        {packet, raw}, {delay_send, true}]
    }),
    try
        {ok, StreamId} = webtransport:open_stream(Session, bidi),
        ok = webtransport:send(Session, StreamId, <<"ping">>, fin)
    after
        webtransport:close_session(Session)
    end.

%% h2: socket options reach the TCP socket under TLS. `keepalive' is off by
%% default and the OS does not change it, so reading it back shows the option
%% was applied (macOS auto-tunes `recbuf', so its value proves nothing).
client_socket_opts_h2_test(Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Session} = webtransport:connect("localhost", Port, <<"/test">>, #{
        transport => h2,
        verify => verify_none,
        socket_opts => [{recbuf, 262144}, {keepalive, true}]
    }),
    try
        ?assertEqual({ok, [{keepalive, true}]},
                     ssl:getopts(h2_client_socket(Session), [keepalive]))
    after
        webtransport:close_session(Session)
    end.

session_transport_state(Session) ->
    {_StateName, Data} = sys:get_state(Session),
    element(3, Data).

h3_client_sockname(Session) ->
    H3Conn = webtransport_h3:h3_conn(session_transport_state(Session)),
    quic:sockname(quic_h3:get_quic_conn(H3Conn)).

h2_client_socket(Session) ->
    H2Conn = webtransport_h2:h2_conn(session_transport_state(Session)),
    find_sslsocket(sys:get_state(H2Conn)).

find_sslsocket(T) when is_tuple(T), element(1, T) =:= sslsocket -> T;
find_sslsocket(T) when is_tuple(T) -> find_sslsocket(tuple_to_list(T));
find_sslsocket([H | Rest]) ->
    case find_sslsocket(H) of
        undefined -> find_sslsocket(Rest);
        S -> S
    end;
find_sslsocket(_) -> undefined.
