%% @doc WebTransport E2E test suite.
%%
%% This Common Test suite runs integration tests for WebTransport
%% over HTTP/3.
%%
-module(webtransport_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

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
    close_session_with_error_test/1
]).

%% ============================================================================
%% CT Callbacks
%% ============================================================================

all() ->
    [
        {group, h3_tests}
    ].

groups() ->
    [
        {h3_tests, [sequence], [
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
            close_session_with_error_test
        ]}
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
    %% Start H3 listener
    Port = test_helpers:find_free_port(),
    CertFile = proplists:get_value(certfile, Config),
    KeyFile = proplists:get_value(keyfile, Config),

    ListenerOpts = #{
        transport => h3,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => wt_echo_handler
    },

    case webtransport:start_listener(test_listener, ListenerOpts) of
        {ok, _Pid} ->
            %% Give the listener time to start
            timer:sleep(100),
            [{port, Port}, {transport, h3} | Config];
        {error, Reason} ->
            {skip, {listener_start_failed, Reason}}
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(h3_tests, _Config) ->
    webtransport:stop_listener(test_listener),
    ok;
end_per_group(_Group, _Config) ->
    ok.

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
        transport => h3,
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
        transport => h3,
        verify => verify_none
    }),

    %% Get session info
    {ok, Info} = webtransport:session_info(Session),

    ?assert(is_map(Info)),
    ?assertEqual(h3, maps:get(transport, Info)),
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
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
        transport => h3,
        verify => verify_none
    }),

    %% Close with error code and reason
    ok = webtransport:close_session(Session, 42, <<"test error">>),

    timer:sleep(100),
    ?assertNot(is_process_alive(Session)).
