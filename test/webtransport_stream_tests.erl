%% @doc Unit tests for webtransport_stream module.
-module(webtransport_stream_tests).

-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Stream Creation Tests
%% ============================================================================

new_stream_test() ->
    S = webtransport_stream:new(4, bidi, 1000),
    ?assertEqual(4, webtransport_stream:id(S)),
    ?assertEqual(bidi, webtransport_stream:type(S)),
    ?assertEqual(open, webtransport_stream:state(S)),
    ?assertEqual(1000, webtransport_stream:send_window(S)),
    ?assertEqual(1000, webtransport_stream:recv_window(S)),
    ?assert(webtransport_stream:is_open(S)),
    ?assert(webtransport_stream:is_writable(S)),
    ?assert(webtransport_stream:is_readable(S)).

new_stream_different_windows_test() ->
    S = webtransport_stream:new(4, uni, 2000, 1000),
    ?assertEqual(2000, webtransport_stream:send_window(S)),
    ?assertEqual(1000, webtransport_stream:recv_window(S)).

%% ============================================================================
%% Send Tests
%% ============================================================================

send_within_window_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, Data, S1} = webtransport_stream:send(S, <<"hello">>),
    ?assertEqual(<<"hello">>, Data),
    ?assertEqual(<<>>, webtransport_stream:send_buffer(S1)).

send_exceeds_window_test() ->
    S = webtransport_stream:new(4, bidi, 5),
    {ok, Data, S1} = webtransport_stream:send(S, <<"hello world">>),
    ?assertEqual(<<"hello">>, Data),
    ?assertEqual(<<" world">>, webtransport_stream:send_buffer(S1)).

send_no_window_test() ->
    S = webtransport_stream:new(4, bidi, 0),
    {ok, Data, S1} = webtransport_stream:send(S, <<"test">>),
    ?assertEqual(<<>>, Data),
    ?assertEqual(<<"test">>, webtransport_stream:send_buffer(S1)).

send_closed_stream_test() ->
    S = webtransport_stream:close(webtransport_stream:new(4, bidi, 100)),
    ?assertEqual({error, stream_closed}, webtransport_stream:send(S, <<"data">>)).

send_half_closed_local_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_local(S),
    ?assertEqual({error, stream_half_closed}, webtransport_stream:send(S1, <<"data">>)).

%% ============================================================================
%% Receive Tests
%% ============================================================================

receive_data_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:receive_data(S, <<"hello">>),
    ?assertEqual(<<"hello">>, webtransport_stream:recv_buffer(S1)).

receive_data_flow_control_error_test() ->
    S = webtransport_stream:new(4, bidi, 5),
    ?assertEqual({error, flow_control_error},
                 webtransport_stream:receive_data(S, <<"hello world">>)).

receive_closed_stream_test() ->
    S = webtransport_stream:close(webtransport_stream:new(4, bidi, 100)),
    ?assertEqual({error, stream_closed}, webtransport_stream:receive_data(S, <<"data">>)).

%% ============================================================================
%% Close Tests
%% ============================================================================

close_local_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_local(S),
    ?assertEqual(half_closed_local, webtransport_stream:state(S1)),
    ?assertNot(webtransport_stream:is_writable(S1)),
    ?assert(webtransport_stream:is_readable(S1)).

close_remote_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_remote(S),
    ?assertEqual(half_closed_remote, webtransport_stream:state(S1)),
    ?assert(webtransport_stream:is_writable(S1)),
    ?assertNot(webtransport_stream:is_readable(S1)).

full_close_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_local(S),
    {ok, S2} = webtransport_stream:close_remote(S1),
    ?assertEqual(closed, webtransport_stream:state(S2)),
    ?assertNot(webtransport_stream:is_writable(S2)),
    ?assertNot(webtransport_stream:is_readable(S2)).

full_close_reverse_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_remote(S),
    {ok, S2} = webtransport_stream:close_local(S1),
    ?assertEqual(closed, webtransport_stream:state(S2)).

close_already_closed_test() ->
    S = webtransport_stream:close(webtransport_stream:new(4, bidi, 100)),
    ?assertEqual({error, stream_closed}, webtransport_stream:close_local(S)),
    ?assertEqual({error, stream_closed}, webtransport_stream:close_remote(S)).

duplicate_close_local_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:close_local(S),
    ?assertEqual({error, duplicate_fin}, webtransport_stream:close_local(S1)).

%% ============================================================================
%% Reset and Stop Sending Tests
%% ============================================================================

reset_stream_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, _, S1} = webtransport_stream:send(S, <<"buffered data">>),
    S2 = webtransport_stream:reset(S1, 42),
    ?assertEqual(closed, webtransport_stream:state(S2)),
    ?assertEqual(<<>>, webtransport_stream:send_buffer(S2)).

stop_sending_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    S1 = webtransport_stream:stop_sending(S, 42),
    ?assertNot(webtransport_stream:is_readable(S1)).

%% ============================================================================
%% Window Update Tests
%% ============================================================================

update_send_window_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    S1 = webtransport_stream:update_send_window(S, 500),
    ?assertEqual(500, webtransport_stream:send_window(S1)).

update_recv_window_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    S1 = webtransport_stream:update_recv_window(S, 500),
    ?assertEqual(500, webtransport_stream:recv_window(S1)).

%% ============================================================================
%% Buffer Management Tests
%% ============================================================================

flush_send_buffer_test() ->
    S = webtransport_stream:new(4, bidi, 5),
    {ok, _, S1} = webtransport_stream:send(S, <<"hello world">>),
    {Buffer, S2} = webtransport_stream:flush_send_buffer(S1),
    ?assertEqual(<<" world">>, Buffer),
    ?assertEqual(<<>>, webtransport_stream:send_buffer(S2)).

flush_recv_buffer_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    {ok, S1} = webtransport_stream:receive_data(S, <<"hello">>),
    {Buffer, S2} = webtransport_stream:flush_recv_buffer(S1),
    ?assertEqual(<<"hello">>, Buffer),
    ?assertEqual(<<>>, webtransport_stream:recv_buffer(S2)).

buffer_send_test() ->
    S = webtransport_stream:new(4, bidi, 100),
    S1 = webtransport_stream:buffer_send(S, <<"data1">>),
    S2 = webtransport_stream:buffer_send(S1, <<"data2">>),
    ?assertEqual(<<"data1data2">>, webtransport_stream:send_buffer(S2)).

%% ============================================================================
%% Stream ID Helper Tests
%% ============================================================================

stream_type_test_() ->
    [
        ?_assertEqual(bidi, webtransport_stream:stream_type(0)),
        ?_assertEqual(bidi, webtransport_stream:stream_type(1)),
        ?_assertEqual(uni, webtransport_stream:stream_type(2)),
        ?_assertEqual(uni, webtransport_stream:stream_type(3)),
        ?_assertEqual(bidi, webtransport_stream:stream_type(4)),
        ?_assertEqual(bidi, webtransport_stream:stream_type(5)),
        ?_assertEqual(uni, webtransport_stream:stream_type(6)),
        ?_assertEqual(uni, webtransport_stream:stream_type(7))
    ].

initiator_test_() ->
    [
        ?_assertEqual(client, webtransport_stream:initiator(0)),
        ?_assertEqual(server, webtransport_stream:initiator(1)),
        ?_assertEqual(client, webtransport_stream:initiator(2)),
        ?_assertEqual(server, webtransport_stream:initiator(3)),
        ?_assertEqual(client, webtransport_stream:initiator(4)),
        ?_assertEqual(server, webtransport_stream:initiator(5))
    ].

is_bidi_test_() ->
    [
        ?_assert(webtransport_stream:is_bidi(0)),
        ?_assert(webtransport_stream:is_bidi(4)),
        ?_assertNot(webtransport_stream:is_bidi(2)),
        ?_assertNot(webtransport_stream:is_bidi(6))
    ].

is_uni_test_() ->
    [
        ?_assertNot(webtransport_stream:is_uni(0)),
        ?_assertNot(webtransport_stream:is_uni(4)),
        ?_assert(webtransport_stream:is_uni(2)),
        ?_assert(webtransport_stream:is_uni(6))
    ].

is_client_initiated_test_() ->
    [
        ?_assert(webtransport_stream:is_client_initiated(0)),
        ?_assert(webtransport_stream:is_client_initiated(2)),
        ?_assert(webtransport_stream:is_client_initiated(4)),
        ?_assertNot(webtransport_stream:is_client_initiated(1)),
        ?_assertNot(webtransport_stream:is_client_initiated(3))
    ].

is_server_initiated_test_() ->
    [
        ?_assertNot(webtransport_stream:is_server_initiated(0)),
        ?_assertNot(webtransport_stream:is_server_initiated(2)),
        ?_assert(webtransport_stream:is_server_initiated(1)),
        ?_assert(webtransport_stream:is_server_initiated(3))
    ].

%% Record-based is_bidi/is_uni tests
is_bidi_stream_record_test() ->
    Bidi = webtransport_stream:new(0, bidi, 100),
    Uni = webtransport_stream:new(2, uni, 100),
    ?assert(webtransport_stream:is_bidi(Bidi)),
    ?assertNot(webtransport_stream:is_bidi(Uni)).

is_uni_stream_record_test() ->
    Bidi = webtransport_stream:new(0, bidi, 100),
    Uni = webtransport_stream:new(2, uni, 100),
    ?assertNot(webtransport_stream:is_uni(Bidi)),
    ?assert(webtransport_stream:is_uni(Uni)).
