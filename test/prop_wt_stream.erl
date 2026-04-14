%% @doc PropEr property-based tests for webtransport_stream module.
-module(prop_wt_stream).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ============================================================================
%% Generators
%% ============================================================================

%% Generate a stream ID
stream_id() ->
    range(0, 1000).

%% Generate a stream type
stream_type() ->
    elements([bidi, uni]).

%% Generate a window size
window_size() ->
    frequency([
        {3, range(0, 100)},
        {5, range(100, 10000)},
        {2, range(10000, 1000000)}
    ]).

%% Generate binary data
data() ->
    frequency([
        {2, <<>>},
        {5, ?LET(Size, range(1, 100), binary(Size))},
        {2, ?LET(Size, range(100, 1000), binary(Size))}
    ]).

%% Generate a new stream
stream() ->
    ?LET({Id, Type, SendWindow, RecvWindow},
         {stream_id(), stream_type(), window_size(), window_size()},
         webtransport_stream:new(Id, Type, SendWindow, RecvWindow)).


%% ============================================================================
%% Properties
%% ============================================================================

%% Property: new stream is always open, readable, and writable
prop_new_stream_state() ->
    ?FORALL({Id, Type, Window}, {stream_id(), stream_type(), window_size()},
        begin
            S = webtransport_stream:new(Id, Type, Window),
            webtransport_stream:is_open(S) andalso
            webtransport_stream:is_writable(S) andalso
            webtransport_stream:is_readable(S) andalso
            webtransport_stream:state(S) =:= open
        end).

%% Property: stream ID is preserved
prop_id_preserved() ->
    ?FORALL({Id, Type, Window}, {stream_id(), stream_type(), window_size()},
        webtransport_stream:id(webtransport_stream:new(Id, Type, Window)) =:= Id).

%% Property: stream type is preserved
prop_type_preserved() ->
    ?FORALL({Id, Type, Window}, {stream_id(), stream_type(), window_size()},
        webtransport_stream:type(webtransport_stream:new(Id, Type, Window)) =:= Type).

%% Property: send window is preserved
prop_send_window_preserved() ->
    ?FORALL({Id, Type, SendW, RecvW}, {stream_id(), stream_type(), window_size(), window_size()},
        webtransport_stream:send_window(webtransport_stream:new(Id, Type, SendW, RecvW)) =:= SendW).

%% Property: recv window is preserved
prop_recv_window_preserved() ->
    ?FORALL({Id, Type, SendW, RecvW}, {stream_id(), stream_type(), window_size(), window_size()},
        webtransport_stream:recv_window(webtransport_stream:new(Id, Type, SendW, RecvW)) =:= RecvW).

%% Property: close/1 always results in closed state
prop_close_always_closes() ->
    ?FORALL(S, stream(),
        webtransport_stream:state(webtransport_stream:close(S)) =:= closed).

%% Property: closed stream is not writable or readable
prop_closed_not_writable_readable() ->
    ?FORALL(S, stream(),
        begin
            Closed = webtransport_stream:close(S),
            not webtransport_stream:is_writable(Closed) andalso
            not webtransport_stream:is_readable(Closed)
        end).

%% Property: send never sends more data than window allows
prop_send_respects_window() ->
    ?FORALL({Window, Data}, {window_size(), data()},
        begin
            S = webtransport_stream:new(0, bidi, Window),
            {ok, Sent, _} = webtransport_stream:send(S, Data),
            byte_size(Sent) =< Window
        end).

%% Property: buffer_send accumulates data
prop_buffer_send_accumulates() ->
    ?FORALL({D1, D2}, {data(), data()},
        begin
            S = webtransport_stream:new(0, bidi, 1000),
            S1 = webtransport_stream:buffer_send(S, D1),
            S2 = webtransport_stream:buffer_send(S1, D2),
            webtransport_stream:send_buffer(S2) =:= <<D1/binary, D2/binary>>
        end).

%% Property: flush_send_buffer clears the buffer
prop_flush_clears_buffer() ->
    ?FORALL(Data, data(),
        begin
            S = webtransport_stream:new(0, bidi, 0),
            {ok, _, S1} = webtransport_stream:send(S, Data),
            {_, S2} = webtransport_stream:flush_send_buffer(S1),
            webtransport_stream:send_buffer(S2) =:= <<>>
        end).

%% Property: receive_data respects flow control
prop_receive_respects_flow_control() ->
    ?FORALL({Window, Data}, {pos_integer(), data()},
        begin
            S = webtransport_stream:new(0, bidi, 1000, Window),
            case webtransport_stream:receive_data(S, Data) of
                {ok, _} -> byte_size(Data) =< Window;
                {error, flow_control_error} -> byte_size(Data) > Window
            end
        end).

%% Property: half-close state transitions are correct
prop_half_close_local_transition() ->
    ?FORALL(S, stream(),
        begin
            case webtransport_stream:close_local(S) of
                {ok, S1} ->
                    State = webtransport_stream:state(S1),
                    (State =:= half_closed_local orelse State =:= closed);
                {error, _} ->
                    true
            end
        end).

prop_half_close_remote_transition() ->
    ?FORALL(S, stream(),
        begin
            case webtransport_stream:close_remote(S) of
                {ok, S1} ->
                    State = webtransport_stream:state(S1),
                    (State =:= half_closed_remote orelse State =:= closed);
                {error, _} ->
                    true
            end
        end).

%% Property: update_send_window changes the window
prop_update_send_window() ->
    ?FORALL({S, NewWindow}, {stream(), window_size()},
        begin
            S1 = webtransport_stream:update_send_window(S, NewWindow),
            webtransport_stream:send_window(S1) =:= NewWindow
        end).

%% Property: update_recv_window changes the window
prop_update_recv_window() ->
    ?FORALL({S, NewWindow}, {stream(), window_size()},
        begin
            S1 = webtransport_stream:update_recv_window(S, NewWindow),
            webtransport_stream:recv_window(S1) =:= NewWindow
        end).

%% Property: stream_type follows ID conventions
prop_stream_type_convention() ->
    ?FORALL(Id, stream_id(),
        begin
            Type = webtransport_stream:stream_type(Id),
            case Id band 2 of
                0 -> Type =:= bidi;
                2 -> Type =:= uni
            end
        end).

%% Property: initiator follows ID conventions
prop_initiator_convention() ->
    ?FORALL(Id, stream_id(),
        begin
            Init = webtransport_stream:initiator(Id),
            case Id band 1 of
                0 -> Init =:= client;
                1 -> Init =:= server
            end
        end).

%% Property: is_bidi and is_uni are mutually exclusive for IDs
prop_bidi_uni_exclusive_id() ->
    ?FORALL(Id, stream_id(),
        webtransport_stream:is_bidi(Id) =/= webtransport_stream:is_uni(Id)).

%% Property: is_client_initiated and is_server_initiated are mutually exclusive
prop_client_server_exclusive() ->
    ?FORALL(Id, stream_id(),
        webtransport_stream:is_client_initiated(Id) =/= webtransport_stream:is_server_initiated(Id)).

%% Property: reset closes the stream
prop_reset_closes() ->
    ?FORALL({S, Code}, {stream(), range(0, 255)},
        webtransport_stream:state(webtransport_stream:reset(S, Code)) =:= closed).

%% Property: reset clears send buffer
prop_reset_clears_send_buffer() ->
    ?FORALL(Data, data(),
        begin
            S = webtransport_stream:new(0, bidi, 0),
            {ok, _, S1} = webtransport_stream:send(S, Data),
            S2 = webtransport_stream:reset(S1, 0),
            webtransport_stream:send_buffer(S2) =:= <<>>
        end).

%% ============================================================================
%% EUnit wrappers
%% ============================================================================

new_stream_state_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_new_stream_state(), [
        {numtests, 200}, {to_file, user}
    ])).

id_preserved_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_id_preserved(), [
        {numtests, 200}, {to_file, user}
    ])).

type_preserved_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_type_preserved(), [
        {numtests, 200}, {to_file, user}
    ])).

send_window_preserved_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_send_window_preserved(), [
        {numtests, 200}, {to_file, user}
    ])).

recv_window_preserved_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_recv_window_preserved(), [
        {numtests, 200}, {to_file, user}
    ])).

close_always_closes_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_close_always_closes(), [
        {numtests, 200}, {to_file, user}
    ])).

closed_not_writable_readable_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_closed_not_writable_readable(), [
        {numtests, 200}, {to_file, user}
    ])).

send_respects_window_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_send_respects_window(), [
        {numtests, 300}, {to_file, user}
    ])).

buffer_send_accumulates_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_buffer_send_accumulates(), [
        {numtests, 200}, {to_file, user}
    ])).

flush_clears_buffer_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_flush_clears_buffer(), [
        {numtests, 200}, {to_file, user}
    ])).

receive_respects_flow_control_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_receive_respects_flow_control(), [
        {numtests, 300}, {to_file, user}
    ])).

half_close_local_transition_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_half_close_local_transition(), [
        {numtests, 200}, {to_file, user}
    ])).

half_close_remote_transition_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_half_close_remote_transition(), [
        {numtests, 200}, {to_file, user}
    ])).

update_send_window_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_update_send_window(), [
        {numtests, 200}, {to_file, user}
    ])).

update_recv_window_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_update_recv_window(), [
        {numtests, 200}, {to_file, user}
    ])).

stream_type_convention_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_stream_type_convention(), [
        {numtests, 200}, {to_file, user}
    ])).

initiator_convention_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_initiator_convention(), [
        {numtests, 200}, {to_file, user}
    ])).

bidi_uni_exclusive_id_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_bidi_uni_exclusive_id(), [
        {numtests, 200}, {to_file, user}
    ])).

client_server_exclusive_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_client_server_exclusive(), [
        {numtests, 200}, {to_file, user}
    ])).

reset_closes_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_reset_closes(), [
        {numtests, 200}, {to_file, user}
    ])).

reset_clears_send_buffer_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_reset_clears_send_buffer(), [
        {numtests, 200}, {to_file, user}
    ])).
