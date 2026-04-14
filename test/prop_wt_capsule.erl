%% @doc PropEr property-based tests for wt_h2_capsule module.
-module(prop_wt_capsule).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("webtransport.hrl").

%% ============================================================================
%% Generators
%% ============================================================================

%% Generate a valid WebTransport stream ID (62-bit max for varint)
stream_id() ->
    frequency([
        {10, range(0, 255)},
        {5, range(256, 16383)},
        {3, range(16384, 1073741823)},
        {1, range(1073741824, 4611686018427387903)}  %% 62-bit max
    ]).

%% Generate a small stream ID for most tests (faster)
small_stream_id() ->
    range(0, 1000).

%% Generate an error code
error_code() ->
    frequency([
        {5, 0},
        {5, range(1, 255)},
        {1, range(256, 16383)}
    ]).

%% Generate flow control values
flow_control_value() ->
    frequency([
        {5, range(0, 65535)},
        {3, range(65536, 16777215)},
        {1, range(16777216, 4294967295)}
    ]).

%% Generate binary payload
payload() ->
    frequency([
        {3, <<>>},
        {10, binary()},
        {2, binary(1024)}
    ]).

%% Generate a reason string for close_session
reason() ->
    frequency([
        {5, <<>>},
        {3, binary()},
        {1, binary(256)}
    ]).

%% Generate any capsule type
capsule() ->
    frequency([
        {2, {padding, padding_data()}},
        {5, {wt_stream, small_stream_id(), payload()}},
        {3, {wt_stream_fin, small_stream_id(), payload()}},
        {2, {reset_stream, small_stream_id(), error_code()}},
        {2, {stop_sending, small_stream_id(), error_code()}},
        {2, {max_data, flow_control_value()}},
        {2, {max_stream_data, small_stream_id(), flow_control_value()}},
        {1, {max_streams_bidi, range(0, 1000)}},
        {1, {max_streams_uni, range(0, 1000)}},
        {1, {data_blocked, flow_control_value()}},
        {1, {stream_data_blocked, small_stream_id(), flow_control_value()}},
        {1, {streams_blocked_bidi, range(0, 1000)}},
        {1, {streams_blocked_uni, range(0, 1000)}},
        {2, {close_session, error_code(), reason()}},
        {1, {drain_session}},
        {3, {datagram, payload()}}
    ]).

padding_data() ->
    frequency([
        {3, <<>>},
        {5, ?LET(Size, range(1, 100), binary(Size))},
        {1, binary(1000)}
    ]).

%% List of capsules for concatenation tests
capsule_list() ->
    non_empty(list(capsule())).

%% ============================================================================
%% Properties
%% ============================================================================

%% Property: encode then decode should return the same capsule
prop_roundtrip() ->
    ?FORALL(C, capsule(),
        begin
            Encoded = wt_h2_capsule:encode(C),
            {ok, Decoded, <<>>} = wt_h2_capsule:decode(Encoded),
            Decoded =:= C
        end).

%% Property: decode_all should correctly decode concatenated capsules
prop_decode_all_concatenation() ->
    ?FORALL(Capsules, capsule_list(),
        begin
            Encoded = << <<(wt_h2_capsule:encode(C))/binary>> || C <- Capsules >>,
            {ok, Decoded, <<>>} = wt_h2_capsule:decode_all(Encoded),
            Decoded =:= Capsules
        end).

%% Property: partial data returns {more, N}
prop_partial_decode() ->
    ?FORALL(C, capsule(),
        begin
            Encoded = wt_h2_capsule:encode(C),
            case byte_size(Encoded) > 1 of
                true ->
                    Partial = binary:part(Encoded, 0, 1),
                    case wt_h2_capsule:decode(Partial) of
                        {more, _} -> true;
                        _ -> false
                    end;
                false ->
                    %% Very small capsules might decode from 1 byte
                    true
            end
        end).

%% Property: large stream IDs encode/decode correctly
prop_large_stream_id() ->
    ?FORALL(Id, stream_id(),
        begin
            C = {wt_stream, Id, <<"data">>},
            Encoded = wt_h2_capsule:encode(C),
            {ok, Decoded, <<>>} = wt_h2_capsule:decode(Encoded),
            Decoded =:= C
        end).

%% Property: type_name returns atom for known types
prop_type_name_known() ->
    KnownTypes = [
        ?WT_PADDING, ?WT_RESET_STREAM, ?WT_STOP_SENDING,
        ?WT_STREAM, ?WT_STREAM_FIN, ?WT_MAX_DATA, ?WT_MAX_STREAM_DATA,
        ?WT_MAX_STREAMS_BIDI, ?WT_MAX_STREAMS_UNI, ?WT_DATA_BLOCKED,
        ?WT_STREAM_DATA_BLOCKED, ?WT_STREAMS_BLOCKED_BIDI,
        ?WT_STREAMS_BLOCKED_UNI, ?WT_CLOSE_SESSION, ?WT_DRAIN_SESSION,
        ?DATAGRAM
    ],
    ?FORALL(Type, elements(KnownTypes),
        is_atom(wt_h2_capsule:type_name(Type))).

%% Property: type_name returns the input for unknown types
prop_type_name_unknown() ->
    %% Use a range that doesn't overlap with known types
    ?FORALL(Type, range(16#FFFFFFFF + 1, 16#FFFFFFFFFFFF),
        wt_h2_capsule:type_name(Type) =:= Type).

%% Property: helper functions produce correct capsule types
prop_wt_stream_helper() ->
    ?FORALL({Id, Data, Fin}, {small_stream_id(), payload(), boolean()},
        begin
            C = wt_h2_capsule:wt_stream(Id, Data, Fin),
            case Fin of
                true -> element(1, C) =:= wt_stream_fin;
                false -> element(1, C) =:= wt_stream
            end
        end).

%% ============================================================================
%% EUnit wrappers
%% ============================================================================

roundtrip_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_roundtrip(), [
        {numtests, 500}, {to_file, user}
    ])).

decode_all_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_decode_all_concatenation(), [
        {numtests, 200}, {to_file, user}
    ])).

partial_decode_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_partial_decode(), [
        {numtests, 200}, {to_file, user}
    ])).

large_stream_id_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_large_stream_id(), [
        {numtests, 200}, {to_file, user}
    ])).

type_name_known_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_type_name_known(), [
        {numtests, 100}, {to_file, user}
    ])).

type_name_unknown_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_type_name_unknown(), [
        {numtests, 100}, {to_file, user}
    ])).

wt_stream_helper_property_test() ->
    ?assertEqual(true, proper:quickcheck(prop_wt_stream_helper(), [
        {numtests, 200}, {to_file, user}
    ])).
