%% @doc Unit tests for wt_h2_capsule module.
-module(wt_h2_capsule_tests).

-include_lib("eunit/include/eunit.hrl").
-include("webtransport.hrl").

close_session_reason_length_test_() ->
    Boundary = binary:copy(<<"x">>, 1024),
    TooLong = <<Boundary/binary, "!">>,
    [
        ?_assertMatch({close_session, 1, _}, wt_h2_capsule:close_session(1, Boundary)),
        ?_assertEqual({error, reason_too_long}, wt_h2_capsule:close_session(1, TooLong))
    ].

close_session_reason_decode_too_long_test() ->
    %% Manually craft a close_session capsule with 2048 bytes of reason.
    Payload = <<(h2_varint:encode(1))/binary, (binary:copy(<<"y">>, 2048))/binary>>,
    Encoded = h2_capsule:encode(?WT_CLOSE_SESSION, Payload),
    ?assertEqual({error, reason_too_long}, wt_h2_capsule:decode(Encoded)).

roundtrip_test_() ->
    Capsules = [
        wt_h2_capsule:padding(10),
        wt_h2_capsule:wt_stream(1, <<"hello">>),
        wt_h2_capsule:wt_stream_fin(2, <<"world">>),
        wt_h2_capsule:reset_stream(3, 0),
        wt_h2_capsule:stop_sending(4, 1),
        wt_h2_capsule:max_data(1000000),
        wt_h2_capsule:max_stream_data(5, 500000),
        wt_h2_capsule:max_streams_bidi(100),
        wt_h2_capsule:max_streams_uni(100),
        wt_h2_capsule:data_blocked(999999),
        wt_h2_capsule:stream_data_blocked(6, 888888),
        wt_h2_capsule:streams_blocked_bidi(50),
        wt_h2_capsule:streams_blocked_uni(25),
        wt_h2_capsule:close_session(0, <<>>),
        wt_h2_capsule:close_session(1, <<"error reason">>),
        wt_h2_capsule:drain_session(),
        wt_h2_capsule:datagram(<<"dgram data">>)
    ],
    [?_assertEqual({ok, C, <<>>}, wt_h2_capsule:decode(wt_h2_capsule:encode(C))) || C <- Capsules].

wt_stream_helper_test() ->
    ?assertEqual({wt_stream, 1, <<"data">>}, wt_h2_capsule:wt_stream(1, <<"data">>, false)),
    ?assertEqual({wt_stream_fin, 1, <<"data">>}, wt_h2_capsule:wt_stream(1, <<"data">>, true)).

decode_all_test() ->
    C1 = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(1, <<"one">>)),
    C2 = wt_h2_capsule:encode(wt_h2_capsule:max_data(1000)),
    C3 = wt_h2_capsule:encode(wt_h2_capsule:datagram(<<"dg">>)),
    Combined = <<C1/binary, C2/binary, C3/binary>>,
    {ok, Capsules, <<>>} = wt_h2_capsule:decode_all(Combined),
    ?assertEqual(3, length(Capsules)),
    ?assertEqual({wt_stream, 1, <<"one">>}, lists:nth(1, Capsules)),
    ?assertEqual({max_data, 1000}, lists:nth(2, Capsules)),
    ?assertEqual({datagram, <<"dg">>}, lists:nth(3, Capsules)).

type_name_test_() ->
    [
        ?_assertEqual(padding, wt_h2_capsule:type_name(?WT_PADDING)),
        ?_assertEqual(wt_stream, wt_h2_capsule:type_name(?WT_STREAM)),
        ?_assertEqual(close_session, wt_h2_capsule:type_name(?WT_CLOSE_SESSION)),
        ?_assertEqual(datagram, wt_h2_capsule:type_name(?DATAGRAM)),
        ?_assertEqual(999, wt_h2_capsule:type_name(999))
    ].

%% Additional edge case tests

empty_padding_test() ->
    C = wt_h2_capsule:padding(0),
    ?assertEqual({ok, C, <<>>}, wt_h2_capsule:decode(wt_h2_capsule:encode(C))).

binary_padding_test() ->
    Padding = <<1, 2, 3, 4, 5>>,
    C = wt_h2_capsule:padding(Padding),
    ?assertEqual({ok, {padding, Padding}, <<>>}, wt_h2_capsule:decode(wt_h2_capsule:encode(C))).

large_stream_id_test() ->
    %% Test with large stream IDs (62-bit max for varint)
    LargeId = 16#3FFFFFFFFFFFFFFF,
    C = wt_h2_capsule:wt_stream(LargeId, <<"data">>),
    ?assertEqual({ok, C, <<>>}, wt_h2_capsule:decode(wt_h2_capsule:encode(C))).

empty_data_test() ->
    C = wt_h2_capsule:wt_stream(1, <<>>),
    ?assertEqual({ok, C, <<>>}, wt_h2_capsule:decode(wt_h2_capsule:encode(C))).

decode_partial_test() ->
    C = wt_h2_capsule:wt_stream(1, <<"hello">>),
    Encoded = wt_h2_capsule:encode(C),
    %% Take first few bytes (incomplete capsule)
    Partial = binary:part(Encoded, 0, 3),
    ?assertMatch({more, _}, wt_h2_capsule:decode(Partial)).

close_session_with_reason_test() ->
    Reason = <<"Session terminated by server">>,
    C = wt_h2_capsule:close_session(42, Reason),
    ?assertEqual({ok, {close_session, 42, Reason}, <<>>},
                 wt_h2_capsule:decode(wt_h2_capsule:encode(C))).
