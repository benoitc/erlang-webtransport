-module(wt_capsule_reader_tests).

-include_lib("eunit/include/eunit.hrl").
-include("webtransport.hrl").

h2() -> wt_capsule_reader:new(wt_h2_capsule, #{max_stream_data => 1024}).
h3() -> wt_capsule_reader:new(wt_h3_capsule, #{}).

%% Feed `Chunks' in order, collecting every capsule returned.
feed_all(Chunks, R) ->
    lists:foldl(fun(Chunk, {Acc, R0}) ->
                    {ok, Caps, R1} = wt_capsule_reader:feed(Chunk, R0),
                    {Acc ++ Caps, R1}
                end, {[], R}, Chunks).

bytes(Bin) -> [<<B>> || <<B>> <= Bin].

%% A capsule split anywhere gives the same capsule as sent whole.
split_capsule_test() ->
    Wire = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(4, <<"hello">>)),
    {Caps, R} = feed_all(bytes(Wire), h2()),
    ?assertEqual([{wt_stream, 4, <<"hello">>}], Caps),
    ?assertNot(wt_capsule_reader:pending(R)).

%% RFC 9297 §3.2, http2-16 §6.1: padding of any length is skipped as it
%% arrives. The capsule after it decodes.
huge_padding_is_skipped_test() ->
    %% Larger than the 64 MiB the reader used to hold.
    Len = 65 * 1024 * 1024,
    Header = <<(h2_varint:encode(?WT_PADDING))/binary, (h2_varint:encode(Len))/binary>>,
    {[{dropped, ?WT_PADDING}], R1} = feed_all([Header, binary:copy(<<0>>, 16384)], h2()),
    ?assert(wt_capsule_reader:pending(R1)),
    %% Skip the rest without holding it, then a capsule follows.
    Rest = Len - 16384,
    R2 = skip(Rest - 1, R1),
    Next = wt_h2_capsule:encode({max_data, 7}),
    {ok, Caps, R3} = wt_capsule_reader:feed(<<0, Next/binary>>, R2),
    ?assertEqual([{max_data, 7}], Caps),
    ?assertNot(wt_capsule_reader:pending(R3)).

%% On h3 any type the CONNECT stream does not use is skipped the same way.
h3_unknown_type_is_skipped_test() ->
    Header = <<(h2_varint:encode(16#21))/binary, (h2_varint:encode(1 bsl 30))/binary>>,
    {Caps, R} = feed_all([Header, <<1, 2, 3>>], h3()),
    ?assertEqual([{dropped, 16#21}], Caps),
    ?assert(wt_capsule_reader:pending(R)).

%% RFC 9297 §3.5: a DATAGRAM too large to use is discarded unread; the next
%% one is delivered.
oversize_datagram_is_dropped_test() ->
    Big = wt_h2_capsule:encode({datagram, binary:copy(<<1>>, 100000)}),
    Small = wt_h2_capsule:encode({datagram, <<"ok">>}),
    {Caps, R} = feed_all(chunks(<<Big/binary, Small/binary>>, 16384), h2()),
    ?assertEqual([{dropped, ?DATAGRAM}, {datagram, <<"ok">>}], Caps),
    ?assertNot(wt_capsule_reader:pending(R)).

%% http2-16 §6.6: a WT_STREAM declaring more than the stream window is a
%% flow-control error from its first bytes.
oversize_stream_capsule_test() ->
    Wire = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(4, binary:copy(<<0>>, 2048))),
    ?assertMatch({error, {flow_control, _}},
                 wt_capsule_reader:feed(binary:part(Wire, 0, 16), h2())).

%% A stream capsule that fits the window is held until whole.
stream_capsule_within_window_test() ->
    Data = binary:copy(<<9>>, 1024),
    Wire = wt_h2_capsule:encode(wt_h2_capsule:wt_stream_fin(4, Data)),
    {Caps, _} = feed_all(chunks(Wire, 100), h2()),
    ?assertEqual([{wt_stream_fin, 4, Data}], Caps).

%% http2-16 §6.12, http3-17 §6: a close reason over 1024 bytes is refused
%% from the header.
oversize_close_test_() ->
    Header = <<(h2_varint:encode(?WT_CLOSE_SESSION))/binary, (h2_varint:encode(2000))/binary>>,
    [?_assertMatch({error, {malformed, _}}, wt_capsule_reader:feed(Header, h2())),
     ?_assertMatch({error, {malformed, _}}, wt_capsule_reader:feed(Header, h3()))].

%% A control capsule longer than any valid one is refused.
oversize_control_test() ->
    Header = <<(h2_varint:encode(?WT_MAX_DATA))/binary, (h2_varint:encode(4096))/binary>>,
    ?assertMatch({error, {malformed, _}}, wt_capsule_reader:feed(Header, h3())).

skip(0, R) -> R;
skip(N, R) ->
    Step = min(N, 1024 * 1024),
    {ok, [], R1} = wt_capsule_reader:feed(binary:copy(<<0>>, Step), R),
    skip(N - Step, R1).

chunks(<<>>, _N) -> [];
chunks(Bin, N) when byte_size(Bin) =< N -> [Bin];
chunks(Bin, N) ->
    <<H:N/binary, T/binary>> = Bin,
    [H | chunks(T, N)].
