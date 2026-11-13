%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc WebTransport-Init header field (http2-16 §4.3.2).
%%
%% WebTransport-Init is a Structured Field Dictionary (RFC 9651) carrying
%% initial per-stream data limits, granted by the endpoint that sends it:
%%   u  - limit for unidirectional streams opened by the recipient
%%   bl - limit for bidirectional streams opened by the sender
%%   br - limit for bidirectional streams opened by the recipient
%%
%% Unknown keys and all parameters are ignored. A defined key whose value is
%% not a non-negative Integer, or a field that does not parse, makes the
%% CONNECT request invalid (4xx).
%%
%% When the peer also sent SETTINGS_WT_INITIAL_MAX_STREAM_DATA_*, the
%% greater of the two values applies to each limit (http2-16 §4.3).
-module(wt_h2_init).

-export([parse/1, encode/1]).
-export([apply_greater_of/2]).
-export([from_settings/1]).

-type init_params() :: #{
    u  => non_neg_integer(),
    bl => non_neg_integer(),
    br => non_neg_integer()
}.

-export_type([init_params/0]).

%% ============================================================================
%% Encoding
%% ============================================================================

%% @doc Serialise the defined keys of `Params' as a Dictionary.
-spec encode(init_params()) -> binary().
encode(Params) when is_map(Params) ->
    Parts = [<<K/binary, "=", (integer_to_binary(maps:get(Key, Params)))/binary>>
             || {Key, K} <- [{u, <<"u">>}, {bl, <<"bl">>}, {br, <<"br">>}],
                maps:is_key(Key, Params)],
    iolist_to_binary(lists:join(<<", ">>, Parts)).

%% ============================================================================
%% Parsing
%% ============================================================================

%% @doc Parse a WebTransport-Init value.
-spec parse(binary()) -> {ok, init_params()} | {error, term()}.
parse(Bin) when is_binary(Bin) ->
    case wt_sf:parse_dictionary(Bin) of
        {ok, Members} -> collect(Members, #{});
        {error, Reason} -> {error, {invalid_webtransport_init, Reason}}
    end.

collect([], Acc) ->
    {ok, Acc};
collect([{KeyBin, Value} | Rest], Acc) ->
    case binary_to_key(KeyBin) of
        undefined ->
            collect(Rest, Acc);
        Key when is_integer(Value), Value >= 0 ->
            collect(Rest, Acc#{Key => Value});
        Key ->
            {error, {invalid_webtransport_init, {Key, Value}}}
    end.

binary_to_key(<<"u">>)  -> u;
binary_to_key(<<"bl">>) -> bl;
binary_to_key(<<"br">>) -> br;
binary_to_key(_)        -> undefined.

%% ============================================================================
%% Combining with SETTINGS
%% ============================================================================

%% @doc Express a peer's SETTINGS_WT_INITIAL_MAX_STREAM_DATA_* as the
%% WebTransport-Init keys they correspond to, from the receiver's view:
%% `_UNI' limits the uni streams we open (`u'), `_BIDI_LOCAL' the bidi
%% streams the peer opened (`bl'), `_BIDI_REMOTE' the bidi streams we
%% opened (`br').
-spec from_settings(map()) -> init_params().
from_settings(Settings) ->
    maps:from_list(
      [{Key, V} || {Setting, Key} <- [{wt_initial_max_stream_data_uni, u},
                                       {wt_initial_max_stream_data_bidi_local, bl},
                                       {wt_initial_max_stream_data_bidi_remote, br}],
                   V <- [maps:get(Setting, Settings, undefined)],
                   is_integer(V)]).

%% @doc Combine two sets of limits, keeping the greater value per key.
-spec apply_greater_of(init_params(), init_params()) -> init_params().
apply_greater_of(Init, Defaults) ->
    maps:fold(fun(Key, Val, Acc) ->
        case maps:find(Key, Acc) of
            {ok, Existing} -> Acc#{Key => max(Val, Existing)};
            error -> Acc#{Key => Val}
        end
    end, Defaults, Init).

%% ============================================================================
%% Eunit
%% ============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

roundtrip_test() ->
    Params = #{u => 100, bl => 50, br => 200},
    ?assertEqual({ok, Params}, parse(encode(Params))).

parse_simple_test_() ->
    [
        ?_assertEqual({ok, #{u => 10}}, parse(<<"u=10">>)),
        ?_assertEqual({ok, #{u => 10, bl => 20}}, parse(<<"u=10, bl=20">>)),
        ?_assertEqual({ok, #{u => 10, bl => 20, br => 30}},
                      parse(<<"u=10,bl=20,br=30">>))
    ].

%% http2-16 §4.3.2: unknown keys and parameters MUST be ignored.
parse_ignores_unknown_test_() ->
    [
        ?_assertEqual({ok, #{u => 5, bl => 3}}, parse(<<"u=5, foo=99, bl=3">>)),
        ?_assertEqual({ok, #{u => 10}}, parse(<<"u=10;x=1, foo">>)),
        ?_assertEqual({ok, #{}}, parse(<<"foo=\"s\"">>))
    ].

%% http2-16 §4.3.2: a defined key with an invalid value, or a field that
%% does not parse, rejects the request.
parse_rejects_test_() ->
    [
        ?_assertMatch({error, _}, parse(<<"u=abc">>)),
        ?_assertMatch({error, _}, parse(<<"u=-1">>)),
        ?_assertMatch({error, _}, parse(<<"u">>)),
        ?_assertMatch({error, _}, parse(<<"u=1.5">>)),
        ?_assertMatch({error, _}, parse(<<"u=1,">>))
    ].

%% RFC 9651 §3.3.1: at most 15 digits. A value of 200,000 digits is refused
%% without being turned into an integer.
parse_integer_limit_test_() ->
    [
        ?_assertEqual({ok, #{u => 999999999999999}}, parse(<<"u=999999999999999">>)),
        ?_assertEqual({error, {invalid_webtransport_init, integer_too_long}},
                      parse(<<"u=", (binary:copy(<<"9">>, 200000))/binary>>))
    ].

from_settings_test() ->
    Settings = #{wt_initial_max_stream_data_uni => 1,
                 wt_initial_max_stream_data_bidi_local => 2,
                 wt_initial_max_stream_data_bidi_remote => 3,
                 wt_initial_max_data => 99},
    ?assertEqual(#{u => 1, bl => 2, br => 3}, from_settings(Settings)).

apply_greater_of_test() ->
    Result = apply_greater_of(#{u => 200, bl => 10}, #{u => 100, bl => 50, br => 30}),
    ?assertEqual(#{u => 200, bl => 50, br => 30}, Result).

-endif.
