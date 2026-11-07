%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc Structured Field Values (RFC 9651), the subset WebTransport uses.
%%
%% WebTransport-Init is a Dictionary (http2-16 §4.3.2). WT-Available-Protocols
%% is a List of Strings and WT-Protocol a String Item (http3-17 §3.3). This
%% module parses and serialises those shapes. Parameters are parsed and
%% dropped, since neither header gives them a meaning.
-module(wt_sf).

-export([parse_dictionary/1, parse_list/1, parse_item/1]).
-export([serialize_string/1, serialize_string_list/1]).

-type bare_item() :: integer() | {decimal, binary()} | {string, binary()}
                   | {token, binary()} | {binary, binary()} | boolean().
-type member() :: bare_item() | {inner_list, [bare_item()]}.

-export_type([bare_item/0, member/0]).

%% ============================================================================
%% API
%% ============================================================================

%% @doc Parse a Dictionary into `[{Key, Member}]', in field order. A key
%% without a value is `true'. A repeated key keeps its last value.
-spec parse_dictionary(binary()) -> {ok, [{binary(), member()}]} | {error, term()}.
parse_dictionary(Bin) ->
    run(fun(B) -> dict_members(skip_sp(B), []) end, Bin).

%% @doc Parse a List into its members, in order.
-spec parse_list(binary()) -> {ok, [member()]} | {error, term()}.
parse_list(Bin) ->
    run(fun(B) -> list_members(skip_sp(B), []) end, Bin).

%% @doc Parse an Item and return its bare value.
-spec parse_item(binary()) -> {ok, bare_item()} | {error, term()}.
parse_item(Bin) ->
    run(fun(B) ->
            {Item, Rest} = bare_item(skip_sp(B)),
            <<>> = skip_sp(skip_params(Rest)),
            Item
        end, Bin).

%% @doc Serialise a String item. Fails on bytes outside printable ASCII.
-spec serialize_string(binary()) -> binary().
serialize_string(S) ->
    Escaped = << <<(escape(C))/binary>> || <<C>> <= S >>,
    <<$", Escaped/binary, $">>.

%% @doc Serialise a List of Strings.
-spec serialize_string_list([binary()]) -> binary().
serialize_string_list(Strings) ->
    iolist_to_binary(lists:join(<<", ">>, [serialize_string(S) || S <- Strings])).

%% ============================================================================
%% Parsing
%% ============================================================================

run(Fun, Bin) when is_binary(Bin) ->
    try {ok, Fun(Bin)}
    catch
        throw:{sf_error, Reason} -> {error, Reason};
        error:{badmatch, _} -> {error, invalid_structured_field}
    end.

dict_members(<<>>, Acc) ->
    lists:reverse(Acc);
dict_members(Bin, Acc) ->
    {Key, Rest0} = key(Bin),
    {Member, Rest1} =
        case Rest0 of
            <<$=, R/binary>> -> member(R);
            _ -> {true, skip_params(Rest0)}
        end,
    Acc1 = [{Key, Member} | lists:keydelete(Key, 1, Acc)],
    next(Rest1, Acc1, fun dict_members/2).

list_members(<<>>, Acc) ->
    lists:reverse(Acc);
list_members(Bin, Acc) ->
    {Member, Rest} = member(Bin),
    next(Rest, [Member | Acc], fun list_members/2).

%% After a member: optional whitespace, then end or a comma and another one.
next(Bin, Acc, Cont) ->
    case skip_ows(Bin) of
        <<>> ->
            lists:reverse(Acc);
        <<$,, Rest/binary>> ->
            case skip_ows(Rest) of
                <<>> -> throw({sf_error, trailing_comma});
                Rest1 -> Cont(Rest1, Acc)
            end;
        _ ->
            throw({sf_error, expected_comma})
    end.

member(<<$(, Rest/binary>>) ->
    inner_list(skip_sp(Rest), []);
member(Bin) ->
    {Item, Rest} = bare_item(Bin),
    {Item, skip_params(Rest)}.

inner_list(<<$), Rest/binary>>, Acc) ->
    {{inner_list, lists:reverse(Acc)}, skip_params(Rest)};
inner_list(Bin, Acc) ->
    {Item, Rest} = bare_item(Bin),
    case skip_params(Rest) of
        <<$), _/binary>> = R -> inner_list(R, [Item | Acc]);
        <<$\s, _/binary>> = R -> inner_list(skip_sp(R), [Item | Acc]);
        _ -> throw({sf_error, bad_inner_list})
    end.

%% Parameters carry no meaning for WebTransport; parse and drop them.
skip_params(<<$;, Rest/binary>>) ->
    {_Key, Rest1} = key(skip_sp(Rest)),
    Rest2 = case Rest1 of
        <<$=, R/binary>> -> element(2, bare_item(R));
        _ -> Rest1
    end,
    skip_params(Rest2);
skip_params(Bin) ->
    Bin.

key(<<C, _/binary>> = Bin) when (C >= $a andalso C =< $z) orelse C =:= $* ->
    key_chars(Bin, <<>>);
key(_) ->
    throw({sf_error, bad_key}).

key_chars(<<C, Rest/binary>>, Acc)
  when (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)
       orelse C =:= $_ orelse C =:= $- orelse C =:= $. orelse C =:= $* ->
    key_chars(Rest, <<Acc/binary, C>>);
key_chars(Rest, Acc) ->
    {Acc, Rest}.

bare_item(<<$-, _/binary>> = Bin) -> number(Bin);
bare_item(<<C, _/binary>> = Bin) when C >= $0, C =< $9 -> number(Bin);
bare_item(<<$", Rest/binary>>) -> string(Rest, <<>>);
bare_item(<<$:, Rest/binary>>) -> byte_seq(Rest);
bare_item(<<$?, $1, Rest/binary>>) -> {true, Rest};
bare_item(<<$?, $0, Rest/binary>>) -> {false, Rest};
bare_item(<<C, _/binary>> = Bin)
  when (C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z) orelse C =:= $* ->
    token(Bin, <<>>);
bare_item(_) ->
    throw({sf_error, bad_item}).

number(Bin) ->
    {Sign, Digits0} = case Bin of
        <<$-, R/binary>> -> {-1, R};
        _ -> {1, Bin}
    end,
    {Int, Rest} = digits(Digits0, <<>>),
    case {byte_size(Int), Rest} of
        {0, _} ->
            throw({sf_error, bad_number});
        {N, <<$., Frac0/binary>>} when N =< 12 ->
            {Frac, Rest1} = digits(Frac0, <<>>),
            case byte_size(Frac) of
                F when F >= 1, F =< 3 ->
                    S = case Sign of -1 -> <<"-">>; 1 -> <<>> end,
                    {{decimal, <<S/binary, Int/binary, ".", Frac/binary>>}, Rest1};
                _ ->
                    throw({sf_error, bad_decimal})
            end;
        {N, _} when N =< 15 ->
            {Sign * binary_to_integer(Int), Rest};
        _ ->
            throw({sf_error, integer_too_long})
    end.

digits(<<C, Rest/binary>>, Acc) when C >= $0, C =< $9 ->
    digits(Rest, <<Acc/binary, C>>);
digits(Rest, Acc) ->
    {Acc, Rest}.

string(<<$\\, C, Rest/binary>>, Acc) when C =:= $"; C =:= $\\ ->
    string(Rest, <<Acc/binary, C>>);
string(<<$\\, _/binary>>, _Acc) ->
    throw({sf_error, bad_escape});
string(<<$", Rest/binary>>, Acc) ->
    {{string, Acc}, Rest};
string(<<C, Rest/binary>>, Acc) when C >= 16#20, C =< 16#7e ->
    string(Rest, <<Acc/binary, C>>);
string(_, _Acc) ->
    throw({sf_error, bad_string}).

token(<<C, Rest/binary>>, Acc)
  when (C >= $a andalso C =< $z) orelse (C >= $A andalso C =< $Z)
       orelse (C >= $0 andalso C =< $9) orelse C =:= $: orelse C =:= $/
       orelse C =:= $! orelse C =:= $# orelse C =:= $$ orelse C =:= $%
       orelse C =:= $& orelse C =:= $' orelse C =:= $* orelse C =:= $+
       orelse C =:= $- orelse C =:= $. orelse C =:= $^ orelse C =:= $_
       orelse C =:= $` orelse C =:= $| orelse C =:= $~ ->
    token(Rest, <<Acc/binary, C>>);
token(Rest, Acc) ->
    {{token, Acc}, Rest}.

byte_seq(Bin) ->
    case binary:split(Bin, <<":">>) of
        [B64, Rest] ->
            try {{binary, base64:decode(B64)}, Rest}
            catch _:_ -> throw({sf_error, bad_byte_sequence})
            end;
        _ ->
            throw({sf_error, bad_byte_sequence})
    end.

skip_sp(<<$\s, Rest/binary>>) -> skip_sp(Rest);
skip_sp(Bin) -> Bin.

skip_ows(<<C, Rest/binary>>) when C =:= $\s; C =:= $\t -> skip_ows(Rest);
skip_ows(Bin) -> Bin.

escape($") -> <<"\\\"">>;
escape($\\) -> <<"\\\\">>;
escape(C) when C >= 16#20, C =< 16#7e -> <<C>>;
escape(C) -> error({not_sf_string_char, C}).

%% ============================================================================
%% Eunit
%% ============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

dictionary_test_() ->
    [
        ?_assertEqual({ok, [{<<"u">>, 10}, {<<"bl">>, 20}]},
                      parse_dictionary(<<"u=10, bl=20">>)),
        ?_assertEqual({ok, [{<<"u">>, 10}, {<<"foo">>, true}]},
                      parse_dictionary(<<"u=10;x=1, foo">>)),
        ?_assertEqual({ok, [{<<"u">>, -1}]}, parse_dictionary(<<"u=-1">>)),
        ?_assertEqual({ok, [{<<"u">>, {token, <<"abc">>}}]},
                      parse_dictionary(<<"u=abc">>)),
        ?_assertEqual({ok, [{<<"u">>, 2}]}, parse_dictionary(<<"u=1, u=2">>)),
        ?_assertEqual({ok, []}, parse_dictionary(<<"">>)),
        ?_assertMatch({error, _}, parse_dictionary(<<"u=10,">>)),
        ?_assertMatch({error, _}, parse_dictionary(<<"U=10">>)),
        ?_assertMatch({error, _}, parse_dictionary(<<"u=1234567890123456">>))
    ].

list_test_() ->
    [
        ?_assertEqual({ok, [{string, <<"a">>}, {string, <<"b c">>}]},
                      parse_list(<<"\"a\", \"b c\"">>)),
        ?_assertEqual({ok, [{string, <<"a">>}, {token, <<"b">>}]},
                      parse_list(<<"\"a\";q=1, b">>)),
        ?_assertEqual({ok, [{inner_list, [1, 2]}]}, parse_list(<<"(1 2)">>)),
        ?_assertMatch({error, _}, parse_list(<<"\"unterminated">>))
    ].

item_test_() ->
    [
        ?_assertEqual({ok, {string, <<"x\"y">>}}, parse_item(<<"\"x\\\"y\"">>)),
        ?_assertEqual({ok, {string, <<"p">>}}, parse_item(<<"\"p\";v=2">>)),
        ?_assertEqual({ok, {decimal, <<"1.5">>}}, parse_item(<<"1.5">>)),
        ?_assertEqual({ok, true}, parse_item(<<"?1">>)),
        ?_assertMatch({error, _}, parse_item(<<"\"a\" \"b\"">>))
    ].

serialize_test_() ->
    [
        ?_assertEqual(<<"\"a\\\"b\"">>, serialize_string(<<"a\"b">>)),
        ?_assertEqual(<<"\"x\", \"y\"">>, serialize_string_list([<<"x">>, <<"y">>])),
        ?_assertEqual({ok, [{string, <<"x">>}, {string, <<"y">>}]},
                      parse_list(serialize_string_list([<<"x">>, <<"y">>])))
    ].

-endif.
