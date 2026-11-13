%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc Capsule reader for the CONNECT stream, shared by HTTP/2 and HTTP/3.
%%
%% Capsules arrive in pieces that do not follow capsule boundaries. The
%% reader returns each capsule once it is whole. For one that is not whole
%% yet, its header decides what is held, through the codec's
%% `buffer_rule/3':
%%
%% - `keep': the bytes are held until the capsule is complete. Nothing is
%%   decoded again before then.
%% - `drop': the capsule is skipped as it arrives, without holding it
%%   (padding and unknown types, RFC 9297 §3.2; a DATAGRAM too large to use,
%%   RFC 9297 §3.5). `{dropped, Type}' takes its place in the result.
%% - `{error, Kind, Reason}': the capsule is refused from its header.
-module(wt_capsule_reader).

-export([new/2, feed/2, pending/1]).

-record(reader, {
    codec :: wt_h2_capsule | wt_h3_capsule,
    opts :: map(),
    %% Bytes of the capsule being waited for, newest first.
    chunks = [] :: [binary()],
    size = 0 :: non_neg_integer(),
    %% Bytes the capsule being waited for needs; 0 while its header is not
    %% complete.
    need = 0 :: non_neg_integer(),
    %% Bytes still to skip of a dropped capsule.
    skip = 0 :: non_neg_integer()
}).

-opaque reader() :: #reader{}.
-type error_kind() :: flow_control | malformed.

-export_type([reader/0, error_kind/0]).

%% @doc A reader for the capsules of `Codec'. `Opts' goes to
%% `Codec:buffer_rule/3'.
-spec new(wt_h2_capsule | wt_h3_capsule, map()) -> reader().
new(Codec, Opts) ->
    #reader{codec = Codec, opts = Opts}.

%% @doc Bytes are held or still to be skipped: the stream ending now ends it
%% inside a capsule (RFC 9297 §3.3).
-spec pending(reader()) -> boolean().
pending(#reader{size = Size, skip = Skip}) ->
    Size > 0 orelse Skip > 0.

%% @doc Read the next bytes of the stream. Returns the capsules completed by
%% them, in order.
-spec feed(binary(), reader()) ->
    {ok, [term()], reader()} | {error, {error_kind(), term()}}.
feed(Data, #reader{skip = Skip} = R) when Skip > 0 ->
    case byte_size(Data) of
        N when N =< Skip ->
            {ok, [], R#reader{skip = Skip - N}};
        _ ->
            <<_:Skip/binary, Rest/binary>> = Data,
            feed(Rest, R#reader{skip = 0})
    end;
feed(<<>>, R) ->
    {ok, [], R};
feed(Data, #reader{chunks = Chunks, size = Size, need = Need} = R) ->
    Size1 = Size + byte_size(Data),
    case Size1 < Need of
        true ->
            {ok, [], R#reader{chunks = [Data | Chunks], size = Size1}};
        false ->
            Bin = iolist_to_binary(lists:reverse([Data | Chunks])),
            decode(Bin, [], R#reader{chunks = [], size = 0, need = 0})
    end.

decode(<<>>, Acc, R) ->
    {ok, lists:reverse(Acc), R};
decode(Bin, Acc, #reader{codec = Codec} = R) ->
    case Codec:decode(Bin) of
        {ok, Capsule, Rest} -> decode(Rest, [Capsule | Acc], R);
        {more, _} -> incomplete(Bin, Acc, R);
        {error, Reason} -> {error, {malformed, Reason}}
    end.

%% `Bin' starts with a capsule that is not whole yet.
incomplete(Bin, Acc, #reader{codec = Codec, opts = Opts} = R) ->
    case header(Bin) of
        more ->
            {ok, lists:reverse(Acc), hold(Bin, 0, R)};
        {Type, Length, HeaderSize} ->
            Total = HeaderSize + Length,
            case Codec:buffer_rule(Type, Length, Opts) of
                keep ->
                    {ok, lists:reverse(Acc), hold(Bin, Total, R)};
                drop ->
                    {ok, lists:reverse([{dropped, Type} | Acc]),
                     R#reader{skip = Total - byte_size(Bin)}};
                {error, Kind, Reason} ->
                    {error, {Kind, Reason}}
            end
    end.

hold(Bin, Need, R) ->
    R#reader{chunks = [Bin], size = byte_size(Bin), need = Need}.

%% Capsule type and length (RFC 9297 §3.2): two variable-length integers.
header(Bin) ->
    case h2_varint:decode(Bin) of
        {ok, Type, Rest1} ->
            case h2_varint:decode(Rest1) of
                {ok, Length, Rest2} -> {Type, Length, byte_size(Bin) - byte_size(Rest2)};
                {error, _} -> more
            end;
        {error, _} ->
            more
    end.
