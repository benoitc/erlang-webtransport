%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc HTTP/2 WebTransport Capsules (draft-ietf-webtrans-http2-14)
%%
%% All WebTransport data over HTTP/2 flows through capsules in the
%% CONNECT stream body. This module handles encoding and decoding of
%% WebTransport-specific capsule types.
%%
-module(wt_h2_capsule).

-export([encode/1, decode/1, decode_all/1]).
-export([type_name/1]).
-export([buffer_rule/3]).

%% Capsule constructors
-export([padding/1]).
-export([wt_stream/2, wt_stream/3, wt_stream_fin/2, wt_stream_fin/3]).
-export([reset_stream/2, reset_stream/3, stop_sending/2]).
-export([max_data/1, max_stream_data/2]).
-export([max_streams_bidi/1, max_streams_uni/1]).
-export([data_blocked/1, stream_data_blocked/2]).
-export([streams_blocked_bidi/1, streams_blocked_uni/1]).
-export([close_session/1, close_session/2, drain_session/0]).
-export([datagram/1]).

-include("webtransport.hrl").

-type stream_id() :: non_neg_integer().
-type error_code() :: non_neg_integer().

-type capsule() ::
    {padding, binary()} |
    {wt_stream, stream_id(), binary()} |
    {wt_stream_fin, stream_id(), binary()} |
    {reset_stream, stream_id(), error_code(), non_neg_integer()} |
    {stop_sending, stream_id(), error_code()} |
    {max_data, non_neg_integer()} |
    {max_stream_data, stream_id(), non_neg_integer()} |
    {max_streams_bidi, non_neg_integer()} |
    {max_streams_uni, non_neg_integer()} |
    {data_blocked, non_neg_integer()} |
    {stream_data_blocked, stream_id(), non_neg_integer()} |
    {streams_blocked_bidi, non_neg_integer()} |
    {streams_blocked_uni, non_neg_integer()} |
    {close_session, error_code(), binary()} |
    {drain_session} |
    {datagram, binary()} |
    {unknown, non_neg_integer(), binary()}.

-export_type([capsule/0]).

%% ============================================================================
%% Capsule Constructors
%% ============================================================================

%% @doc Construct a PADDING capsule of the given size or from raw bytes.
-spec padding(non_neg_integer() | binary()) -> capsule().
padding(Size) when is_integer(Size) ->
    {padding, binary:copy(<<0>>, Size)};
padding(Data) when is_binary(Data) ->
    {padding, Data}.

%% @doc Construct a WT_STREAM capsule carrying data for the given stream.
-spec wt_stream(stream_id(), binary()) -> capsule().
wt_stream(StreamId, Data) ->
    {wt_stream, StreamId, Data}.

%% @doc Construct a WT_STREAM or WT_STREAM_FIN capsule depending on the fin flag.
-spec wt_stream(stream_id(), binary(), boolean()) -> capsule().
wt_stream(StreamId, Data, true) ->
    {wt_stream_fin, StreamId, Data};
wt_stream(StreamId, Data, false) ->
    {wt_stream, StreamId, Data}.

%% @doc Construct a WT_STREAM_FIN capsule carrying data with the fin bit set.
-spec wt_stream_fin(stream_id(), binary()) -> capsule().
wt_stream_fin(StreamId, Data) ->
    {wt_stream_fin, StreamId, Data}.

%% @doc Construct a WT_STREAM_FIN or WT_STREAM capsule depending on the fin flag.
-spec wt_stream_fin(stream_id(), binary(), boolean()) -> capsule().
wt_stream_fin(StreamId, Data, true) ->
    {wt_stream_fin, StreamId, Data};
wt_stream_fin(StreamId, Data, false) ->
    {wt_stream, StreamId, Data}.

%% @doc WT_RESET_STREAM with a Reliable Size of 0, for a stream we never
%% sent data on.
-spec reset_stream(stream_id(), error_code()) -> capsule().
reset_stream(StreamId, ErrorCode) ->
    reset_stream(StreamId, ErrorCode, 0).

%% @doc WT_RESET_STREAM. http2-16 §6.2: `ReliableSize' MUST equal the bytes
%% sent on the stream in WT_STREAM capsules.
-spec reset_stream(stream_id(), error_code(), non_neg_integer()) -> capsule().
reset_stream(StreamId, ErrorCode, ReliableSize) ->
    {reset_stream, StreamId, ErrorCode, ReliableSize}.

%% @doc Construct a STOP_SENDING capsule for the given stream and error code.
-spec stop_sending(stream_id(), error_code()) -> capsule().
stop_sending(StreamId, ErrorCode) ->
    {stop_sending, StreamId, ErrorCode}.

%% @doc Construct a MAX_DATA capsule with the given connection-level limit.
-spec max_data(non_neg_integer()) -> capsule().
max_data(Limit) ->
    {max_data, Limit}.

%% @doc Construct a MAX_STREAM_DATA capsule for the given stream and limit.
-spec max_stream_data(stream_id(), non_neg_integer()) -> capsule().
max_stream_data(StreamId, Limit) ->
    {max_stream_data, StreamId, Limit}.

%% @doc Construct a MAX_STREAMS capsule for bidirectional streams.
-spec max_streams_bidi(non_neg_integer()) -> capsule().
max_streams_bidi(Limit) ->
    {max_streams_bidi, Limit}.

%% @doc Construct a MAX_STREAMS capsule for unidirectional streams.
-spec max_streams_uni(non_neg_integer()) -> capsule().
max_streams_uni(Limit) ->
    {max_streams_uni, Limit}.

%% @doc Construct a DATA_BLOCKED capsule indicating the connection-level limit reached.
-spec data_blocked(non_neg_integer()) -> capsule().
data_blocked(Limit) ->
    {data_blocked, Limit}.

%% @doc Construct a STREAM_DATA_BLOCKED capsule for the given stream and limit.
-spec stream_data_blocked(stream_id(), non_neg_integer()) -> capsule().
stream_data_blocked(StreamId, Limit) ->
    {stream_data_blocked, StreamId, Limit}.

%% @doc Construct a STREAMS_BLOCKED capsule for bidirectional streams.
-spec streams_blocked_bidi(non_neg_integer()) -> capsule().
streams_blocked_bidi(Limit) ->
    {streams_blocked_bidi, Limit}.

%% @doc Construct a STREAMS_BLOCKED capsule for unidirectional streams.
-spec streams_blocked_uni(non_neg_integer()) -> capsule().
streams_blocked_uni(Limit) ->
    {streams_blocked_uni, Limit}.

%% @doc Construct a CLOSE_SESSION capsule with the given error code and no reason.
-spec close_session(error_code()) -> capsule().
close_session(ErrorCode) ->
    {close_session, ErrorCode, <<>>}.

%% @doc WT_CLOSE_SESSION, shared with h3 (http2-16 §6.12). Same rules as
%% {@link wt_h3_capsule:close_session/2}.
-spec close_session(error_code(), binary()) -> capsule() | {error, bad_close_reason}.
close_session(ErrorCode, Reason) ->
    wt_h3_capsule:close_session(ErrorCode, Reason).

%% @doc Construct a DRAIN_SESSION capsule to signal graceful shutdown.
-spec drain_session() -> capsule().
drain_session() ->
    {drain_session}.

%% @doc Construct a DATAGRAM capsule wrapping the given payload.
-spec datagram(binary()) -> capsule().
datagram(Data) ->
    {datagram, Data}.

%% ============================================================================
%% Encoding
%% ============================================================================

%% @doc Encode a capsule record into its wire-format binary.
-spec encode(capsule()) -> binary().
encode({padding, Data}) ->
    h2_capsule:encode(?WT_PADDING, Data);

encode({wt_stream, StreamId, Data}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary, Data/binary>>,
    h2_capsule:encode(?WT_STREAM, Payload);

encode({wt_stream_fin, StreamId, Data}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary, Data/binary>>,
    h2_capsule:encode(?WT_STREAM_FIN, Payload);

encode({reset_stream, StreamId, ErrorCode, ReliableSize}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary,
                (h2_varint:encode(ErrorCode))/binary,
                (h2_varint:encode(ReliableSize))/binary>>,
    h2_capsule:encode(?WT_RESET_STREAM, Payload);

encode({stop_sending, StreamId, ErrorCode}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary,
                (h2_varint:encode(ErrorCode))/binary>>,
    h2_capsule:encode(?WT_STOP_SENDING, Payload);

encode({max_data, Limit}) ->
    h2_capsule:encode(?WT_MAX_DATA, h2_varint:encode(Limit));

encode({max_stream_data, StreamId, Limit}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary,
                (h2_varint:encode(Limit))/binary>>,
    h2_capsule:encode(?WT_MAX_STREAM_DATA, Payload);

encode({max_streams_bidi, Limit}) ->
    h2_capsule:encode(?WT_MAX_STREAMS_BIDI, h2_varint:encode(Limit));

encode({max_streams_uni, Limit}) ->
    h2_capsule:encode(?WT_MAX_STREAMS_UNI, h2_varint:encode(Limit));

encode({data_blocked, Limit}) ->
    h2_capsule:encode(?WT_DATA_BLOCKED, h2_varint:encode(Limit));

encode({stream_data_blocked, StreamId, Limit}) ->
    Payload = <<(h2_varint:encode(StreamId))/binary,
                (h2_varint:encode(Limit))/binary>>,
    h2_capsule:encode(?WT_STREAM_DATA_BLOCKED, Payload);

encode({streams_blocked_bidi, Limit}) ->
    h2_capsule:encode(?WT_STREAMS_BLOCKED_BIDI, h2_varint:encode(Limit));

encode({streams_blocked_uni, Limit}) ->
    h2_capsule:encode(?WT_STREAMS_BLOCKED_UNI, h2_varint:encode(Limit));

encode({close_session, ErrorCode, Reason}) ->
    h2_capsule:encode(?WT_CLOSE_SESSION,
                      wt_h3_capsule:encode_close_payload(ErrorCode, Reason));

encode({drain_session}) ->
    h2_capsule:encode(?WT_DRAIN_SESSION, <<>>);

encode({datagram, Data}) ->
    h2_capsule:encode(?DATAGRAM, Data).

%% ============================================================================
%% Decoding
%% ============================================================================

%% @doc Decode the first capsule from a binary, returning the capsule and remaining bytes.
-spec decode(binary()) -> {ok, capsule(), binary()} | {more, pos_integer()} | {error, term()}.
decode(Bin) ->
    case h2_capsule:decode(Bin) of
        {ok, {Type, Payload}, Rest} ->
            case decode_payload(Type, Payload) of
                {ok, Capsule} -> {ok, Capsule, Rest};
                {error, _} = Err -> Err
            end;
        {more, N} ->
            {more, N}
    end.

decode_payload(?WT_PADDING, Data) ->
    {ok, {padding, Data}};

decode_payload(?WT_STREAM, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, StreamId, Data} -> {ok, {wt_stream, StreamId, Data}};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_STREAM_FIN, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, StreamId, Data} -> {ok, {wt_stream_fin, StreamId, Data}};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_RESET_STREAM, Payload) ->
    case decode_varints(Payload, 3) of
        {ok, [StreamId, ErrorCode, ReliableSize]} ->
            {ok, {reset_stream, StreamId, ErrorCode, ReliableSize}};
        {error, _} = Err ->
            Err
    end;

decode_payload(?WT_STOP_SENDING, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, StreamId, Rest} ->
            case h2_varint:decode(Rest) of
                {ok, ErrorCode, <<>>} -> {ok, {stop_sending, StreamId, ErrorCode}};
                {ok, _, _} -> {error, extra_data};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end;

decode_payload(?WT_MAX_DATA, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {max_data, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_MAX_STREAM_DATA, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, StreamId, Rest} ->
            case h2_varint:decode(Rest) of
                {ok, Limit, <<>>} -> {ok, {max_stream_data, StreamId, Limit}};
                {ok, _, _} -> {error, extra_data};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end;

decode_payload(?WT_MAX_STREAMS_BIDI, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {max_streams_bidi, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_MAX_STREAMS_UNI, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {max_streams_uni, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_DATA_BLOCKED, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {data_blocked, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_STREAM_DATA_BLOCKED, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, StreamId, Rest} ->
            case h2_varint:decode(Rest) of
                {ok, Limit, <<>>} -> {ok, {stream_data_blocked, StreamId, Limit}};
                {ok, _, _} -> {error, extra_data};
                {error, _} = Err -> Err
            end;
        {error, _} = Err -> Err
    end;

decode_payload(?WT_STREAMS_BLOCKED_BIDI, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {streams_blocked_bidi, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_STREAMS_BLOCKED_UNI, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {streams_blocked_uni, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end;

decode_payload(?WT_CLOSE_SESSION, Payload) ->
    wt_h3_capsule:decode_close_payload(Payload);

decode_payload(?WT_DRAIN_SESSION, <<>>) ->
    {ok, {drain_session}};
decode_payload(?WT_DRAIN_SESSION, _) ->
    {error, extra_data};

decode_payload(?DATAGRAM, Data) ->
    {ok, {datagram, Data}};
decode_payload(datagram, Data) ->
    {ok, {datagram, Data}};

decode_payload(Type, Payload) when is_integer(Type) ->
    {ok, {unknown, Type, Payload}};
decode_payload(Type, Payload) when is_atom(Type) ->
    {ok, {unknown, Type, Payload}}.

%% @doc Decode all capsules from binary.
%% Decode exactly N varints that fill the payload.
decode_varints(Bin, N) ->
    decode_varints(Bin, N, []).

decode_varints(<<>>, 0, Acc) ->
    {ok, lists:reverse(Acc)};
decode_varints(_Bin, 0, _Acc) ->
    {error, extra_data};
decode_varints(Bin, N, Acc) ->
    case h2_varint:decode(Bin) of
        {ok, V, Rest} -> decode_varints(Rest, N - 1, [V | Acc]);
        {error, _} = Err -> Err
    end.

-spec decode_all(binary()) -> {ok, [capsule()], binary()} | {error, term()}.
decode_all(Bin) ->
    decode_all(Bin, []).

decode_all(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
decode_all(Bin, Acc) ->
    case decode(Bin) of
        {ok, Capsule, Rest} ->
            decode_all(Rest, [Capsule|Acc]);
        {more, _} ->
            {ok, lists:reverse(Acc), Bin};
        {error, _} = Err ->
            Err
    end.

%% @doc What the CONNECT stream reader may hold of an unfinished capsule of
%% `Type' that declares `Length' bytes (see wt_capsule_reader).
%%
%% Padding and unknown types are skipped as they arrive (RFC 9297 §3.2,
%% http2-16 §6.1). A DATAGRAM too large to use is discarded (RFC 9297 §3.5).
%% WT_STREAM data beyond the stream's flow-control window is a flow-control
%% error (http2-16 §6.6): the window is `max_stream_data', plus 8 bytes for
%% the stream id.
-spec buffer_rule(non_neg_integer(), non_neg_integer(), map()) ->
    keep | drop | {error, flow_control | malformed, binary()}.
buffer_rule(?WT_PADDING, _Length, _Opts) ->
    drop;
buffer_rule(Type, Length, Opts) when Type =:= ?WT_STREAM; Type =:= ?WT_STREAM_FIN ->
    Window = maps:get(max_stream_data, Opts, ?DEFAULT_MAX_STREAM_DATA),
    case Length =< Window + 8 of
        true -> keep;
        false -> {error, flow_control, <<"stream capsule exceeds flow-control window">>}
    end;
buffer_rule(?DATAGRAM, Length, _Opts) when Length > ?WT_DATAGRAM_CAPSULE_MAX ->
    drop;
buffer_rule(?DATAGRAM, _Length, _Opts) ->
    keep;
buffer_rule(?WT_CLOSE_SESSION, Length, _Opts) when Length > ?WT_CLOSE_CAPSULE_MAX ->
    {error, malformed, <<"close reason too long">>};
buffer_rule(?WT_CLOSE_SESSION, _Length, _Opts) ->
    keep;
buffer_rule(Type, Length, _Opts) ->
    case type_name(Type) of
        Type -> drop;  %% unknown type
        _ when Length > ?WT_CONTROL_CAPSULE_MAX ->
            {error, malformed, <<"control capsule too long">>};
        _ -> keep
    end.

%% @doc Get the human-readable name of a capsule type.
-spec type_name(non_neg_integer()) -> atom() | non_neg_integer().
type_name(?WT_PADDING) -> padding;
type_name(?WT_RESET_STREAM) -> reset_stream;
type_name(?WT_STOP_SENDING) -> stop_sending;
type_name(?WT_STREAM) -> wt_stream;
type_name(?WT_STREAM_FIN) -> wt_stream_fin;
type_name(?WT_MAX_DATA) -> max_data;
type_name(?WT_MAX_STREAM_DATA) -> max_stream_data;
type_name(?WT_MAX_STREAMS_BIDI) -> max_streams_bidi;
type_name(?WT_MAX_STREAMS_UNI) -> max_streams_uni;
type_name(?WT_DATA_BLOCKED) -> data_blocked;
type_name(?WT_STREAM_DATA_BLOCKED) -> stream_data_blocked;
type_name(?WT_STREAMS_BLOCKED_BIDI) -> streams_blocked_bidi;
type_name(?WT_STREAMS_BLOCKED_UNI) -> streams_blocked_uni;
type_name(?WT_CLOSE_SESSION) -> close_session;
type_name(?WT_DRAIN_SESSION) -> drain_session;
type_name(?DATAGRAM) -> datagram;
type_name(N) -> N.

