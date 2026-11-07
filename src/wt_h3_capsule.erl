%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc HTTP/3 WebTransport capsules and stream/datagram headers.
%%
%% HTTP/3 WebTransport uses:
%% - capsules on the CONNECT stream for session control and flow control
%% - native QUIC stream headers for data streams
%% - HTTP Datagram framing for unreliable datagrams
%%
-module(wt_h3_capsule).

-export([encode/1, decode/1, decode_all/1]).
-export([type_name/1]).

%% Capsule constructors
-export([max_data/1, data_blocked/1]).
-export([max_streams_bidi/1, max_streams_uni/1]).
-export([streams_blocked_bidi/1, streams_blocked_uni/1]).
-export([close_session/1, close_session/2, drain_session/0]).
-export([encode_close_payload/2, decode_close_payload/1, valid_close_reason/1]).

%% Native stream helpers
-export([encode_uni_stream_header/1, encode_bidi_stream_header/1]).
-export([decode_stream_header/1]).

%% HTTP Datagram helpers
-export([encode_datagram/2, decode_datagram/1]).
-export([quarter_stream_id/1, session_id_from_quarter_stream_id/1]).

-include("webtransport.hrl").

-type capsule() ::
    {max_data, non_neg_integer()} |
    {data_blocked, non_neg_integer()} |
    {max_streams_bidi, non_neg_integer()} |
    {max_streams_uni, non_neg_integer()} |
    {streams_blocked_bidi, non_neg_integer()} |
    {streams_blocked_uni, non_neg_integer()} |
    {close_session, non_neg_integer(), binary()} |
    {drain_session} |
    {datagram, binary()} |
    {unknown, non_neg_integer(), binary()}.

-type stream_kind() :: bidi | uni.

-export_type([capsule/0, stream_kind/0]).

%% ============================================================================
%% Constructors
%% ============================================================================

%% @doc Construct a MAX_DATA capsule with the given connection-level limit.
-spec max_data(non_neg_integer()) -> capsule().
max_data(Limit) ->
    {max_data, Limit}.

%% @doc Construct a DATA_BLOCKED capsule indicating the connection-level limit reached.
-spec data_blocked(non_neg_integer()) -> capsule().
data_blocked(Limit) ->
    {data_blocked, Limit}.

%% @doc Construct a MAX_STREAMS capsule for bidirectional streams.
-spec max_streams_bidi(non_neg_integer()) -> capsule().
max_streams_bidi(Limit) ->
    {max_streams_bidi, Limit}.

%% @doc Construct a MAX_STREAMS capsule for unidirectional streams.
-spec max_streams_uni(non_neg_integer()) -> capsule().
max_streams_uni(Limit) ->
    {max_streams_uni, Limit}.

%% @doc Construct a STREAMS_BLOCKED capsule for bidirectional streams.
-spec streams_blocked_bidi(non_neg_integer()) -> capsule().
streams_blocked_bidi(Limit) ->
    {streams_blocked_bidi, Limit}.

%% @doc Construct a STREAMS_BLOCKED capsule for unidirectional streams.
-spec streams_blocked_uni(non_neg_integer()) -> capsule().
streams_blocked_uni(Limit) ->
    {streams_blocked_uni, Limit}.

%% @doc Construct a CLOSE_SESSION capsule with the given error code and no reason.
-spec close_session(non_neg_integer()) -> capsule().
close_session(ErrorCode) ->
    {close_session, ErrorCode, <<>>}.

%% @doc Construct a CLOSE_SESSION capsule with the given error code and reason.
%% draft-15 §5: the Reason field MUST be at most 1024 UTF-8 bytes.
%% @doc Build a WT_CLOSE_SESSION capsule. The code must fit in 32 bits and
%% the reason must be valid UTF-8 of at most 1024 bytes (http3-17 §6).
-spec close_session(non_neg_integer(), binary()) -> capsule() | {error, bad_close_reason}.
close_session(ErrorCode, Reason) when is_integer(ErrorCode), ErrorCode >= 0,
                                      ErrorCode =< 16#ffffffff ->
    case valid_close_reason(Reason) of
        true -> {close_session, ErrorCode, Reason};
        false -> {error, bad_close_reason}
    end;
close_session(_ErrorCode, _Reason) ->
    {error, bad_close_reason}.

%% @doc True when `Reason' is valid UTF-8 of at most 1024 bytes.
-spec valid_close_reason(binary()) -> boolean().
valid_close_reason(Reason) when is_binary(Reason),
                                byte_size(Reason) =< ?WT_CLOSE_REASON_MAX ->
    unicode:characters_to_binary(Reason) =:= Reason;
valid_close_reason(_) ->
    false.

%% @doc Payload of a WT_CLOSE_SESSION capsule: a 32-bit error code followed
%% by the reason. Shared by the h2 and h3 codecs.
-spec encode_close_payload(non_neg_integer(), binary()) -> binary().
encode_close_payload(ErrorCode, Reason) ->
    <<ErrorCode:32, Reason/binary>>.

%% @doc Decode a WT_CLOSE_SESSION payload. Fails when the payload is shorter
%% than the error code, or when the reason is too long or not UTF-8.
-spec decode_close_payload(binary()) ->
    {ok, {close_session, non_neg_integer(), binary()}} | {error, term()}.
decode_close_payload(<<ErrorCode:32, Reason/binary>>) ->
    case valid_close_reason(Reason) of
        true -> {ok, {close_session, ErrorCode, Reason}};
        false -> {error, bad_close_reason}
    end;
decode_close_payload(_) ->
    {error, truncated_close_session}.

%% @doc Construct a DRAIN_SESSION capsule to signal graceful shutdown.
-spec drain_session() -> capsule().
drain_session() ->
    {drain_session}.

%% ============================================================================
%% Encoding
%% ============================================================================

%% @doc Encode a capsule record into its wire-format binary.
-spec encode(capsule()) -> binary().
encode({max_data, Limit}) ->
    h2_capsule:encode(?WT_MAX_DATA, h2_varint:encode(Limit));
encode({data_blocked, Limit}) ->
    h2_capsule:encode(?WT_DATA_BLOCKED, h2_varint:encode(Limit));
encode({max_streams_bidi, Limit}) ->
    h2_capsule:encode(?WT_MAX_STREAMS_BIDI, h2_varint:encode(Limit));
encode({max_streams_uni, Limit}) ->
    h2_capsule:encode(?WT_MAX_STREAMS_UNI, h2_varint:encode(Limit));
encode({streams_blocked_bidi, Limit}) ->
    h2_capsule:encode(?WT_STREAMS_BLOCKED_BIDI, h2_varint:encode(Limit));
encode({streams_blocked_uni, Limit}) ->
    h2_capsule:encode(?WT_STREAMS_BLOCKED_UNI, h2_varint:encode(Limit));
encode({close_session, ErrorCode, Reason}) ->
    h2_capsule:encode(?WT_CLOSE_SESSION, encode_close_payload(ErrorCode, Reason));
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

decode_payload(?WT_MAX_DATA, Payload) ->
    decode_limit(max_data, Payload);
decode_payload(?WT_DATA_BLOCKED, Payload) ->
    decode_limit(data_blocked, Payload);
decode_payload(?WT_MAX_STREAMS_BIDI, Payload) ->
    decode_limit(max_streams_bidi, Payload);
decode_payload(?WT_MAX_STREAMS_UNI, Payload) ->
    decode_limit(max_streams_uni, Payload);
decode_payload(?WT_STREAMS_BLOCKED_BIDI, Payload) ->
    decode_limit(streams_blocked_bidi, Payload);
decode_payload(?WT_STREAMS_BLOCKED_UNI, Payload) ->
    decode_limit(streams_blocked_uni, Payload);
decode_payload(?WT_CLOSE_SESSION, Payload) ->
    decode_close_payload(Payload);
decode_payload(?WT_DRAIN_SESSION, <<>>) ->
    {ok, {drain_session}};
decode_payload(?WT_DRAIN_SESSION, _) ->
    {error, extra_data};
decode_payload(?DATAGRAM, Payload) ->
    {ok, {datagram, Payload}};
decode_payload(datagram, Payload) ->
    %% h2_capsule names the RFC 9297 type.
    {ok, {datagram, Payload}};
decode_payload(Type, Payload) when is_integer(Type) ->
    {ok, {unknown, Type, Payload}};
decode_payload(Type, Payload) when is_atom(Type) ->
    {ok, {unknown, Type, Payload}}.

%% @doc Decode all capsules from a binary, returning the list and any trailing bytes.
-spec decode_all(binary()) -> {ok, [capsule()], binary()} | {error, term()}.
decode_all(Bin) ->
    decode_all(Bin, []).

decode_all(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
decode_all(Bin, Acc) ->
    case decode(Bin) of
        {ok, Capsule, Rest} ->
            decode_all(Rest, [Capsule | Acc]);
        {more, _} ->
            {ok, lists:reverse(Acc), Bin};
        {error, _} = Err ->
            Err
    end.

decode_limit(Name, Payload) ->
    case h2_varint:decode(Payload) of
        {ok, Limit, <<>>} -> {ok, {Name, Limit}};
        {ok, _, _} -> {error, extra_data};
        {error, _} = Err -> Err
    end.

%% ============================================================================
%% Native Stream Headers
%% ============================================================================

%% @doc Encode a unidirectional WebTransport stream header for the given session.
-spec encode_uni_stream_header(non_neg_integer()) -> binary().
encode_uni_stream_header(SessionId) ->
    validate_session_id(SessionId),
    <<(h2_varint:encode(?WT_UNI_STREAM_TYPE))/binary, (h2_varint:encode(SessionId))/binary>>.

%% @doc Encode a bidirectional WebTransport stream header for the given session.
-spec encode_bidi_stream_header(non_neg_integer()) -> binary().
encode_bidi_stream_header(SessionId) ->
    validate_session_id(SessionId),
    <<(h2_varint:encode(?WT_BIDI_SIGNAL))/binary, (h2_varint:encode(SessionId))/binary>>.

%% @doc Decode a WebTransport stream header, returning the session ID and stream kind.
-spec decode_stream_header(binary()) ->
    {ok, non_neg_integer(), stream_kind(), binary()} | {more, pos_integer()} | {error, term()}.
decode_stream_header(Bin) ->
    case h2_varint:decode(Bin) of
        {ok, ?WT_UNI_STREAM_TYPE, Rest} ->
            decode_stream_session_id(uni, Rest);
        {ok, ?WT_BIDI_SIGNAL, Rest} ->
            decode_stream_session_id(bidi, Rest);
        {ok, Type, _Rest} ->
            {error, {unknown_stream_header, Type}};
        {error, incomplete} ->
            {more, 1}
    end.

decode_stream_session_id(Kind, Bin) ->
    case h2_varint:decode(Bin) of
        {ok, SessionId, Rest} -> {ok, SessionId, Kind, Rest};
        {error, incomplete} -> {more, 1}
    end.

%% ============================================================================
%% HTTP Datagrams
%% ============================================================================

%% @doc Convert a session ID to its quarter stream ID for datagram framing.
-spec quarter_stream_id(non_neg_integer()) -> non_neg_integer().
quarter_stream_id(SessionId) when is_integer(SessionId), SessionId >= 0, SessionId rem 4 =:= 0 ->
    SessionId div 4;
quarter_stream_id(SessionId) ->
    error({invalid_session_id, SessionId}).

%% @doc Convert a quarter stream ID back to the original session ID.
-spec session_id_from_quarter_stream_id(non_neg_integer()) -> non_neg_integer().
session_id_from_quarter_stream_id(QuarterStreamId) when is_integer(QuarterStreamId), QuarterStreamId >= 0 ->
    QuarterStreamId * 4.

%% @doc Encode an HTTP Datagram payload with the quarter stream ID prefix.
-spec encode_datagram(non_neg_integer(), binary()) -> binary().
encode_datagram(SessionId, Data) ->
    QuarterStreamId = quarter_stream_id(SessionId),
    <<(h2_varint:encode(QuarterStreamId))/binary, Data/binary>>.

%% @doc Decode an HTTP Datagram, returning the session ID and payload.
-spec decode_datagram(binary()) ->
    {ok, non_neg_integer(), binary()} | {more, pos_integer()} | {error, term()}.
decode_datagram(Bin) ->
    case h2_varint:decode(Bin) of
        {ok, QuarterStreamId, Data} ->
            {ok, session_id_from_quarter_stream_id(QuarterStreamId), Data};
        {error, incomplete} ->
            {more, 1}
    end.

%% ============================================================================
%% Helpers
%% ============================================================================

%% @doc Return the human-readable atom for a capsule type code.
-spec type_name(non_neg_integer()) -> atom() | non_neg_integer().
type_name(?WT_MAX_DATA) -> max_data;
type_name(?WT_DATA_BLOCKED) -> data_blocked;
type_name(?WT_MAX_STREAMS_BIDI) -> max_streams_bidi;
type_name(?WT_MAX_STREAMS_UNI) -> max_streams_uni;
type_name(?WT_STREAMS_BLOCKED_BIDI) -> streams_blocked_bidi;
type_name(?WT_STREAMS_BLOCKED_UNI) -> streams_blocked_uni;
type_name(?WT_CLOSE_SESSION) -> close_session;
type_name(?WT_DRAIN_SESSION) -> drain_session;
type_name(N) -> N.

validate_session_id(SessionId) when is_integer(SessionId), SessionId >= 0, SessionId rem 4 =:= 0 ->
    ok;
validate_session_id(SessionId) ->
    error({invalid_session_id, SessionId}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

capsule_roundtrip_test_() ->
    Capsules = [
        max_data(1000),
        data_blocked(999),
        max_streams_bidi(10),
        max_streams_uni(11),
        streams_blocked_bidi(5),
        streams_blocked_uni(6),
        close_session(16#42, <<"done">>),
        drain_session()
    ],
    [?_assertEqual({ok, Capsule, <<>>}, decode(encode(Capsule))) || Capsule <- Capsules].

stream_header_roundtrip_test_() ->
    SessionId = 16,
    [
        ?_assertEqual({ok, SessionId, bidi, <<>>}, decode_stream_header(encode_bidi_stream_header(SessionId))),
        ?_assertEqual({ok, SessionId, uni, <<>>}, decode_stream_header(encode_uni_stream_header(SessionId)))
    ].

datagram_roundtrip_test() ->
    Encoded = encode_datagram(8, <<"payload">>),
    ?assertEqual({ok, 8, <<"payload">>}, decode_datagram(Encoded)).

invalid_session_id_test() ->
    ?assertError({invalid_session_id, 3}, encode_bidi_stream_header(3)).

close_session_reason_test_() ->
    Boundary = binary:copy(<<"x">>, 1024),
    TooLong = <<Boundary/binary, "!">>,
    [
        ?_assertMatch({close_session, 1, _}, close_session(1, Boundary)),
        ?_assertEqual({error, bad_close_reason}, close_session(1, TooLong)),
        ?_assertEqual({error, bad_close_reason}, close_session(1, <<255>>)),
        ?_assertEqual({error, bad_close_reason}, close_session(1 bsl 32, <<>>))
    ].

%% http3-17 §6: the error code is a fixed 32 bits, not a varint.
close_session_wire_format_test() ->
    Encoded = encode(close_session(7, <<"x">>)),
    ?assertEqual(h2_capsule:encode(16#2843, <<0, 0, 0, 7, "x">>), Encoded),
    ?assertEqual({ok, {close_session, 7, <<"x">>}, <<>>},
                 decode(h2_capsule:encode(16#2843, <<0, 0, 0, 7, "x">>))).

close_session_decode_bad_reason_test_() ->
    TooLong = h2_capsule:encode(?WT_CLOSE_SESSION,
                                <<7:32, (binary:copy(<<"z">>, 1025))/binary>>),
    NotUtf8 = h2_capsule:encode(?WT_CLOSE_SESSION, <<7:32, 16#ff>>),
    Short = h2_capsule:encode(?WT_CLOSE_SESSION, <<7:16>>),
    [
        ?_assertEqual({error, bad_close_reason}, decode(TooLong)),
        ?_assertEqual({error, bad_close_reason}, decode(NotUtf8)),
        ?_assertEqual({error, truncated_close_session}, decode(Short))
    ].

drain_session_wire_format_test() ->
    ?assertEqual(h2_capsule:encode(16#78ae, <<>>), encode(drain_session())).

unknown_and_datagram_capsule_test_() ->
    [
        ?_assertEqual({ok, {unknown, 16#17, <<"x">>}, <<>>},
                      decode(h2_capsule:encode(16#17, <<"x">>))),
        ?_assertEqual({ok, {datagram, <<"d">>}, <<>>},
                      decode(encode({datagram, <<"d">>})))
    ].

-endif.
