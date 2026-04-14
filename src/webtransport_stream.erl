%% @doc WebTransport stream state management.
%%
%% This module provides functional state management for individual WebTransport
%% streams. Streams are managed by the session process and don't run as separate
%% processes.
%%
%% Stream IDs follow these conventions:
%% - Client-initiated bidirectional: 0, 4, 8, ... (4n)
%% - Server-initiated bidirectional: 1, 5, 9, ... (4n+1)
%% - Client-initiated unidirectional: 2, 6, 10, ... (4n+2)
%% - Server-initiated unidirectional: 3, 7, 11, ... (4n+3)
%%
-module(webtransport_stream).

-include("webtransport.hrl").

%% Stream creation
-export([new/3, new/4]).

%% State queries
-export([id/1, type/1, state/1]).
-export([is_open/1, is_writable/1, is_readable/1]).
-export([send_window/1, recv_window/1]).
-export([send_buffer/1, recv_buffer/1]).

%% State transitions
-export([send/2, receive_data/2, receive_fin/1]).
-export([close_local/1, close_remote/1, close/1]).
-export([reset/2, stop_sending/2]).
-export([update_send_window/2, update_recv_window/2]).

%% Buffer management
-export([flush_send_buffer/1, flush_recv_buffer/1]).
-export([buffer_send/2]).

%% Stream ID helpers
-export([stream_type/1, initiator/1]).
-export([is_bidi/1, is_uni/1]).
-export([is_client_initiated/1, is_server_initiated/1]).

-record(stream, {
    id :: non_neg_integer(),
    type :: bidi | uni,
    state = open :: stream_state(),
    %% Flow control
    send_window :: non_neg_integer(),
    recv_window :: non_neg_integer(),
    bytes_sent = 0 :: non_neg_integer(),
    bytes_received = 0 :: non_neg_integer(),
    %% Buffers
    send_buffer = <<>> :: binary(),
    recv_buffer = <<>> :: binary(),
    %% Flags
    local_fin = false :: boolean(),
    remote_fin = false :: boolean(),
    reset_code :: undefined | non_neg_integer(),
    stop_sending_code :: undefined | non_neg_integer()
}).

-opaque stream() :: #stream{}.
-type stream_state() :: open | half_closed_local | half_closed_remote | closed.

-export_type([stream/0, stream_state/0]).

%% ============================================================================
%% Stream Creation
%% ============================================================================

-spec new(non_neg_integer(), bidi | uni, non_neg_integer()) -> stream().
new(Id, Type, Window) ->
    new(Id, Type, Window, Window).

-spec new(non_neg_integer(), bidi | uni, non_neg_integer(), non_neg_integer()) -> stream().
new(Id, Type, SendWindow, RecvWindow) ->
    #stream{
        id = Id,
        type = Type,
        state = open,
        send_window = SendWindow,
        recv_window = RecvWindow
    }.

%% ============================================================================
%% State Queries
%% ============================================================================

-spec id(stream()) -> non_neg_integer().
id(#stream{id = Id}) -> Id.

-spec type(stream()) -> bidi | uni.
type(#stream{type = Type}) -> Type.

-spec state(stream()) -> stream_state().
state(#stream{state = State}) -> State.

-spec is_open(stream()) -> boolean().
is_open(#stream{state = State}) ->
    State =:= open orelse State =:= half_closed_local orelse State =:= half_closed_remote.

-spec is_writable(stream()) -> boolean().
is_writable(#stream{state = State, local_fin = Fin, reset_code = Reset}) ->
    Reset =:= undefined andalso not Fin andalso
    (State =:= open orelse State =:= half_closed_remote).

-spec is_readable(stream()) -> boolean().
is_readable(#stream{state = State, remote_fin = Fin, stop_sending_code = Stop}) ->
    Stop =:= undefined andalso not Fin andalso
    (State =:= open orelse State =:= half_closed_local).

-spec send_window(stream()) -> non_neg_integer().
send_window(#stream{send_window = W}) -> W.

-spec recv_window(stream()) -> non_neg_integer().
recv_window(#stream{recv_window = W}) -> W.

-spec send_buffer(stream()) -> binary().
send_buffer(#stream{send_buffer = B}) -> B.

-spec recv_buffer(stream()) -> binary().
recv_buffer(#stream{recv_buffer = B}) -> B.

%% ============================================================================
%% State Transitions
%% ============================================================================

-spec send(stream(), binary()) -> {ok, binary(), stream()} | {error, term()}.
send(#stream{state = closed}, _Data) ->
    {error, stream_closed};
send(#stream{state = half_closed_local}, _Data) ->
    {error, stream_half_closed};
send(#stream{local_fin = true}, _Data) ->
    {error, stream_fin_sent};
send(#stream{send_window = Window, bytes_sent = Sent} = Stream, Data) ->
    Available = Window - Sent,
    DataSize = byte_size(Data),
    case DataSize =< Available of
        true ->
            {ok, Data, Stream#stream{bytes_sent = Sent + DataSize}};
        false when Available > 0 ->
            <<ToSend:Available/binary, Rest/binary>> = Data,
            NewStream = Stream#stream{
                bytes_sent = Sent + Available,
                send_buffer = <<(Stream#stream.send_buffer)/binary, Rest/binary>>
            },
            {ok, ToSend, NewStream};
        false ->
            NewStream = Stream#stream{
                send_buffer = <<(Stream#stream.send_buffer)/binary, Data/binary>>
            },
            {ok, <<>>, NewStream}
    end.

-spec receive_data(stream(), binary()) -> {ok, stream()} | {error, term()}.
receive_data(#stream{state = closed}, _Data) ->
    {error, stream_closed};
receive_data(#stream{state = half_closed_remote}, _Data) ->
    {error, stream_half_closed};
receive_data(#stream{remote_fin = true}, _Data) ->
    {error, stream_fin_received};
receive_data(#stream{recv_window = Window, bytes_received = Received} = Stream, Data) ->
    DataSize = byte_size(Data),
    NewReceived = Received + DataSize,
    case NewReceived > Window of
        true ->
            {error, flow_control_error};
        false ->
            NewStream = Stream#stream{
                bytes_received = NewReceived,
                recv_buffer = <<(Stream#stream.recv_buffer)/binary, Data/binary>>
            },
            {ok, NewStream}
    end.

-spec receive_fin(stream()) -> {ok, stream()} | {error, term()}.
receive_fin(#stream{state = closed}) ->
    {error, stream_closed};
receive_fin(#stream{remote_fin = true}) ->
    {error, duplicate_fin};
receive_fin(#stream{state = open} = Stream) ->
    {ok, Stream#stream{state = half_closed_remote, remote_fin = true}};
receive_fin(#stream{state = half_closed_local} = Stream) ->
    {ok, Stream#stream{state = closed, remote_fin = true}};
receive_fin(#stream{state = half_closed_remote}) ->
    {error, stream_already_half_closed}.

-spec close_local(stream()) -> {ok, stream()} | {error, term()}.
close_local(#stream{state = closed}) ->
    {error, stream_closed};
close_local(#stream{local_fin = true}) ->
    {error, duplicate_fin};
close_local(#stream{state = open} = Stream) ->
    {ok, Stream#stream{state = half_closed_local, local_fin = true}};
close_local(#stream{state = half_closed_remote} = Stream) ->
    {ok, Stream#stream{state = closed, local_fin = true}};
close_local(#stream{state = half_closed_local}) ->
    {error, stream_already_half_closed}.

-spec close_remote(stream()) -> {ok, stream()} | {error, term()}.
close_remote(Stream) ->
    receive_fin(Stream).

-spec close(stream()) -> stream().
close(Stream) ->
    Stream#stream{state = closed, local_fin = true, remote_fin = true}.

-spec reset(stream(), non_neg_integer()) -> stream().
reset(Stream, ErrorCode) ->
    Stream#stream{
        state = closed,
        reset_code = ErrorCode,
        send_buffer = <<>>
    }.

-spec stop_sending(stream(), non_neg_integer()) -> stream().
stop_sending(Stream, ErrorCode) ->
    Stream#stream{stop_sending_code = ErrorCode}.

-spec update_send_window(stream(), non_neg_integer()) -> stream().
update_send_window(Stream, NewWindow) ->
    Stream#stream{send_window = NewWindow}.

-spec update_recv_window(stream(), non_neg_integer()) -> stream().
update_recv_window(Stream, NewWindow) ->
    Stream#stream{recv_window = NewWindow}.

%% ============================================================================
%% Buffer Management
%% ============================================================================

-spec flush_send_buffer(stream()) -> {binary(), stream()}.
flush_send_buffer(#stream{send_buffer = Buffer} = Stream) ->
    {Buffer, Stream#stream{send_buffer = <<>>}}.

-spec flush_recv_buffer(stream()) -> {binary(), stream()}.
flush_recv_buffer(#stream{recv_buffer = Buffer} = Stream) ->
    {Buffer, Stream#stream{recv_buffer = <<>>}}.

-spec buffer_send(stream(), binary()) -> stream().
buffer_send(#stream{send_buffer = Buffer} = Stream, Data) ->
    Stream#stream{send_buffer = <<Buffer/binary, Data/binary>>}.

%% ============================================================================
%% Stream ID Helpers
%% ============================================================================

-spec stream_type(non_neg_integer()) -> bidi | uni.
stream_type(StreamId) when StreamId band 2 =:= 0 -> bidi;
stream_type(_StreamId) -> uni.

-spec initiator(non_neg_integer()) -> client | server.
initiator(StreamId) when StreamId band 1 =:= 0 -> client;
initiator(_StreamId) -> server.

-spec is_bidi(non_neg_integer() | stream()) -> boolean().
is_bidi(#stream{type = Type}) -> Type =:= bidi;
is_bidi(StreamId) when is_integer(StreamId) -> stream_type(StreamId) =:= bidi.

-spec is_uni(non_neg_integer() | stream()) -> boolean().
is_uni(#stream{type = Type}) -> Type =:= uni;
is_uni(StreamId) when is_integer(StreamId) -> stream_type(StreamId) =:= uni.

-spec is_client_initiated(non_neg_integer()) -> boolean().
is_client_initiated(StreamId) -> initiator(StreamId) =:= client.

-spec is_server_initiated(non_neg_integer()) -> boolean().
is_server_initiated(StreamId) -> initiator(StreamId) =:= server.

