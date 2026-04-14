%% @doc WebTransport interoperability protocol.
%%
%% This module implements the shared protocol for interop testing,
%% matching the quic-go/webtransport-go implementation.
%%
%% Request format: GET /path/to/file\n
%% Response format: PUSH filename\n<binary_payload>
%%
-module(interop).

-export([parse_request/1, format_request/1]).
-export([parse_response/1, format_response/2]).
-export([testcases/0]).

%% ============================================================================
%% Protocol Constants
%% ============================================================================

-define(REQUEST_PREFIX, <<"GET ">>).
-define(RESPONSE_PREFIX, <<"PUSH ">>).
-define(NEWLINE, <<"\n">>).

%% ============================================================================
%% Testcase Definitions
%% ============================================================================

%% @doc Returns the list of supported test cases.
-spec testcases() -> [atom()].
testcases() ->
    [
        handshake,
        transfer,
        'transfer-bidirectional',
        'transfer-unidirectional',
        'transfer-datagram'
    ].

%% ============================================================================
%% Request Parsing/Formatting
%% ============================================================================

%% @doc Parse an interop request.
%% Format: "GET /path/to/file\n"
-spec parse_request(binary()) -> {ok, Path :: binary()} | {error, term()}.
parse_request(Data) ->
    PrefixLen = byte_size(?REQUEST_PREFIX),
    case Data of
        <<Prefix:PrefixLen/binary, Rest/binary>> when Prefix =:= ?REQUEST_PREFIX ->
            case binary:split(Rest, ?NEWLINE) of
                [Path, <<>>] ->
                    {ok, Path};
                [Path, _Remaining] ->
                    {ok, Path};
                _ ->
                    {error, missing_newline}
            end;
        _ ->
            {error, invalid_request}
    end.

%% @doc Format an interop request.
-spec format_request(Path :: binary()) -> binary().
format_request(Path) ->
    <<?REQUEST_PREFIX/binary, Path/binary, ?NEWLINE/binary>>.

%% ============================================================================
%% Response Parsing/Formatting
%% ============================================================================

%% @doc Parse an interop response.
%% Format: "PUSH filename\n<payload>"
-spec parse_response(binary()) -> {ok, Filename :: binary(), Payload :: binary()} | {error, term()}.
parse_response(Data) ->
    PrefixLen = byte_size(?RESPONSE_PREFIX),
    case Data of
        <<Prefix:PrefixLen/binary, Rest/binary>> when Prefix =:= ?RESPONSE_PREFIX ->
            case binary:split(Rest, ?NEWLINE) of
                [Filename, Payload] ->
                    {ok, Filename, Payload};
                _ ->
                    {error, invalid_response}
            end;
        _ ->
            {error, invalid_response}
    end.

%% @doc Format an interop response.
-spec format_response(Filename :: binary(), Payload :: binary()) -> binary().
format_response(Filename, Payload) ->
    <<?RESPONSE_PREFIX/binary, Filename/binary, ?NEWLINE/binary, Payload/binary>>.
