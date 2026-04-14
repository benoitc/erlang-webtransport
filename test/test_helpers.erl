%% @doc Test helpers for WebTransport E2E tests.
-module(test_helpers).

-export([generate_self_signed_cert/1]).
-export([echo_handler/0]).
-export([wait_for/2, wait_for/3]).
-export([random_data/1]).
-export([find_free_port/0]).

%% ============================================================================
%% Certificate Generation
%% ============================================================================

%% @doc Generate a self-signed certificate for testing.
%% Returns the paths to the generated cert and key files.
-spec generate_self_signed_cert(Dir :: file:filename()) ->
    {ok, #{certfile := file:filename(), keyfile := file:filename()}} |
    {error, term()}.
generate_self_signed_cert(Dir) ->
    KeyFile = filename:join(Dir, "test_key.pem"),
    CertFile = filename:join(Dir, "test_cert.pem"),

    %% Generate RSA private key
    KeyCmd = io_lib:format(
        "openssl genrsa -out ~s 2048 2>/dev/null",
        [KeyFile]
    ),
    case os:cmd(lists:flatten(KeyCmd)) of
        "" ->
            %% Generate self-signed certificate
            CertCmd = io_lib:format(
                "openssl req -new -x509 -key ~s -out ~s -days 1 "
                "-subj \"/CN=localhost\" 2>/dev/null",
                [KeyFile, CertFile]
            ),
            case os:cmd(lists:flatten(CertCmd)) of
                "" ->
                    {ok, #{certfile => CertFile, keyfile => KeyFile}};
                Err2 ->
                    {error, {cert_generation_failed, Err2}}
            end;
        Err1 ->
            {error, {key_generation_failed, Err1}}
    end.

%% ============================================================================
%% Test Handlers
%% ============================================================================

%% @doc Returns a simple echo handler module name.
%% This handler echoes back data received on streams and datagrams.
-spec echo_handler() -> module().
echo_handler() ->
    wt_echo_handler.

%% ============================================================================
%% Wait Utilities
%% ============================================================================

%% @doc Wait for a condition to be true, polling every 100ms.
-spec wait_for(Fun :: fun(() -> boolean()), Timeout :: timeout()) ->
    ok | {error, timeout}.
wait_for(Fun, Timeout) ->
    wait_for(Fun, Timeout, 100).

%% @doc Wait for a condition to be true, polling at specified interval.
-spec wait_for(Fun :: fun(() -> boolean()), Timeout :: timeout(),
               Interval :: pos_integer()) -> ok | {error, timeout}.
wait_for(Fun, Timeout, Interval) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_loop(Fun, Deadline, Interval).

wait_for_loop(Fun, Deadline, Interval) ->
    case Fun() of
        true ->
            ok;
        false ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    {error, timeout};
                false ->
                    timer:sleep(Interval),
                    wait_for_loop(Fun, Deadline, Interval)
            end
    end.

%% ============================================================================
%% Data Generation
%% ============================================================================

%% @doc Generate random binary data of specified size.
-spec random_data(Size :: pos_integer()) -> binary().
random_data(Size) when Size > 0 ->
    crypto:strong_rand_bytes(Size).

%% ============================================================================
%% Port Utilities
%% ============================================================================

%% @doc Find a free port on localhost.
-spec find_free_port() -> inet:port_number().
find_free_port() ->
    {ok, Socket} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(Socket),
    gen_tcp:close(Socket),
    Port.
