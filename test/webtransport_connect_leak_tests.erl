-module(webtransport_connect_leak_tests).

-include_lib("eunit/include/eunit.hrl").

links() ->
    {links, L} = process_info(self(), links),
    lists:sort(L).

%% A failed h3 connect must not leave its per-connection router linked to the
%% caller. The router is start_link'd to the caller and traps exits, so the
%% caller link alone never reaps it — connect_h3 must stop it on the error
%% path. We connect to an unused UDP port with a short timeout so the QUIC
%% handshake fails fast, then assert no new linked process lingers.
failed_h3_connect_leaves_no_router_test_() ->
    {timeout, 30, fun() ->
        {ok, _} = application:ensure_all_started(quic),
        Before = links(),
        Opts = #{transport => h3, timeout => 300, verify => verify_none},
        ?assertMatch({error, _},
                     webtransport:connect("127.0.0.1", 64999, <<"/x">>, Opts)),
        timer:sleep(50),
        After = links(),
        ?assertEqual([], After -- Before)
    end}.

%% An h3 server that refuses the client right after the handshake (here it
%% requires a client certificate and gets none, 0x174 certificate_required)
%% must give `connect' an error with the server's close reason. With quic
%% before 3.3.0 the close could race the HTTP/3 hand-off: the router crashed
%% inside the call, taking the caller down through its link, or the connect
%% waited until its timeout. Repeat it so the race is hit.
refused_after_handshake_h3_connect_test_() ->
    {setup, fun start_mtls_server/0, fun stop_mtls_server/1,
     fun(#{port := Port}) ->
         {timeout, 120, fun() -> refused_connects(Port, 30) end}
     end}.

refused_connects(Port, N) ->
    Before = links(),
    Opts = #{transport => h3, timeout => 5000, verify => verify_none},
    Results = [timed_connect(Port, Opts) || _ <- lists:seq(1, N)],
    [?assertMatch({{error, {peer_closed, transport, 16#174, _, _}}, _}, R)
     || R <- Results],
    %% Each refusal is reported as it arrives, not at the timeout.
    [?assert(Ms < 2000) || {_, Ms} <- Results],
    timer:sleep(50),
    ?assertEqual([], links() -- Before).

%% Run the connect in a fresh process so a crash or link exit shows up as a
%% result instead of killing the test process.
timed_connect(Port, Opts) ->
    {Pid, Ref} = spawn_monitor(fun() ->
        T0 = erlang:monotonic_time(millisecond),
        Result = webtransport:connect("127.0.0.1", Port, <<"/x">>, Opts),
        Ms = erlang:monotonic_time(millisecond) - T0,
        exit({done, Result, Ms})
    end),
    receive
        {'DOWN', Ref, process, Pid, {done, Result, Ms}} -> {Result, Ms};
        {'DOWN', Ref, process, Pid, Other} -> {{crashed, Other}, 0}
    after 10000 ->
        exit(Pid, kill),
        {timeout, 10000}
    end.

start_mtls_server() ->
    {ok, _} = application:ensure_all_started(quic),
    #{server_config := S} = public_key:pkix_test_data(#{
        server_chain => test_chain(),
        client_chain => test_chain()
    }),
    {KeyType, KeyDer} = proplists:get_value(key, S),
    Name = list_to_atom("wt_mtls_" ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, _} = quic_h3:start_server(Name, 0, #{
        cert => proplists:get_value(cert, S),
        key => public_key:der_decode(KeyType, KeyDer),
        cacerts => proplists:get_value(cacerts, S),
        quic_opts => #{require_client_cert => true},
        handler => fun(Conn, StreamId, _M, _P, _H) ->
            quic_h3:respond(Conn, StreamId, 200, [], <<>>)
        end
    }),
    {ok, Port} = quic:get_server_port(Name),
    #{name => Name, port => Port}.

stop_mtls_server(#{name := Name}) ->
    quic_h3:stop_server(Name).

%% TLS 1.3 refuses the default SHA-1 digest of pkix_test_data.
test_chain() ->
    Key = {key, {namedCurve, secp256r1}},
    Digest = {digest, sha256},
    #{root => [Key, Digest], intermediates => [], peer => [Key, Digest]}.
