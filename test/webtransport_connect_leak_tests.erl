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
