-module(webtransport_data_loop_tests).

-include_lib("eunit/include/eunit.hrl").

%% A bare process that blocks until told to stop. Stands in for the h2
%% connection or the session, neither of which the loop touches except by
%% pid identity and gen_statem casts (which a plain process just drops).
spawn_idle() ->
    spawn(fun idle/0).

idle() ->
    receive stop -> ok; _ -> idle() end.

wait_down(Ref) ->
    receive
        {'DOWN', Ref, process, _, _} -> ok
    after 1000 -> timeout
    end.

%% The loop must exit when the h2 connection dies, even without a `closed'
%% message — otherwise it orphans for the life of the VM.
loop_exits_when_conn_dies_test() ->
    Conn = spawn_idle(),
    Session = spawn_idle(),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    LoopRef = erlang:monitor(process, Loop),
    exit(Conn, kill),
    ?assertEqual(ok, wait_down(LoopRef)),
    Session ! stop.

%% The loop must exit when the session dies; there is nothing left to forward
%% to, so it must not linger for the connection's lifetime.
loop_exits_when_session_dies_test() ->
    Conn = spawn_idle(),
    Session = spawn_idle(),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    LoopRef = erlang:monitor(process, Loop),
    exit(Session, kill),
    ?assertEqual(ok, wait_down(LoopRef)),
    Conn ! stop.
