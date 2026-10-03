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

%% h2 0.12 reports a close as `{h2, Conn, {closed, Reason}}'. The loop must
%% exit on it while the connection process is still alive.
loop_exits_on_closed_with_reason_test() ->
    Conn = spawn_idle(),
    Session = spawn_idle(),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    LoopRef = erlang:monitor(process, Loop),
    Loop ! {h2, Conn, {closed, normal}},
    ?assertEqual(ok, wait_down(LoopRef)),
    Conn ! stop,
    Session ! stop.

%% A capsule split across several h2 DATA frames must be reassembled: h2
%% caps DATA frames at SETTINGS_MAX_FRAME_SIZE (16 KiB by default) while a
%% WT_STREAM capsule carries one whole send/4 payload. The loop used to
%% decode each frame on its own and drop the partial tail, truncating every
%% payload over one frame.
collect_casts(Acc, Timeout) ->
    receive
        {'$gen_cast', Msg} -> collect_casts([Msg | Acc], Timeout)
    after Timeout -> lists:reverse(Acc)
    end.

reassembles_capsule_split_across_frames_test() ->
    Conn = spawn_idle(),
    Self = self(),
    %% The session stand-in forwards every cast it receives to the test.
    Session = spawn(fun() -> relay(Self) end),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    Payload = crypto:strong_rand_bytes(1024 * 1024),
    Capsule = wt_h2_capsule:encode(wt_h2_capsule:wt_stream_fin(4, Payload)),
    %% Two capsules back to back, so a frame boundary also falls inside the
    %% second one and the loop must carry the tail between frames.
    Small = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(8, <<"tail">>)),
    Wire = <<Capsule/binary, Small/binary, Capsule/binary>>,
    lists:foreach(
        fun(Chunk) -> Loop ! {h2, Conn, {data, 1, Chunk, false}} end,
        chunks(Wire, 16384)),
    Casts = collect_casts([], 500),
    Data4 = iolist_to_binary([D || {stream_data, 4, D, _Fin} <- Casts]),
    Fins4 = [Fin || {stream_data, 4, _D, Fin} <- Casts, Fin],
    Data8 = iolist_to_binary([D || {stream_data, 8, D, _Fin} <- Casts]),
    ?assertEqual(<<Payload/binary, Payload/binary>>, Data4),
    ?assertEqual([true, true], Fins4),
    ?assertEqual(<<"tail">>, Data8),
    exit(Loop, kill),
    Conn ! stop,
    Session ! stop.

%% A frame that ends exactly on a capsule boundary leaves an empty buffer
%% and must not break the next capsule.
exact_boundary_then_next_capsule_test() ->
    Conn = spawn_idle(),
    Self = self(),
    Session = spawn(fun() -> relay(Self) end),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    C1 = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(4, <<"one">>)),
    C2 = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(4, <<"two">>)),
    Loop ! {h2, Conn, {data, 1, C1, false}},
    Loop ! {h2, Conn, {data, 1, C2, false}},
    Casts = collect_casts([], 300),
    ?assertEqual(<<"onetwo">>,
                 iolist_to_binary([D || {stream_data, 4, D, _} <- Casts])),
    exit(Loop, kill),
    Conn ! stop,
    Session ! stop.

relay(To) ->
    receive
        stop -> ok;
        Msg -> To ! Msg, relay(To)
    end.

chunks(<<>>, _N) -> [];
chunks(Bin, N) when byte_size(Bin) =< N -> [Bin];
chunks(Bin, N) ->
    <<H:N/binary, T/binary>> = Bin,
    [H | chunks(T, N)].
