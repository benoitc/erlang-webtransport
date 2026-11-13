-module(webtransport_data_loop_tests).

-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

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

%% Stands in for an h2 connection that answers calls (goaway) and can be
%% stopped like one (close is gen_statem:stop/1).
spawn_conn() ->
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    Pid.

init([]) -> {ok, #{}}.
handle_call(_Req, _From, S) -> {reply, ok, S}.
handle_cast(_Msg, S) -> {noreply, S}.
handle_info(_Msg, S) -> {noreply, S}.

%% http2-16 §3.1: the server raising SETTINGS_WT_ENABLED above 1 later is a
%% connection error; the session is told and the loop ends.
loop_stops_on_bad_wt_enabled_test() ->
    Conn = spawn_conn(),
    Parent = self(),
    Session = spawn(fun() ->
        receive {'$gen_cast', Msg} -> Parent ! {session_got, Msg} end
    end),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    LoopRef = erlang:monitor(process, Loop),
    Loop ! {h2, Conn, {peer_settings, #{16#2b67 => 1}}},
    Loop ! {h2, Conn, {peer_settings, #{16#2b67 => 2}}},
    receive
        {session_got, M} ->
            ?assertMatch({capsule, {connection_error, _}}, M)
    after 1000 ->
        error(no_cast)
    end,
    ?assertEqual(ok, wait_down(LoopRef)).

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
    %% 200 KiB: 13 frames, within the default 256 KiB stream window.
    Payload = crypto:strong_rand_bytes(200 * 1024),
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

%% http2-16 §6.6: a WT_STREAM capsule that declares more data than the
%% stream's flow-control window (256 KiB by default) is a flow-control
%% error, known from its header. The loop does not wait for the body.
oversize_stream_capsule_is_flow_control_error_test() ->
    Conn = spawn_idle(),
    Self = self(),
    Session = spawn(fun() -> relay(Self) end),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    Capsule = wt_h2_capsule:encode(
                wt_h2_capsule:wt_stream(4, binary:copy(<<0>>, 1024 * 1024))),
    <<First:16384/binary, _/binary>> = Capsule,
    Loop ! {h2, Conn, {data, 1, First, false}},
    receive
        {'$gen_cast', {capsule, {flow_control_error, _}}} -> ok
    after 1000 ->
        error(no_flow_control_error)
    end,
    exit(Loop, kill),
    Conn ! stop,
    Session ! stop.

%% RFC 9297 §3.2, http2-16 §6.1: padding of any length is skipped as it
%% arrives, not held until it ends.
padding_is_not_buffered_test() ->
    Conn = spawn_idle(),
    Self = self(),
    Session = spawn(fun() -> relay(Self) end),
    Loop = spawn(fun() -> webtransport:h2_data_loop(Conn, 1, Session) end),
    Body = binary:copy(<<0>>, 2 * 1024 * 1024),
    Capsule = wt_h2_capsule:encode({padding, Body}),
    Next = wt_h2_capsule:encode(wt_h2_capsule:wt_stream(4, <<"after">>)),
    %% Everything but the last byte of the padding.
    Partial = binary:part(Capsule, 0, byte_size(Capsule) - 1),
    lists:foreach(fun(Chunk) -> Loop ! {h2, Conn, {data, 1, Chunk, false}} end,
                  chunks(Partial, 16384)),
    ok = wait_drained(Loop, 100),
    true = erlang:garbage_collect(Loop),
    {binary, Bins} = erlang:process_info(Loop, binary),
    Held = lists:sum([Size || {_, Size, _} <- Bins]),
    ?assert(Held < 65536),
    Loop ! {h2, Conn, {data, 1, <<0, Next/binary>>, false}},
    Casts = collect_casts([], 300),
    ?assertEqual(<<"after">>,
                 iolist_to_binary([D || {stream_data, 4, D, _} <- Casts])),
    exit(Loop, kill),
    Conn ! stop,
    Session ! stop.

wait_drained(_Pid, 0) ->
    timeout;
wait_drained(Pid, N) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, 0} -> timer:sleep(50), ok;
        _ -> timer:sleep(50), wait_drained(Pid, N - 1)
    end.
