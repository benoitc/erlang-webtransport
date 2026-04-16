-module(webtransport_h3_router_tests).

-include_lib("eunit/include/eunit.hrl").
-include("webtransport.hrl").

%% Fake session: relays all gen_server:casts it receives as plain
%% messages back to Parent so the test body can assert on them.
start_fake_session(Parent) ->
    spawn_link(fun() -> fake_session_loop(Parent) end).

fake_session_loop(Parent) ->
    receive
        stop -> ok;
        {'$gen_cast', Msg} ->
            Parent ! Msg,
            fake_session_loop(Parent);
        _ ->
            fake_session_loop(Parent)
    end.

recv(Timeout) ->
    receive Msg -> Msg
    after Timeout -> timeout
    end.

stream_open_and_data_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14,
                                    <<0, "GET /x\n">>, true}},

    ?assertEqual({stream_opened, 14, uni}, recv(200)),
    ?assertEqual({stream_data, 14, <<"GET /x\n">>, true}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).

header_split_across_messages_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<>>, false}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<0, "hi">>, true}},

    ?assertEqual({stream_opened, 14, uni}, recv(200)),
    ?assertEqual({stream_data, 14, <<"hi">>, true}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).

unknown_session_dropped_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    %% Session-id 4 is NOT registered so the router must reset the stream
    %% with WT_SESSION_GONE (draft-15 §5) rather than silently dropping.
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<4, "nope">>, true}},

    %% We stub the h3_conn with self(); cancel/3 is a gen_statem cast, so
    %% the test process sees the raw '$gen_cast' with the expected code.
    ?assertMatch({'$gen_cast', {cancel_stream, 14, ?WT_SESSION_GONE}}, recv(200)),
    %% Fake session must have received nothing.
    ?assertEqual(timeout, recv(100)),
    Session ! stop,
    gen_server:stop(Router).

reset_event_reported_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<0>>, false}},
    ?assertEqual({stream_opened, 14, uni}, recv(200)),

    Router ! {quic_h3, self(), {stream_type_reset, uni, 14, 42}},
    ?assertEqual({stream_closed, 14, {reset, 42}}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).

datagram_routed_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {datagram, 0, <<"ping">>}},
    ?assertEqual({datagram_data, <<"ping">>}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).
