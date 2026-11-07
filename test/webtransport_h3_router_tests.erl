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

%% http3-17 §4.6: a stream for a session that is not registered yet is held
%% and delivered, in order, once the session registers.
unknown_session_parked_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<4, "early">>, false}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<"-more">>, true}},
    Router ! {quic_h3, self(), {datagram, 4, <<"dg">>}},
    ?assertEqual(timeout, recv(100)),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 4, Session),
    ?assertEqual({stream_opened, 14, uni}, recv(200)),
    ?assertEqual({stream_data, 14, <<"early">>, false}, recv(200)),
    ?assertEqual({stream_data, 14, <<"-more">>, true}, recv(200)),
    ?assertEqual({datagram_data, <<"dg">>}, recv(200)),
    Session ! stop,
    gen_server:stop(Router).

%% A stream for a session that has ended is reset with WT_SESSION_GONE.
ended_session_stream_reset_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 4, Session),
    ok = webtransport_h3_router:unregister_session(Router, 4),
    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    %% We stub the h3_conn with self(); cancel/3 is a gen_statem cast, so
    %% the test process sees the raw '$gen_cast' with the expected code.
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<4, "late">>, true}},
    ?assertMatch({'$gen_cast', {cancel_stream, 14, ?WT_SESSION_GONE}}, recv(200)),
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

    %% An application code arrives mapped into the WT_APPLICATION_ERROR
    %% range and is reported unmapped.
    Router ! {quic_h3, self(), {stream_type_reset, uni, 14, wt_error:to_quic(42)}},
    ?assertEqual({stream_closed, 14, {reset, 42}}, recv(200)),

    %% http3-17 §4.4: a code outside that range carries no application code.
    Router ! {quic_h3, self(), {stream_type_open, uni, 18, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 18, <<0>>, false}},
    ?assertEqual({stream_opened, 18, uni}, recv(200)),
    Router ! {quic_h3, self(), {stream_type_reset, uni, 18, 16#10c}},
    ?assertEqual({stream_closed, 18, {reset, undefined}}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).

%% http3-17 §4: a session id that is not a client-initiated bidi stream id
%% closes the connection with H3_ID_ERROR.
bad_session_id_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Router ! {quic_h3, self(), {stream_type_open, uni, 14, 16#54}},
    Router ! {quic_h3, self(), {stream_type_data, uni, 14, <<1>>, false}},
    ?assertEqual({'$gen_cast', {close, ?H3_ID_ERROR}}, recv(200)),
    gen_server:stop(Router).

datagram_routed_test() ->
    {ok, Router} = webtransport_h3_router:start_link(),
    Session = start_fake_session(self()),
    ok = webtransport_h3_router:register_session(Router, 0, Session),

    Router ! {quic_h3, self(), {datagram, 0, <<"ping">>}},
    ?assertEqual({datagram_data, <<"ping">>}, recv(200)),

    Session ! stop,
    gen_server:stop(Router).
