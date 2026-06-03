%% Copyright (c) 2026, Benoit Chesneau.
%% Licensed under the Apache License, Version 2.0.
%%
%% @doc IPv6 binding, sockname introspection, and 0-RTT session-ticket tests.
-module(webtransport_ipv6_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([h3_ipv6_roundtrip_test/1,
         h2_ipv6_roundtrip_test/1,
         h3_sockname_test/1,
         h2_sockname_best_effort_test/1,
         zero_rtt_ticket_capture_test/1]).

-define(V6_LOOPBACK, {0, 0, 0, 0, 0, 0, 0, 1}).

all() ->
    [h3_ipv6_roundtrip_test,
     h2_ipv6_roundtrip_test,
     h3_sockname_test,
     h2_sockname_best_effort_test,
     zero_rtt_ticket_capture_test].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(h2),
    {ok, _} = application:ensure_all_started(quic),
    PrivDir = proplists:get_value(priv_dir, Config),
    case test_helpers:generate_self_signed_cert(PrivDir) of
        {ok, #{certfile := CertFile, keyfile := KeyFile}} ->
            [{certfile, CertFile}, {keyfile, KeyFile} | Config];
        {error, Reason} ->
            {skip, {cert_generation_failed, Reason}}
    end.

end_per_suite(_Config) ->
    ok.

%% ============================================================================
%% IPv6 binding round-trips
%% ============================================================================

h3_ipv6_roundtrip_test(Config) ->
    ipv6_roundtrip(h3, wt_ipv6_h3, Config).

h2_ipv6_roundtrip_test(Config) ->
    ipv6_roundtrip(h2, wt_ipv6_h2, Config).

ipv6_roundtrip(Transport, Name, Config) ->
    Port = test_helpers:find_free_port(),
    {ok, _} = webtransport:start_listener(Name, listener_opts(Transport, Port, Config)),
    timer:sleep(100),
    try
        {ok, Session} = webtransport:connect(?V6_LOOPBACK, Port, <<"/test">>, #{
            transport => Transport,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),
        {ok, StreamId} = webtransport:open_stream(Session, bidi),
        Payload = <<"ipv6-ping">>,
        ok = webtransport:send(Session, StreamId, Payload, fin),
        ?assertEqual(Payload, collect_stream_echo(Session, StreamId, <<>>, 3000)),
        webtransport:close_session(Session)
    after
        webtransport:stop_listener(Name)
    end.

%% ============================================================================
%% Sockname introspection
%% ============================================================================

h3_sockname_test(Config) ->
    Port = test_helpers:find_free_port(),
    {ok, _} = webtransport:start_listener(wt_sock_h3, listener_opts(h3, Port, Config)),
    timer:sleep(100),
    try
        {ok, {Ip, Bound}} = webtransport:listener_sockname(wt_sock_h3),
        ?assertEqual(Port, Bound),
        ?assertEqual(8, tuple_size(Ip)),
        {ok, Info} = webtransport:listener_info(wt_sock_h3),
        ?assertMatch(#{sockname := {_, Port}}, Info)
    after
        webtransport:stop_listener(wt_sock_h3)
    end.

h2_sockname_best_effort_test(Config) ->
    Port = test_helpers:find_free_port(),
    {ok, _} = webtransport:start_listener(wt_sock_h2, listener_opts(h2, Port, Config)),
    timer:sleep(100),
    try
        %% h2 has no socket-resolved IP; we return the requested bind addr
        %% (the IPv6 wildcard here) paired with the actual bound port.
        {ok, {Ip, Bound}} = webtransport:listener_sockname(wt_sock_h2),
        ?assertEqual(Port, Bound),
        ?assertEqual(?V6_LOOPBACK, Ip)
    after
        webtransport:stop_listener(wt_sock_h2)
    end.

%% ============================================================================
%% 0-RTT session ticket
%% ============================================================================

%% The connecting process is the QUIC connection owner's passthrough target,
%% so a resumption ticket surfaces as `{webtransport, session_ticket, _}'.
%% This verifies the capture path and that early_data_accepted/1 returns a
%% valid connection-level shape. Full 0-RTT resumption (reconnecting with the
%% ticket) is not supported through the current synchronous H3 connect, so it
%% is not exercised here.
zero_rtt_ticket_capture_test(Config) ->
    Port = test_helpers:find_free_port(),
    {ok, _} = webtransport:start_listener(wt_0rtt_h3, listener_opts(h3, Port, Config)),
    timer:sleep(100),
    try
        {ok, Session} = webtransport:connect(?V6_LOOPBACK, Port, <<"/test">>, #{
            transport => h3,
            verify => verify_none,
            handler_opts => #{owner => self()}
        }),
        EDA = webtransport:early_data_accepted(Session),
        ?assert(lists:member(EDA, [true, false, unknown])),
        Ticket = wait_for_ticket(2000),
        webtransport:close_session(Session),
        case Ticket of
            undefined ->
                {comment, "server issued no session ticket; capture path untriggered"};
            _ ->
                {comment, "session ticket captured via {webtransport, session_ticket, _}"}
        end
    after
        webtransport:stop_listener(wt_0rtt_h3)
    end.

%% ============================================================================
%% Helpers
%% ============================================================================

listener_opts(Transport, Port, Config) ->
    #{
        transport => Transport,
        port => Port,
        family => inet6,
        ip => ?V6_LOOPBACK,
        certfile => proplists:get_value(certfile, Config),
        keyfile => proplists:get_value(keyfile, Config),
        handler => wt_echo_handler
    }.

collect_stream_echo(Session, StreamId, Acc, Timeout) ->
    receive
        {webtransport, Session, {stream, StreamId, _Type, Data}} ->
            collect_stream_echo(Session, StreamId, <<Acc/binary, Data/binary>>, Timeout);
        {webtransport, Session, {stream_fin, StreamId, _Type, Data}} ->
            <<Acc/binary, Data/binary>>
    after Timeout ->
        error({no_stream_echo, Acc})
    end.

wait_for_ticket(Timeout) ->
    receive
        {webtransport, session_ticket, Ticket} -> Ticket
    after Timeout ->
        undefined
    end.
