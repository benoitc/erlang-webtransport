%% @doc Server-side handler that exercises server-initiated bidi streams.
%%
%% When the peer sends `<<"push">>' (with fin) on any bidi stream, the
%% handler spawns a helper process that opens a new bidi stream from the
%% server side and writes a known payload. The spawn is necessary because
%% `webtransport:open_stream/2' and `webtransport:send/4' are
%% `gen_statem:call/2' round-trips and the handler callback already runs
%% inside the session gen_statem, so calling them inline would self-deadlock.
-module(wt_server_push_handler).
-behaviour(webtransport_handler).

-export([init/3, handle_stream/4, handle_stream_fin/4,
         handle_datagram/2, handle_stream_closed/3, terminate/2]).

-define(PUSH_PAYLOAD, <<"pushed-from-server">>).

-record(state, {
    session :: pid(),
    buffers = #{} :: #{non_neg_integer() => binary()}
}).

init(Session, _Request, _Opts) ->
    {ok, #state{session = Session}}.

handle_stream(StreamId, _Type, Data, #state{buffers = Bufs} = State) ->
    Existing = maps:get(StreamId, Bufs, <<>>),
    {ok, State#state{buffers = Bufs#{StreamId => <<Existing/binary, Data/binary>>}}}.

handle_stream_fin(StreamId, Type, Data,
                  #state{session = Session, buffers = Bufs} = State) ->
    Existing = maps:get(StreamId, Bufs, <<>>),
    Full = <<Existing/binary, Data/binary>>,
    Bufs1 = maps:remove(StreamId, Bufs),
    case {Type, Full} of
        {bidi, <<"push">>} ->
            _ = spawn(fun() -> push_bidi(Session) end),
            {ok, State#state{buffers = Bufs1}};
        _ ->
            {ok, State#state{buffers = Bufs1}}
    end.

handle_datagram(_Data, State) ->
    {ok, State}.

handle_stream_closed(_StreamId, _Reason, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

push_bidi(Session) ->
    case webtransport:open_stream(Session, bidi) of
        {ok, StreamId} ->
            webtransport:send(Session, StreamId, ?PUSH_PAYLOAD, fin);
        {error, _} ->
            ok
    end.
