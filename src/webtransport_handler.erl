%% @doc WebTransport handler behaviour.
%%
%% Applications implement this behaviour to handle WebTransport sessions.
%% The callbacks are invoked by the session process when events occur.
%%
%% == Example Handler ==
%% ```
%% -module(my_wt_handler).
%% -behaviour(webtransport_handler).
%%
%% -export([init/2, handle_stream/4, handle_datagram/2,
%%          handle_stream_closed/3, terminate/2]).
%%
%% init(_Session, _Req) ->
%%     {ok, #{}}.
%%
%% handle_stream(Stream, Type, Data, State) ->
%%     webtransport:send(Stream, <<"echo: ", Data/binary>>),
%%     {ok, State}.
%%
%% handle_datagram(Data, State) ->
%%     {ok, State}.
%%
%% handle_stream_closed(_Stream, _Reason, State) ->
%%     {ok, State}.
%%
%% terminate(_Reason, _State) ->
%%     ok.
%% '''
-module(webtransport_handler).

%% Callback declarations
-callback init(Session, Req) -> {ok, State} | {ok, State, Actions} | {error, Reason} when
    Session :: webtransport:session(),
    Req :: webtransport:request(),
    State :: term(),
    Actions :: [action()],
    Reason :: term().

-callback handle_stream(Stream, Type, Data, State) -> Result when
    Stream :: webtransport:stream(),
    Type :: bidi | uni,
    Data :: binary(),
    State :: term(),
    Result :: {ok, NewState} | {ok, NewState, Actions} | {stop, Reason, NewState},
    NewState :: term(),
    Actions :: [action()],
    Reason :: term().

-callback handle_stream_fin(Stream, Type, Data, State) -> Result when
    Stream :: webtransport:stream(),
    Type :: bidi | uni,
    Data :: binary(),
    State :: term(),
    Result :: {ok, NewState} | {ok, NewState, Actions} | {stop, Reason, NewState},
    NewState :: term(),
    Actions :: [action()],
    Reason :: term().

-callback handle_datagram(Data, State) -> Result when
    Data :: binary(),
    State :: term(),
    Result :: {ok, NewState} | {ok, NewState, Actions} | {stop, Reason, NewState},
    NewState :: term(),
    Actions :: [action()],
    Reason :: term().

-callback handle_stream_closed(Stream, Reason, State) -> Result when
    Stream :: webtransport:stream(),
    Reason :: normal | {reset, non_neg_integer()} | {error, term()},
    State :: term(),
    Result :: {ok, NewState} | {stop, Reason, NewState},
    NewState :: term().

-callback handle_info(Info, State) -> Result when
    Info :: term(),
    State :: term(),
    Result :: {ok, NewState} | {ok, NewState, Actions} | {stop, Reason, NewState},
    NewState :: term(),
    Actions :: [action()],
    Reason :: term().

-callback terminate(Reason, State) -> term() when
    Reason :: normal | {error, term()} | term(),
    State :: term().

%% Optional callbacks
-optional_callbacks([handle_stream_fin/4, handle_info/2]).

%% Types
-type action() ::
    {send, webtransport:stream(), iodata()} |
    {send, webtransport:stream(), iodata(), fin} |
    {send_datagram, iodata()} |
    {open_stream, bidi | uni} |
    {close_stream, webtransport:stream()} |
    {reset_stream, webtransport:stream(), non_neg_integer()} |
    {stop_sending, webtransport:stream(), non_neg_integer()} |
    drain_session |
    {close_session, non_neg_integer(), binary()}.

-export_type([action/0]).

%% Utility exports
-export([execute_actions/2]).

%% @doc Execute a list of actions on a session.
%% Called internally by webtransport_session after callback returns.
-spec execute_actions(webtransport:session(), [action()]) -> ok | {error, term()}.
execute_actions(_Session, []) ->
    ok;
execute_actions(Session, [Action | Rest]) ->
    case execute_action(Session, Action) of
        ok -> execute_actions(Session, Rest);
        {ok, _} -> execute_actions(Session, Rest);
        {error, _} = Err -> Err
    end.

execute_action(Session, {send, Stream, Data}) ->
    webtransport_session:send(Session, Stream, Data, false);
execute_action(Session, {send, Stream, Data, fin}) ->
    webtransport_session:send(Session, Stream, Data, true);
execute_action(Session, {send_datagram, Data}) ->
    webtransport_session:send_datagram(Session, Data);
execute_action(Session, {open_stream, Type}) ->
    webtransport_session:open_stream(Session, Type);
execute_action(Session, {close_stream, Stream}) ->
    webtransport_session:close_stream(Session, Stream);
execute_action(Session, {reset_stream, Stream, ErrorCode}) ->
    webtransport_session:reset_stream(Session, Stream, ErrorCode);
execute_action(Session, {stop_sending, Stream, ErrorCode}) ->
    webtransport_session:stop_sending(Session, Stream, ErrorCode);
execute_action(Session, drain_session) ->
    webtransport_session:drain(Session);
execute_action(Session, {close_session, ErrorCode, Reason}) ->
    webtransport_session:close(Session, ErrorCode, Reason).
