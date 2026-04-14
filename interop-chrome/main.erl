%% @doc WebTransport Chrome interoperability test server.
%%
%% This server provides both an HTTP server for the test page and
%% a WebTransport endpoint for browser testing.
%%
-module(main).

-export([start/0, start/1]).

-define(HTTP_PORT, 8080).
-define(WT_PORT, 4433).

%% ============================================================================
%% Entry Points
%% ============================================================================

%% @doc Start the test server with default ports.
start() ->
    start(#{http_port => ?HTTP_PORT, wt_port => ?WT_PORT}).

%% @doc Start the test server with custom configuration.
-spec start(Config :: map()) -> ok | {error, term()}.
start(Config) ->
    HttpPort = maps:get(http_port, Config, ?HTTP_PORT),
    WtPort = maps:get(wt_port, Config, ?WT_PORT),

    io:format("Starting Chrome interop test server~n"),
    io:format("  HTTP server: http://localhost:~p~n", [HttpPort]),
    io:format("  WebTransport: https://localhost:~p~n", [WtPort]),

    %% Generate certificates
    case generate_certs() of
        {ok, CertFile, KeyFile} ->
            %% Start the HTTP server for test page
            start_http_server(HttpPort, CertFile),

            %% Start the WebTransport server
            start_wt_server(WtPort, CertFile, KeyFile),

            io:format("~nServers started successfully~n"),
            io:format("Open http://localhost:~p in Chrome to run tests~n", [HttpPort]),
            io:format("~nMake sure Chrome is started with:~n"),
            io:format("  --enable-features=WebTransportDeveloperMode~n"),
            io:format("  --ignore-certificate-errors-spki-list=<spki>~n"),
            io:format("~n"),

            %% Keep running
            receive
                stop -> ok
            end;
        {error, Reason} ->
            io:format("Failed to generate certificates: ~p~n", [Reason]),
            {error, Reason}
    end.

%% ============================================================================
%% Certificate Generation
%% ============================================================================

generate_certs() ->
    PrivDir = filename:join([code:priv_dir(webtransport), "interop-chrome"]),
    filelib:ensure_dir(filename:join(PrivDir, "placeholder")),

    CertFile = filename:join(PrivDir, "cert.pem"),
    KeyFile = filename:join(PrivDir, "key.pem"),

    %% Generate if not exists
    case filelib:is_file(CertFile) of
        true ->
            {ok, CertFile, KeyFile};
        false ->
            KeyCmd = io_lib:format(
                "openssl genrsa -out ~s 2048 2>/dev/null",
                [KeyFile]
            ),
            case os:cmd(lists:flatten(KeyCmd)) of
                "" ->
                    CertCmd = io_lib:format(
                        "openssl req -new -x509 -key ~s -out ~s -days 30 "
                        "-subj \"/CN=localhost\" 2>/dev/null",
                        [KeyFile, CertFile]
                    ),
                    case os:cmd(lists:flatten(CertCmd)) of
                        "" -> {ok, CertFile, KeyFile};
                        Err2 -> {error, {cert_failed, Err2}}
                    end;
                Err1 ->
                    {error, {key_failed, Err1}}
            end
    end.

%% ============================================================================
%% HTTP Server (for test page)
%% ============================================================================

start_http_server(Port, CertFile) ->
    %% Simple HTTP server using inets
    inets:start(),
    ServerRoot = filename:dirname(CertFile),
    DocRoot = filename:dirname(code:which(?MODULE)),

    %% Create mime.types if needed
    MimeTypes = filename:join(ServerRoot, "mime.types"),
    file:write_file(MimeTypes, "text/html html\napplication/javascript js\ntext/css css\n"),

    Options = [
        {port, Port},
        {server_name, "webtransport-test"},
        {server_root, ServerRoot},
        {document_root, DocRoot},
        {mime_types, [{"html", "text/html"}, {"js", "application/javascript"}]},
        {modules, [mod_get]}
    ],

    case inets:start(httpd, Options) of
        {ok, _Pid} ->
            io:format("  HTTP server started on port ~p~n", [Port]);
        {error, Reason} ->
            io:format("  HTTP server failed: ~p~n", [Reason])
    end.

%% ============================================================================
%% WebTransport Server
%% ============================================================================

start_wt_server(Port, CertFile, KeyFile) ->
    ListenerOpts = #{
        transport => h3,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => chrome_test_handler
    },

    case webtransport:start_listener(chrome_interop, ListenerOpts) of
        {ok, _Pid} ->
            io:format("  WebTransport server started on port ~p~n", [Port]);
        {error, Reason} ->
            io:format("  WebTransport server failed: ~p~n", [Reason])
    end.
