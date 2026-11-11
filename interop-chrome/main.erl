%% @doc WebTransport Chrome interoperability test server.
%%
%% Serves the test page over HTTP and runs a WebTransport listener for it.
%% The certificate is a short-lived ECDSA P-256 one, so the page can pin it
%% with `serverCertificateHashes'; its SHA-256 is served as `cert-hash.txt'.
-module(main).

-export([start/0, start/1]).

-define(HTTP_PORT, 8080).
-define(WT_PORT, 4433).
-define(HOST, {127, 0, 0, 1}).

%% ============================================================================
%% Entry Points
%% ============================================================================

%% @doc Start the test server with default ports.
start() ->
    start(#{}).

%% @doc Start the test server. Keys: `http_port', `wt_port', `work_dir'
%% (writable, holds the cert and the served files) and `page_dir' (holds
%% `index.html'). Halts the node when a server cannot start.
-spec start(Config :: map()) -> no_return().
start(Config) ->
    HttpPort = maps:get(http_port, Config, ?HTTP_PORT),
    WtPort = maps:get(wt_port, Config, ?WT_PORT),
    PageDir = maps:get(page_dir, Config, filename:dirname(code:which(?MODULE))),
    WorkDir = maps:get(work_dir, Config,
                       filename:basedir(user_cache, "webtransport-chrome-interop")),

    io:format("Starting Chrome interop test server~n"),
    try
        ok = filelib:ensure_path(WorkDir),
        {ok, _} = application:ensure_all_started([webtransport, inets]),
        {CertFile, KeyFile} = generate_cert(WorkDir),
        {ok, _} = file:copy(filename:join(PageDir, "index.html"),
                            filename:join(WorkDir, "index.html")),
        Origins = [iolist_to_binary(io_lib:format("http://~s:~b", [H, HttpPort]))
                   || H <- ["127.0.0.1", "localhost"]],
        %% WebTransport first: the page port being up means both are ready.
        ok = start_wt_server(WtPort, CertFile, KeyFile, Origins),
        ok = start_http_server(HttpPort, WorkDir)
    of
        ok ->
            io:format("  WebTransport: https://127.0.0.1:~b/test~n", [WtPort]),
            io:format("  Test page:    http://127.0.0.1:~b/index.html~n", [HttpPort]),
            receive stop -> erlang:halt(0) end
    catch
        Class:Reason ->
            io:format("Server failed to start: ~p:~p~n", [Class, Reason]),
            erlang:halt(1)
    end.

%% ============================================================================
%% Certificate Generation
%% ============================================================================

%% serverCertificateHashes needs ECDSA and a validity of at most 14 days.
generate_cert(Dir) ->
    CertFile = filename:join(Dir, "cert.pem"),
    KeyFile = filename:join(Dir, "key.pem"),
    Cmd = io_lib:format(
        "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 "
        "-nodes -days 10 -subj /CN=localhost "
        "-addext subjectAltName=DNS:localhost,IP:127.0.0.1 "
        "-keyout '~s' -out '~s' 2>&1",
        [KeyFile, CertFile]),
    _ = os:cmd(lists:flatten(Cmd)),
    {ok, Pem} = file:read_file(CertFile),
    [{'Certificate', Der, not_encrypted} | _] = public_key:pem_decode(Pem),
    Hash = binary:encode_hex(crypto:hash(sha256, Der), lowercase),
    ok = file:write_file(filename:join(Dir, "cert-hash.txt"), Hash),
    {CertFile, KeyFile}.

%% ============================================================================
%% HTTP Server (for test page)
%% ============================================================================

start_http_server(Port, Dir) ->
    Options = [
        {port, Port},
        {bind_address, ?HOST},
        {server_name, "webtransport-test"},
        {server_root, Dir},
        {document_root, Dir},
        {mime_types, [{"html", "text/html"}, {"txt", "text/plain"}]},
        {modules, [mod_get]}
    ],
    case inets:start(httpd, Options) of
        {ok, _Pid} -> ok;
        {error, Reason} -> error({http_server, Reason})
    end.

%% ============================================================================
%% WebTransport Server
%% ============================================================================

start_wt_server(Port, CertFile, KeyFile, Origins) ->
    ListenerOpts = #{
        transport => h3,
        ip => ?HOST,
        port => Port,
        certfile => CertFile,
        keyfile => KeyFile,
        handler => chrome_test_handler,
        handler_opts => #{origins => Origins}
    },
    case webtransport:start_listener(chrome_interop, ListenerOpts) of
        {ok, _Pid} -> ok;
        {error, Reason} -> error({wt_server, Reason})
    end.
