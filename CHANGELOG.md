# Changelog

## 0.2.5

- Bump `quic` dep to 1.4.5, `h2` dep to 0.6.1.
- OTP 29 support: replace deprecated `catch Expr` expressions with `try ... catch ... end` (`webtransport_h2`, `webtransport_session`, `webtransport`).
- CI: add OTP 28 and 29 to the test matrix; bump rebar3 to 3.25.0.

## 0.2.4

- Bump `quic` dep to 1.4.2.

## 0.2.3

- Embedding (h3): `accept/4` now works when called from a process other than the QUIC connection process. The per-connection stream router is keyed by the QUIC connection pid, shared by the `h3_settings/1` connection_handler and by `accept/4` (via `quic_h3:get_quic_conn/1`), instead of the caller's process dictionary. Incoming bidi/uni streams and datagrams now reach the session even when the embedder dispatches each request to its own worker (e.g. Livery). The router's registry row and process are reaped when its QUIC connection ends.
- CT: `embedded_accept_round_trip_test` (h3) drives a raw `quic_h3` server with `h3_settings/0` merged in, accepts from a worker process, and asserts that a bidi stream (with FIN) and a datagram both round-trip through the echo handler.

## 0.2.2

- Docs: README gains a Sponsors section (Enki Multimedia logo vendored under `docs/images/`).
- Docs: fix invalid `webtransport:open_stream(Session, bidi | uni)` snippet — type-union pipe was in expression position; now `bidi  %% or uni`.
- CT: cover `listeners/0`, `listener_info/1`, and `start_listener/2` cert/key error paths (`webtransport.erl` line coverage 51% → 54%).

## 0.2.1

- `start_listener/2` (h3): switch the listener loop from `spawn_link/3` to `spawn/3` so `stop_listener/1` no longer propagates a `shutdown` exit to the caller. The interactive shell (and any non-trapping caller) now stays clean across a full lifecycle.
- `webtransport_session:init/1`: distinguish a handler module that fails to load (`{handler_not_loaded, M, Reason}`) from a loaded module missing `init/2` or `init/3`.
- README: quick-start now launches the shell with `ERL_FLAGS="-pa examples" rebar3 shell --apps webtransport` so handler modules outside `src/` are reachable.
- Regression test: `stop_listener_does_not_kill_caller_test` (h3 + h2 groups).

Known issue (tracked upstream in `quic`): a graceful `close_session/1` still emits a `quic_h3_connection` `quic_closed` CRASH REPORT because the dep treats any QUIC-conn `'DOWN'` as abnormal. Will resolve when `quic` is bumped.

## 0.2.0

- Switch `h2` and `quic` deps to hex packages: `h2 0.5.0`, `quic 1.3.0`.
- Add `public_key` to the application list.
- Clean dialyzer run: dropped dead `{error, _}` clauses around `h2_capsule:decode/1` and `h2_varint:decode/1`, removed unreachable `Router =:= undefined` branch, dropped unsupported `h3_datagram_enabled` from `quic_h3:start_server/3` opts, switched listener loop to `spawn_link/3` for static reachability.
- Configure `xref_checks` (skip `exports_not_used` for the public API) and `dialyzer` (`plt_extra_apps: [public_key, ssl, crypto, inets]`).
- Add GitHub Actions CI: OTP 26/27 matrix runs compile, xref, eunit, dialyzer; separate job builds ex_doc.

## 0.1.0

- Initial release.
