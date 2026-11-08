# Agents

Instructions for AI coding agents working on this project.

## Project Overview

Erlang implementation of WebTransport over HTTP/3
(draft-ietf-webtrans-http3-17) and HTTP/2 (draft-ietf-webtrans-http2-16),
built on [erlang_quic](https://github.com/benoitc/erlang_quic) (`quic`,
`quic_h3`) and [erlang_h2](https://github.com/benoitc/erlang_h2) (`h2`).
Requires Erlang/OTP 27 or later and rebar3.

## Required Checks

Every change must pass these before committing. CI runs the first four and
the docs build; run Common Test locally, CI does not.

```bash
rebar3 compile              # Must compile cleanly (warnings are errors)
rebar3 xref                 # Cross-reference analysis must pass
rebar3 eunit                # Unit tests and PropEr properties
rebar3 dialyzer             # Type checking must pass
rebar3 ct                   # End to end over h3 and h2
rebar3 ex_doc               # Docs must build
```

A change to what goes on the wire also runs the interop harnesses; see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Pull Requests

Work on one issue goes in one pull request. Give each change a reviewer has to
look at its own commit, with its tests, rather than opening a pull request per
change. Docs and CHANGELOG entries go in the same pull request.

## Skills

Task guides for agents live in `.agents/skills/<name>/SKILL.md`; read the one
that matches the task before starting. `.claude/skills` links to the same
directories, so there is one copy.

- [spec-compliance](.agents/skills/spec-compliance/SKILL.md): audit the
  library against the current WebTransport drafts and record the gaps in
  `docs/compliance.md`.
- [spec-review](.agents/skills/spec-review/SKILL.md): check a change or pull
  request against the drafts and RFCs it claims to follow.

## Dependencies

Some behaviour lives in `quic` and `h2`, not here: SETTINGS handling,
RESET_STREAM_AT, extended CONNECT, send buffers. Read the dependency source in
`_build/default/lib/quic` and `_build/default/lib/h2` before claiming what it
does. A fix that belongs in a dependency is written up as a prompt for that
repository rather than worked around here.

## Build & Development Commands

```bash
rebar3 compile                                         # Build
rebar3 eunit                                           # All EUnit tests
rebar3 eunit --module=wt_h3                            # One module's tests
rebar3 ct --suite webtransport_SUITE                   # End-to-end suite
rebar3 ct --suite webtransport_SUITE --group h2_tests  # One transport
rebar3 ct --suite webtransport_SUITE --group h3_tests --case bidi_echo_test
scripts/interop_cross.sh                               # Docker: erlang <-> webtransport-go
interop-chrome/run.sh --headless                       # Chrome against a local server
```

## Architecture

### Modules

**Public API:** `webtransport.erl` (listeners, `connect`, `accept/4` for
embedding, the server CONNECT checks), `webtransport_handler.erl` (handler
behaviour), `webtransport_client_handler.erl` (default client handler that
forwards events to the owner).

**Session:** `webtransport_session.erl` (gen_statem per session: streams,
flow control, datagrams, drain and close; the central module),
`webtransport_stream.erl` (per-stream state).

**HTTP/3:** `wt_h3.erl` (SETTINGS, CONNECT headers, draft selection,
compat modes), `webtransport_h3.erl` (transport calls on quic/quic_h3),
`webtransport_h3_router.erl` (one per connection: routes streams and
datagrams to sessions, parks early ones, session slots), `wt_h3_capsule.erl`
(capsule codec).

**HTTP/2:** `webtransport_h2.erl` (transport calls on h2, TLS checks),
`wt_h2_capsule.erl` (capsule codec), `wt_h2_init.erl` (WebTransport-Init
header).

**Shared:** `wt_sf.erl` (RFC 9651 structured fields), `wt_error.erl` (error
code mapping), `include/webtransport.hrl` (codepoints, limits, defaults).

### Compat modes

HTTP/3 has a latest path (http3-17 and -15, `:protocol webtransport-h3`) and
a legacy path (draft-02 for Chrome and Firefox, http3-07 for Safari 26,
`:protocol webtransport`). Keep them disjoint: no legacy SETTINGS, headers or
`:protocol` in a latest handshake, and the draft is chosen once per CONNECT.
Listeners default to `auto`, clients to `latest`.

### Test Organization

- `src/*.erl` carry most unit tests in `-ifdef(TEST)` blocks.
- `test/*_tests.erl` are EUnit modules; `test/prop_*.erl` are PropEr
  properties, run from EUnit.
- `test/webtransport_SUITE.erl` runs every case over h3 and h2 against a
  listener in the same node; `test/wt_*_handler.erl` are its handlers.
- `test/webtransport_ipv6_SUITE.erl` covers IPv6 listeners and clients.
