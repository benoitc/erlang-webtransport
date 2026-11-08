# Agents

Instructions for AI coding agents working on erlang-webtransport. Read this
file before you change anything; it applies to every agent and every change.

## Project Overview

Erlang implementation of WebTransport over HTTP/3
(draft-ietf-webtrans-http3-17) and HTTP/2 (draft-ietf-webtrans-http2-16),
built on [erlang_quic](https://github.com/benoitc/erlang_quic) (`quic`,
`quic_h3`) and [erlang_h2](https://github.com/benoitc/erlang_h2) (`h2`).
Requires Erlang/OTP 27 or later and rebar3.

## First principle: compliance, latest spec, safety

The library faces untrusted peers on both sides: browsers and clients talk to
its servers, and its clients talk to servers nobody vetted. Every change that
touches how capsules, frames, SETTINGS or headers are read or written, or how
a session or stream changes state, must pass three checks before it is
called done, committed or opened as a pull request:

1. **Compliance**: the behaviour matches the governing spec section, and a
   test shows the accept and the reject path. The test fails on main without
   the change.
2. **Latest specification**: you checked that the draft you cite is the
   current revision (WebTransport is still a set of drafts and moves), and
   you read the section itself instead of quoting from memory.
3. **Safety**: the change does not open a known abuse class (unbounded
   buffering of early streams or datagrams, capsule or reason sizes without a
   cap, stream or session counts without a limit, flow-control credit a peer
   can inflate or never return) and every per-peer resource it touches stays
   bounded.

Use the [spec-review](.agents/skills/spec-review/SKILL.md) skill to run
them. When in doubt whether a change is protocol code, it is.

## Specification issues are bugs, not security issues

A report is a specification issue when the fix is to make the library do
what a spec it implements says (see [Specifications](#specifications)):

- input accepted, rejected or framed differently from the spec (SETTINGS,
  the CONNECT request, capsules, stream headers, datagrams, the
  WebTransport-Init header);
- the HTTP/3 and HTTP/2 paths disagreeing on the same rule;
- a session or stream state change the spec does not allow, or a missing
  reset, close or error code it requires;
- a missing or wrong bound that the spec or an option defines (flow-control
  limits, stream and session limits, buffered streams and datagrams, close
  reason length, capsule size).

This holds even when the report calls it denial of service, resource
exhaustion or a desync. Treat it as a bug:

- fix it in a public pull request, with a public issue if one is needed;
- add a plain CHANGELOG line citing the spec section;
- do not open a GitHub Security Advisory, request a CVE, embargo the fix,
  give it a severity, or describe how to attack it;
- when it arrived as an advisory, close the advisory as not a vulnerability
  and link the pull request that fixes it.

Only a problem that is not a spec deviation goes through
[SECURITY.md](SECURITY.md): code execution, file access, one session's data
reaching another session or another peer, or the library weakening TLS or
certificate checks. A spec deviation stays a bug even when its effect is on
this list. If unsure, ask the maintainer before treating anything as a
security issue.

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

## Conventions

- Every bug fix and feature has a test. Show it failing on main.
- A rule fixed on one transport is checked on the other: the HTTP/3 and
  HTTP/2 paths must accept and reject the same things.
- Never relax a limit or a rejection to make a peer pass; exclude the
  legitimate case precisely.
- CHANGELOG: plain lines under `## Unreleased`, citing the spec section when
  there is one. No implementation details, no attack narratives.
- Pull request descriptions state what was wrong, the fix in a few lines,
  and breaking changes. No test plan, no file-by-file walkthrough, no
  `Co-Authored-By`, "Generated with" or similar trailers.
- Follow "Write for the person who reads it" in
  [CONTRIBUTING.md](CONTRIBUTING.md#write-for-the-person-who-reads-it): plain
  English, no inflated or pseudo-technical wording, and a person has read the
  change before it is opened.

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

## Specifications

The specs the library implements. Cite the current revision in code and the
CHANGELOG. Last checked: 2026-11-08.

| Spec | Scope | Code |
|---|---|---|
| draft-ietf-webtrans-http3-17 (and -15, same wire format) | HTTP/3 SETTINGS, CONNECT, stream headers, capsules, flow control, errors, GOAWAY, termination | `wt_h3.erl`, `webtransport_h3*.erl`, `wt_h3_capsule.erl`, `webtransport_session.erl` |
| draft-ietf-webtrans-http2-16 | HTTP/2 SETTINGS, CONNECT, WT_* capsules, flow control, WebTransport-Init | `webtransport_h2.erl`, `wt_h2_capsule.erl`, `wt_h2_init.erl`, `webtransport_session.erl` |
| draft-ietf-webtrans-overview-14 | session model, protocol negotiation, datagram size | `webtransport_session.erl`, `webtransport.erl` |
| draft-ietf-webtrans-http3-07, draft-ietf-webtrans-http3-02 | legacy path only (`compat_mode`): Safari 26, Chrome, Firefox | `wt_h3.erl`, `webtransport.erl` |
| RFC 9297 HTTP Datagrams and the Capsule Protocol | capsule framing, datagrams | `wt_h3_capsule.erl`, `wt_h2_capsule.erl` |
| RFC 9220, RFC 8441 Extended CONNECT | `:protocol` over HTTP/3 and HTTP/2 | `webtransport.erl` (through `quic_h3` and `h2`) |
| RFC 9114 HTTP/3, RFC 9000 QUIC | SETTINGS rules, error codes, streams (through `quic`) | `quic`, `webtransport_h3*.erl` |
| draft-ietf-quic-reliable-stream-reset | RESET_STREAM_AT for stream headers (through `quic`) | `webtransport_h3.erl` |
| RFC 9113 HTTP/2, RFC 8446 TLS 1.3 | HTTP/2 framing and TLS (through `h2` and `ssl`) | `h2`, `webtransport_h2.erl` |
| RFC 9651 Structured Field Values | `WT-Available-Protocols`, `WT-Protocol`, `WebTransport-Init` | `wt_sf.erl` |

[docs/compliance.md](docs/compliance.md) records what is implemented from
each and the open gaps.
