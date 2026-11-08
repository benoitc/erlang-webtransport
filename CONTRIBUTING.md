# Contributing

This page is for people changing erlang-webtransport itself. For using the
library, start with [docs/getting-started.md](docs/getting-started.md).
[docs/compliance.md](docs/compliance.md) lists what the library implements
from each draft and the known gaps.

## Set up

You need Erlang/OTP 27 or later and rebar3. rebar3 fetches the two
dependencies, `quic` and `h2`.

```bash
git clone https://github.com/benoitc/erlang-webtransport.git
cd erlang-webtransport
rebar3 compile
```

The test suites generate their own certificates.

## Before you commit

Run what CI runs, plus Common Test, which CI does not run:

```bash
rebar3 compile
rebar3 xref
rebar3 eunit
rebar3 dialyzer
rebar3 ct
rebar3 ex_doc
```

## Tests

- Most unit tests live next to the code in `-ifdef(TEST)` blocks; the rest
  are `test/*_tests.erl`. `rebar3 eunit` runs both, and the PropEr
  properties in `test/prop_*.erl` with them.
- `test/webtransport_SUITE.erl` runs each case over HTTP/3 and HTTP/2
  against a listener in the same node. Run one transport or one case with
  `rebar3 ct --suite webtransport_SUITE --group h2_tests --case bidi_echo_test`.
- A change that touches both transports needs a case in both groups. A rule
  fixed on one transport is often missing on the other.

A test must fail for one reason. A test that also passes when the call
crashes, or that asserts a value it computed itself, is worse than no test
because it reads as coverage.

## Interop

Unit and CT tests run this library against itself, so they miss wire
mismatches. A change to what goes on the wire also runs:

```bash
cd interop
docker compose --profile cross up -d --build server server-go
docker compose --profile cross config --services | grep '^client'   # the test clients
docker compose --profile cross run --rm client-bidi                 # run each one
docker compose --profile cross down
```

`client-*` run against the Erlang server, `client-go-vs-erlang-*` run
webtransport-go against it, and `client-erlang-vs-go-*` run the Erlang client
against webtransport-go. Run them one at a time: started together they
interfere. `scripts/interop_cross.sh` starts them all at once.

When browsers are affected, check them too:

- Chrome: `interop-chrome/run.sh` starts a server and drives Chrome.
- Safari: Docker cannot run it. Use the iOS simulators that come with Xcode
  (`xcrun simctl list runtimes`). Boot one, then open your test page with
  `xcrun simctl openurl <device> <url>`. Safari 26.2 has no WebTransport;
  26.4 and later do. Use a short-lived ECDSA certificate with
  `serverCertificateHashes`.
- Chrome may resolve `localhost` to `::1`. Use `127.0.0.1` if the server
  listens on IPv4 only.

## Specs

Quote a draft only from its fetched text, never from memory, and name the
revision. The `spec-review` and `spec-compliance` skills in `.agents/skills`
describe how to check a change and how to audit the library. When a rule is
left open, add it to `docs/compliance.md`.

Draft-02 and http3-07 stay on the legacy path (`compat_mode`). Never send
their SETTINGS, headers or `:protocol` value in a latest-draft handshake.

## Dependencies

When a bug or a missing feature is in `quic` or `h2`, fix it there. Open an
issue or pull request on [erlang_quic](https://github.com/benoitc/erlang_quic)
or [erlang_h2](https://github.com/benoitc/erlang_h2), then bump the
dependency here once it is released.

## Documentation

Write guides in the task-oriented style the existing ones use: say what the
thing is, when you need it, then show the steps with short notes between
code blocks. When you rename an option or change behaviour, update `README.md`
and `docs/` in the same change.

## Pull requests

Branch from `main` using the prefix that fits: `fix/`, `feat/`, `perf/`,
`refactor/`, `docs/`, `test/` or `chore/`.

Write the commit subject in the imperative, describing what the code now does
rather than what it used to do. Keep the body to short bullets. Do not add
generated-by or co-authored-by trailers.

Say what the change is for and how it works, and list any breaking change.
Skip the test inventory: CI reports that. If the change touches the public
API, show it in a short code block.

Add a CHANGELOG entry under `## Unreleased`: plain English, saying what was
wrong or what is new and what it means for the user, with no implementation
details.

Sync a branch that has fallen behind by merging `main` into it, not by
rebasing.

## Releases

The version lives in `src/webtransport.app.src`. To release:

1. Merge the pull requests.
2. On `main`, commit `Release X.Y.Z`. It renames `## Unreleased` in the
   CHANGELOG to `## X.Y.Z - YYYY-MM-DD` and bumps the version.
3. Tag the commit `vX.Y.Z`.
4. Create a GitHub release named `X.Y.Z`. Its notes are that CHANGELOG
   section.
