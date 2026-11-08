# Contributing

This page is for people changing erlang-webtransport itself. For using the
library, start with [docs/getting-started.md](docs/getting-started.md).
[docs/compliance.md](docs/compliance.md) lists what the library implements
from each draft and the known gaps.

## Write for the person who reads it

Every issue, pull request and comment is read by a person. Make it easy for
them.

- Read what you send before you send it. You are responsible for every line,
  whether you wrote it or a tool did.
- Use plain English and short sentences. Say what happens, what you
  expected, and how to reproduce it. Explain any term a reader may not know.
- Don't inflate it. No buzzwords, no severity scores, CVSS or CWE labels, no
  "critical" for a bug, no long generated write-ups. If it takes a page to
  explain, cut it down until it doesn't.
- A bug report gives: the library version, the transport (HTTP/3 or
  HTTP/2), the peer (browser and version, or library), the options you set,
  a minimal handler and the steps that show the problem, what you got and
  what you expected.
- A pull request says what was wrong and why it matters, the fix in a few
  lines, and any breaking change. Link the issue. Don't walk through the diff
  file by file; the reviewer reads the code.
- A fix comes with a test that fails without it.

AI tools are fine to use. Before you open anything, you must have read the
change, run the tests, and be able to answer questions about it yourself.

Maintainers read every issue and pull request before acting on it. We close
without further review:

- text we cannot understand after one request to clarify;
- descriptions that don't match the code or the reproduction;
- reports that cannot be reproduced;
- submissions opened by an automated agent that no person has read.

## Security issues and specification bugs

The library implements WebTransport over HTTP/3 and HTTP/2 and the specs
they build on. The list is in [AGENTS.md](https://github.com/benoitc/erlang-webtransport/blob/main/AGENTS.md#specifications).

When the fix is to make the library do what one of these specifications
says, it is a bug, not a security issue. That includes reports described as
denial of service, resource exhaustion or desync, when they come from:

- input accepted, rejected or framed differently from the specification;
- the HTTP/3 and HTTP/2 paths disagreeing on the same rule;
- a session or stream state change the specification does not allow;
- a missing bound that the specification or an option defines.

Report these as a normal bug, in public. Cite the specification section.
The fix goes in a public pull request and in the CHANGELOG. We don't publish
advisories or request CVEs for them, and advisories filed for them are
closed with a link to the fix.

Only problems that are not a specification deviation go through
[SECURITY.md](SECURITY.md): code execution, file access, one session's data
reaching another session or peer, or the library weakening TLS or
certificate checks.

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
