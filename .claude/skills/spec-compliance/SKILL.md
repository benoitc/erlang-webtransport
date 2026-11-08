---
name: spec-compliance
description: Audit erlang-webtransport against the current WebTransport drafts (http3, http2, overview) and the RFCs they depend on, then record gaps in docs/compliance.md. Use when a new draft revision is published, before a release, after touching handshake, capsule, flow-control, stream or session-termination code, or when interop shows a wire mismatch.
---

# Spec compliance audit

This skill checks the library against the current WebTransport specs and
records the result in `docs/compliance.md`. Run it when the IETF publishes a
new draft revision, before tagging a release, or after changing anything
that goes on the wire.

## Specs in scope

| Spec | What it covers here |
|---|---|
| draft-ietf-webtrans-http3 | h3 path: SETTINGS, `:protocol`, stream headers 0x54/0x41, capsules, error codes, flow control, GOAWAY, termination |
| draft-ietf-webtrans-http2 | h2 path: extended CONNECT, WT_* capsules, flow control, WebTransport-Init header, termination |
| draft-ietf-webtrans-overview | session model, protocol negotiation, required features |
| RFC 9297 | capsule protocol, HTTP datagrams, `Capsule-Protocol` header |
| RFC 9220 / RFC 8441 | extended CONNECT over h3 / h2 |
| RFC 9114, RFC 9000 | h3 SETTINGS rules, GOAWAY, stream and error-code rules |

Draft-02 (`compat_mode => legacy_browser_compat`) is out of scope for gap
finding. Only check that it never leaks into the `latest` path: no draft-02
SETTINGS, `:protocol` value or `Sec-Webtransport-Http3-Draft02` header in a
latest-mode handshake.

## Step 1: find the current revisions

```sh
for d in webtrans-http3 webtrans-http2 webtrans-overview; do
  curl -s "https://datatracker.ietf.org/api/v1/doc/document/draft-ietf-$d/?format=json" \
    | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['name'],d['rev'])"
done
grep -n "draft-ietf-webtrans" include/webtransport.hrl docs/compliance.md
```

If a draft has become an RFC, the datatracker entry says so; audit against
the RFC instead.

## Step 2: fetch the texts

Put them in the scratchpad, never in the repo. Fetch both the revision the
code targets and the latest one so you can diff them.

```sh
S=<scratchpad>/specs; mkdir -p $S
for f in webtrans-http3-NN webtrans-http2-NN webtrans-overview-NN; do
  curl -sfo $S/draft-ietf-$f.txt https://www.ietf.org/archive/id/draft-ietf-$f.txt
done
curl -sfo $S/rfc9297.txt https://www.rfc-editor.org/rfc/rfc9297.txt
```

Diff old against new with page headers stripped to see what moved:

```sh
strip() { grep -v "Internet-Draft\|Expires\|\[Page\|^$" "$1" | sed 's/  */ /g'; }
diff <(strip old.txt) <(strip new.txt)
```

Most churn is editorial. Look for changed codepoints, new or removed
MUST/SHOULD, new capsules, settings, headers or error codes.

## Step 3: check the code

Walk each spec section that defines behaviour. For every MUST, MUST NOT,
SHOULD and codepoint, find the code that implements it or note that none
does. Where to look:

| Area | Files |
|---|---|
| Codepoints | `include/webtransport.hrl`, `src/wt_error.erl` |
| h3 handshake, SETTINGS, `:protocol`, compat split | `src/wt_h3.erl`, `src/webtransport_h3.erl` |
| h3 stream routing, unknown session, buffering | `src/webtransport_h3_router.erl` |
| h3 capsules | `src/wt_h3_capsule.erl` |
| h2 handshake, WebTransport-Init | `src/webtransport_h2.erl`, `src/wt_h2_init.erl` |
| h2 capsules | `src/wt_h2_capsule.erl` |
| Session state, flow control, termination, drain | `src/webtransport_session.erl` |
| Stream state, resets, FIN | `src/webtransport_stream.erl` |
| Public API, client connect | `src/webtransport.erl` |

Also check what the dependencies do, since some rules live there:
`_build/default/lib/quic` (h3 SETTINGS, datagrams, RESET_STREAM_AT) and
`_build/default/lib/h2` (extended CONNECT, SETTINGS). Read the dep source;
don't assume.

Things that have gone wrong before and are worth checking every time:

- Codepoint values against the IANA section of the latest draft.
- Unknown SETTINGS being stripped by quic or h2 before we can see them.
- Flow-control limits that may only grow (monotonicity) and the error code
  sent when a peer violates one.
- Session termination: every stream of the session reset with
  WT_SESSION_GONE, CLOSE_SESSION reason length cap, FIN on the CONNECT stream.
- Streams or datagrams for an unknown session: buffer limits and rejection
  code.
- Same handling on h2 and h3. A rule fixed on one path is often missing on
  the other.

## Step 4: confirm before reporting

For each candidate gap, quote the spec sentence (section number) and the
code location (`file:line`). Drop it if you cannot point at both. If a test
exists or can be written in a few lines, use it to confirm; a gap backed by
a failing test is worth more than one backed by reading.

Classify each gap:

- **wire**: we send or accept bytes the spec forbids, or use a stale
  codepoint. Breaks interop. Fix first.
- **must**: a MUST/MUST NOT we don't enforce, without breaking well-behaved
  peers.
- **should**: a SHOULD we skip.
- **upstream**: blocked on quic or h2. Name the dep function.

## Step 5: record the result

Update `docs/compliance.md`: the revisions audited, the date, and the gap
table (class, spec section, code location, one-line description). Remove
gaps that were fixed. Keep it short; it is a status page, not a report.

When the code moves to a new revision, also update the revision comments in
`include/webtransport.hrl` and add a CHANGELOG entry under Unreleased saying
which draft revision is now supported and what changed for the user.

Run the checks after any fix:

```sh
rebar3 compile && rebar3 xref && rebar3 dialyzer && rebar3 eunit && rebar3 ct
```

The cross-implementation harness (`scripts/interop_cross.sh`, needs docker)
catches wire gaps that unit tests miss.
