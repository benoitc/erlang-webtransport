---
name: spec-review
description: Check a change, commit or pull request in erlang-webtransport for compliance, latest spec revision and safety against the WebTransport drafts and RFCs it claims to follow (draft-ietf-webtrans-http3, -http2, -overview, RFC 9297, 9220, 8441, 9114, 9651). Use when reviewing a PR, before merging a protocol fix, or when asked whether code complies with the spec. For a full audit of the library, use spec-compliance instead.
---

# Spec review

Use this to decide whether a change does what the spec says, no more and no
less, and stays safe against a hostile peer. These are the three checks
AGENTS.md requires: compliance, latest spec, safety. It applies to one
commit at a time; a PR with several commits gets one verdict per commit.

## 1. Get the spec text

Never quote a draft from memory. Fetch the plain text once into the
scratchpad and grep it. Check which revision the code targets first
(`include/webtransport.hrl`, `docs/compliance.md`):

```bash
mkdir -p "$SCRATCH/specs" && cd "$SCRATCH/specs"
for d in webtrans-http3-17 webtrans-http2-16 webtrans-overview-14; do
  [ -f draft-ietf-$d.txt ] || \
    curl -sfL https://www.ietf.org/archive/id/draft-ietf-$d.txt -o draft-ietf-$d.txt
done
for r in 9297 9220 8441 9114 9651; do
  [ -f rfc$r.txt ] || curl -sfL https://www.rfc-editor.org/rfc/rfc$r.txt -o rfc$r.txt
done
grep -n "WT_SESSION_GONE" draft-ietf-webtrans-http3-17.txt
```

Name the draft revision you checked. A change for a legacy draft (draft-02,
http3-07, `compat_mode => legacy_browser_compat`) is checked against that
draft, and must not leak into the latest path.

## 2. Check the claim

For each commit:

1. Find every spec sentence the commit message or PR body quotes. Confirm it
   is verbatim and in the section cited.
2. Read the whole section around it, not only the sentence: exceptions,
   "unless", and the matching rule for the other role (client/server,
   sender/receiver) are usually a paragraph away.
3. Note the requirement level: MUST / MUST NOT is a fix; SHOULD needs a
   reason not to; MAY is a choice, so the change must say why it is worth it.
4. Check the error code and the action (reset the stream, close the session
   with WT_CLOSE_SESSION, H3_MESSAGE_ERROR, H3_REQUEST_REJECTED, a 4xx
   response, a connection error) match the text exactly.
5. Check the code does what the text says on every path that reaches it:
   - both transports: a rule fixed on h3 is often missing on h2, and the
     other way round;
   - client and server;
   - every compat mode: `latest`, `legacy_browser_compat`, `auto`;
   - every session state: `open`, `draining`, closing;
   - streams we opened and streams the peer opened, uni and bidi.
6. Check what the dependencies do before claiming a rule is met or missing:
   read `_build/default/lib/quic` and `_build/default/lib/h2`. Some rules
   live there (SETTINGS, RESET_STREAM_AT, extended CONNECT).
7. Check it does not go further than the spec: a new session close where the
   draft says "MAY" or "SHOULD" breaks interop with peers that are allowed
   to do it.

## 3. Check safety

The peer is untrusted, on both the server and the client side. For every
resource the change touches, find the bound:

- streams or datagrams that arrive before their session (router parking);
- capsules being reassembled, close reasons, header fields;
- the number of streams, sessions and reservations per connection;
- flow-control credit: a peer that never returns credit, or raises it
  without limit, must not make us buffer without bound;
- send buffers, on both transports (the h2 per-stream cap, quic's stream
  buffers).

An unbounded resource that the spec bounds is a spec bug (see AGENTS.md), not
a security issue: fix it in public and cite the section.

## 4. Check the test

- The test fails on main without the fix and passes with it. Run it both
  ways.
- It asserts the outcome the spec names (the close code, the reset, the
  capsule sent), and cannot also pass on a crash or a missing result.
- A wire change also needs the interop harnesses: `interop/` and
  `scripts/interop_cross.sh` (docker), and a browser when browsers are
  affected (see CONTRIBUTING.md).

## 5. Report

One line per commit:

```
<sha> <subject>
  spec: draft-ietf-webtrans-http3-17 §6 MUST, quote verified | quote not found | paraphrased
  verdict: compliant | partial (<what is missing>) | wrong (<why>) | not a spec item (<what it is>)
  safety: bounded | unbounded (<resource>) | not applicable
  test: fails before, passes after | does not discriminate | missing
```

Then list anything that needs the maintainer's decision (a SHOULD left out,
a deliberate deviation). A gap that stays open goes in `docs/compliance.md`.
