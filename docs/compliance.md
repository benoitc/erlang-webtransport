# Spec compliance

This page lists the WebTransport spec revisions the library implements and
the gaps that are known. Read it when you need to know whether a peer on a
given draft will interoperate, or before you pick up compliance work. To
re-run the audit, use the `spec-compliance` Claude Code skill in
`.claude/skills/spec-compliance/`.

## Revisions

| Spec | Implemented | Notes |
|---|---|---|
| draft-ietf-webtrans-http3 | 17 | Draft-15 clients are accepted: the server advertises both SETTINGS_WT_ENABLED codepoints and picks the version per §7.1. The client speaks draft-17 only. |
| draft-ietf-webtrans-http2 | 16 | Draft-14 peers do not interoperate (the WT_STREAM FIN bit and the close capsule changed). |
| draft-ietf-webtrans-overview | 14 | |
| RFC 9297 | | Capsules, HTTP datagrams. |
| RFC 9220 / RFC 8441 | | Extended CONNECT over h3 / h2. |

Draft-02 (`compat_mode => legacy_browser_compat`) is kept on a separate path
for Chrome, Firefox and webtransport-go. It never mixes with the latest
drafts in one handshake.

Audited 2026-11-07 against quic 2.4.0 and h2 0.12.7.

## What is covered

- **Handshake.** SETTINGS_WT_ENABLED on both transports (h3 0x20b8933b and
  0x2c7cf000, h2 0x2b67), with values above 1 refused as settings errors.
  The client requires HTTP/3 datagrams, QUIC datagrams and RESET_STREAM_AT
  from the server; the server checks the same on the client and waits for
  its SETTINGS before handling a request. Origin check, 403 on rejection.
- **Subprotocols.** WT-Available-Protocols and WT-Protocol as structured
  fields, `WT_ALPN_ERROR` when the server's pick was not offered or is
  missing and required.
- **Wire format.** h3 stream headers, 32-bit close codes, UTF-8 close reasons
  of at most 1024 bytes, h2 capsule codepoints of http2-16, Reliable Size on
  WT_RESET_STREAM, RESET_STREAM_AT on h3, unknown capsules dropped.
- **Flow control.** Initial limits from SETTINGS (and WebTransport-Init on
  h2), flow control on or off per http3-17 §5.1, one session per h3
  connection without it, strictly increasing limits, the 2^60 stream cap,
  overruns as WT_FLOW_CONTROL_ERROR, credit returned with WT_MAX_DATA,
  WT_MAX_STREAM_DATA and WT_MAX_STREAMS, blocked capsules sent.
- **Stream state (h2).** Resets, STOP_SENDING, data and flow-control capsules
  on a stream in the wrong state are WT_STREAM_STATE_ERROR session errors;
  STOP_SENDING is answered with WT_RESET_STREAM.
- **Termination.** WT_SESSION_GONE resets and STOP_SENDING on every stream,
  END_STREAM on h2 when the peer closes, H3_MESSAGE_ERROR for malformed
  capsules or data after WT_CLOSE_SESSION, the session ends when its CONNECT
  stream ends or is reset, the QUIC connection closes only with its last
  session.
- **Buffering.** Streams and datagrams that arrive before their session are
  held and replayed; streams of an ended session are reset with
  WT_SESSION_GONE. Session ids that are not client bidi stream ids close the
  connection with H3_ID_ERROR.

## Known gaps

| Path | Spec | Gap |
|---|---|---|
| h2 | http2-16 §3.4 | WT_ERROR, WT_STREAM_STATE_ERROR and WT_FLOW_CONTROL_ERROR have no HTTP/2 codepoints yet (0xTBD). Session errors use RST_STREAM with PROTOCOL_ERROR or FLOW_CONTROL_ERROR, after a WT_CLOSE_SESSION that carries 0 or the h3 WT_FLOW_CONTROL_ERROR value. Switch when IANA assigns them. |
| h2 | http2-16 §7 | The TLS requirement (TLS 1.3, or 1.2 with extended master secret) is not checked: the h2 library does not expose the negotiated version per connection. |
| h2 | http2-16 §4.3.1 | Initial limits use the peer SETTINGS current when the session starts, not the ones acknowledged at the moment of the request. They differ only if the peer changes its WT SETTINGS mid-connection. |
| h3 | http3-17 §4.7 | After WT_DRAIN_SESSION or GOAWAY we refuse to send new datagrams. The draft allows sending them. |
| h3 | http3-17 §6 | STOP_SENDING(WT_SESSION_GONE) after receiving WT_CLOSE_SESSION, which the draft allows, is sent only through the general termination path. |

## Classes used by the audit

- **wire**: we send or accept bytes the latest draft does not. Breaks
  interop with up-to-date peers.
- **must**: a MUST we don't enforce. Well-behaved peers still work.
- **should**: a SHOULD we skip.
- **upstream**: needs a change in `quic` or `h2`.
