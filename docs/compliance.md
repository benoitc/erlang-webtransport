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

Draft-02 and draft-07 (`compat_mode => legacy_browser_compat`) are kept on a
separate path for Chrome, Firefox, Safari 26 and webtransport-go. They never
mix with the latest drafts in one handshake.

Audited 2026-11-13 against quic 2.7.0 and h2 0.17.0.

## What is covered

- **Handshake.** SETTINGS_WT_ENABLED on both transports (h3 0x20b8933b and
  0x2c7cf000, h2 0x2b67), with values above 1 refused as settings errors.
  The client requires HTTP/3 datagrams, QUIC datagrams and RESET_STREAM_AT
  from the server; the server checks the same on the client and waits for
  its SETTINGS before handling a request. Later changes to the server's h2
  SETTINGS are watched, and limits use the SETTINGS in effect when the
  request or response was sent. h2 runs over TLS 1.3 only (§7; OTP's `ssl`
  has no extended master secret for TLS 1.2). Origin check, 403 on
  rejection.
- **Subprotocols.** WT-Available-Protocols and WT-Protocol as structured
  fields, `WT_ALPN_ERROR` when the server's pick was not offered or is
  missing and required.
- **Wire format.** h3 stream headers, 32-bit close codes, UTF-8 close reasons
  of at most 1024 bytes, h2 capsule codepoints of http2-16, Reliable Size on
  WT_RESET_STREAM, RESET_STREAM_AT on h3 for every reset of our streams
  (including quic's answer to STOP_SENDING), unknown capsules dropped.
- **Flow control.** Initial limits from SETTINGS (and WebTransport-Init on
  h2), flow control on or off per http3-17 §5.1, one session per h3
  connection without it, strictly increasing limits, the 2^60 stream cap,
  overruns as WT_FLOW_CONTROL_ERROR, bytes of a reset stream counted from its
  final size, credit returned with WT_MAX_DATA,
  WT_MAX_STREAM_DATA and WT_MAX_STREAMS, blocked capsules sent.
- **Stream state (h2).** Resets, STOP_SENDING, data and flow-control capsules
  on a stream in the wrong state are WT_STREAM_STATE_ERROR session errors;
  STOP_SENDING is answered with WT_RESET_STREAM.
- **Termination.** WT_SESSION_GONE resets and STOP_SENDING on every stream,
  END_STREAM on h2 when the peer closes, H3_MESSAGE_ERROR for malformed
  capsules or data after WT_CLOSE_SESSION (also when it arrives after the
  session ended), the session ends when its CONNECT stream ends or is reset.
  A client closes its QUIC connection once its last session ended and the
  server closed the CONNECT stream (or after 1 s); a server leaves it open.
- **Buffering.** Streams and datagrams that arrive before their session are
  held and replayed, within caps (64 streams, 64 datagrams, 1 MiB of data per
  connection); streams of an ended or refused session are reset with
  WT_SESSION_GONE. Session ids that are not client bidi stream ids close the
  connection with H3_ID_ERROR.
- **Lifecycle.** A handler's `init/3` actions run once the session is
  established. A session that ends for any reason sends WT_CLOSE_SESSION.
  GOAWAY moves sessions to draining; a draining session still sends and
  receives datagrams and stops once its streams are done. Finished streams
  leave the session state.

## Known gaps

| Class | Path | Spec | Gap |
|---|---|---|---|
| upstream | h2 | http2-16 §3.4 | WT_ERROR, WT_STREAM_STATE_ERROR and WT_FLOW_CONTROL_ERROR have no HTTP/2 codepoints yet (0xTBD). Session errors use RST_STREAM with PROTOCOL_ERROR or FLOW_CONTROL_ERROR, after a WT_CLOSE_SESSION that carries 0 or the h3 WT_FLOW_CONTROL_ERROR value. Switch when IANA assigns them. |

## Classes used by the audit

- **wire**: we send or accept bytes the latest draft does not. Breaks
  interop with up-to-date peers.
- **must**: a MUST we don't enforce. Well-behaved peers still work.
- **should**: a SHOULD we skip.
- **upstream**: needs a change in `quic` or `h2`.
