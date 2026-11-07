# Spec compliance

This page lists the WebTransport spec revisions the library is checked
against and the gaps that are known. Read it when you need to know whether a
peer on a given draft will interoperate, or before you pick up compliance
work. To re-run the audit, use the `spec-compliance` Claude Code skill in
`.claude/skills/spec-compliance/`.

## Revisions

| Spec | Implemented | Latest (audited 2026-11-07) |
|---|---|---|
| draft-ietf-webtrans-http3 | 15 | 17 |
| draft-ietf-webtrans-http2 | 14 | 16 |
| draft-ietf-webtrans-overview | 12 | 14 |

Draft-02 (`compat_mode => legacy_browser_compat`) is not covered here. The
audit confirmed that it does not leak into latest-mode handshakes.

## Classes

- **wire**: we send or accept bytes the latest draft does not. Breaks
  interop with up-to-date peers.
- **must**: a MUST we don't enforce. Well-behaved peers still work.
- **should**: a SHOULD we skip.
- **upstream**: needs a change in `quic` or `h2`.

## Wire gaps

| Path | Spec | Code | Gap |
|---|---|---|---|
| h3 | http3-17 §9.2 | `quic_h3.hrl:54` (quic) | SETTINGS_WT_ENABLED moved from 0x2c7cf000 to 0x20b8933b. Peers on -17 don't see our setting. quic passes integer keys through, so a local fix is possible. |
| h3 | http3-17 §6 | `wt_h3_capsule.erl:117,152` | WT_CLOSE_SESSION error code is encoded as a varint; the spec says 32 bits. |
| h3 | http3-17 §6 | `webtransport_session.erl:767,1023,1155` | WT_SESSION_GONE and WT_BUFFERED_STREAM_REJECTED are mapped into the application error range before the reset, so the peer sees the wrong code. |
| h3 | http3-17 §5.4, §5.5 | `webtransport_session.erl:188,525` | Peer `wt_initial_max_*` SETTINGS are never read; send-side data and stream limits use our own defaults. |
| h2 | http2-16 §6.4 | `webtransport.hrl:21`, `wt_h2_capsule.erl:169,245` | WT_STREAM FIN bit is reversed: -16 uses 0x190B4D3C for data and 0x190B4D3B for the final capsule. |
| h2 | http2-16 §6.12, §6.13 | `webtransport.hrl:29`, `wt_h2_capsule.erl:215,343` | Close and drain use 0x190B4D45/46; the spec uses the h3 capsules 0x2843/0x78ae with a 32-bit error code. |
| h2 | http2-16 §6.2 | `wt_h2_capsule.erl:177,257` | WT_RESET_STREAM has no Reliable Size field; a compliant reset fails to decode and closes the session. |
| h2 | http2-16 §3.1 | `webtransport.erl:805` | Server never sends SETTINGS_WT_ENABLED (0x2b67); -16 clients won't open a session. |
| h2 | http2-16 §4.3.2 | `wt_h2_init.erl:8`, `webtransport.erl:362,1025` | WebTransport-Init `u`/`bl`/`br` are read as stream counts and session data; the spec defines them as per-stream data limits. |
| h2 | http2-16 §4.3.1 | `webtransport_session.erl:188` | Peer WT SETTINGS are never read and the client sends none; a compliant peer stalls. |
| both | RFC 9297 §3.2 | `webtransport_session.erl:885` | Unknown capsule types close the session instead of being dropped. |

## MUST gaps

| Path | Spec | Code | Gap |
|---|---|---|---|
| h3 | http3-17 §3.1 | `wt_h3.erl:186` | Client does not close with H3_SETTINGS_ERROR when WT_ENABLED > 1. |
| h3 | http3-17 §3.1 | `wt_h3.erl:166-182` | Client accepts a server missing h3_datagram or reset_stream_at. |
| h3 | http3-17 §3.1 | `webtransport.erl:1131,310` | Server never checks the client's SETTINGS or transport parameters. |
| h3 | http3-17 §3.2 | `webtransport.erl:1078,347` | Buffered capsules are released before the 2xx is sent. |
| h3 | http3-17 §4 | `webtransport_h3_router.erl:239` | Session IDs that are not client-initiated bidi IDs are not rejected with H3_ID_ERROR. |
| h3 | http3-17 §4.4 | `webtransport_h3.erl:195` | Resets use RESET_STREAM instead of RESET_STREAM_AT with the header size as Reliable Size. `quic:reset_stream_at/4` exists. |
| h3 | http3-17 §5.1 | `webtransport_session.erl:790` | No "flow control disabled" state: capsules are always applied and extra sessions are not refused. |
| h3 | http3-17 §6 | `webtransport_session.erl:370,409` | Data after WT_CLOSE_SESSION on the CONNECT stream is dropped instead of resetting with H3_MESSAGE_ERROR. |
| h3 | http3-17 §6 | `webtransport_session.erl:767` | On termination only send sides are reset; no STOP_SENDING on receive sides. |
| h3 | http3-17 §6 | `webtransport.erl:1455` | The client session never receives CONNECT-stream data, so server CLOSE, DRAIN and FIN are ignored. A server `stream_reset` on the CONNECT stream goes to the handler; `draining` has no `info` clause. |
| h3 | RFC 9297 §3.2 | `webtransport_session.erl:338` | A capsule split across two DATA frames is dropped (h2 buffers it). |
| h2 | http2-16 §3.1 | `webtransport_h2.erl:96` | Client sends CONNECT without waiting for WT_ENABLED and does not reject values > 1. |
| h2 | http2-16 §4.3.2 | `wt_h2_init.erl:55`, `webtransport.erl:372` | Unparseable WebTransport-Init is accepted with defaults instead of a 4xx; unknown keys and parameters fail to parse instead of being ignored. |
| h2 | http2-16 §6.2-§6.9 | `webtransport.erl:1227`, `webtransport_session.erl:803,851,919` | Stream-state violations (reset or data on a closed stream, duplicate STOP_SENDING, MAX_STREAM_DATA after STOP_SENDING) are ignored instead of WT_STREAM_STATE_ERROR. |
| h2 | http2-16 §6.3 | `webtransport.erl:1229` | STOP_SENDING closes the whole stream; no WT_RESET_STREAM is sent back. |
| h2 | http2-16 §6.2, §6.3 | `wt_h2_capsule.erl:257` | Error codes above 0xffffffff are accepted. |
| h2 | http2-16 §6.12, §3.4 | `webtransport_session.erl:409,492`, `webtransport.erl:1166` | No END_STREAM after a peer's close; a peer FIN without a capsule does not end the session. |
| both | http3-17 §5.6, http2-16 §6.5-§6.7 | `webtransport_session.erl:792,808,821,828` | WT_MAX_* equal to the previous value is accepted; the spec requires a strict increase. |
| both | http3-17 §5.6.2, http2-16 §6.7 | `webtransport_session.erl:820,845` | Maximum Streams above 2^60 is not rejected. |
| both | http3-17 §5.4, http2-16 §6.5 | `webtransport_session.erl:893-925` | Incoming data beyond our stream or session limit is dropped or ignored instead of WT_FLOW_CONTROL_ERROR. |
| both | http3-17 §5.6.2, http2-16 §6.7 | `webtransport_session.erl:995` | A stream over our limit is reset instead of closing the session; on h2 the count ignores stream-ID ordering. |
| both | http3-17 §6, http2-16 §6.12 | `wt_h3_capsule.erl:154`, `wt_h2_capsule.erl:343` | CLOSE_SESSION reason is not checked for UTF-8, and an over-long reason gets the wrong error. |
| both | overview-14 §2, http2-16 §3.3 | `webtransport.erl:1015,358` | No subprotocol negotiation (WT-Available-Protocols / WT-Protocol). |
| both | overview-14 §4.3 | `webtransport_session.erl:820` | Handlers are not told when new stream credit arrives. |

## SHOULD gaps

| Path | Spec | Code | Gap |
|---|---|---|---|
| both | http3-17 §5.6, http2-16 §4.4 | `webtransport_session.erl:932-959` | WT_MAX_STREAMS is never sent; on h3 WT_MAX_DATA is never sent either. Peers stall at the initial limits. |
| both | http3-17 §5.6.3, http2-16 §6.10 | `webtransport_session.erl:651,691,711` | WT_STREAMS_BLOCKED is never sent; WT_DATA_BLOCKED only on h2. |
| h3 | http3-17 §4.6 | `webtransport_h3_router.erl:260,310` | Streams and datagrams for a not-yet-known session are rejected instead of buffered; server streams that beat the 2xx are lost on the client. |
| h3 | http3-17 §4.4 | `webtransport_h3_router.erl:354` | Reset codes outside the application range are delivered as if they were application codes. |
| h3 | http3-17 §6 | `webtransport_session.erl:470` | CONNECTION_CLOSE is sent right after WT_CLOSE_SESSION and can overtake it. |

## Upstream gaps

| Dep | Spec | Gap |
|---|---|---|
| quic | http3-17 §9.2 | `H3_SETTINGS_WT_ENABLED` still 0x2c7cf000. |
| quic | http3-17 §4.3 | WT_STREAM (0x41) in an unexpected place is skipped instead of H3_FRAME_UNEXPECTED (`quic_h3_connection.erl:1899,2828`). |
| h2 | http2-16 §7 | No API exposes the negotiated TLS version or extended master secret, so the TLS requirement can't be checked. |
