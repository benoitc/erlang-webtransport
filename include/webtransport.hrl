%% @doc WebTransport Protocol Definitions
%%
%% draft-ietf-webtrans-overview-12 - Protocol Framework
%% draft-ietf-webtrans-http2-14 - WebTransport over HTTP/2
%% draft-ietf-webtrans-http3-15 - WebTransport over HTTP/3

-ifndef(WEBTRANSPORT_HRL).
-define(WEBTRANSPORT_HRL, 1).

%% ============================================================================
%% HTTP/2 WebTransport Capsule Types (draft-ietf-webtrans-http2-14)
%% ============================================================================

-define(WT_PADDING,              16#190B4D38).
-define(WT_RESET_STREAM,         16#190B4D39).
-define(WT_STOP_SENDING,         16#190B4D3A).
-define(WT_STREAM,               16#190B4D3B).
-define(WT_STREAM_FIN,           16#190B4D3C).
-define(WT_MAX_DATA,             16#190B4D3D).
-define(WT_MAX_STREAM_DATA,      16#190B4D3E).
-define(WT_MAX_STREAMS_BIDI,     16#190B4D3F).
-define(WT_MAX_STREAMS_UNI,      16#190B4D40).
-define(WT_DATA_BLOCKED,         16#190B4D41).
-define(WT_STREAM_DATA_BLOCKED,  16#190B4D42).
-define(WT_STREAMS_BLOCKED_BIDI, 16#190B4D43).
-define(WT_STREAMS_BLOCKED_UNI,  16#190B4D44).
-define(WT_CLOSE_SESSION,        16#190B4D45).
-define(WT_DRAIN_SESSION,        16#190B4D46).

%% HTTP Datagram capsule (RFC 9297)
-define(DATAGRAM,                16#00).

%% ============================================================================
%% HTTP/3 WebTransport (draft-ietf-webtrans-http3-15)
%% ============================================================================

%% HTTP/3 Settings.
%%
%% Default (latest-spec) path, draft-15 §9.2:
%%   SETTINGS_WT_ENABLED                   0x2c7cf000  boolean enabling WebTransport
%%   SETTINGS_WT_INITIAL_MAX_DATA          0x2b61      initial session flow-control window
%%   SETTINGS_WT_INITIAL_MAX_STREAMS_UNI   0x2b64      initial peer uni stream count
%%   SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI  0x2b65      initial peer bidi stream count
%% plus SETTINGS_ENABLE_CONNECT_PROTOCOL and SETTINGS_H3_DATAGRAM from RFC 8441 / 9297.
%%
%% Legacy-browser-compat path (draft-02, still what Chrome / Firefox / quic-go
%% v0.9 ship) uses a separate boolean gate plus a Sec-Webtransport-Http3-Draft02
%% request header. The two paths are disjoint: never advertise both in the same
%% handshake.
-define(SETTINGS_WT_ENABLED,                  16#2c7cf000).
-define(SETTINGS_WT_INITIAL_MAX_DATA,         16#2b61).
-define(SETTINGS_WT_INITIAL_MAX_STREAMS_UNI,  16#2b64).
-define(SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, 16#2b65).
-define(SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02, 16#2b603742).
-define(SETTINGS_ENABLE_CONNECT_PROTOCOL,     16#08).
-define(SETTINGS_H3_DATAGRAM,                 16#33).

%% HTTP/2 WebTransport SETTINGS (draft-14 §11.2).
%% These are defined here for completeness but cannot be advertised via
%% the vendored erlang_h2 library today (h2_settings strips unknown IDs).
%% Upstream fix needed: <https://github.com/benoitc/erlang_h2/issues/N>.
-define(SETTINGS_WT_INITIAL_MAX_STREAM_DATA_UNI,         16#2b62).
-define(SETTINGS_WT_INITIAL_MAX_STREAM_DATA_BIDI_LOCAL,  16#2b63).
-define(SETTINGS_WT_INITIAL_MAX_STREAM_DATA_BIDI_REMOTE, 16#2b66).

%% HTTP/3 Unidirectional Stream Type
-define(WT_UNI_STREAM_TYPE, 16#54).

%% HTTP/3 Bidirectional Stream Signal
-define(WT_BIDI_SIGNAL, 16#41).

%% HTTP/3 Capsule Types (session management)
-define(WT_CLOSE_SESSION_H3, 16#2843).
-define(WT_DRAIN_SESSION_H3, 16#78ae).

%% HTTP/3 Error Codes
-define(WT_BUFFERED_STREAM_REJECTED, 16#3994bd84).
-define(WT_SESSION_GONE,             16#170d7b68).
-define(WT_FLOW_CONTROL_ERROR,       16#045d4487).
-define(WT_ALPN_ERROR,               16#0817b3dd).
-define(WT_REQUIREMENTS_NOT_MET,     16#212c0d48).

%% Application error code base
-define(WT_APP_ERROR_FIRST, 16#52e4a40fa8db).

%% ============================================================================
%% Stream State (defined first, referenced by session)
%% ============================================================================

-record(wt_stream, {
    id :: non_neg_integer(),
    type :: bidi | uni,
    state = open :: open | half_closed_local | half_closed_remote | closed,

    %% Flow control
    send_window :: non_neg_integer(),
    recv_window :: non_neg_integer(),

    %% Buffers
    send_buffer = <<>> :: binary(),
    recv_buffer = <<>> :: binary()
}).

%% ============================================================================
%% Session State
%% ============================================================================

-record(wt_session, {
    id :: non_neg_integer(),
    transport :: h2 | h3,
    path :: binary(),
    authority :: binary(),
    state = open :: open | draining | closed,

    %% Flow control (for H2, managed by capsules)
    local_max_data :: non_neg_integer(),
    remote_max_data :: non_neg_integer(),
    local_max_streams_bidi :: non_neg_integer(),
    local_max_streams_uni :: non_neg_integer(),
    remote_max_streams_bidi :: non_neg_integer(),
    remote_max_streams_uni :: non_neg_integer(),

    %% Active streams
    streams :: #{non_neg_integer() => #wt_stream{}},
    stream_count_bidi :: non_neg_integer(),
    stream_count_uni :: non_neg_integer()
}).

%% ============================================================================
%% Default Settings
%% ============================================================================

-define(DEFAULT_MAX_DATA, 1048576).  %% 1 MB
-define(DEFAULT_MAX_STREAM_DATA, 262144).  %% 256 KB
-define(DEFAULT_MAX_STREAMS_BIDI, 100).
-define(DEFAULT_MAX_STREAMS_UNI, 100).
-define(DEFAULT_MAX_SESSIONS, 100).

%% ============================================================================
%% Datagram size ceilings
%% ============================================================================
%%
%% H2 datagrams travel as capsules over the CONNECT stream, bounded by the
%% HTTP/2 default initial stream window (65535 bytes). The capsule header
%% (type varint + length varint) consumes a few bytes; we reserve 64 bytes
%% of headroom to leave room for the RFC 9297 DATAGRAM capsule framing and
%% any intermediate HTTP/2 framing overhead.
-define(WT_H2_DATAGRAM_MAX, 65471).  %% 65535 - 64

%% H3 datagrams are QUIC datagrams prefixed with a quarter-stream-id varint.
%% The peer-advertised `max_datagram_frame_size' (we set 65535) caps the
%% whole encoded datagram — payload + session-id varint — so we reserve
%% 8 bytes for the worst-case varint encoding of the session id.
-define(WT_H3_DATAGRAM_MAX, 65527).  %% 65535 - 8

-endif.
