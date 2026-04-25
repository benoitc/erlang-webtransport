# Changelog

## 0.2.0

- Switch `h2` and `quic` deps to hex packages: `h2 0.5.0`, `quic 1.3.0`.
- Add `public_key` to the application list.
- Clean dialyzer run: dropped dead `{error, _}` clauses around `h2_capsule:decode/1` and `h2_varint:decode/1`, removed unreachable `Router =:= undefined` branch, dropped unsupported `h3_datagram_enabled` from `quic_h3:start_server/3` opts, switched listener loop to `spawn_link/3` for static reachability.
- Configure `xref_checks` (skip `exports_not_used` for the public API) and `dialyzer` (`plt_extra_apps: [public_key, ssl, crypto, inets]`).
- Add GitHub Actions CI: OTP 26/27 matrix runs compile, xref, eunit, dialyzer; separate job builds ex_doc.

## 0.1.0

- Initial release.
