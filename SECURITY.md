# Security Policy

## Supported Versions

Security fixes are applied to the latest minor release on the `main` branch.

| Version | Supported |
| ------- | --------- |
| 0.5.x   | yes       |
| < 0.5   | no        |

Issues in the QUIC or HTTP/2 layers belong to
[erlang_quic](https://github.com/benoitc/erlang_quic/security) and
[erlang_h2](https://github.com/benoitc/erlang_h2/security). If you are not
sure which project is affected, report it here.

## What is a security issue

Private reports are for problems that are not a deviation from a
specification the library implements: code execution, file access, one
session's data reaching another session or peer, or the library weakening
TLS or certificate checks.

## What is a bug

When the fix is to make the library follow a specification it implements
(the WebTransport drafts for HTTP/3 and HTTP/2, RFC 9297, extended CONNECT),
it is a bug, even when described as denial of service, resource exhaustion
or desync. Report it as a public bug and cite the specification section; see
[Security issues and specification bugs](CONTRIBUTING.md#security-issues-and-specification-bugs).
We don't publish advisories or request CVEs for these, and advisories filed
for them are closed as bugs with a link to the fix.

Write the report in plain English, with a minimal reproduction, as described
in [CONTRIBUTING.md](CONTRIBUTING.md#write-for-the-person-who-reads-it).

## Reporting a Vulnerability

Please report suspected vulnerabilities privately through GitHub's
[Private Vulnerability Reporting](https://github.com/benoitc/erlang-webtransport/security/advisories/new).

Do not open public issues or pull requests for security problems. Spec
deviations are bugs and go in public issues, as above.

When reporting, include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept.
- Affected versions and platform details.
- Any suggested fix or mitigation.

You can expect an initial acknowledgement within 7 days. We will work with
you on a fix and coordinate disclosure once a patched release is available.
Credit will be given in the advisory unless you prefer to remain anonymous.
