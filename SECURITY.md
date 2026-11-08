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

## Reporting a Vulnerability

Please report suspected vulnerabilities privately through GitHub's
[Private Vulnerability Reporting](https://github.com/benoitc/erlang-webtransport/security/advisories/new).

Do not open public issues or pull requests for security problems.

When reporting, include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept.
- Affected versions and platform details.
- Any suggested fix or mitigation.

You can expect an initial acknowledgement within 7 days. We will work with
you on a fix and coordinate disclosure once a patched release is available.
Credit will be given in the advisory unless you prefer to remain anonymous.
