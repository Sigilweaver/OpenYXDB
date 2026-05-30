# Security policy

## Supported versions

Only the latest minor release of OpenYXDB is supported with
security fixes.

| Version | Supported |
| ------- | --------- |
| 1.3.x   | Yes       |
| < 1.3   | No        |

## Reporting a vulnerability

Please report security vulnerabilities privately via
[GitHub Security Advisories](https://github.com/Sigilweaver/OpenYXDB/security/advisories/new).

Do **not** open a public issue for security reports. We will
acknowledge within 7 days and aim to publish a fix or mitigation
within 30 days for confirmed issues.

## Scope

In scope:

- Memory-safety bugs in the C++ core or the Python bindings.
- Decompression bombs, path traversal, or arbitrary file write
  triggered by a malformed `.yxdb` file.
- Crashes that can be triggered by an attacker-supplied file.
- Supply-chain integrity issues affecting published wheels.

Out of scope:

- Incorrect parsing of malformed-but-non-crashing files. Open a
  normal issue with a reproducer.
- Vulnerabilities in third-party libraries: please report those
  upstream.

## Disclosure

Coordinated disclosure is preferred. Once a fix is released, the
advisory will be made public and credited to the reporter unless
they request anonymity.
