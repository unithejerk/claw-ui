# Security Policy

## Supported versions

Only the latest release is actively supported with security patches.

## Reporting a vulnerability

**Do not open a public issue.** Instead, report vulnerabilities privately
via GitHub's [Security Advisories](https://github.com/unithejerk/claw-ui/security/advisories/new)
or email `security@<domain>` (replace with actual contact).

You should receive a response within 48 hours. If the issue is confirmed,
we will release a patch as soon as possible depending on complexity.

## Scope

Security reports are welcome for:

- Authentication bypass or token leakage between Gateway and Pipe
- WebSocket message injection or frame smuggling
- Sensitive data exposure in logs, telemetry exports, or debug output
- Denial-of-service vectors (e.g., resource exhaustion from
  unbounded reconnection loops or message queues)

## Out of scope

- Issues that require physical access to the machine running the Gateway
  or Open WebUI
- Social engineering
- Vulnerabilities in third-party dependencies (please report those to
  the upstream project instead)
