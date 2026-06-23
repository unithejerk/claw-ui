# Changelog

All notable changes to this project follow [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), and
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## [Unreleased]

### Features
- Eager agent discovery from model selector via background task

### Bug Fixes
- Reconnect self-destruct: successful reconnect no longer closes its own WebSocket
- Non-streaming mode: HTML strings from tool calls and approvals no longer crash with `AttributeError`
- Agent-run failures: pipe metrics now distinguish agent errors from transport errors
- Disconnect conflation: synthetic local terminal events not misclassified as Gateway agent-run errors
- Direct install: re-running install against an already-enabled function no longer toggles it off
- CLI validation: partial `--owui-url`/`--owui-key` args now error instead of silently falling back to bundle mode
- Gateway status: agent-final errors leave `_gateway_status` as `"connected"` (Gateway responded), not stale from prior transport failure

### CI/CD
- commitlint conventional-commit enforcement added to CI workflow

### Documentation
- Contributor Covenant upgraded from 2.1 to 3.0
- Issue template chooser config with security/docs contact links
- Conventional Commits documented in CONTRIBUTING.md and AGENTS.md

### Security
- SECURITY.md added with private vulnerability reporting path
- Gateway URL sanitized in user-facing error messages
- Gateway token moved from CLI arg to env var in debug script

### Miscellaneous
- Standalone OpenTelemetry path removed (OWUI instance-wide OTel is the correct layer)
- Structured event identifiers added to operational log entries
