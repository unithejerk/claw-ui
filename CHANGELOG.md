# Changelog

All notable changes to this project follow [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[Semantic Versioning](https://semver.org/spec/v2.0.0.html), and
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## [Unreleased]

## [0.1.0] — 2026-06-22

Initial release of the OpenClaw Pipe for Open WebUI.

### Features
- Pipe Function that exposes OpenClaw agents as selectable models in Open WebUI
- Gateway WebSocket RPC protocol v4 client with lazy connect, request/response
  correlation, and exponential-backoff reconnection
- Streaming agent output token-by-token as SSE deltas with thinking/reasoning
  status events
- Tool calls rendered as expandable `<details>` HTML cards (avoids OWUI's
  `delta.tool_calls` retry-loop)
- Four approval modes: `auto_deny` (safe default), `auto_approve`,
  `render` (card with timeout), `interactive` (browser confirmation dialog
  via `__event_call__`)
- Agent discovery: `__auto__` via Gateway RPC or static `AGENT_LIST`;
  eager background discovery from model selector
- Session persistence scoped to user × chat × agent
- File attachment forwarding to Gateway agents
- Gateway tool definition passthrough
- User-stop mid-stream sends abort to Gateway
- OpenTelemetry traces, metrics, and logs — piggybacks on OWUI's native OTel
  (`ENABLE_OTEL=true`); degrades to no-ops when disabled
- Direct installer (`install.py`) that pushes to OWUI via REST API;
  single-file bundle generation for manual paste
- Diagnostic script (`debug_events.py`) for raw Gateway frame inspection
- 12-valve Pydantic config schema with live reload

### Bug Fixes
- Reconnect self-destruct: successful reconnect no longer closes its own
  newly established WebSocket
- Non-streaming mode: HTML strings from tool calls and approvals no longer
  crash with `AttributeError`
- Agent-run failures: pipe metrics now distinguish agent errors from
  transport errors; error-text still surfaced to user
- Disconnect conflation: synthetic local terminal events not misclassified
  as Gateway agent-run errors
- Gateway status: receiving any terminal Gateway response sets
  `_gateway_status` to `"connected"` (Gateway responded), even when the
  run failed, preventing stale transport-error state from persisting
- Direct install: re-running against an already-enabled function no longer
  toggles it off (OWUI `/toggle` flips `is_active`)
- CLI validation: partial `--owui-url`/`--owui-key` args now error instead
  of silently falling back to bundle mode
- `APPROVAL_MODE` and `MESSAGE_MODE` now use `Literal` types — Pydantic
  catches typos at config load
- Approval handling: unknown `APPROVAL_MODE` now auto-denies with a warning
  instead of silently hanging
- Per-run subscriber queues signaled on disconnect and `close()` so
  consumers don't hang until timeout
- Background fire-and-forget tasks use `_safe_task` with structured error
  logging
- Gateway URL sanitized in user-facing error messages
- Gateway token moved from CLI arg to `GATEWAY_TOKEN` env var in debug script
- Unused imports removed, stale docs corrected

### Security
- `SECURITY.md` with private vulnerability reporting via GitHub Security
  Advisories
- `CODEOWNERS` set to `@unithejerk`
- `CODE_OF_CONDUCT.md` adopted (Contributor Covenant 3.0)

### Documentation
- `CONTRIBUTING.md` with setup, testing, code style, and conventional
  commit guidelines
- `AGENTS.md` for AI-assisted development
- `README.md` with badges, quickstart, feature tables, mermaid architecture
  diagram, limitations, and future work
- `docs/architecture.md`, `docs/configuration.md`,
  `docs/protocol-mapping.md`, `docs/integration-report.md`,
  `docs/agent-install.md`
- Issue template chooser with bug report and feature request templates
- Pull request template

### CI/CD
- CI workflow: test matrix across Python 3.11/3.12/3.13, bundle-check,
  and commitlint conventional-commit enforcement
- Release workflow with SemVer tag validation, CI gating, git-cliff
  changelog generation, and bundle attachment
- Branch protection: require PR with approval, all CI checks, and signed
  commits for `main`
- GitHub issue labels configured (`bug`, `enhancement`, `documentation`,
  `good first issue`, `help wanted`, `question`)
- `requirements.txt` for pip caching in CI
- `cliff.toml` for git-cliff changelog generation

### Miscellaneous
- Standalone OpenTelemetry path removed — Pipe always runs inside OWUI,
  so instance-wide OTel is the correct layer
- Structured event identifiers (`event`, `run_id`, `agent_id`, `attempt`,
  `error_type`, etc.) added to all operational log entries via `extra=`
- `Iterator` and `SpanKind` unused imports removed
- `import base64` moved to module level
- Hardcoded test count and stale paths purged from docs

### Tests
- 110 tests (up from 78 initial): event mapper, gateway client, pipe
  helpers, protocol, valves, install, and regression coverage for all
  fixed bugs

[0.1.0]: https://github.com/unithejerk/claw-ui/releases/tag/v0.1.0
