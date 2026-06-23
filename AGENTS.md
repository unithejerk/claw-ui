# AGENTS.md

## Project overview

OpenClaw Pipe for Open WebUI — a Pipe Function that makes OpenClaw agents
available as selectable models in the Open WebUI chat interface.  Speaks
the OpenClaw Gateway WebSocket RPC protocol (v4), not the OpenAI HTTP API.

Runs as an in-process Python plugin inside Open WebUI.  No separate
process, no deployment infra — just paste the code into the Functions
editor, or run `install.py` to push it via OWUI's REST API.

## Setup commands

```bash
# Create venv and install deps
python3 -m venv .venv
.venv/bin/pip install pydantic websockets pytest pytest-asyncio

# Optional: OpenTelemetry
.venv/bin/pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp
```

## Build and bundle

```bash
# Generate single-file bundle (prints to stdout)
.venv/bin/python install.py

# Write to file
.venv/bin/python install.py -o openclaw_pipe_bundle.py

# Push directly to a running OWUI instance
.venv/bin/python install.py \
    --owui-url http://localhost:3000 \
    --owui-key sk-your-admin-api-key \
    --valves '{"GATEWAY_URL":"ws://127.0.0.1:18789","GATEWAY_TOKEN":"..."}'

# Verify bundle parses without installing
.venv/bin/python install.py --check
```

## Testing

```bash
# Run all tests (asyncio auto-mode)
.venv/bin/python -m pytest tests/ -v

# Single file
.venv/bin/python -m pytest tests/test_gateway_client.py -v

# With coverage
.venv/bin/pip install pytest-cov
.venv/bin/python -m pytest tests/ --cov=. --cov-report=term-missing
```

Tests use `asyncio_mode = auto` (configured in `pytest.ini`).  No mocking
framework needed — tests use fakes defined inline.

Always run the full suite before committing.  Add or update tests for any
behavior change.

## Code style

- Python 3.11+ (`from __future__ import annotations`, `str | None` syntax)
- Stdlib-first — only `pydantic` and `websockets` are required deps.
  `opentelemetry-*` packages are optional and guarded by import fallbacks
- Absolute imports within the package: `from valves import Valves`, not
  `from .valves import Valves` (the Pipe runs flat in OWUI, no package
  hierarchy)
- Pydantic for config validation (`valves.py`), dataclasses for protocol
  frames (`protocol.py`)
- Async throughout — `asyncio.Lock` for shared state, `asyncio.Queue` for
  event routing, `asyncio.create_task` for fire-and-forget
- Docstrings on all public methods (Google-style sections in RST)
- 88-char line width, 4-space indent
- No `delta.tool_calls` in SSE chunks — render tool calls as
  `<details type="tool_calls">` HTML blocks instead (otherwise OWUI
  triggers its retry loop)

## Architecture

```
openclaw_pipe.py   →  Pipe.pipes() / Pipe.pipe()  (entry point; pipes() triggers eager agent discovery)
gateway_client.py  →  WebSocket client (connect, RPC, streaming, reconnect)  [989 lines]
protocol.py        →  Frame types, parsers, constructors, auth                [312 lines]
event_mapper.py    →  Gateway events → OWUI SSE chunks                        [249 lines]
valves.py          →  Pydantic config schema                                  [112 lines]
telemetry.py       →  OTel traces + metrics + logs (no-op when disabled)      [365 lines]
install.py         →  Bundler + OWUI REST API installer                       [400 lines]
debug_events.py    →  Diagnostic script for Gateway event inspection          [195 lines]
```

The `install.py` bundler inlines all modules in dependency order (valves →
protocol → telemetry → event_mapper → gateway_client → openclaw_pipe),
stripping docstrings and intra-package imports.  External imports
(`pydantic`, `websockets`, `opentelemetry`) are left intact.

## Key design rules

1. **Never emit `delta.tool_calls`** — OWUI's tool-execution retry loop
   will re-invoke the Pipe up to 256 times.  Use `<details>` HTML blocks.
2. **Token-only auth** — `build_connect()` sends `auth.token`, no device
   keypair.  `sign_challenge()` and `_derive_device_id()` in `protocol.py`
   are retained for a future device-auth path but are not wired in.
3. **Session keys scoped to user × chat × agent** —
   `owui:user:<id>:chat:<id>:agent:<id>`.  Same chat = same session;
   switching agent or opening a new chat = fresh session.
4. **Valves are live-reloaded** — `_get_client()` re-hashes the config
   tuple on every call and recreates the client when values change.
5. **OTel degrades to no-ops** — every span/metric/log call works
   whether or not the OTel SDK is installed.  `telemetry.py` provides
   `_NoOpSpan`, `_NoOpTracer`, `_NoOpMeter`, etc.
6. **Interactive approval uses `__event_call__`** — OWUI's bidirectional
   WebSocket back-channel.  Falls back to auto-deny when unavailable.
7. **Eager agent discovery on first `pipes()` call** — when the model
   selector value is `__auto__`, `pipes()` launches a background task to
   discover agents via the Gateway.  This populates the model selector
   asynchronously so the UI stays responsive.  Falls back to lazy
   discovery on the first `pipe()` call if eager discovery hasn't
   completed or failed.

## File structure note

The repo root is `repo/`.  The docs live in `repo/docs/`.  `install.py`
expects to find all modules in its own directory — don't add subpackages
without updating the bundler's `MODULES` list.

## Commit style

Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
Format: `type(scope): summary`.  Types: `feat`, `fix`, `docs`, `style`,
`refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.  Scopes
are optional but encouraged (e.g. `gateway_client`, `install`,
`logging`).

- Summary ≤72 characters, imperative mood, lowercase
- Reference issues when applicable: `fix: summary of the fix (#2)`
- Body begins one blank line after the summary; explain *why*, not *what*
- Breaking changes: `feat!:` or `feat(scope)!:` prefix, or
  `BREAKING CHANGE:` footer
- Co-authored-by trailers welcome for AI-assisted changes
