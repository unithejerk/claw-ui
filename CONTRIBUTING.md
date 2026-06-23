# Contributing

Thanks for contributing to the OpenClaw Pipe for Open WebUI.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install pydantic websockets pytest pytest-asyncio

# Optional: OpenTelemetry
.venv/bin/pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp
```

## Testing

Run from the **repo root** (the directory containing `valves.py`):

```bash
# Full suite — run before opening a PR
.venv/bin/python -m pytest tests/ -v

# Single file during development
.venv/bin/python -m pytest tests/test_valves.py -v

# Single test
.venv/bin/python -m pytest tests/test_valves.py -k test_defaults -v
```

Tests use `asyncio_mode = auto` (configured in `pytest.ini`). No mocking
framework — tests use fakes defined inline.

Add or update tests for any behaviour change.

## Code style

- Python 3.11+ (`from __future__ import annotations`, `str | None` syntax)
- Stdlib-first — only `pydantic` and `websockets` are required deps
- Absolute imports (`from valves import Valves`, not relative), because the
  Pipe runs flat inside Open WebUI (no package hierarchy)
- Pydantic for config validation, dataclasses for protocol frames
- Async throughout — `asyncio.Lock`, `asyncio.Queue`, `asyncio.create_task`
- Docstrings on all public methods (Google-style sections in RST)
- 88-char line width, 4-space indent
- No `delta.tool_calls` in SSE chunks — use `<details type="tool_calls">`
  HTML blocks instead

## Commit style

Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
Commits are linted in CI via commitlint.

Format: `type(scope): summary` where *type* is one of `feat`, `fix`,
`docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`,
`revert`. Scopes are optional but encouraged (e.g. `gateway_client`,
`install`, `logging`).

- Summary ≤72 characters, imperative mood, lowercase
- Reference issues when applicable: `fix(gateway_client): close race in reconnect loop (#42)`
- Body begins one blank line after the summary; explain *why*, not *what*
- Breaking changes: append `!` before the colon (`feat!:`), or add a
  `BREAKING CHANGE:` footer

## PR checklist

- [ ] Tests pass (`pytest tests/ -v`)
- [ ] New behaviour is tested
- [ ] Docstrings are updated if the public API changed
- [ ] Bundle verifies (`python3 install.py --check`)
