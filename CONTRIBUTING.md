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

- Imperative, lowercase, ≤72 character subject line
- Reference issue numbers when applicable
- Body should explain *why*, not *what*

## PR checklist

- [ ] Tests pass (`pytest tests/ -v`)
- [ ] New behaviour is tested
- [ ] Docstrings are updated if the public API changed
- [ ] Bundle verifies (`python3 install.py --check`)
