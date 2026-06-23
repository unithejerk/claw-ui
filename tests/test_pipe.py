"""Tests for the Pipe class — regressions for the non-stream span leak (#1),
static-list validation + pre-populated cache (#5), and the status-placeholder
model (#6)."""
import asyncio

import pytest

import openclaw_pipe
import telemetry
from openclaw_pipe import Pipe


# ── Helpers ────────────────────────────────────────────────────────────────

def _fake_getter(client):
    """Return an async function that returns *client*, for monkey-patching
    the now-async ``Pipe._get_client`` in tests."""
    async def _get():
        return client
    return _get


# ── Fakes ──────────────────────────────────────────────────────────────────

class SimpleFakeClient:
    """Minimal client whose agent_stream yields canned events, so pipe() can
    run end-to-end without a Gateway."""

    async def connect(self):
        pass

    async def agent_stream(self, agent_id, messages, session_key=None, **kw):
        yield {"kind": "delta", "delta": {"content": "hello world"}}
        yield {"kind": "final", "status": "ok"}

    async def abort_agent(self, *a, **k):
        pass


class RecordingSpan:
    def __init__(self):
        self.ended = False
        self.attrs = {}
        self.events = []

    def set_attribute(self, k, v): self.attrs[k] = v
    def set_attributes(self, a): self.attrs.update(a)
    def set_status(self, s): pass
    def add_event(self, n, a=None): self.events.append(n)
    def record_exception(self, exc, attributes=None, **kw): pass
    def get_span_context(self): return None
    def end(self, t=None): self.ended = True
    def is_recording(self): return True


class RecordingTracer:
    def __init__(self):
        self.spans = []

    def start_span(self, name, context=None, kind=None, attributes=None,
                   links=None, start_time=None):
        s = RecordingSpan()
        if attributes:
            s.attrs.update(attributes)
        self.spans.append(s)
        return s

    def get_current_span(self):
        return RecordingSpan()


def _make_pipe(agent_list="__auto__", prefix="OpenClaw/"):
    p = Pipe()
    p.valves = p.Valves(AGENT_LIST=agent_list, AGENT_PREFIX=prefix)
    # Re-run the static pre-population that __init__ did with the OLD valves.
    if p._is_static_agent_list():
        p._agent_cache = p._build_static_models()
    return p


# ── #6: status placeholder is not runnable ─────────────────────────────────

async def test_status_placeholder_rejected_non_stream():
    p = _make_pipe()
    p._get_client = _fake_getter(SimpleFakeClient())
    result = await p.pipe({"model": "__openclaw_status__", "stream": False})
    assert "status indicator" in result
    assert "not a runnable model" in result


async def test_status_placeholder_rejected_stream():
    p = _make_pipe()
    p._get_client = _fake_getter(SimpleFakeClient())
    gen = await p.pipe({"model": "__openclaw_status__", "stream": True})
    chunks = [c async for c in gen]
    assert len(chunks) == 1
    assert "status indicator" in chunks[0]["choices"][0]["delta"]["content"]


def test_status_placeholder_listed_as_indicator():
    # Auto mode, pre-discovery: the connecting placeholder is shown.
    p = _make_pipe()
    ids = [m["id"] for m in p.pipes()]
    assert "__openclaw_status__" in ids


# ── #5: static list validation + pre-populated cache ───────────────────────

def test_static_list_pre_populates_cache_in_init():
    # A freshly built static Pipe has the configured models cached already.
    p = Pipe()
    p.valves = p.Valves(AGENT_LIST="default,coding,research")
    # Mimic __init__'s static branch (valves were changed after construction).
    if p._is_static_agent_list():
        p._agent_cache = p._build_static_models()
    assert p._agent_cache is not None
    assert [m["id"] for m in p._agent_cache] == [
        "openclaw/default", "openclaw/coding", "openclaw/research"
    ]


def test_static_pipes_shows_configured_models_no_connecting_placeholder():
    p = _make_pipe(agent_list="default,coding")
    ids = [m["id"] for m in p.pipes()]
    assert ids == ["openclaw/default", "openclaw/coding"]
    # No spurious "⏳ Connecting..." placeholder in static mode.
    assert "__openclaw_status__" not in ids


async def test_static_validation_rejects_unknown_agent():
    """Regression for #5: static mode now validates model IDs (previously it
    forwarded any openclaw/<x> to the Gateway unvalidated)."""
    p = _make_pipe(agent_list="default,coding")
    p._get_client = _fake_getter(SimpleFakeClient())
    result = await p.pipe({"model": "openclaw/typo", "stream": False})
    assert "Unknown agent 'openclaw/typo'" in result
    assert "openclaw/coding" in result  # available list mentioned


async def test_static_validation_accepts_known_agent():
    p = _make_pipe(agent_list="default,coding")
    p._get_client = _fake_getter(SimpleFakeClient())
    result = await p.pipe({"model": "openclaw/coding", "stream": False})
    assert result == "hello world"


# ── #1: non-stream success ends the root span ──────────────────────────────

async def test_nonstream_success_ends_root_span():
    """Regression for #1: the non-stream success path previously returned
    without calling span.end(), so the root span was never exported."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(SimpleFakeClient())

    tracer = RecordingTracer()
    telemetry._tracer = tracer  # inject a recording tracer
    try:
        result = await p.pipe({"model": "openclaw/default", "stream": False})
    finally:
        # Restore the no-op tracer so other tests are unaffected.
        telemetry._tracer = telemetry._NOOP_TRACER

    assert result == "hello world"
    root_spans = [s for s in tracer.spans]  # the root openclaw.pipe span
    assert root_spans, "expected a root span to be started"
    assert root_spans[0].ended is True, "root span must be ended on non-stream success"


async def test_nonstream_error_also_ends_root_span():
    p = _make_pipe(agent_list="default")

    class ExplodingClient(SimpleFakeClient):
        async def agent_stream(self, *a, **kw):
            raise RuntimeError("gateway blew up")
            yield  # pragma: no cover

    p._get_client = _fake_getter(ExplodingClient())
    tracer = RecordingTracer()
    telemetry._tracer = tracer
    try:
        await p.pipe({"model": "openclaw/default", "stream": False})
    finally:
        telemetry._tracer = telemetry._NOOP_TRACER
    assert tracer.spans[0].ended is True


# ── _launch_eager_discovery ────────────────────────────────────────────────

def test_launch_eager_discovery_idempotent():
    p = Pipe()
    p.valves = p.Valves(AGENT_LIST="__auto__", AGENT_PREFIX="OpenClaw/")
    assert p._discovery_launched is False

    p._launch_eager_discovery()
    assert p._discovery_launched is True

    # Second call must not crash and must stay True.
    p._launch_eager_discovery()
    assert p._discovery_launched is True


async def test_eager_discovery_triggered_by_pipes():
    p = Pipe()
    p.valves = p.Valves(AGENT_LIST="__auto__", AGENT_PREFIX="OpenClaw/")
    assert p._discovery_launched is False

    # pipes() triggers _launch_eager_discovery when cache is None
    _ = p.pipes()
    assert p._discovery_launched is True
    await asyncio.sleep(0)  # let the background task schedule