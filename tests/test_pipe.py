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

    async def agent_stream(self, agent_id, message, session_key=None, **kw):
        yield {"kind": "delta", "stream": "assistant", "data": {"delta": "hello world"}}
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


class RecordingCounter:
    """Captures every ``add(amount, attributes)`` call so tests can
    assert that the right metric branch was taken."""

    def __init__(self):
        self.calls: list[tuple[float, dict]] = []

    def add(self, amount: float = 1, attributes: dict | None = None) -> None:
        self.calls.append((amount, attributes or {}))


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


# ── Issue 2: non-streaming string chunks do not crash ────────────────────────


class ToolCallFakeClient(SimpleFakeClient):
    """Yields a tool/item event (which the mapper renders as an HTML
    string) followed by a successful final event."""

    async def agent_stream(self, agent_id, message, session_key=None, **kw):
        yield {"kind": "delta", "stream": "tool", "data": {
            "itemId": "i1", "phase": "start", "kind": "tool",
            "title": "bash", "status": "running", "toolCallId": "c1",
        }}
        yield {"kind": "final", "status": "ok"}


class ApprovalFakeClient(SimpleFakeClient):
    """Yields an approval resolution event (which the mapper renders as an
    HTML string) followed by a successful final event."""

    async def agent_stream(self, agent_id, message, session_key=None, **kw):
        yield {"kind": "delta", "stream": "approval", "data": {
            "phase": "resolved", "status": "denied", "title": "rm",
            "kind": "exec", "reason": "Auto-denied by Pipe",
        }}
        yield {"kind": "final", "status": "ok"}


async def test_nonstream_tool_call_string_no_crash():
    """Regression for Issue 2: HTML tool-call strings from the mapper
    must not crash _nonstream_response with AttributeError."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(ToolCallFakeClient())
    result = await p.pipe({"model": "openclaw/default", "stream": False})
    assert isinstance(result, str)
    assert "Calling: bash" in result


async def test_nonstream_approval_string_no_crash():
    """Regression for Issue 2: HTML approval strings from the mapper
    must not crash _nonstream_response with AttributeError."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(ApprovalFakeClient())
    result = await p.pipe({"model": "openclaw/default", "stream": False})
    assert isinstance(result, str)
    assert "Denied" in result
    assert "Auto-denied" in result


# ── Issue 3: agent-final error outcomes arc counted correctly ─────────────────


class AgentErrorFakeClient(SimpleFakeClient):
    """Yields a delta followed by an agent-level error final event."""

    async def agent_stream(self, agent_id, message, session_key=None, **kw):
        yield {"kind": "delta", "stream": "assistant", "data": {"delta": "trying..."}}
        yield {"kind": "final", "status": "error", "error": "tool execution failed"}


async def test_nonstream_agent_error_returns_error_tuple():
    """The _nonstream_response helper returns (text, True) on agent error."""
    p = _make_pipe(agent_list="default")
    p._client = await _fake_getter(AgentErrorFakeClient())()
    text, agent_error = await p._nonstream_response(
        p._client, "default", {"messages": []}, session_key=None,
    )
    assert "[Error:" in text
    assert agent_error is True


async def test_nonstream_agent_success_returns_success_tuple():
    """The _nonstream_response helper returns (text, False) on success."""
    p = _make_pipe(agent_list="default")
    p._client = await _fake_getter(SimpleFakeClient())()
    text, agent_error = await p._nonstream_response(
        p._client, "default", {"messages": []}, session_key=None,
    )
    assert "hello world" in text
    assert agent_error is False


async def test_nonstream_agent_error_records_error_metrics():
    """Non-streaming pipe() records status=error and sets gateway
    status to 'connected' (the Gateway is reachable — the run just
    failed)."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(AgentErrorFakeClient())

    counter = RecordingCounter()
    telemetry._counter_pipe_requests = counter
    try:
        result = await p.pipe({"model": "openclaw/default", "stream": False})
    finally:
        telemetry._counter_pipe_requests = telemetry._NoOpCounter()

    # Gateway IS reachable — we got a valid terminal response.
    assert p._gateway_status == "connected"
    assert "Agent run failed" in p._gateway_error
    assert "[Error:" in result
    # The error-accounting branch was taken.
    error_calls = [c for c in counter.calls
                   if c[1].get("openclaw.agent.id") == "default"
                   and c[1].get("status") == "error"]
    assert error_calls, "pipe_requests counter must record status=error"


async def test_stream_agent_error_records_error_metrics():
    """Streaming pipe() records status=error and sets gateway status
    to 'connected' (the Gateway is reachable — the run just failed)."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(AgentErrorFakeClient())

    counter = RecordingCounter()
    telemetry._counter_pipe_requests = counter
    try:
        stream = await p.pipe({"model": "openclaw/default", "stream": True})
        chunks = [c async for c in stream]
    finally:
        telemetry._counter_pipe_requests = telemetry._NoOpCounter()

    assert p._gateway_status == "connected"
    assert "Agent run failed" in p._gateway_error
    error_chunks = [c for c in chunks if isinstance(c, dict)
                    and c.get("choices", [{}])[0].get("delta", {}).get("content", "").startswith("\n\n[Error:")]
    assert error_chunks
    error_calls = [c for c in counter.calls
                   if c[1].get("openclaw.agent.id") == "default"
                   and c[1].get("status") == "error"]
    assert error_calls, "pipe_requests counter must record status=error"


async def test_agent_error_resets_stale_gateway_status():
    """A successful Gateway response (even with agent-run error) must
    overwrite a stale _gateway_status from a prior transport failure."""
    p = _make_pipe(agent_list="default")
    p._get_client = _fake_getter(AgentErrorFakeClient())
    # Simulate a prior transport failure leaving stale state.
    p._gateway_status = "error"
    p._gateway_error = "prior transport error"

    result = await p.pipe({"model": "openclaw/default", "stream": False})

    # The stale "error" must be replaced — the Gateway responded.
    assert p._gateway_status == "connected"
    assert p._gateway_error == "Agent run failed"
    assert "[Error:" in result