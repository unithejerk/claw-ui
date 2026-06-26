"""Tests for gateway_client.py — frame routing, the agent_stream consumer
loop, approval handling, and regressions for the task-leak (#2) and
cross-wired-approval (#3) bugs."""
import asyncio

import pytest

from gateway_client import GatewayClient, GatewayRPCError
from protocol import EventFrame


# ── Fakes ──────────────────────────────────────────────────────────────────

class RoutingClient(GatewayClient):
    """Client whose connect/request are stubbed so _route_event/_route_response
    can be driven directly without a real WebSocket."""

    def __init__(self):
        super().__init__("ws://x", "tok", request_timeout=2.0)


class _Resp:
    def __init__(self, rid, ok, payload=None, error=None):
        self.id = rid
        self.ok = ok
        self.payload = payload
        self.error = error


class FeedClient(GatewayClient):
    """Client that returns a canned ack and lets the test feed the run queue
    directly, exercising the real agent_stream consumer loop."""

    def __init__(self):
        super().__init__("ws://x", "tok", request_timeout=2.0)
        self._connected = True
        self.approvals = []
        self.agent_request_params = None  # params the real agent_stream built

    async def connect(self):
        pass

    async def request(self, method, params=None, *, idempotent=False):
        # Capture the params the real GatewayClient.agent_stream constructs so
        # tests can assert they conform to AgentParamsSchema (not just that
        # build_request formats them).
        if method == "agent":
            self.agent_request_params = dict(params or {})
        return {"status": "accepted", "runId": "run-1"}

    async def resolve_approval(self, approval_id, approved, *, kind="exec"):
        self.approvals.append((approval_id, approved, kind))


# ── Routing: agent deltas routed per-run (#3 baseline) ────────────────────

def test_route_event_agent_delta_routed_by_runid():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_event(EventFrame(event="agent", payload={
        "runId": "run-1", "stream": "assistant", "data": {"delta": "hi"},
    }))
    ev = q.get_nowait()
    assert ev["kind"] == "delta"
    assert ev["runId"] == "run-1"
    assert ev["stream"] == "assistant"
    assert ev["data"]["delta"] == "hi"


def test_route_event_unknown_runid_dropped():
    c = RoutingClient()
    c._route_event(EventFrame(event="agent", payload={
        "runId": "nope", "stream": "assistant", "data": {},
    }))
    assert c._run_subscribers == {}  # nothing registered, no error


# ── Routing: approvals routed per-run, no cross-wire (#3 regression) ───────
# Agent tool approvals travel in the agent event stream as stream=="approval"
# with data.phase=="requested"; _route_event tags them _event_type="approval".

def _approval_payload(run_id, *, approval_id="ap-1", title="web_search"):
    return {
        "runId": run_id, "stream": "approval",
        "data": {"phase": "requested", "status": "pending", "title": title,
                 "kind": "exec", "approvalId": approval_id},
    }


def test_route_event_approval_tagged_and_per_run():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_event(EventFrame(event="agent", payload=_approval_payload("run-1")))
    ev = q.get_nowait()
    assert ev["_event_type"] == "approval"
    assert ev["kind"] == "delta"
    assert ev["runId"] == "run-1"
    assert ev["stream"] == "approval"
    assert ev["data"]["approvalId"] == "ap-1"


def test_route_event_approval_does_not_cross_wire_to_other_run():
    """Regression for #3: an approval for run-A must NOT land in run-B's queue."""
    c = RoutingClient()
    q_a = asyncio.Queue()
    q_b = asyncio.Queue()
    c._run_subscribers["run-A"] = q_a
    c._run_subscribers["run-B"] = q_b
    c._route_event(EventFrame(event="agent", payload=_approval_payload("run-A", title="t")))
    assert q_b.empty()
    assert not q_a.empty()  # A's queue got it


def test_route_event_approval_for_unknown_run_dropped():
    c = RoutingClient()
    c._route_event(EventFrame(event="agent", payload=_approval_payload("ghost")))
    assert c._run_subscribers == {}


# ── Routing: final result via _route_response ─────────────────────────────

def test_route_response_final_ok_routed_to_run_queue():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_response(_Resp("req-1", True, payload={"status": "ok", "runId": "run-1", "summary": "done"}))
    ev = q.get_nowait()
    assert ev["kind"] == "final"
    assert ev["status"] == "ok"


def test_route_response_final_error_routed_to_run_queue():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_response(_Resp("req-1", False, payload={"runId": "run-1"}, error={"message": "boom"}))
    ev = q.get_nowait()
    assert ev["kind"] == "final"
    assert ev["status"] == "error"
    assert ev["error"] == "boom"


# ── agent_stream consumer loop ─────────────────────────────────────────────

async def _feed(c, run_id, events, delay=0.0):
    """Wait for the run's queue to exist, then push events into it."""
    while run_id not in c._run_subscribers:
        await asyncio.sleep(0)
    q = c._run_subscribers[run_id]
    for ev in events:
        await q.put(ev)
        if delay:
            await asyncio.sleep(delay)


async def test_agent_stream_yields_deltas_and_final():
    c = FeedClient()
    feeder = asyncio.create_task(_feed(c, "run-1", [
        {"kind": "delta", "stream": "assistant", "data": {"delta": "Hel"}},
        {"kind": "delta", "stream": "assistant", "data": {"delta": "lo"}},
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", "hi", "sess"):
        out.append(ev.get("kind"))
    await feeder
    assert out == ["delta", "delta", "final"]
    assert "run-1" not in c._run_subscribers  # cleaned up


async def test_agent_stream_builds_v4_schema_valid_params():
    """The real GatewayClient.agent_stream must build params that conform to
    AgentParamsSchema: a single `message` string, `extraSystemPrompt`/
    `attachments` (not systemPrompt/files), and no OAI model params or tools.
    The idempotencyKey is added by build_request (tested in conformance)."""
    from openclaw_pipe import _extract_agent_message, _extract_system_prompt

    c = FeedClient()
    body = {
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "Hello"},
        ],
        "temperature": 0.7,  # must NOT be forwarded
    }
    feeder = asyncio.create_task(_feed(c, "run-1", [
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    async for _ in c.agent_stream("default", _extract_agent_message(body),
                                   "sess",
                                   extra_system_prompt=_extract_system_prompt(body)):
        pass
    await feeder

    assert c.agent_request_params is not None
    p = c.agent_request_params
    assert p.get("agentId") == "default"
    assert p.get("message") == "Hello"          # single string, not messages[]
    assert p.get("extraSystemPrompt") == "be brief"  # not systemPrompt
    assert p.get("sessionKey") == "sess"
    # Forbidden legacy / OAI fields must not be forwarded.
    for forbidden in ("messages", "systemPrompt", "files", "tools",
                      "temperature", "top_p", "max_tokens", "reasoning_effort",
                      "response_format"):
        assert forbidden not in p, f"{forbidden} must not be forwarded to agent RPC"


async def test_agent_stream_timeout_raises_rpc_error():
    c = FeedClient()
    c._request_timeout = 0.05  # very short

    async def never_feed():
        await asyncio.sleep(10)

    feeder = asyncio.create_task(never_feed())
    with pytest.raises(GatewayRPCError):
        async for _ in c.agent_stream("default", "hi", "sess"):
            pass
    feeder.cancel()
    assert "run-1" not in c._run_subscribers  # cleaned up even on timeout


# ── Approval handling end-to-end ───────────────────────────────────────────

def _approval_queue_event(run_id, *, approval_id="ap-1", title="web_search"):
    """An approval-requested event as it reaches the agent_stream consumer
    queue (tagged _event_type="approval" by _route_event)."""
    return {
        "kind": "delta", "_event_type": "approval", "runId": run_id,
        "stream": "approval",
        "data": {"phase": "requested", "status": "pending", "title": title,
                 "kind": "exec", "approvalId": approval_id},
    }


async def test_approval_auto_deny_yields_denial_and_resolves_false():
    c = FeedClient()
    feeder = asyncio.create_task(_feed(c, "run-1", [
        _approval_queue_event("run-1"),
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", "hi", "sess",
                                    approval_mode="auto_deny", approval_timeout=1):
        out.append(ev)
    await feeder
    await asyncio.sleep(0.05)  # let fire-and-forget resolve_approval drain
    # auto_deny resolves the approval as denied (decision="deny").
    assert ("ap-1", False, "exec") in c.approvals
    # The auto_deny branch yields a resolved/denied approval delta, then final.
    assert any(ev.get("stream") == "approval" and ev.get("data", {}).get("status") == "denied"
               for ev in out)


async def test_approval_auto_approve_resolves_true_and_yields_nothing():
    c = FeedClient()
    feeder = asyncio.create_task(_feed(c, "run-1", [
        _approval_queue_event("run-1", title="t"),
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", "hi", "sess",
                                    approval_mode="auto_approve", approval_timeout=1):
        out.append(ev)
    await feeder
    await asyncio.sleep(0.05)
    # auto_approve resolves as approved (decision="allow-once"), yields nothing.
    assert ("ap-1", True, "exec") in c.approvals
    assert len(out) == 1 and out[0].get("kind") == "final"


# ── Task-leak regression (#2) ──────────────────────────────────────────────

async def test_agent_stream_does_not_leak_tasks_across_many_events():
    """Regression for #2: the old asyncio.wait(FIRST_COMPLETED) loop leaked one
    uncancelled queue.get() task per event.  The single-queue wait_for design
    must not accumulate lingering tasks over a long stream."""
    c = FeedClient()
    c._request_timeout = 5.0

    n_events = 2000
    events = [{"kind": "delta", "stream": "assistant", "data": {"delta": "x"}}
              for _ in range(n_events)]
    events.append({"kind": "final", "status": "ok", "runId": "run-1"})

    feeder = asyncio.create_task(_feed(c, "run-1", events))

    # Baseline task count once feeder is scheduled but before consuming.
    await asyncio.sleep(0)  # let feeder register the queue
    baseline = len(asyncio.all_tasks())

    count = 0
    async for _ in c.agent_stream("default", "hi", "sess"):
        count += 1
    await feeder
    await asyncio.sleep(0)  # let any done callbacks settle

    after = len(asyncio.all_tasks())
    assert count == n_events + 1
    # No accumulation of leaked getter tasks.  Allow a tiny slack for the
    # event loop's own bookkeeping; the old code would leak ~n_events tasks.
    assert after - baseline < 5, f"task leak detected: {after - baseline} extra tasks"


# ── _fail_run_subscribers ──────────────────────────────────────────────────

def test_fail_run_subscribers_pushes_final_error_to_queue():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q

    c._fail_run_subscribers("connection lost")

    ev = q.get_nowait()
    assert ev["kind"] == "final"
    assert ev["status"] == "error"
    assert ev["error"] == "connection lost"
    assert ev.get("_local") is True, "synthetic events must carry _local marker"


# ── Issue 2: synthetic disconnect events not misclassified ───────────────────


async def test_synthetic_local_events_not_agent_error():
    """_AgentRunStream must NOT set agent_error for synthetic local
    terminal events from _fail_run_subscribers (disconnect path)."""
    from openclaw_pipe import _AgentRunStream

    async def fake_raw():
        yield {"kind": "delta", "delta": {"content": "hi"}}
        yield {"kind": "final", "status": "error", "error": "conn lost",
               "_local": True}

    stream = _AgentRunStream(fake_raw())
    chunks = [c async for c in stream]
    assert len(chunks) == 2
    assert stream.agent_error is False, (
        "synthetic local disconnect event must not set agent_error"
    )


async def test_real_gateway_error_still_sets_agent_error():
    """_AgentRunStream MUST set agent_error for real Gateway terminal
    errors (no _local marker)."""
    from openclaw_pipe import _AgentRunStream

    async def fake_raw():
        yield {"kind": "delta", "delta": {"content": "trying"}}
        yield {"kind": "final", "status": "error", "error": "tool failed"}

    stream = _AgentRunStream(fake_raw())
    chunks = [c async for c in stream]
    assert len(chunks) == 2
    assert stream.agent_error is True, (
        "real Gateway terminal error must set agent_error"
    )


# ── _safe_task ─────────────────────────────────────────────────────────────

async def test_safe_task_catches_exception():
    c = RoutingClient()

    async def _will_raise():
        raise ValueError("test error from safe_task")

    task = c._safe_task(_will_raise(), label="test_task")
    await asyncio.sleep(0.05)

    # The exception was caught by _safe_task's done callback and logged,
    # not propagated to the test.
    assert task.done()


# ── _handle_approval unknown mode ──────────────────────────────────────────

async def test_handle_approval_unknown_mode_auto_denies():
    c = RoutingClient()

    approvals = []

    async def fake_resolve(approval_id, approved, *, kind="exec"):
        approvals.append((approval_id, approved, kind))

    c.resolve_approval = fake_resolve

    class _FakeSpan:
        last_event: tuple | None = None

        def add_event(self, name, attributes=None):
            self.last_event = (name, attributes)

    span = _FakeSpan()

    event = {
        "runId": "run-1",
        "stream": "approval",
        "data": {"phase": "requested", "status": "pending", "title": "web_search",
                 "kind": "exec", "approvalId": "ap-1"},
    }

    results = []
    async for item in c._handle_approval(event, "run-1", "bogus", 30, span):
        results.append(item)

    assert len(results) == 1
    assert results[0]["kind"] == "delta"
    assert results[0]["stream"] == "approval"
    assert results[0]["data"]["status"] == "denied"
    assert "bogus" in results[0]["data"]["reason"]

    # Let the fire-and-forget _safe_task complete
    await asyncio.sleep(0.05)
    assert ("ap-1", False, "exec") in approvals


# ── Bounded queue (QueueFull) ──────────────────────────────────────────────

def test_route_event_queue_full_agent_delta_handled_gracefully():
    c = RoutingClient()
    q = asyncio.Queue(maxsize=1)
    q.put_nowait({"blocking": True})  # fill the queue
    c._run_subscribers["run-1"] = q

    # Must not crash when the queue is full — logs a warning instead.
    c._route_event(EventFrame(
        event="agent",
        payload={"runId": "run-1", "stream": "assistant", "data": {"delta": "hello"}},
    ))

    assert q.qsize() == 1  # original item unchanged


def test_route_event_queue_full_approval_handled_gracefully():
    c = RoutingClient()
    q = asyncio.Queue(maxsize=1)
    q.put_nowait({"blocking": True})  # fill the queue
    c._run_subscribers["run-1"] = q

    c._route_event(EventFrame(
        event="agent",
        payload=_approval_payload("run-1"),
    ))

    assert q.qsize() == 1


# ── Issue 1: successful reconnect survives inner finally ─────────────────────


async def test_reconnect_successful_websocket_survives_finally():
    """Regression for Issue 1: a successful reconnect must not have its
    newly established websocket closed by the inner ``finally`` block."""
    c = RoutingClient()
    c._connected = False
    c._max_reconnect = 1
    c._base_delay = 0.01

    class FakeWS:
        async def close(self):
            pass
        async def recv(self):
            pass

    async def fake_handshake():
        pass

    async def fake_connect(*a, **kw):
        return FakeWS()

    async def fake_reader_loop():
        pass

    with _monkeypatch(c, "_handshake", fake_handshake), \
         _monkeypatch(c, "_reader_loop", fake_reader_loop):
        import gateway_client as _gcm
        from unittest import mock as _m
        with _m.patch.object(_gcm.websockets, "connect",
                             side_effect=fake_connect):
            await c._handle_disconnect()

    # After a successful reconnect, self._ws must still be set
    # (the inner finally must NOT have nulled it).
    assert c._ws is not None, (
        "self._ws is None after a successful reconnect — "
        "the inner finally block nuked it"
    )
    assert c._connected is True
    assert c._reader_task is not None


# ── Helpers ──────────────────────────────────────────────────────────────────

from contextlib import contextmanager


@contextmanager
def _monkeypatch(obj, attr_name, replacement):
    """Temporarily patch *obj.attr_name* with *replacement*."""
    original = getattr(obj, attr_name, None)
    setattr(obj, attr_name, replacement)
    try:
        yield
    finally:
        if original is None:
            delattr(obj, attr_name)
        else:
            setattr(obj, attr_name, original)