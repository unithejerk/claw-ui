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

    async def connect(self):
        pass

    async def request(self, method, params=None, *, idempotent=False):
        return {"status": "accepted", "runId": "run-1"}

    async def resolve_approval(self, run_id, approved):
        self.approvals.append((run_id, approved))


# ── Routing: agent deltas routed per-run (#3 baseline) ────────────────────

def test_route_event_agent_delta_routed_by_runid():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_event(EventFrame(event="agent", payload={"runId": "run-1", "delta": {"content": "hi"}}))
    ev = q.get_nowait()
    assert ev["kind"] == "delta"
    assert ev["runId"] == "run-1"
    assert ev["delta"]["content"] == "hi"


def test_route_event_unknown_runid_dropped():
    c = RoutingClient()
    c._route_event(EventFrame(event="agent", payload={"runId": "nope", "delta": {}}))
    assert c._run_subscribers == {}  # nothing registered, no error


# ── Routing: approvals routed per-run, no cross-wire (#3 regression) ───────

def test_route_event_approval_tagged_and_per_run():
    c = RoutingClient()
    q = asyncio.Queue()
    c._run_subscribers["run-1"] = q
    c._route_event(EventFrame(event="approval.requested", payload={
        "runId": "run-1", "request": {"toolName": "web_search", "arguments": {"q": "x"}}
    }))
    ev = q.get_nowait()
    assert ev["_event_type"] == "approval"
    assert ev["kind"] == "approval"
    assert ev["runId"] == "run-1"


def test_route_event_approval_does_not_cross_wire_to_other_run():
    """Regression for #3: an approval for run-A must NOT land in run-B's queue."""
    c = RoutingClient()
    q_a = asyncio.Queue()
    q_b = asyncio.Queue()
    c._run_subscribers["run-A"] = q_a
    c._run_subscribers["run-B"] = q_b
    c._route_event(EventFrame(event="approval.requested", payload={
        "runId": "run-A", "request": {"toolName": "t"}
    }))
    assert not q_b.empty() is False  # B's queue is empty
    assert q_b.empty()
    assert not q_a.empty()  # A's queue got it


def test_route_event_approval_for_unknown_run_dropped():
    c = RoutingClient()
    c._route_event(EventFrame(event="approval.requested", payload={"runId": "ghost"}))
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
        {"kind": "delta", "delta": {"content": "Hel"}},
        {"kind": "delta", "delta": {"content": "lo"}},
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", [{"role": "user", "content": "hi"}], "sess"):
        out.append(ev.get("kind"))
    await feeder
    assert out == ["delta", "delta", "final"]
    assert "run-1" not in c._run_subscribers  # cleaned up


async def test_agent_stream_timeout_raises_rpc_error():
    c = FeedClient()
    c._request_timeout = 0.05  # very short

    async def never_feed():
        await asyncio.sleep(10)

    feeder = asyncio.create_task(never_feed())
    with pytest.raises(GatewayRPCError):
        async for _ in c.agent_stream("default", [{"role": "user", "content": "hi"}], "sess"):
            pass
    feeder.cancel()
    assert "run-1" not in c._run_subscribers  # cleaned up even on timeout


# ── Approval handling end-to-end ───────────────────────────────────────────

async def test_approval_auto_deny_yields_denial_and_resolves_false():
    c = FeedClient()
    feeder = asyncio.create_task(_feed(c, "run-1", [
        {"kind": "approval", "_event_type": "approval", "runId": "run-1",
         "request": {"toolName": "web_search", "arguments": {"q": "x"}}},
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", [{"role": "user", "content": "hi"}], "sess",
                                    approval_mode="auto_deny", approval_timeout=1):
        out.append(ev)
    await feeder
    await asyncio.sleep(0.05)  # let fire-and-forget resolve_approval drain
    # The auto_deny branch yields one status delta, then the final.
    assert ("run-1", False) in c.approvals


async def test_approval_auto_approve_resolves_true_and_yields_nothing():
    c = FeedClient()
    feeder = asyncio.create_task(_feed(c, "run-1", [
        {"kind": "approval", "_event_type": "approval", "runId": "run-1",
         "request": {"toolName": "t", "arguments": {}}},
        {"kind": "final", "status": "ok", "runId": "run-1"},
    ]))
    out = []
    async for ev in c.agent_stream("default", [{"role": "user", "content": "hi"}], "sess",
                                    approval_mode="auto_approve", approval_timeout=1):
        out.append(ev)
    await feeder
    await asyncio.sleep(0.05)
    # auto_approve yields nothing for the approval; only the final is yielded.
    assert ("run-1", True) in c.approvals
    assert len(out) == 1 and out[0].get("kind") == "final"


# ── Task-leak regression (#2) ──────────────────────────────────────────────

async def test_agent_stream_does_not_leak_tasks_across_many_events():
    """Regression for #2: the old asyncio.wait(FIRST_COMPLETED) loop leaked one
    uncancelled queue.get() task per event.  The single-queue wait_for design
    must not accumulate lingering tasks over a long stream."""
    c = FeedClient()
    c._request_timeout = 5.0

    n_events = 2000
    events = [{"kind": "delta", "delta": {"content": "x"}} for _ in range(n_events)]
    events.append({"kind": "final", "status": "ok", "runId": "run-1"})

    feeder = asyncio.create_task(_feed(c, "run-1", events))

    # Baseline task count once feeder is scheduled but before consuming.
    await asyncio.sleep(0)  # let feeder register the queue
    baseline = len(asyncio.all_tasks())

    count = 0
    async for _ in c.agent_stream("default", [{"role": "user", "content": "hi"}], "sess"):
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

    async def fake_resolve(run_id, approved):
        approvals.append((run_id, approved))

    c.resolve_approval = fake_resolve

    class _FakeSpan:
        last_event: tuple | None = None

        def add_event(self, name, attributes=None):
            self.last_event = (name, attributes)

    span = _FakeSpan()

    event = {
        "request": {"toolName": "web_search", "arguments": {"q": "x"}},
        "runId": "run-1",
    }

    results = []
    async for item in c._handle_approval(event, "run-1", "bogus", 30, span):
        results.append(item)

    assert len(results) == 1
    assert results[0]["kind"] == "delta"
    assert results[0]["delta"]["approval_denied"] is True
    assert "bogus" in results[0]["delta"]["status"]

    # Let the fire-and-forget _safe_task complete
    await asyncio.sleep(0.05)
    assert ("run-1", False) in approvals


# ── Bounded queue (QueueFull) ──────────────────────────────────────────────

def test_route_event_queue_full_agent_delta_handled_gracefully():
    c = RoutingClient()
    q = asyncio.Queue(maxsize=1)
    q.put_nowait({"blocking": True})  # fill the queue
    c._run_subscribers["run-1"] = q

    # Must not crash when the queue is full — logs a warning instead.
    c._route_event(EventFrame(
        event="agent",
        payload={"runId": "run-1", "delta": {"content": "hello"}},
    ))

    assert q.qsize() == 1  # original item unchanged


def test_route_event_queue_full_approval_handled_gracefully():
    c = RoutingClient()
    q = asyncio.Queue(maxsize=1)
    q.put_nowait({"blocking": True})  # fill the queue
    c._run_subscribers["run-1"] = q

    c._route_event(EventFrame(
        event="approval.requested",
        payload={"runId": "run-1", "request": {"toolName": "web_search"}},
    ))

    assert q.qsize() == 1