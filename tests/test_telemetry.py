"""Tests for telemetry.py.

Two layers:

* **No-op path** (always run, even without opentelemetry installed): the
  Pipe must function with no-op spans/metrics when OTel is unavailable or
  OWUI OTel is off.  These guard the degradation contract.
* **Real path** (skipped unless ``opentelemetry.sdk`` is importable): runs
  in a subprocess with a real TracerProvider + in-memory exporter and
  verifies spans actually nest under ``openclaw.pipe`` (the bug where the
  root span was never made current fragmented the trace) and that
  ``record_exception_on_span`` sets ``StatusCode.ERROR``.
"""
import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import telemetry

REPO_DIR = Path(__file__).resolve().parent.parent


# ── No-op path (degradation contract) ───────────────────────────────────────


def test_noop_is_disabled_when_owui_otel_off():
    # OWUI OTel is off by default in the test process → no-op, even when the
    # opentelemetry package is installed.  is_enabled() must be False.
    assert telemetry.is_enabled() is False


def test_noop_span_and_metrics_do_not_crash():
    tracer = telemetry.get_tracer()
    span = tracer.start_span("openclaw.pipe", attributes={"a": 1})
    assert span.is_recording() is False
    span.set_attribute("x", "y")
    span.add_event("ev", {"k": "v"})
    telemetry.record_exception_on_span(span, ValueError("boom"))  # must not raise
    span.end()
    # All metric instruments must be no-op-safe.
    telemetry.pipe_requests().add(1, {"openclaw.agent.id": "d", "status": "success"})
    telemetry.pipe_duration().record(0.5, {"openclaw.agent.id": "d", "status": "success"})
    telemetry.gateway_connections().add(1)
    telemetry.gateway_connections().add(-1)
    telemetry.gateway_rpc_requests().add(1, {"rpc.method": "agent", "status": "success"})
    telemetry.agent_stream_events().add(1, {"openclaw.agent.id": "d", "openclaw.event.kind": "final"})


def test_use_span_is_passthrough_when_disabled():
    # When telemetry is off, use_span must be a transparent pass-through so
    # the streaming/non-streaming bodies run unchanged.
    span = telemetry.get_tracer().start_span("x")
    with telemetry.use_span(span):
        pass  # no raise
    # Also works as a plain block with real work inside.
    result = []
    with telemetry.use_span(span):
        result.append(1)
    assert result == [1]


# ── Real path (subprocess; skipped without opentelemetry.sdk) ───────────────


def test_real_otel_spans_nest_and_record_exception():
    """With a real TracerProvider + ENABLE_OTEL=true, child spans must nest
    under ``openclaw.pipe`` (root made current via use_span) and
    record_exception_on_span must set StatusCode.ERROR."""
    pytest.importorskip("opentelemetry.sdk")
    pytest.importorskip("opentelemetry.sdk.metrics")

    script = textwrap.dedent(f"""
        import json, os, sys
        sys.path.insert(0, {str(REPO_DIR)!r})
        os.environ["ENABLE_OTEL"] = "true"
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        captured = []
        class _M:
            def export(self, b): captured.extend(list(b)); return None
            def shutdown(self): pass
        tp = TracerProvider(); tp.add_span_processor(SimpleSpanProcessor(_M()))
        trace.set_tracer_provider(tp)
        metrics.set_meter_provider(MeterProvider(metric_readers=[InMemoryMetricReader()]))
        import telemetry
        telemetry.init_telemetry()
        assert telemetry.is_enabled(), "telemetry should be enabled"
        tracer = telemetry.get_tracer()
        root = tracer.start_span("openclaw.pipe", attributes={{"gen_ai.system": "openclaw"}})
        # Match real code order: agent_stream span created before request:agent.
        with telemetry.use_span(root):
            with tracer.start_as_current_span("openclaw.gateway.connect") as c:
                c.set_attribute("openclaw.gateway.url", "ws://x")
            aspan = tracer.start_span("openclaw.gateway.agent_stream")
            with tracer.start_as_current_span("openclaw.gateway.request:agent") as r:
                r.set_attribute("rpc.method", "agent")
            aspan.end()
        root.end(); tp.force_flush()
        by = {{s.name: s for s in captured}}
        rid = root.get_span_context().span_id
        def pid(s): return s.parent.span_id if s.parent else None
        nesting = all(pid(by[n]) == rid for n in
                      ("openclaw.gateway.connect", "openclaw.gateway.request:agent",
                       "openclaw.gateway.agent_stream"))
        # record_exception sets ERROR on a real span.
        s2 = tracer.start_span("errspan"); telemetry.record_exception_on_span(s2, ValueError("boom")); s2.end()
        tp.force_flush()
        errspan = [s for s in captured if s.name == "errspan"][0]
        error_set = errspan.status.is_ok is False
        print(json.dumps({{"enabled": telemetry.is_enabled(), "nesting": nesting,
                           "error_status_set": error_set,
                           "spans": [s.name for s in captured]}}))
    """)
    r = subprocess.run([sys.executable, "-c", script],
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"real-otel subprocess failed:\n{r.stderr}"
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["enabled"] is True
    assert out["nesting"] is True, f"spans did not nest under openclaw.pipe: {out['spans']}"
    assert out["error_status_set"] is True, "record_exception_on_span did not set ERROR"


def test_real_otel_streaming_spans_nest_under_pipe():
    """End-to-end: a streaming pipe() run under a real TracerProvider must
    keep the root ``openclaw.pipe`` span current across ``yield`` boundaries
    (via ``use_span``) so the ``openclaw.gateway.agent_stream`` child span
    created by the real ``GatewayClient.agent_stream`` consumer loop nests
    under it.  This is the streaming-specific guard for the nesting fix."""
    pytest.importorskip("opentelemetry.sdk")
    pytest.importorskip("opentelemetry.sdk.metrics")

    script = textwrap.dedent(f"""
        import json, os, sys, asyncio
        sys.path.insert(0, {str(REPO_DIR)!r})
        os.environ["ENABLE_OTEL"] = "true"
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader
        captured = []
        class _M:
            def export(self, b): captured.extend(list(b)); return None
            def shutdown(self): pass
        tp = TracerProvider(); tp.add_span_processor(SimpleSpanProcessor(_M()))
        trace.set_tracer_provider(tp)
        metrics.set_meter_provider(MeterProvider(metric_readers=[InMemoryMetricReader()]))
        import telemetry; telemetry.init_telemetry()
        from openclaw_pipe import Pipe
        from gateway_client import GatewayClient

        class FakeClient(GatewayClient):
            def __init__(self): super().__init__("ws://x","t",request_timeout=2.0); self._connected=True
            async def connect(self): pass
            async def request(self, method, params=None, *, idempotent=False):
                return {{"status":"accepted","runId":"run-1"}}

        async def feed(c, run_id, events):
            while run_id not in c._run_subscribers: await asyncio.sleep(0)
            q = c._run_subscribers[run_id]
            for e in events: await q.put(e)

        async def main():
            p = Pipe(); p.valves = p.Valves(AGENT_LIST="default")
            if p._is_static_agent_list(): p._agent_cache = p._build_static_models()
            c = FakeClient()
            async def _gc(): return c
            p._get_client = _gc
            feeder = asyncio.create_task(feed(c, "run-1", [
                {{"kind":"delta","stream":"assistant","data":{{"delta":"Hel"}}}},
                {{"kind":"delta","stream":"assistant","data":{{"delta":"lo"}}}},
                {{"kind":"final","status":"ok","runId":"run-1"}},
            ]))
            gen = await p.pipe({{"model":"openclaw/default","stream":True,
                                 "messages":[{{"role":"user","content":"hi"}}]}})
            out = [x async for x in gen]
            await feeder
            return out

        out = asyncio.run(main())
        tp.force_flush()
        by = {{s.name: s for s in captured}}
        pipe_id = by["openclaw.pipe"].context.span_id if "openclaw.pipe" in by else None
        aspan = by.get("openclaw.gateway.agent_stream")
        nested = bool(aspan and aspan.parent and aspan.parent.span_id == pipe_id)
        print(json.dumps({{"chunks": len(out), "spans": [s.name for s in captured],
                           "pipe_present": "openclaw.pipe" in by,
                           "agent_stream_present": aspan is not None,
                           "agent_stream_nests_under_pipe": nested}}))
    """)
    r = subprocess.run([sys.executable, "-c", script],
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, f"streaming otel subprocess failed:\n{r.stderr}"
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["chunks"] == 3, f"expected 3 streamed chunks, got {out['chunks']}"
    assert out["pipe_present"] is True
    assert out["agent_stream_present"] is True
    assert out["agent_stream_nests_under_pipe"] is True, (
        f"agent_stream did not nest under openclaw.pipe: {out['spans']}"
    )