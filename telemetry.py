"""
OpenTelemetry instrumentation for the OpenClaw Pipe Function.

Provides tracing spans, metrics counters/histograms/gauges, and
automatic integration with Open WebUI's native OTel setup.

Open WebUI has its own OTel support controlled by the ``ENABLE_OTEL``
environment variable.  When that is ``"true"``, OWUI installs global
``TracerProvider`` and ``MeterProvider`` instances with OTLP exporters.

This module detects that setup and **piggybacks on it** — it never
overwrites OWUI's providers.  It simply calls
``trace.get_tracer(...)`` / ``metrics.get_meter(...)`` and creates
spans and metric instruments that flow through OWUI's existing pipeline.

When OWUI-level OTel is off the module degrades to silent no-ops.
There is no standalone telemetry path — the Pipe is always installed
inside Open WebUI, so instance-wide OTel is the right layer for it.

Reference:
    https://docs.openwebui.com/reference/monitoring/otel/
    https://opentelemetry.io/docs/languages/python/
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

logger = logging.getLogger("openclaw_pipe.telemetry")

# ---------------------------------------------------------------------------
# Try importing OpenTelemetry — fall back to no-ops if unavailable.
# ---------------------------------------------------------------------------

_OTEL_AVAILABLE = False

try:
    from opentelemetry import trace as _trace_api
    from opentelemetry import metrics as _metrics_api
    from opentelemetry.sdk._logs import LoggingHandler as _SDKLoggingHandler
    from opentelemetry.trace import (
        Status,
        StatusCode,
    )

    _OTEL_AVAILABLE = True

except ImportError:  # pragma: no cover
    Status = None  # type: ignore[misc,assignment]


# ---------------------------------------------------------------------------
# No-op implementations — tracing
# ---------------------------------------------------------------------------

class _NoOpSpan:
    """No-op stand-in for an OTel ``Span`` used when telemetry is disabled.

    Every method is a no-op so calling code can use the same span API
    regardless of whether OTel is active.  ``is_recording`` returns
    ``False``, which ``record_exception_on_span`` checks to short-circuit.
    """
    def set_attribute(self, key: str, value: Any) -> None: pass
    def set_attributes(self, attrs: dict[str, Any]) -> None: pass
    def set_status(self, status: Any) -> None: pass
    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None: pass
    def record_exception(self, exception: BaseException, attributes: dict[str, Any] | None = None) -> None: pass
    def get_span_context(self) -> "_NoOpSpanContext": return _NoOpSpanContext()
    def end(self, end_time: int | None = None) -> None: pass
    def is_recording(self) -> bool: return False
    def __enter__(self) -> "_NoOpSpan": return self
    def __exit__(self, *args: Any) -> None: pass

class _NoOpSpanContext:
    """No-op stand-in for an OTel ``SpanContext`` (zeroed trace/span ids)."""
    trace_id: int = 0; span_id: int = 0; trace_flags: int = 0; is_remote: bool = False

class _NoOpTracer:
    """No-op stand-in for an OTel ``Tracer`` used when telemetry is disabled.

    ``start_span``/``start_as_current_span`` return :class:`_NoOpSpan`
    instances, so callers need no OTel awareness.
    """
    def start_span(self, name: str, context: Any = None, kind: Any = None,
                   attributes: dict[str, Any] | None = None, links: list[Any] | None = None,
                   start_time: int | None = None) -> _NoOpSpan:
        return _NoOpSpan()
    @contextmanager
    def start_as_current_span(self, name: str, context: Any = None, kind: Any = None,
                              attributes: dict[str, Any] | None = None,
                              links: list[Any] | None = None, start_time: int | None = None,
                              end_on_exit: bool = True) -> Iterator[_NoOpSpan]:
        yield _NoOpSpan()
    def get_current_span(self) -> _NoOpSpan: return _NoOpSpan()

_NOOP_TRACER = _NoOpTracer()

# ---------------------------------------------------------------------------
# No-op implementations — metrics
# ---------------------------------------------------------------------------

class _NoOpCounter:
    """No-op stand-in for an OTel ``Counter`` (``add`` does nothing)."""
    def add(self, amount: float = 1, attributes: dict[str, Any] | None = None) -> None: pass

class _NoOpHistogram:
    """No-op stand-in for an OTel ``Histogram`` (``record`` does nothing)."""
    def record(self, amount: float, attributes: dict[str, Any] | None = None) -> None: pass

class _NoOpUpDownCounter:
    """No-op stand-in for an OTel ``UpDownCounter`` (``add`` does nothing)."""
    def add(self, amount: float, attributes: dict[str, Any] | None = None) -> None: pass

class _NoOpMeter:
    """No-op stand-in for an OTel ``Meter``; its ``create_*`` factories return
    the no-op instruments above.  Used when telemetry is disabled."""
    def create_counter(self, name: str, unit: str = "1", description: str = "") -> _NoOpCounter:
        return _NoOpCounter()
    def create_histogram(self, name: str, unit: str = "1", description: str = "") -> _NoOpHistogram:
        return _NoOpHistogram()
    def create_up_down_counter(self, name: str, unit: str = "1", description: str = "") -> _NoOpUpDownCounter:
        return _NoOpUpDownCounter()

_NOOP_METER = _NoOpMeter()

# Default instruments — reassigned by init_telemetry() to real impls
_counter_pipe_requests: Any = _NoOpCounter()
_histogram_pipe_duration: Any = _NoOpHistogram()
_updown_gateway_connections: Any = _NoOpUpDownCounter()
_counter_gateway_rpc: Any = _NoOpCounter()
_counter_agent_events: Any = _NoOpCounter()


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_tracer: Any = _NOOP_TRACER
_meter: Any = _NOOP_METER
_log_handler: Any = None  # LoggingHandler attached to openclaw_pipe logger
_initialized: bool = False


# ---------------------------------------------------------------------------
# Public API — lifecycle
# ---------------------------------------------------------------------------

def init_telemetry() -> None:
    """Initialise tracing and metrics, respecting OWUI's existing OTel setup.

    Call once at startup (idempotent — subsequent calls are no-ops).

    If OWUI has ``ENABLE_OTEL=true`` this piggybacks on its global
    ``TracerProvider`` and ``MeterProvider``.  Otherwise telemetry
    degrades to no-ops — there is no standalone path since the Pipe
    always runs inside Open WebUI and instance-wide OTel is the
    right layer.
    """
    global _tracer, _meter, _log_handler, _initialized
    global _counter_pipe_requests, _histogram_pipe_duration
    global _updown_gateway_connections, _counter_gateway_rpc, _counter_agent_events

    if _initialized:
        return
    _initialized = True

    if not _OTEL_AVAILABLE:
        return

    if not _owui_otel_active():
        logger.debug("Telemetry: OWUI OTel off — no-op.")
        return

    _tracer = _trace_api.get_tracer("openclaw-owui-pipe")
    _meter = _metrics_api.get_meter("openclaw-owui-pipe")
    _setup_log_bridge()
    logger.info(
        "Telemetry: piggybacking on Open WebUI OTel "
        "(ENABLE_OTEL=true). Traces + metrics + logs flow "
        "through OWUI's existing pipeline."
    )
    _create_instruments()


def shutdown_telemetry() -> None:
    """Remove the log bridge.  Provider shutdown is OWUI's responsibility."""
    global _initialized, _log_handler
    if not _initialized:
        return
    if _log_handler is not None:
        _pipe_logger().removeHandler(_log_handler)
        _log_handler = None
    _initialized = False


# ---------------------------------------------------------------------------
# Public API — tracing
# ---------------------------------------------------------------------------

def get_tracer() -> Any:
    """Return the current tracer (or a no-op stand-in)."""
    return _tracer


def is_enabled() -> bool:
    """Return ``True`` if real telemetry is active."""
    return _tracer is not _NOOP_TRACER


@contextmanager
def use_span(span: Any) -> Iterator[None]:
    """Activate *span* as the current span for the duration of the block.

    The Pipe's root ``openclaw.pipe`` span is created with ``start_span``
    (not ``start_as_current_span``) so it can be ended manually across async
    yields.  Without activation, child spans created by ``GatewayClient``
    (``openclaw.gateway.connect`` / ``request:agent`` / ``agent_stream``)
    would nest under OWUI's incoming request span instead of under
    ``openclaw.pipe`` — fragmenting the trace.  Wrapping the request body in
    ``with use_span(span):`` makes the root current so its children attach.

    No-op when telemetry is disabled: no-op spans carry no real context and
    there is nothing to correlate.  ``end_on_exit=False`` keeps span lifetime
    under the caller's explicit control.
    """
    if not is_enabled() or not _OTEL_AVAILABLE:
        yield
        return
    from opentelemetry.trace import use_span as _otel_use_span
    with _otel_use_span(span, end_on_exit=False):
        yield


# ---------------------------------------------------------------------------
# Public API — metrics instruments
# ---------------------------------------------------------------------------

def pipe_requests() -> Any:
    """Counter: ``openclaw.pipe.requests`` — total Pipe requests.

    Attributes: ``openclaw.agent.id``, ``status`` (success/error).
    """
    return _counter_pipe_requests

def pipe_duration() -> Any:
    """Histogram: ``openclaw.pipe.duration`` — request duration in seconds.

    Attributes: ``openclaw.agent.id``, ``status`` (success/error).
    """
    return _histogram_pipe_duration

def gateway_connections() -> Any:
    """UpDownCounter: ``openclaw.gateway.connections`` — active connections."""
    return _updown_gateway_connections

def gateway_rpc_requests() -> Any:
    """Counter: ``openclaw.gateway.rpc.requests`` — RPC calls.

    Attributes: ``rpc.method``, ``status`` (success/error).
    """
    return _counter_gateway_rpc

def agent_stream_events() -> Any:
    """Counter: ``openclaw.agent.stream.events`` — streaming events yielded.

    Attributes: ``openclaw.agent.id``, ``openclaw.event.kind``.
    """
    return _counter_agent_events


# ---------------------------------------------------------------------------
# Public API — span helpers
# ---------------------------------------------------------------------------

class Attr:
    """Well-known span attribute names and metric attribute keys."""

    # Service
    SERVICE_NAME = "service.name"

    # GenAI (emerging conventions)
    GEN_AI_SYSTEM = "gen_ai.system"
    GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
    GEN_AI_RESPONSE_ID = "gen_ai.response.id"
    GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
    GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"

    # RPC
    RPC_METHOD = "rpc.method"
    RPC_SERVICE = "rpc.service"

    # Custom — OpenClaw
    OPENCLAW_AGENT_ID = "openclaw.agent.id"
    OPENCLAW_RUN_ID = "openclaw.run.id"
    OPENCLAW_SESSION_KEY = "openclaw.session.key"
    OPENCLAW_GATEWAY_URL = "openclaw.gateway.url"
    OPENCLAW_EVENT_KIND = "openclaw.event.kind"

    # Custom — Open WebUI
    OWUI_USER_ID = "openwebui.user.id"
    OWUI_CHAT_ID = "openwebui.chat.id"
    OWUI_MODEL_ID = "openwebui.model.id"

    # Error / status
    ERROR_TYPE = "error.type"
    ERROR_MESSAGE = "error.message"
    STATUS = "status"


def record_exception_on_span(span: Any, exc: BaseException) -> None:
    """Record an exception on *span* with status and attributes."""
    if not span.is_recording():
        return
    if _OTEL_AVAILABLE:
        span.set_status(Status(StatusCode.ERROR))
    span.record_exception(exc, attributes={
        Attr.ERROR_TYPE: type(exc).__name__,
        Attr.ERROR_MESSAGE: str(exc),
    })


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _owui_otel_active() -> bool:
    """Return ``True`` if OWUI's native OTel is enabled."""
    return os.environ.get("ENABLE_OTEL", "").lower() == "true"


def _create_instruments() -> None:
    """Create metric instruments from the current meter."""
    global _counter_pipe_requests, _histogram_pipe_duration
    global _updown_gateway_connections, _counter_gateway_rpc, _counter_agent_events

    m = _meter
    _counter_pipe_requests = m.create_counter(
        "openclaw.pipe.requests", unit="1",
        description="Total Pipe requests")
    _histogram_pipe_duration = m.create_histogram(
        "openclaw.pipe.duration", unit="s",
        description="Request duration in seconds")
    _updown_gateway_connections = m.create_up_down_counter(
        "openclaw.gateway.connections", unit="1",
        description="Active Gateway WebSocket connections")
    _counter_gateway_rpc = m.create_counter(
        "openclaw.gateway.rpc.requests", unit="1",
        description="Gateway RPC calls")
    _counter_agent_events = m.create_counter(
        "openclaw.agent.stream.events", unit="1",
        description="Agent streaming events yielded")


def _pipe_logger() -> logging.Logger:
    """Return the ``openclaw_pipe`` logger that all Pipe modules use."""
    return logging.getLogger("openclaw_pipe")


def _setup_log_bridge() -> None:
    """Route Python ``logging`` from the ``openclaw_pipe`` hierarchy into OTel.

    Attaches a :class:`LoggingHandler` to the ``openclaw_pipe`` logger
    so that ``logger.info(...)`` / ``logger.error(...)`` calls appear as
    OTel log records with trace-context correlation, using OWUI's global
    ``LoggerProvider``.
    """
    global _log_handler

    from opentelemetry._logs import get_logger_provider as _get_logger_provider
    try:
        log_provider = _get_logger_provider()
    except Exception:
        logger.debug("No OTel LoggerProvider available; log bridge skipped.")
        return

    # Attach a LoggingHandler scoped to our logger hierarchy.
    # INFO-level and above are exported; DEBUG stays local-only.
    handler = _SDKLoggingHandler(level=logging.INFO, logger_provider=log_provider)
    pl = _pipe_logger()
    pl.addHandler(handler)
    _log_handler = handler

    pl.info(
        "OTel log bridge active. openclaw_pipe logs now exported "
        "with trace context via OWUI's OTel pipeline."
    )
