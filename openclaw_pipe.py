"""
OpenClaw Pipe Function for Open WebUI.

Adds OpenClaw agents as selectable models in the Open WebUI chat
interface.  Communicates with the OpenClaw Gateway over its native
WebSocket RPC protocol — not the OpenAI-compatible HTTP API.

Load this file as a **Pipe Function** in the Open WebUI Admin panel
(Workspace → Functions → + → Pipe).

Requirements (available in the Open WebUI Python environment):
    - pydantic >= 2.0
    - websockets >= 12.0

Reference:
    https://docs.openwebui.com/features/extensibility/plugin/functions/pipe/
    https://docs.openclaw.ai/gateway/protocol
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time as _time
from typing import Any, AsyncIterator, Callable


from valves import Valves
from gateway_client import (
    GatewayClient,
    GatewayConnectionError,
    GatewayRPCError,
)
from event_mapper import map_agent_events
from telemetry import (
    Attr,
    get_tracer,
    init_telemetry,
    pipe_duration,
    pipe_requests,
    record_exception_on_span,
    shutdown_telemetry,
    use_span,
)

logger = logging.getLogger("openclaw_pipe")

# ---------------------------------------------------------------------------
# Pipe class
# ---------------------------------------------------------------------------


class Pipe:
    """
    Open WebUI Pipe Function that bridges to OpenClaw Gateway.

    Implements the two-method contract:

    * ``pipes()`` — returns the list of models shown in the selector.
    * ``pipe()`` — handles a chat completion request and streams the
      response back to the UI.
    """

    # Expose the Valves schema so Open WebUI renders a config form.
    Valves = Valves  # type: ignore[assignment]

    def __init__(self):
        """Initialise the Pipe with default valves and lazy state.

        No Gateway connection is opened here — the WebSocket client is
        created lazily on the first ``pipe()`` call (see ``_get_client``)
        and recreated only when the relevant valves change.  For a static
        ``AGENT_LIST`` the model cache is pre-populated immediately so the
        model selector shows configured agents from the first render and
        ``pipe()`` validates model IDs from the first request; for
        ``__auto__`` a background discovery task is kicked off on the
        first ``pipes()`` call so the real agent list is ready by the
        time the user opens the dropdown a second time (falling back to
        lazy discovery on the first ``pipe()`` if it hasn't completed).
        Telemetry is initialised (a no-op unless OWUI has
        ``ENABLE_OTEL=true``).
        """
        self.valves = self.Valves()
        self._client: GatewayClient | None = None
        self._client_config_hash: int = 0
        self._client_lock = asyncio.Lock()
        self._agent_cache: list[dict[str, str]] | None = None
        self._agent_cache_lock = asyncio.Lock()
        self._gateway_status: str = "unknown"  # unknown | connected | error
        self._gateway_error: str = ""
        self._discovery_launched: bool = False

        # For a static AGENT_LIST, populate the cache immediately so the
        # model selector shows the configured agents (not a placeholder)
        # and pipe() validates the selected model against the configured
        # list from the very first request.  Auto-discovery ("__auto__")
        # leaves the cache None until the first pipe() call.
        if self._is_static_agent_list():
            self._agent_cache = self._build_static_models()

        # Initialise telemetry.  When OWUI has ENABLE_OTEL=true this
        # piggybacks on OWUI's existing TracerProvider automatically.
        # Otherwise telemetry degrades to no-ops (no standalone mode).
        init_telemetry()

    # ------------------------------------------------------------------
    # Model discovery
    # ------------------------------------------------------------------

    def pipes(self) -> list[dict[str, str]]:
        """Return the list of OpenClaw agents exposed as models.

        Called by Open WebUI when the user opens the model selector.
        Must be synchronous, so it never talks to the Gateway directly:

        * Static ``AGENT_LIST`` — returns the configured agents (rebuilt
          from the current valves, so live edits show immediately).
        * ``__auto__`` — returns the cached agent list if discovery has
          run, otherwise a single ``OpenClaw/Default`` placeholder.  On
          the first call a background task is launched to eagerly discover
          agents from the Gateway so the real list is ready for the next
          dropdown open; the first ``pipe()`` call also triggers
          discovery as a fallback.

        A ``__openclaw_status__`` entry (id starting with ``__``) is
        appended to surface Gateway health — it is an informational
        indicator only; ``pipe()`` refuses to run it (see the guard there).
        If the Gateway is known-unreachable, a warning entry is appended
        so the admin sees the issue in the selector without checking logs.
        """
        agent_list = self.valves.AGENT_LIST.strip()

        if not agent_list or agent_list == "__auto__":
            if self._agent_cache is not None:
                models = list(self._agent_cache)
            else:
                models = [{"id": "openclaw/default", "name": f"{self.valves.AGENT_PREFIX}Default"}]
                # Kick off eager discovery on first model-selector open so
                # the real agent list is ready for the next dropdown visit.
                self._launch_eager_discovery()
        else:
            # Static list — cache is pre-populated in __init__, but rebuild
            # from the current valves so live valve edits show immediately.
            models = self._build_static_models()

        # Append health status if Gateway is known-bad
        if self._gateway_status == "error":
            models.append({
                "id": "__openclaw_status__",
                "name": f"⚠️ Gateway unreachable — {self._gateway_error or 'check GATEWAY_URL and token'}",
            })
        elif self._gateway_status == "unknown" and self._agent_cache is None:
            models.append({
                "id": "__openclaw_status__",
                "name": "⏳ Discovering agents...",
            })

        return models

    def _launch_eager_discovery(self) -> None:
        """Kick off a background task to populate the agent cache eagerly.

        Idempotent — only launches one task per Pipe lifetime.  Fires
        discovery in the background so the model-selector UI stays
        responsive.  On failure the existing lazy-discovery fallback
        in ``pipe()`` / ``_ensure_agent_cache`` handles the first message.
        """
        if self._discovery_launched:
            return
        self._discovery_launched = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no event loop yet; lazy discovery on first pipe() call
        loop.create_task(self._eager_discover())

    async def _eager_discover(self) -> None:
        """Populate the agent cache in the background, swallowing errors.

        On success the model selector shows real agents on the next open.
        On failure the existing lazy-discovery path in ``pipe()`` handles
        it — the user sees a placeholder on the first dropdown open and
        the real list after discovery completes on the first message.
        """
        try:
            client = await self._get_client()
            await self._ensure_agent_cache(client)
        except Exception:
            pass  # silently degrade to lazy discovery

    async def _ensure_agent_cache(self, client: GatewayClient) -> list[dict[str, str]]:
        """Populate the agent cache from the Gateway if needed.

        For ``__auto__`` mode this calls ``list_agents()`` RPC on first
        use.  For static lists the cache is pre-populated in ``__init__``
        (and rebuilt from valves on live edits), so this is a no-op that
        just returns the current cache.  Thread-safe via lock.

        The lock is NOT held across the RPC call to ``list_agents()``:
        it is released before the network I/O and re-acquired to write
        the result back to the cache.  This prevents a slow Gateway
        from blocking other ``pipe()`` invocations that only need to
        *read* the cache.
        """
        if self._is_static_agent_list():
            # Static list — keep the cache in sync with the current valves
            # so live edits take effect, but never call the Gateway.
            async with self._agent_cache_lock:
                self._agent_cache = self._build_static_models()
                return self._agent_cache

        # Fast path — avoid lock contention when cache is already warm.
        if self._agent_cache is not None:
            return self._agent_cache

        async with self._agent_cache_lock:
            # Double-check under lock — another caller may have populated it.
            if self._agent_cache is not None:
                return self._agent_cache

        # --- Lock released before RPC call ---
        try:
            agents = await client.list_agents()
            gateway_status = "connected"
            gateway_error = ""
        except Exception as exc:
            logger.warning(
                "Agent auto-discovery failed: %s", exc,
                extra={"event": "agent_discovery_failed", "error_type": type(exc).__name__},
            )
            gateway_status = "error"
            gateway_error = "Agent discovery failed — check Gateway configuration"
            agents = [{"id": "default", "name": "Default"}]

        # Re-acquire lock to write results back to the cache.
        async with self._agent_cache_lock:
            self._gateway_status = gateway_status
            self._gateway_error = gateway_error

            # Validate and prefix
            cached: list[dict[str, str]] = []
            for a in agents:
                a_id = a.get("id", "")
                a_name = a.get("name", a_id)
                if not a_id:
                    continue
                cached.append({
                    "id": f"openclaw/{a_id}",
                    "name": f"{self.valves.AGENT_PREFIX}{a_name}",
                })
            self._agent_cache = cached or [
                {"id": "openclaw/default", "name": f"{self.valves.AGENT_PREFIX}Default"}
            ]
            return self._agent_cache

    def _is_static_agent_list(self) -> bool:
        """Return True if AGENT_LIST names specific agent IDs (not auto)."""
        agent_list = self.valves.AGENT_LIST.strip()
        return bool(agent_list) and agent_list != "__auto__"

    def _build_static_models(self) -> list[dict[str, str]]:
        """Build the model list for a static AGENT_LIST (no Gateway call).

        Returns one ``openclaw/<id>`` entry per configured agent ID,
        falling back to a single ``default`` entry if the list is blank.
        """
        models: list[dict[str, str]] = []
        for agent_id in self.valves.AGENT_LIST.split(","):
            agent_id = agent_id.strip()
            if agent_id:
                models.append({
                    "id": f"openclaw/{agent_id}",
                    "name": f"{self.valves.AGENT_PREFIX}{agent_id}",
                })
        return models or [
            {"id": "openclaw/default", "name": f"{self.valves.AGENT_PREFIX}Default"}
        ]

    # ------------------------------------------------------------------
    # Chat completion
    # ------------------------------------------------------------------

    async def pipe(
        self,
        body: dict[str, Any],
        *,
        __user__: dict[str, Any] | None = None,
        __metadata__: dict[str, Any] | None = None,
        __event_emitter__: Callable | None = None,
        __event_call__: Callable | None = None,
        __files__: list[dict[str, Any]] | None = None,
        __tools__: list[dict[str, Any]] | None = None,
    ) -> AsyncIterator[str | dict[str, Any]] | str:
        """Handle a chat completion request.

        Open WebUI calls this when the user sends a message with one
        of our models selected.

        Parameters
        ----------
        body:
            The request body with keys like ``model``, ``messages``,
            ``stream``, ``temperature``, etc.
        __user__:
            The authenticated user dict (id, email, name, role).
            Used as the basis for the session key.
        __metadata__:
            Chat metadata (chat_id, message_id, session_id, files, etc.).
        __event_emitter__:
            Callable for pushing status updates to the UI.
        __event_call__:
            Callable for bidirectional event communication
            (confirmation dialogs, etc.).
        __files__:
            List of uploaded file metadata dicts with keys
            like ``name``, ``mimeType``, ``data``, etc.  Forwarded to the
            Gateway agent RPC as ``attachments``.
        __tools__:
            List of tool definitions available to the agent.  **Not
            forwarded** — ``AgentParamsSchema`` (protocol v4) has no
            ``tools`` field and rejects unknown fields; the Gateway agent
            uses its own configured tools.

        Returns
        -------
        str or AsyncIterator[str | dict[str, Any]]
            Either a plain string (non-streaming) or an async generator
            of SSE-compatible dicts (streaming).
        """
        # Extract the agent ID from the model name
        model_id: str = body.get("model", "")

        # Status-indicator entries (id starts with "__", e.g.
        # ``__openclaw_status__``) are shown in the model selector to
        # surface Gateway health, but they are not runnable models.
        # Refuse them up front with a clear message instead of silently
        # routing to the default agent or emitting a confusing
        # "unknown agent" error.
        if model_id.startswith("__"):
            msg = (
                "This entry is a connection-status indicator, not a "
                "runnable model. Please select an OpenClaw agent "
                "(e.g. OpenClaw/Default) from the model selector."
            )
            logger.info("Rejected status-indicator model selection: %s", model_id)
            if body.get("stream", False):
                return _error_stream_generator(msg)
            return msg

        agent_id = _parse_agent_id(model_id, self.valves.AGENT_PREFIX)

        # Build the session key for conversation continuity.
        # Scoped to user × chat × agent so each chat gets its own
        # Gateway session and switching agents isolates context.
        session_key = _build_session_key(__user__, __metadata__, agent_id)

        # Get or create the Gateway client
        client = await self._get_client()

        # Populate agent cache on first call (auto-discovery)
        await self._ensure_agent_cache(client)

        # Validate the requested agent exists
        stream = body.get("stream", False)
        if self._agent_cache is not None:
            known = {a["id"] for a in self._agent_cache}
            if model_id not in known:
                error_msg = (
                    f"Unknown agent '{model_id}'. Available: "
                    + ", ".join(sorted(known))
                )
                logger.warning(
                    "Unknown agent requested: %s",
                    model_id,
                    extra={"event": "unknown_agent", "model_id": model_id},
                )
                if stream:
                    return _error_stream_generator(error_msg)
                return error_msg

        # Start the root span for this request.
        tracer = get_tracer()
        span = tracer.start_span(
            "openclaw.pipe",
            attributes={
                Attr.GEN_AI_SYSTEM: "openclaw",
                Attr.GEN_AI_REQUEST_MODEL: model_id,
                Attr.OPENCLAW_AGENT_ID: agent_id,
                Attr.OWUI_MODEL_ID: model_id,
            },
        )
        if __user__ and __user__.get("id"):
            span.set_attribute(Attr.OWUI_USER_ID, __user__["id"])
        if __metadata__ and __metadata__.get("chat_id"):
            span.set_attribute(Attr.OWUI_CHAT_ID, __metadata__["chat_id"])
        # Individual user_id/chat_id/agent_id attributes are already set
        # above — the combined session key is intentionally omitted to
        # avoid exposing sensitive identifiers as span attributes.
        # if session_key:
        #     span.set_attribute(Attr.OPENCLAW_SESSION_KEY, session_key)

        # AgentParamsSchema (protocol v4) accepts a single `message`
        # string plus optional `extraSystemPrompt` / `attachments`.  OWUI
        # tool definitions (__tools__) and OAI model params are not
        # forwardable — the Gateway agent uses its own tools and rejects
        # unknown fields (additionalProperties:false) — so they are
        # intentionally dropped here.
        attachments = _extract_file_payloads(__files__)

        # Emit initial status
        if __event_emitter__:
            try:
                await __event_emitter__({
                    "type": "status",
                    "data": {
                        "description": f"Connecting to OpenClaw ({agent_id})...",
                        "done": False,
                    },
                })
            except Exception:
                pass

        t0 = _time.monotonic()

        if stream:
            return self._traced_stream_response(
                client, agent_id, body, session_key, __event_emitter__, __event_call__,
                span, t0, attachments,
            )
        else:
            with use_span(span):
                try:
                    result, agent_error = await self._nonstream_response(
                        client, agent_id, body, session_key, attachments,
                        event_call=__event_call__,
                    )
                    if agent_error:
                        self._gateway_status = "connected"
                        self._gateway_error = "Agent run failed"
                        pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"})
                        pipe_duration().record(_time.monotonic() - t0,
                                               {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"})
                        span.add_event("agent.run.error")
                    else:
                        self._gateway_status = "connected"
                        self._gateway_error = ""
                        pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "success"})
                        pipe_duration().record(_time.monotonic() - t0,
                                               {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "success"})
                    span.end()
                    return result
                except GatewayConnectionError as exc:
                    self._gateway_status = "error"
                    self._gateway_error = str(exc)[:120]
                    self._finalize_span_with_error(span, exc, agent_id, t0)
                    error_msg = _format_error(exc)
                    return error_msg
                except (GatewayRPCError, Exception) as exc:
                    self._finalize_span_with_error(span, exc, agent_id, t0)
                    error_msg = _format_error(exc)
                    return error_msg

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def _traced_stream_response(
        self,
        client: GatewayClient,
        agent_id: str,
        body: dict[str, Any],
        session_key: str | None,
        event_emitter: Callable | None,
        event_call: Callable | None,
        span: Any,
        t0: float,
        attachments: list[dict[str, Any]] | None = None,
    ) -> AsyncIterator[str | dict[str, Any]]:
        """Stream agent output with span + metrics lifecycle across yields."""
        # Activate the root span as current so GatewayClient child spans
        # (connect / request:agent / agent_stream) nest under openclaw.pipe.
        # The sync context manager is held across yields; the no-op case
        # (telemetry disabled) is a pass-through.
        with use_span(span):
            try:
                outcome: dict = {}
                async for chunk in self._stream_response(
                    client, agent_id, body, session_key, event_emitter, attachments,
                    event_call=event_call, outcome=outcome,
                ):
                    yield chunk
                # Stream exhausted — branch on the raw run outcome.
                if outcome.get("agent_error"):
                    self._gateway_status = "connected"
                    self._gateway_error = "Agent run failed"
                    pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"})
                    pipe_duration().record(_time.monotonic() - t0,
                                           {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"})
                    span.add_event("agent.run.error")
                else:
                    self._gateway_status = "connected"
                    self._gateway_error = ""
                    pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "success"})
                    pipe_duration().record(_time.monotonic() - t0,
                                           {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "success"})
            except GeneratorExit:
                # User cancelled — best-effort abort, fire-and-forget
                _fire_and_forget(client.abort_agent(agent_id, session_key),
                                label="abort_agent")
                pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "cancelled"})
                pipe_duration().record(_time.monotonic() - t0,
                                       {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "cancelled"})
                span.add_event("pipe.cancelled")
                span.end()
                return
            except GatewayConnectionError as exc:
                self._gateway_status = "error"
                self._gateway_error = str(exc)[:120]
                self._finalize_span_with_error(span, exc, agent_id, t0)
                error_msg = _format_error(exc)
                yield _error_chunk(error_msg)
                return
            except (GatewayRPCError, Exception) as exc:
                self._finalize_span_with_error(span, exc, agent_id, t0)
                # Yield the error as content so the UI shows it
                error_msg = _format_error(exc)
                yield _error_chunk(error_msg)
                return
            span.end()

    async def _stream_response(
        self,
        client: GatewayClient,
        agent_id: str,
        body: dict[str, Any],
        session_key: str | None,
        event_emitter: Callable | None,
        attachments: list[dict[str, Any]] | None = None,
        *,
        event_call: Callable | None = None,
        outcome: dict | None = None,
    ) -> AsyncIterator[str | dict[str, Any]]:
        """Stream agent output as SSE chunks.

        If *outcome* is provided it is populated with ``agent_error``
        based on the raw terminal event captured by the stream wrapper.
        """
        if event_emitter:
            try:
                await event_emitter({
                    "type": "status",
                    "data": {"description": f"OpenClaw/{agent_id} is thinking...", "done": False},
                })
            except Exception:
                pass

        event_stream = self._build_agent_stream(
            client, agent_id, body, session_key, attachments,
            event_call=event_call,
        )

        async for chunk in map_agent_events(event_stream, event_emitter=event_emitter):
            yield chunk

        # Capture the raw run outcome from the wrapper so the caller
        # can branch metrics/status without inspecting rendered chunks.
        if outcome is not None:
            outcome["agent_error"] = event_stream.agent_error

        if event_emitter:
            try:
                await event_emitter({
                    "type": "status",
                    "data": {"description": f"OpenClaw/{agent_id} finished", "done": True},
                })
            except Exception:
                pass

    async def _nonstream_response(
        self,
        client: GatewayClient,
        agent_id: str,
        body: dict[str, Any],
        session_key: str | None,
        attachments: list[dict[str, Any]] | None = None,
        *,
        event_call: Callable | None = None,
    ) -> tuple[str, bool]:
        """Collect streaming output into a single string for non-streaming mode.

        Returns ``(rendered_text, agent_error)`` — the bool is ``True``
        when the Gateway reported a terminal ``status != "ok"``.
        """
        parts: list[str] = []

        event_stream = self._build_agent_stream(
            client, agent_id, body, session_key, attachments,
            event_call=event_call,
        )

        async for chunk in map_agent_events(event_stream, event_emitter=None):
            # Extract text content from SSE chunks.
            # Check for str first — tool calls and approval cards from
            # the mapper are HTML strings, not dicts.
            if isinstance(chunk, str):
                parts.append(chunk)
            elif isinstance(chunk, dict):
                choices = chunk.get("choices", [])
                for choice in choices:
                    delta = choice.get("delta", {})
                    text = delta.get("content", "")
                    if text:
                        parts.append(text)

        return "".join(parts), event_stream.agent_error

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    # (Error handling is inline in pipe() and _traced_stream_response()
    # via _format_error and _error_chunk below.)

    # ------------------------------------------------------------------
    # Client management
    # ------------------------------------------------------------------

    async def _get_client(self) -> GatewayClient:
        """Return the GatewayClient, creating it if necessary.

        Re-creates the client if the Valves configuration has changed
        since the last call.  Serialised under a lock so concurrent
        ``pipe()`` calls cannot race to create duplicate clients.
        """
        config = (
            self.valves.GATEWAY_URL,
            self.valves.GATEWAY_TOKEN,
            self.valves.REQUEST_TIMEOUT,
            self.valves.MAX_RECONNECT_ATTEMPTS,
            self.valves.RECONNECT_BASE_DELAY,
        )
        current_hash = hash(config)

        # Fast path — no lock needed when the client is current.
        if self._client is not None and current_hash == self._client_config_hash:
            return self._client

        async with self._client_lock:
            # Double-check under lock — another caller may have created it.
            if self._client is not None and current_hash == self._client_config_hash:
                return self._client

            if self._client is not None:
                # Drain and close the old client gracefully — give
                # in-flight requests 2 s to finish before closing.
                old = self._client
                self._client = None
                _fire_and_forget(self._drain_and_close(old), label="drain_and_close")

            self._client = GatewayClient(
                gateway_url=self.valves.GATEWAY_URL,
                token=self.valves.GATEWAY_TOKEN,
                request_timeout=float(self.valves.REQUEST_TIMEOUT),
                max_reconnect_attempts=self.valves.MAX_RECONNECT_ATTEMPTS,
                reconnect_base_delay=self.valves.RECONNECT_BASE_DELAY,
            )
            self._client_config_hash = current_hash

        return self._client

    async def _drain_and_close(self, client: GatewayClient) -> None:
        """Gracefully close an old client after a short drain period.

        Called on valve changes.  Gives in-flight requests time to
        complete before closing the WebSocket, avoiding cascading
        failures mid-stream.
        """
        try:
            await asyncio.sleep(2)
        except Exception:
            pass
        try:
            await client.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Span + metrics helpers
    # ------------------------------------------------------------------

    def _finalize_span_with_error(
        self, span: Any, exc: BaseException, agent_id: str, t0: float
    ) -> None:
        """Record the exception on *span*, emit error metrics, and close.

        Called from both the streaming and non-streaming paths to avoid
        duplicating the same four statements across both error handlers.
        """
        record_exception_on_span(span, exc)
        pipe_requests().add(1, {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"})
        pipe_duration().record(
            _time.monotonic() - t0,
            {Attr.OPENCLAW_AGENT_ID: agent_id, Attr.STATUS: "error"},
        )
        span.end()

    def _build_agent_stream(
        self,
        client: GatewayClient,
        agent_id: str,
        body: dict[str, Any],
        session_key: str | None,
        attachments: list[dict[str, Any]] | None = None,
        *,
        event_call: Callable | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Build and return a Gateway agent event stream.

        Factors out the ``_extract_agent_message``,
        ``_extract_system_prompt``, and ``client.agent_stream(…)``
        call shared by ``_stream_response`` and ``_nonstream_response``.

        The agent RPC takes a single ``message`` string (the Gateway
        session holds prior history), an optional ``extraSystemPrompt``,
        and optional ``attachments`` — see ``AgentParamsSchema``.
        """
        message = _extract_agent_message(body, mode=self.valves.MESSAGE_MODE)
        extra_system_prompt = _extract_system_prompt(body)

        raw = client.agent_stream(
            agent_id=agent_id,
            message=message,
            session_key=session_key,
            extra_system_prompt=extra_system_prompt,
            attachments=attachments,
            approval_mode=self.valves.APPROVAL_MODE,
            approval_timeout=self.valves.APPROVAL_TIMEOUT,
            event_call=event_call,
        )
        return _AgentRunStream(raw)


# ---------------------------------------------------------------------------
# Agent run stream wrapper
# ---------------------------------------------------------------------------


class _AgentRunStream:
    """Wraps a raw Gateway agent event stream and captures the terminal
    run outcome before the mapper processes it.

    This is an async iterator that yields every event through unchanged.
    After the stream exhausts, ``agent_error`` is ``True`` when the
    Gateway reported a terminal ``status != "ok"`` on the run, and
    ``False`` otherwise (including when no final event arrived).
    """

    def __init__(self, raw_stream: AsyncIterator[dict[str, Any]]) -> None:
        self._raw = raw_stream
        self.agent_error: bool = False

    def __aiter__(self) -> "_AgentRunStream":
        return self

    async def __anext__(self) -> dict[str, Any]:
        event = await self._raw.__anext__()
        if (
            event.get("kind") == "final"
            and event.get("status", "ok") != "ok"
            and not event.get("_local")
        ):
            self.agent_error = True
        return event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fire_and_forget(coro, *, label: str = "") -> None:
    """Schedule *coro* as a task that logs any unhandled exception.

    Use for best-effort operations (abort, drain) where the caller
    cannot await the result and a silent failure would hide bugs.
    """
    task = asyncio.create_task(coro)

    def _on_done(t: asyncio.Task[object]) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.warning(
                "Background task %r failed: %s",
                label or t.get_name(), exc,
                extra={"event": "background_task_failed", "error_type": type(exc).__name__},
            )

    task.add_done_callback(_on_done)


def _parse_agent_id(model_id: str, prefix: str) -> str:
    """Extract the OpenClaw agent ID from the OWUI model identifier.

    >>> _parse_agent_id("openclaw/default", "OpenClaw/")
    'default'
    >>> _parse_agent_id("openclaw/coding-agent", "OpenClaw/")
    'coding-agent'
    >>> _parse_agent_id("some-other-model", "OpenClaw/")
    'default'
    """
    prefix_lower = prefix.lower().rstrip("/")
    model_lower = model_id.lower()

    # Try to extract the agent ID after the prefix.
    # IMPORTANT: Gateway agent IDs are case-sensitive, so we lowercase
    # both sides only for the *prefix comparison*, then slice the
    # *original* model_id to preserve the casing the Gateway expects.
    if model_lower.startswith(f"{prefix_lower}/"):
        return model_id[len(prefix_lower) + 1:]
    if "/" in model_id:
        return model_id.split("/", 1)[1]
    return "default"


def _build_session_key(
    user: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
    agent_id: str,
) -> str | None:
    """Derive a stable session key scoped to user × chat × agent.

    Produces keys like ``owui:user:abc:chat:xyz:agent:default``.

    * Each OWUI chat gets its own Gateway session — new chat = clean slate.
    * Returning to the same chat resumes the same Gateway session.
    * Switching agents within a chat gives a separate session per agent.
    * Returns ``None`` only if user AND chat are unavailable (rare).
    """
    parts = ["owui"]
    if user and user.get("id"):
        parts.append(f"user:{user['id']}")
    if metadata and metadata.get("chat_id"):
        parts.append(f"chat:{metadata['chat_id']}")
    parts.append(f"agent:{agent_id}")
    return ":".join(parts) if len(parts) > 2 else None


def _extract_agent_message(
    body: dict[str, Any],
    *,
    mode: str = "last",
) -> str:
    """Return the single ``message`` string to send to the Gateway agent RPC.

    ``AgentParamsSchema`` (protocol v4) takes one ``message`` string; the
    Gateway session holds the prior conversation history.  In ``last``
    mode (the default) this is the newest user message.  In ``full`` mode
    the conversation is flattened into a ``role: content`` transcript
    string — the RPC cannot accept a message list, so this is the only
    way to forward history to a stateless backend.
    """
    messages = body.get("messages", [])
    if not messages:
        return ""

    if mode == "full":
        lines: list[str] = []
        for msg in messages:
            role = msg.get("role", "user")
            text = _coerce_text(msg.get("content", ""))
            if text:
                lines.append(f"{role}: {text}")
        return "\n".join(lines)

    # last — newest user message (Gateway session has the rest).
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return _coerce_text(msg.get("content", ""))
    # No user message — fall back to the last message of any role.
    return _coerce_text(messages[-1].get("content", ""))


def _coerce_text(content: Any) -> str:
    """Coerce an OWUI message ``content`` value to a plain string.

    Open WebUI content may be a string or a list of content parts
    (``[{"type": "text", "text": "…"}, …]``); concatenate the text parts.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text") or part.get("content") or ""
                if text:
                    parts.append(str(text))
            elif isinstance(part, str):
                parts.append(part)
        return "".join(parts)
    if content is None:
        return ""
    return str(content)


def _extract_system_prompt(body: dict[str, Any]) -> str | None:
    """Pull the system prompt if one is set in the messages.

    Mapped to the agent RPC's ``extraSystemPrompt`` parameter.
    """
    messages = body.get("messages", [])
    for msg in messages:
        if msg.get("role") == "system":
            return _coerce_text(msg.get("content"))
    return None


_MAX_FILE_BYTES = 10 * 1024 * 1024  # 10 MiB


def _extract_file_payloads(files: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    """Convert OWUI file metadata into Gateway-compatible payloads.

    Returns ``None`` if no files are provided.  Each payload has
    ``name``, ``mimeType``, and ``data`` (base64 string or raw text).

    The cap is measured on the **base64-encoded** ``data`` length (which
    is ~33% larger than the raw bytes), so the effective raw-byte limit is
    ~7.5 MiB against the 10 MiB constant.  Once the cap is exceeded the
    loop stops and any remaining files are silently dropped (logged at
    WARNING), so callers should not assume every input file is forwarded.
    """
    if not files:
        return None

    payloads: list[dict[str, Any]] = []
    total_bytes = 0
    for f in files:
        name = f.get("name") or f.get("filename") or "unnamed"
        mime = f.get("mimeType") or f.get("mime_type") or f.get("type") or "application/octet-stream"
        data = f.get("data") or f.get("content") or b""

        if isinstance(data, bytes):
            data = base64.b64encode(data).decode("ascii")

        total_bytes += len(data)
        if total_bytes > _MAX_FILE_BYTES:
            logger.warning("File data exceeds %d MiB cap; truncating.", _MAX_FILE_BYTES // (1024 * 1024))
            break

        payloads.append({"name": name, "mimeType": mime, "data": data})

    return payloads if payloads else None


def _format_error(exc: BaseException) -> str:
    """Format an exception as a human-readable error string."""
    if isinstance(exc, GatewayConnectionError):
        return "OpenClaw Gateway unavailable — check configuration"
    elif isinstance(exc, GatewayRPCError):
        return f"OpenClaw request failed: {exc}"
    else:
        logger.exception(
            "Unexpected error in pipe()",
            extra={"event": "pipe_unexpected_error"},
        )
        return f"OpenClaw Pipe error: {exc}"


def _error_chunk(message: str) -> dict[str, Any]:
    """A single SSE chunk representing an error."""
    return {
        "choices": [{
            "delta": {"content": message},
            "finish_reason": "stop",
        }]
    }


async def _error_stream_generator(message: str) -> AsyncIterator[str | dict[str, Any]]:
    """Async generator that yields a single error chunk.

    Used for validation errors that occur before the main streaming
    response is set up, so the return type matches ``pipe()``.
    """
    yield _error_chunk(message)
