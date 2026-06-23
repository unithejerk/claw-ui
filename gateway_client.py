"""
OpenClaw Gateway WebSocket client.

Manages a persistent WebSocket connection to the Gateway, handles the
``connect`` challenge handshake, correlates RPC requests with responses,
and provides an async-generator interface for streaming agent runs.

Reference: https://docs.openclaw.ai/gateway/protocol
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Callable

import websockets
import websockets.exceptions
from websockets.asyncio.client import ClientConnection

from protocol import (
    PROTOCOL_VERSION,
    FrameType,
    ParsedFrame,
    build_connect,
    build_request,
    generate_idempotency_key,
    generate_request_id,
    parse_connect_challenge,
    parse_frame,
)
from telemetry import (
    Attr,
    agent_stream_events,
    gateway_connections,
    gateway_rpc_requests,
    get_tracer,
    record_exception_on_span,
)

logger = logging.getLogger("openclaw_pipe.gateway_client")

# ---------------------------------------------------------------------------
# Connection state
# ---------------------------------------------------------------------------


class GatewayConnectionError(Exception):
    """Raised when the Gateway connection fails or is lost irrecoverably."""


class GatewayRPCError(Exception):
    """Raised when a Gateway RPC returns ``ok: false``."""


class HandshakeError(GatewayConnectionError):
    """Raised when the connect handshake fails."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class GatewayClient:
    """Persistent, multiplexed WebSocket connection to an OpenClaw Gateway.

    Usage::

        client = GatewayClient("ws://127.0.0.1:18789", "my-token")
        await client.connect()

        # Blocking RPC call
        res = await client.request("sessions.list", {"agentId": "default"})

        # Streaming agent run
        async for event in client.agent_stream("default", messages, "user-1"):
            print(event)

        await client.close()
    """

    def __init__(
        self,
        gateway_url: str,
        token: str,
        *,
        request_timeout: float = 120.0,
        max_reconnect_attempts: int = 5,
        reconnect_base_delay: float = 1.0,
    ):
        """Create a client for *gateway_url* authenticated with *token*.

        The connection is **lazy** — the WebSocket opens on the first
        ``request()``/``agent_stream()`` call (or explicit ``connect()``),
        not here.  One client is meant to be shared across many requests
        (the Pipe keeps a single instance and only recreates it on valve
        changes).

        Parameters
        ----------
        gateway_url:
            ``ws://``/``wss://`` URL of the OpenClaw Gateway.
        token:
            Gateway operator auth token (token-only auth; no device keypair).
        request_timeout:
            Seconds to wait for a single RPC response, and per-event in an
            agent stream (see the ``REQUEST_TIMEOUT`` valve caveat — this is
            *not* a wall-clock cap on a whole streaming run).
        max_reconnect_attempts:
            Reconnect tries after an unexpected disconnect (0 = none).
        reconnect_base_delay:
            Initial backoff in seconds; doubles per attempt.
        """
        self._url = gateway_url
        self._token = token
        self._request_timeout = request_timeout
        self._max_reconnect = max_reconnect_attempts
        self._base_delay = reconnect_base_delay

        # Internal state
        self._ws: ClientConnection | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._connected = False
        self._connect_lock = asyncio.Lock()

        # Request/response correlation
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

        # Event subscribers: event_name -> list of async queues
        self._subscribers: dict[str, list[asyncio.Queue[dict[str, Any]]]] = {}

        # Run-level subscribers: runId -> queue for agent events
        self._run_subscribers: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the WebSocket and complete the ``connect`` handshake.

        Idempotent — if already connected this is a no-op.  Thread-safe
        via an asyncio lock so concurrent ``pipe()`` calls don't race.
        """
        if self._connected:
            return

        async with self._connect_lock:
            if self._connected:  # double-check under lock
                return
            tracer = get_tracer()
            with tracer.start_as_current_span("openclaw.gateway.connect") as span:
                span.set_attribute(Attr.OPENCLAW_GATEWAY_URL, self._url)
                await self._do_connect()

    async def close(self) -> None:
        """Gracefully close the WebSocket and cancel the reader task."""
        was_connected = self._connected
        self._connected = False

        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        # Fail all pending requests
        for req_id, future in self._pending.items():
            if not future.done():
                future.set_exception(
                    GatewayConnectionError("Connection closed")
                )
        self._pending.clear()

        # Signal all run subscriber queues
        self._fail_run_subscribers("Client shutting down")

        if was_connected:
            gateway_connections().add(-1)

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        idempotent: bool = False,
    ) -> dict[str, Any]:
        """Send an RPC request and wait for the matching response.

        Returns the ``payload`` dict from the response frame.

        Raises :exc:`GatewayRPCError` if the response has ``ok: false``.
        """
        await self.connect()

        request_id = generate_request_id()
        idem_key = generate_idempotency_key() if idempotent else None

        future: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
        self._pending[request_id] = future

        tracer = get_tracer()
        span_name = f"openclaw.gateway.request:{method}"
        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute(Attr.RPC_METHOD, method)
            span.set_attribute(Attr.RPC_SERVICE, "openclaw-gateway")
            if idempotent:
                span.set_attribute("rpc.idempotent", True)

            try:
                frame_json = build_request(
                    method,
                    params or {},
                    request_id=request_id,
                    idempotency_key=idem_key,
                )
                await self._send(frame_json)

                result = await asyncio.wait_for(future, timeout=self._request_timeout)
                gateway_rpc_requests().add(1, {Attr.RPC_METHOD: method, Attr.STATUS: "success"})
                return result

            except asyncio.TimeoutError:
                exc = GatewayRPCError(
                    f"Request {method} timed out after {self._request_timeout}s"
                )
                gateway_rpc_requests().add(1, {Attr.RPC_METHOD: method, Attr.STATUS: "error"})
                record_exception_on_span(span, exc)
                raise exc
            finally:
                self._pending.pop(request_id, None)

    async def agent_stream(
        self,
        agent_id: str,
        messages: list[dict[str, Any]],
        session_key: str | None = None,
        *,
        system_prompt: str | None = None,
        model_params: dict[str, Any] | None = None,
        files: list[dict[str, Any]] | None = None,
        tools: list[dict[str, Any]] | None = None,
        approval_mode: str = "auto_deny",
        approval_timeout: int = 30,
        event_call: Callable | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Run an agent and yield streaming events as they arrive.

        Each yielded dict represents a Gateway agent event payload.
        The caller (``event_mapper``) translates these into Open WebUI
        SSE chunks.

        The stream ends when the Gateway sends the final result response,
        or when an error occurs.
        """
        await self.connect()

        tracer = get_tracer()
        span = tracer.start_span(
            "openclaw.gateway.agent_stream",
            attributes={
                Attr.OPENCLAW_AGENT_ID: agent_id,
                Attr.GEN_AI_SYSTEM: "openclaw",
                Attr.GEN_AI_REQUEST_MODEL: f"openclaw/{agent_id}",
            },
        )
        # Individual user_id/chat_id/agent_id attributes are already set
        # by the caller — the combined session key is intentionally
        # omitted to avoid exposing sensitive identifiers as span
        # attributes.
        # if session_key:
        #     span.set_attribute(Attr.OPENCLAW_SESSION_KEY, session_key)

        params: dict[str, Any] = {
            "agentId": agent_id,
            "messages": messages,
        }
        if session_key:
            params["sessionKey"] = session_key
        if system_prompt:
            params["systemPrompt"] = system_prompt
        if model_params:
            params.update(model_params)
        if files:
            params["files"] = files
        if tools:
            params["tools"] = tools

        run_id: str | None = None

        try:
            # Send the agent request and get the initial ack
            ack = await self.request("agent", params, idempotent=True)

            if ack.get("status") != "accepted":
                exc = GatewayRPCError(
                    f"Agent run rejected: {ack.get('error', 'unknown reason')}"
                )
                record_exception_on_span(span, exc)
                raise exc

            run_id = ack["runId"]
            span.set_attribute(Attr.OPENCLAW_RUN_ID, run_id)
            span.add_event("agent.run.accepted", {"runId": run_id})
            logger.info("Agent run accepted: runId=%s agentId=%s", run_id, agent_id)

            # A single per-run queue carries both agent deltas and approval
            # requests, both routed by runId in _route_event.  Using one
            # queue lets us wait via asyncio.wait_for, which cancels its
            # pending getter on timeout — no leaked tasks.  (Previously
            # we waited on two queues with asyncio.wait(FIRST_COMPLETED)
            # and never cancelled the non-completed getter, leaking one
            # task per streamed event; approval events were also fanned
            # out globally instead of per-run, cross-wiring concurrent
            # runs.)
            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
            self._run_subscribers[run_id] = queue

            while True:
                event = await asyncio.wait_for(
                    queue.get(), timeout=self._request_timeout
                )
                event_type = event.get("_event_type", "delta")
                kind = event.get("kind", "delta")

                if event_type == "approval":
                    # Approval request for this run — resolve per mode.
                    async for item in self._handle_approval(
                        event, run_id, approval_mode, approval_timeout, span,
                        event_call=event_call,
                    ):
                        yield item
                elif kind == "final":
                    agent_stream_events().add(1, {
                        Attr.OPENCLAW_AGENT_ID: agent_id, Attr.OPENCLAW_EVENT_KIND: "final",
                    })
                    span.add_event("agent.run.completed", {
                        "runId": run_id,
                        "status": event.get("status", "ok"),
                    })
                    yield event
                    return
                else:
                    agent_stream_events().add(1, {
                        Attr.OPENCLAW_AGENT_ID: agent_id, Attr.OPENCLAW_EVENT_KIND: kind,
                    })
                    span.add_event("agent.delta", {
                        Attr.OPENCLAW_EVENT_KIND: kind,
                    })
                    yield event

        except asyncio.TimeoutError:
            exc = GatewayRPCError(
                f"Agent run {run_id} timed out after {self._request_timeout}s"
            )
            record_exception_on_span(span, exc)
            raise exc
        except Exception as exc:
            record_exception_on_span(span, exc)
            raise
        finally:
            if run_id is not None:
                self._run_subscribers.pop(run_id, None)
            span.end()

    async def resolve_approval(
        self,
        run_id: str,
        approved: bool,
    ) -> None:
        """Resolve a pending approval request for *run_id*.

        Best-effort — errors are swallowed with a warning.
        No-op if not connected.
        """
        if not self._connected:
            return
        try:
            await self.request(
                "approval.resolve",
                {"runId": run_id, "approved": approved},
            )
            logger.info(
                "Approval resolved: runId=%s approved=%s", run_id, approved
            )
        except (GatewayConnectionError, GatewayRPCError) as exc:
            logger.warning("Approval resolve may not have reached Gateway: %s", exc)

    async def abort_agent(
        self,
        agent_id: str,
        session_key: str | None = None,
    ) -> None:
        """Best-effort abort of a running agent.

        Sends ``sessions.abort`` RPC with a short timeout.  Errors are
        swallowed (logged as warnings) — this is fire-and-forget.
        No-op if the client is not connected.
        """
        if not self._connected:
            return
        params: dict[str, Any] = {"agentId": agent_id}
        if session_key:
            params["sessionKey"] = session_key
        try:
            await self.request("sessions.abort", params)
            logger.info("Agent aborted: agentId=%s", agent_id)
        except (GatewayConnectionError, GatewayRPCError) as exc:
            logger.warning("Abort may not have reached Gateway: %s", exc)

    async def list_agents(self) -> list[dict[str, str]]:
        """Discover available agents from the Gateway.

        Returns a list of ``{"id": "...", "name": "..."}`` dicts
        suitable for use in ``Pipe.pipes()``.
        """
        try:
            result = await self.request("agents.list")
            agents = result.get("agents", [])
            return [
                {"id": a.get("id", a.get("name", "")), "name": a.get("name", a.get("id", ""))}
                for a in agents
            ]
        except GatewayRPCError:
            logger.warning("agents.list RPC failed; falling back to default agent")
            return [{"id": "default", "name": "Default"}]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _do_connect(self) -> None:
        """Establish WebSocket and complete the connect handshake."""
        logger.info("Connecting to Gateway at %s", self._url)

        try:
            self._ws = await websockets.connect(
                self._url,
                max_size=32 * 1024 * 1024,  # 32 MiB — generous for agent responses
                ping_interval=30,
                ping_timeout=10,
                compression="deflate",
            )
        except Exception as exc:
            raise GatewayConnectionError(
                f"Failed to open WebSocket to {self._url}: {exc}"
            ) from exc

        try:
            await self._handshake()
        except Exception:
            # Don't leave a half-open socket on handshake failure
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:
                    pass
                self._ws = None
            raise

        self._connected = True
        self._reader_task = asyncio.create_task(self._reader_loop())
        gateway_connections().add(1)
        logger.info("Gateway connected successfully")

    async def _handshake(self) -> None:
        """Complete the connect challenge handshake.

        1. Wait for the ``connect.challenge`` event from the server
           (consumed from the socket so it isn't mistaken for the
           connect response).
        2. Send the ``connect`` request with **token-only auth** — no
           challenge signature is produced (see
           :func:`protocol.build_connect` and the note on
           :func:`protocol.sign_challenge`).
        3. Verify the response is ``ok: true``.
        """
        assert self._ws is not None

        # Step 1 — receive challenge
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            raise HandshakeError("Timed out waiting for connect.challenge event")

        try:
            challenge = parse_connect_challenge(raw)
        except (ValueError, KeyError) as exc:
            raise HandshakeError(f"Invalid connect.challenge: {exc}") from exc

        logger.debug("Received connect.challenge nonce=%s", challenge["nonce"][:8])

        # Step 2 — send connect request
        connect_frame = build_connect(
            request_id=generate_request_id(),
            token=self._token,
            challenge_nonce=challenge["nonce"],
            challenge_ts=challenge["ts"],
        )
        await self._ws.send(connect_frame)

        # Step 3 — wait for response
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
        except asyncio.TimeoutError:
            raise HandshakeError("Timed out waiting for connect response")

        frame = parse_frame(raw)
        if frame.response is None:
            raise HandshakeError(
                f"Expected connect response, got {frame.type.value}"
            )
        if not frame.response.ok:
            error = frame.response.error or {}
            raise HandshakeError(
                f"Connect rejected: {error.get('message', 'unknown')}"
            )

        logger.debug("Connect handshake complete: %s", frame.response.payload)

    async def _send(self, message: str) -> None:
        """Send a text frame, with reconnection if the socket is dead."""
        assert self._ws is not None
        try:
            await self._ws.send(message)
        except websockets.exceptions.ConnectionClosed as exc:
            raise GatewayConnectionError(f"WebSocket closed: {exc}") from exc

    async def _reader_loop(self) -> None:
        """Background task that reads frames and routes them.

        - ``res`` frames → resolve the matching pending Future.
        - ``event`` frames → dispatch to subscribers.
        - ``req`` frames → ignored for now (server-to-client requests).
        """
        assert self._ws is not None

        while self._connected:
            try:
                raw = await self._ws.recv()
            except websockets.exceptions.ConnectionClosed as exc:
                logger.info("Gateway WebSocket closed: %s", exc)
                self._connected = False
                await self._handle_disconnect()
                return
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("Unexpected error in reader loop: %s", exc)
                continue

            try:
                frame = parse_frame(raw)
            except Exception:
                logger.debug("Failed to parse frame: type=%s length=%d", type(raw).__name__, len(raw))
                continue

            if frame.type == FrameType.RES and frame.response is not None:
                self._route_response(frame.response)

            elif frame.type == FrameType.EVENT and frame.event is not None:
                self._route_event(frame.event)

            # Server-to-client requests are not handled in v1

    async def _handle_disconnect(self) -> None:
        """Attempt reconnection with exponential backoff."""
        gateway_connections().add(-1)  # connection lost

        reconnected = False

        try:
            for attempt in range(self._max_reconnect):
                delay = self._base_delay * (2 ** attempt)
                logger.info(
                    "Reconnecting in %.1fs (attempt %d/%d)",
                    delay,
                    attempt + 1,
                    self._max_reconnect,
                )
                await asyncio.sleep(delay)

                async with self._connect_lock:
                    if self._connected:
                        logger.info(
                            "Another task already reconnected; exiting"
                        )
                        reconnected = True
                        return

                    try:
                        self._ws = await websockets.connect(
                            self._url,
                            max_size=32 * 1024 * 1024,
                            ping_interval=30,
                            ping_timeout=10,
                            compression="deflate",
                        )
                        await self._handshake()
                        self._connected = True
                        self._reader_task = asyncio.create_task(
                            self._reader_loop()
                        )
                        gateway_connections().add(1)  # reconnected
                        logger.info("Reconnected successfully")
                        reconnected = True

                        # Signal all run subscribers that their runs
                        # were interrupted by the disconnect
                        self._fail_run_subscribers(
                            "Connection lost; agent run interrupted"
                        )

                        return
                    except Exception as exc:
                        logger.warning(
                            "Reconnect attempt %d failed: %s",
                            attempt + 1,
                            exc,
                        )
                    finally:
                        if self._ws:
                            try:
                                await self._ws.close()
                            except Exception:
                                pass
                            self._ws = None

            logger.error("All reconnect attempts exhausted")
        finally:
            if not reconnected:
                self._fail_run_subscribers(
                    "Connection lost and all reconnect attempts failed"
                )
                # Fail pending requests so callers don't hang
                for future in self._pending.values():
                    if not future.done():
                        future.set_exception(
                            GatewayConnectionError(
                                "Connection lost and all reconnect "
                                "attempts failed"
                            )
                        )
                self._pending.clear()

    def _fail_run_subscribers(self, error_message: str) -> None:
        """Put a terminal ``final`` event into every active run subscriber
        queue so that consumers blocked on ``queue.get()`` do not hang
        until their request timeout."""
        for queue in list(self._run_subscribers.values()):
            try:
                queue.put_nowait({
                    "kind": "final",
                    "status": "error",
                    "error": error_message,
                })
            except asyncio.QueueFull:
                logger.debug(
                    "Run subscriber queue full; dropping terminal event: %s",
                    error_message,
                )

    # ------------------------------------------------------------------
    # Approval handling
    # ------------------------------------------------------------------

    def _safe_task(self, coro, *, label: str = "") -> asyncio.Task:
        """Schedule *coro* as a background task with error logging.

        Use for fire-and-forget operations where the caller cannot await
        the result and a silent failure would hide bugs (e.g. approval
        resolution).  Logs any unhandled exception at WARNING level.
        """
        task = asyncio.create_task(coro)

        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.warning(
                    "Background task %r failed: %s", label or t.get_name(), exc
                )

        task.add_done_callback(_on_done)
        return task

    async def _handle_approval(
        self,
        event: dict[str, Any],
        run_id: str,
        mode: str,
        timeout: int,
        span: Any,
        *,
        event_call: Callable | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Handle a Gateway approval request.

        Resolution depends on *mode*:

        * ``auto_approve`` — resolves the approval as approved and yields
          nothing.
        * ``auto_deny`` — resolves as denied **and yields one delta** so
          the user sees a "auto-denied approval for tool: X" note in chat.
        * ``render`` — yields an approval-request card, then schedules an
          auto-deny after *timeout* seconds.
        * ``interactive`` — prompts the user via ``__event_call__``
          confirmation dialog, waits for their response, then resolves
          accordingly.  Falls back to auto-deny when ``event_call`` is
          unavailable (older OWUI, or non-streaming mode).

        In all modes the resolution is fire-and-forget
        (``asyncio.create_task``); callers should not await it.
        """
        request = event.get("request") or event.get("payload") or event
        tool_name = request.get("toolName") or request.get("tool") or "unknown"

        if mode == "auto_approve":
            self._safe_task(self.resolve_approval(run_id, True), label="resolve_approval")
            span.add_event("approval.auto_approved", {"tool": tool_name})
            return

        elif mode == "auto_deny":
            self._safe_task(self.resolve_approval(run_id, False), label="resolve_approval")
            span.add_event("approval.auto_denied", {"tool": tool_name})
            yield {
                "kind": "delta",
                "delta": {
                    "status": f"Auto-denied approval for tool: {tool_name}",
                    "approval_denied": True,
                    "toolName": tool_name,
                },
            }
            return

        elif mode == "interactive":
            if event_call is None:
                # No back-channel available — fall back to auto-deny.
                self._safe_task(self.resolve_approval(run_id, False), label="resolve_approval")
                span.add_event("approval.interactive_fallback_denied",
                               {"tool": tool_name})
                yield {
                    "kind": "delta",
                    "delta": {
                        "status": (
                            f"Approval required for '{tool_name}' but "
                            "interactive mode is unavailable (update Open "
                            "WebUI for confirmation dialogs). Auto-denied."
                        ),
                        "approval_denied": True,
                        "toolName": tool_name,
                    },
                }
                return

            # Prompt the user via OWUI's bidirectional event system.
            arguments = request.get("arguments", {})
            try:
                response = await event_call({
                    "type": "confirmation",
                    "data": {
                        "title": "Approve tool execution?",
                        "message": (
                            f"**{tool_name}**\n\n"
                            f"```json\n{json.dumps(arguments, indent=2)}\n```"
                        ),
                        "confirmText": "Approve",
                        "cancelText": "Deny",
                    },
                })
            except Exception:
                # Timeout, disconnect, or other error → auto-deny.
                span.add_event("approval.interactive_timeout",
                               {"tool": tool_name})
                self._safe_task(self.resolve_approval(run_id, False), label="resolve_approval")
                yield {
                    "kind": "delta",
                    "delta": {
                        "status": (
                            f"Approval for '{tool_name}' timed out "
                            "(auto-denied)."
                        ),
                        "approval_denied": True,
                        "toolName": tool_name,
                    },
                }
                return

            if response:
                self._safe_task(self.resolve_approval(run_id, True), label="resolve_approval")
                span.add_event("approval.interactive_approved",
                               {"tool": tool_name})
            else:
                self._safe_task(self.resolve_approval(run_id, False), label="resolve_approval")
                span.add_event("approval.interactive_denied",
                               {"tool": tool_name})
                yield {
                    "kind": "delta",
                    "delta": {
                        "status": f"User denied approval for tool: {tool_name}",
                        "approval_denied": True,
                        "toolName": tool_name,
                    },
                }
            return

        elif mode == "render":
            yield {
                "kind": "delta",
                "delta": {
                    "approval_request": True,
                    "toolName": tool_name,
                    "arguments": request.get("arguments", {}),
                    "timeout": timeout,
                },
            }
            async def _deny_after_timeout():
                await asyncio.sleep(timeout)
                try:
                    await self.resolve_approval(run_id, False)
                except Exception:
                    pass
            self._safe_task(_deny_after_timeout(), label="approval_render_timeout")
            span.add_event("approval.rendered",
                           {"tool": tool_name, "timeout": timeout})
            return

        else:
            logger.warning(
                "Unknown approval mode '%s' for run %s; auto-denying",
                mode, run_id,
            )
            self._safe_task(
                self.resolve_approval(run_id, False),
                label="resolve_approval",
            )
            span.add_event(
                "approval.unknown_mode",
                {"mode": mode, "tool": tool_name},
            )
            yield {
                "kind": "delta",
                "delta": {
                    "status": f"Unknown approval mode '{mode}'; auto-denied",
                    "approval_denied": True,
                    "toolName": tool_name,
                },
            }

    # ------------------------------------------------------------------
    # Response / event routing
    # ------------------------------------------------------------------

    def _route_response(self, response) -> None:
        """Resolve the pending Future for this response, or handle final
        agent results via the run subscriber queue."""
        req_id = response.id
        payload = response.payload or {}
        error = response.error

        # Check if this is the final result of an active agent run.
        # Skip when a caller is explicitly awaiting this request id via
        # _pending (e.g. approval.resolve) — those responses go through
        # the standard correlation path even if they carry a runId.
        run_id = payload.get("runId", "")
        if run_id and run_id in self._run_subscribers and req_id not in self._pending:
            if response.ok:
                # Final result
                try:
                    self._run_subscribers[run_id].put_nowait({
                        "kind": "final",
                        "status": payload.get("status", "ok"),
                        "runId": run_id,
                        "summary": payload.get("summary", ""),
                    })
                except asyncio.QueueFull:
                    logger.warning(
                        "Run subscriber queue full for run %s; dropping final event",
                        run_id,
                    )
            else:
                # Error
                try:
                    self._run_subscribers[run_id].put_nowait({
                        "kind": "final",
                        "status": "error",
                        "runId": run_id,
                        "error": (error or {}).get("message", "Unknown error"),
                    })
                except asyncio.QueueFull:
                    logger.warning(
                        "Run subscriber queue full for run %s; dropping error event",
                        run_id,
                    )
            return

        # Standard RPC response correlation
        future = self._pending.get(req_id)
        if future is None:
            logger.debug("No pending request for response id=%s", req_id)
            return

        if response.ok:
            future.set_result(payload)
        else:
            future.set_exception(
                GatewayRPCError(
                    (error or {}).get("message", f"RPC {req_id} failed")
                )
            )

    def _route_event(self, event) -> None:
        """Dispatch an event frame to subscribers.

        Agent deltas and approval events are both routed to the per-run
        queue keyed by ``runId``, so concurrent agent runs never receive
        each other's events.  Approval events are tagged with
        ``_event_type="approval"`` so the ``agent_stream`` consumer hands
        them to ``_handle_approval`` rather than treating them as deltas.
        """
        event_name = event.event
        payload = event.payload

        # Agent events are routed by runId
        if event_name == "agent":
            run_id = payload.get("runId", "")
            if run_id and run_id in self._run_subscribers:
                try:
                    self._run_subscribers[run_id].put_nowait({
                        "kind": "delta",
                        "runId": run_id,
                        **payload,
                    })
                except asyncio.QueueFull:
                    logger.warning(
                        "Run subscriber queue full for run %s; dropping delta event",
                        run_id,
                    )
                return

        # Only route approval.requested events per-run — other approval
        # subtypes (resolved, timeout, …) are informational and should not
        # trigger approval-handling logic in agent_stream.
        if event_name == "approval.requested":
            run_id = payload.get("runId", "")
            if run_id and run_id in self._run_subscribers:
                try:
                    self._run_subscribers[run_id].put_nowait({
                        "kind": "approval",
                        "runId": run_id,
                        **payload,
                        "_event_type": "approval",
                    })
                except asyncio.QueueFull:
                    logger.warning(
                        "Run subscriber queue full for run %s; dropping approval event",
                        run_id,
                    )
                return
            # No active subscriber for this runId — drop.
            return

        # Named subscribers (legacy fanout path; currently unused).
        for queue in self._subscribers.get(event_name, []):
            try:
                payload["_event_type"] = event_name.replace(".", "_")
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                logger.debug("Event queue full for %s; dropping event", event_name)
