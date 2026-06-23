"""
Translate OpenClaw Gateway agent streaming events into Open WebUI
SSE-compatible chunk dictionaries.

The Gateway emits ``agent`` events with incremental payloads during a
run.  This module maps those payloads into the formats Open WebUI
expects from a Pipe Function's ``yield``.

Key design rule (from Open WebUI docs):
    Do NOT emit ``delta.tool_calls`` — it triggers OWUI's tool-execution
    retry loop (up to 256 iterations).  Render tool execution as
    ``<details type="tool_calls">`` HTML blocks instead.
"""

from __future__ import annotations

import html
import json
import logging
from typing import Any, AsyncIterator, Callable

logger = logging.getLogger("openclaw_pipe.event_mapper")

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def map_agent_events(
    event_stream: AsyncIterator[dict[str, Any]],
    *,
    event_emitter: Callable | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Consume a Gateway agent event stream and yield OWUI chunks.

    Parameters
    ----------
    event_stream:
        Async iterator of Gateway agent event payloads, as produced by
        :meth:`GatewayClient.agent_stream`.
    event_emitter:
        Optional ``__event_emitter__`` callable from the OWUI Pipe context.
        Used to push status/progress updates to the UI.

    Yields
    ------
    dict
        Open WebUI SSE-compatible chunk dictionaries.  Each is either a
        ``{"choices": [{"delta": {...}}]}`` dict or a rich content string
        for tool calls / status blocks.
    """
    async for event in event_stream:
        kind = event.get("kind", "delta")

        if kind == "final":
            yield _map_final(event)
            return

        elif kind == "delta":
            async for chunk in _map_delta(event, event_emitter=event_emitter):
                yield chunk

        else:
            logger.debug(
                "Unknown event kind: %s", kind,
                extra={"event": "unknown_event_kind", "kind": kind},
            )


# ---------------------------------------------------------------------------
# Delta mappers
# ---------------------------------------------------------------------------


async def _map_delta(
    event: dict[str, Any],
    *,
    event_emitter: Callable | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Map a single Gateway agent delta event to one or more OWUI chunks."""
    delta = event.get("delta", event)
    content = delta.get("content", "")
    tool_call = delta.get("toolCall") or delta.get("tool_call")
    thinking = delta.get("thinking") or delta.get("reasoning")
    status = delta.get("status") or delta.get("step")
    approval_request = delta.get("approval_request", False)
    approval_denied = delta.get("approval_denied", False)

    # Approval events — render as details cards
    if approval_request or approval_denied:
        yield _render_approval(delta)
        return

    # Text content — streamed as SSE delta chunks
    if content:
        yield _sse_delta(content)

    # Tool calls — rendered as details blocks, NOT delta.tool_calls
    if tool_call:
        yield _render_tool_call(tool_call)

    # Thinking / reasoning — shown as status events
    if thinking and event_emitter:
        try:
            await event_emitter({
                "type": "status",
                "data": {
                    "description": str(thinking)[:256],
                    "done": False,
                },
            })
        except Exception:
            logger.debug(
                "event_emitter call failed for thinking status",
                extra={"event": "emitter_failed", "status_type": "thinking"},
            )

    # Explicit status steps
    if status and event_emitter:
        try:
            await event_emitter({
                "type": "status",
                "data": {
                    "description": str(status)[:256],
                    "done": False,
                },
            })
        except Exception:
            logger.debug(
                "event_emitter call failed for step status",
                extra={"event": "emitter_failed", "status_type": "step"},
            )


def _map_final(event: dict[str, Any]) -> dict[str, Any]:
    """Map the terminal agent event to a completion chunk."""
    status = event.get("status", "ok")

    if status == "ok":
        return {
            "choices": [{
                "delta": {"content": ""},
                "finish_reason": "stop",
            }]
        }
    else:
        error_msg = event.get("error", "Agent run failed")
        return {
            "choices": [{
                "delta": {"content": f"\n\n[Error: {error_msg}]"},
                "finish_reason": "stop",
            }]
        }


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------


def _sse_delta(text: str) -> dict[str, Any]:
    """Build an OpenAI-compatible SSE delta chunk."""
    return {
        "choices": [{
            "delta": {"content": text},
            "finish_reason": None,
        }]
    }


# ---------------------------------------------------------------------------
# Tool call rendering
# ---------------------------------------------------------------------------


def _render_tool_call(tool_call: dict[str, Any]) -> str:
    """Render a tool call as a ``<details>`` HTML block.

    Open WebUI recognises ``<details type="tool_calls">`` and renders
    them as expandable tool-execution cards in the chat UI — without
    triggering the internal tool-execution retry loop.

    The tool_call dict may have the following shapes (all handled):

    * In-progress call:
      ``{"id": "...", "name": "...", "arguments": {...}}``

    * Completed call:
      ``{"id": "...", "name": "...", "arguments": {...}, "result": ...}``

    * Gateway-specific shape:
      ``{"callId": "...", "tool": "...", "input": {...}, "output": ...}``
    """
    call_id = tool_call.get("id") or tool_call.get("callId") or ""
    name = tool_call.get("name") or tool_call.get("tool") or "tool"
    arguments = tool_call.get("arguments") or tool_call.get("input") or {}
    result = tool_call.get("result") or tool_call.get("output")

    args_json = html.escape(
        json.dumps(arguments, ensure_ascii=False)
    )
    done = result is not None

    if done:
        result_str = html.escape(
            json.dumps(result, ensure_ascii=False)
        )
        block = (
            f'<details type="tool_calls" done="true" '
            f'id="{html.escape(call_id)}" '
            f'name="{html.escape(name)}" '
            f'arguments="{args_json}">\n'
            f"<summary>Tool: {html.escape(name)}</summary>\n"
            f"{result_str}\n"
            f"</details>\n"
        )
    else:
        block = (
            f'<details type="tool_calls" done="false" '
            f'id="{html.escape(call_id)}" '
            f'name="{html.escape(name)}" '
            f'arguments="{args_json}">\n'
            f"<summary>Calling: {html.escape(name)}...</summary>\n"
            f"</details>\n"
        )

    return block


def _render_approval(delta: dict[str, Any]) -> str:
    """Render an approval request or denial as an HTML details card.

    Approval requests get ``type="approval"`` with the tool name and
    arguments.  Denials get a brief note.  Open WebUI renders these as
    expandable cards.
    """
    tool_name = html.escape(delta.get("toolName", "unknown"))
    arguments = delta.get("arguments", {})
    args_json = html.escape(json.dumps(arguments, ensure_ascii=False))
    timeout = delta.get("timeout", 30)

    if delta.get("approval_request"):
        return (
            f'<details type="approval" done="false" '
            f'name="{tool_name}" arguments="{args_json}">\n'
            f"<summary>🔐 Approval requested: {tool_name}</summary>\n"
            f"<p>Arguments: <code>{args_json}</code></p>\n"
            f"<p><em>Auto-denied after {timeout}s "
            f"(no interactive approval in Open WebUI streaming).</em></p>\n"
            f"</details>\n"
        )
    # Auto-denied
    return (
        f'<details type="approval" done="true" name="{tool_name}">\n'
        f"<summary>🔐 Auto-denied: {tool_name}</summary>\n"
        f"<p>The approval was automatically denied by the Pipe.</p>\n"
        f"</details>\n"
    )
