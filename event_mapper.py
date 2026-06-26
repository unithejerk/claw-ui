"""
Translate OpenClaw Gateway agent streaming events into Open WebUI
SSE-compatible chunk dictionaries.

The Gateway streams agent runs as ``agent`` events whose payload is an
``AgentEventPayload`` (``runId`` / ``seq`` / ``stream`` / ``ts`` /
``data``), discriminated by ``stream``:

* ``assistant``  — incremental text (``data.delta``) or a full snapshot
  (``data.text`` / ``data.replaceable``).
* ``tool`` / ``item`` — an item in the agent activity feed
  (``AgentItemEventData``: ``phase`` / ``kind`` / ``title`` / ``status`` /
  ``summary`` / ``progressText`` / ``error`` …).
* ``command_output`` — incremental command stdout/stderr
  (``AgentCommandOutputEventData``).
* ``approval`` — an exec/plugin approval request or its later resolution
  (``AgentApprovalEventData``).  Requests are intercepted upstream by
  ``GatewayClient._handle_approval``; this mapper renders whichever
  approval events reach it.
* ``thinking`` / ``lifecycle`` / ``error`` — reasoning, lifecycle phase,
  and error streams.
* ``plan`` / ``patch`` / ``compaction`` — currently logged and dropped.

The terminal run result arrives as a ``res`` frame that
``GatewayClient`` turns into a ``kind="final"`` event.

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
    dict | str
        Open WebUI SSE-compatible chunk dictionaries (``{"choices":
        [{"delta": {...}}]}``) for text, or rich HTML strings for tool /
        approval cards.
    """
    # Cumulative assistant snapshot, used to diff full-text ("data.text")
    # deltas into incremental output so we never re-emit already-shown
    # text (the Gateway may send either incremental "data.delta" or full
    # "data.text" snapshots depending on provider/runtime).
    assistant_text = ""

    async for event in event_stream:
        kind = event.get("kind", "delta")

        if kind == "final":
            yield _map_final(event)
            return

        if kind != "delta":
            logger.debug(
                "Unknown event kind: %s", kind,
                extra={"event": "unknown_event_kind", "kind": kind},
            )
            continue

        stream = event.get("stream", "")
        data = event.get("data") or {}

        if stream == "assistant":
            delta_text, assistant_text = _assistant_delta(data, assistant_text)
            if delta_text:
                yield _sse_delta(delta_text)
            continue

        if stream == "approval":
            yield _render_approval(data)
            continue

        if stream in ("tool", "item"):
            yield _render_item(data)
            continue

        if stream == "command_output":
            yield _render_command_output(data)
            continue

        if stream == "thinking":
            await _emit_status(
                event_emitter,
                _text_of(data) or data.get("title") or "thinking…",
                "thinking",
            )
            continue

        if stream == "error":
            msg = data.get("message") or data.get("text") or data.get("error")
            if msg:
                yield _sse_delta(str(msg))
            continue

        if stream == "lifecycle":
            await _emit_status(
                event_emitter,
                data.get("title") or data.get("phase") or "lifecycle",
                "lifecycle",
            )
            continue

        # plan / patch / compaction / future streams — not rendered.
        logger.debug(
            "Unhandled agent stream: %s", stream,
            extra={"event": "unhandled_stream", "stream": stream},
        )


# ---------------------------------------------------------------------------
# Assistant text
# ---------------------------------------------------------------------------


def _assistant_delta(data: dict[str, Any], prev: str) -> tuple[str, str]:
    """Return ``(text_to_emit, new_cumulative_snapshot)`` for an assistant event.

    Handles both delivery styles the Gateway uses:

    * Incremental: ``data.delta`` (string) — emit verbatim, append to the
      cumulative snapshot.
    * Full snapshot: ``data.text`` (string, optionally ``replaceable``) —
      emit only the suffix beyond what was already shown.  If the snapshot
      does not extend the prior text (a true replacement), emit the full
      text as a best effort — OWUI's SSE stream can't un-emit prior text.
    """
    delta = data.get("delta")
    if isinstance(delta, str) and delta:
        return delta, prev + delta

    text = data.get("text")
    if isinstance(text, str) and text:
        if text.startswith(prev):
            return text[len(prev):], text
        return text, text

    return "", prev


def _text_of(data: dict[str, Any]) -> str:
    """Pull a displayable text string from a thinking/error payload."""
    for key in ("delta", "text", "message"):
        val = data.get(key)
        if isinstance(val, str) and val:
            return val
    return ""


# ---------------------------------------------------------------------------
# Final
# ---------------------------------------------------------------------------


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


async def _emit_status(
    event_emitter: Callable | None, description: Any, kind: str
) -> None:
    """Push a status update to the UI, swallowing emitter errors."""
    if not event_emitter:
        return
    try:
        await event_emitter({
            "type": "status",
            "data": {"description": str(description)[:256], "done": False},
        })
    except Exception:
        logger.debug(
            "event_emitter call failed for %s status", kind,
            extra={"event": "emitter_failed", "status_type": kind},
        )


# ---------------------------------------------------------------------------
# Tool / item rendering
# ---------------------------------------------------------------------------


def _render_item(data: dict[str, Any]) -> str:
    """Render an agent activity item as a ``<details>`` tool card.

    Maps ``AgentItemEventData`` (``itemId`` / ``phase`` / ``kind`` /
    ``title`` / ``status`` / ``name`` / ``summary`` / ``progressText`` /
    ``error`` / ``toolCallId``) to the ``<details type="tool_calls">``
    block Open WebUI renders as an expandable card — without triggering
    its tool-execution retry loop.
    """
    call_id = data.get("toolCallId") or data.get("itemId") or ""
    name = data.get("name") or data.get("title") or "tool"
    status = data.get("status", "")
    phase = data.get("phase", "")
    error = data.get("error")
    summary = data.get("summary")
    progress = data.get("progressText")

    # Terminal when the item ends or reaches a final status.
    done = phase == "end" or status in ("completed", "failed", "blocked")
    name_esc = html.escape(str(name))
    id_esc = html.escape(str(call_id))

    if done:
        if error:
            result_str = html.escape(str(error))
        elif summary:
            result_str = html.escape(str(summary))
        elif progress:
            result_str = html.escape(str(progress))
        else:
            result_str = html.escape(str(status or "done"))
        return (
            f'<details type="tool_calls" done="true" '
            f'id="{id_esc}" name="{name_esc}">\n'
            f"<summary>Tool: {name_esc}</summary>\n"
            f"{result_str}\n"
            f"</details>\n"
        )

    return (
        f'<details type="tool_calls" done="false" '
        f'id="{id_esc}" name="{name_esc}">\n'
        f"<summary>Calling: {name_esc}…</summary>\n"
        f"</details>\n"
    )


def _render_command_output(data: dict[str, Any]) -> str:
    """Render a ``command_output`` event as a tool card.

    ``AgentCommandOutputEventData`` carries incremental command stdout/stderr
    (``phase`` ``"delta"``/``"end"``, ``output``, ``exitCode``, ``cwd``).
    """
    call_id = data.get("toolCallId") or data.get("itemId") or ""
    name = data.get("name") or data.get("title") or "command"
    output = data.get("output") or ""
    exit_code = data.get("exitCode")
    done = data.get("phase") == "end"

    name_esc = html.escape(str(name))
    id_esc = html.escape(str(call_id))
    parts: list[str] = []
    if output:
        parts.append(html.escape(str(output)))
    if exit_code is not None:
        parts.append(html.escape(f"[exit {exit_code}]"))
    body = "\n".join(parts) or (html.escape(str(data.get("status", ""))) if not done else "")

    done_attr = "true" if done else "false"
    summary = f"Tool: {name_esc}" if done else f"Running: {name_esc}…"
    return (
        f'<details type="tool_calls" done="{done_attr}" '
        f'id="{id_esc}" name="{name_esc}">\n'
        f"<summary>{summary}</summary>\n"
        f"{body}\n"
        f"</details>\n"
    )


# ---------------------------------------------------------------------------
# Approval rendering
# ---------------------------------------------------------------------------


def _render_approval(data: dict[str, Any]) -> str:
    """Render an approval event (request or resolution) as an HTML card.

    ``AgentApprovalEventData`` carries ``phase`` (``"requested"`` /
    ``"resolved"``), ``status``, ``title``, ``kind`` (``"exec"`` /
    ``"plugin"`` / ``"unknown"``), ``command``, ``reason``, and (for
    rendered requests) ``timeout``.
    """
    phase = data.get("phase", "requested")
    title = html.escape(str(data.get("title") or "tool"))
    command = data.get("command")

    if phase == "requested":
        timeout = data.get("timeout")
        body = ""
        if command:
            body += f'\n<p>Command: <code>{html.escape(str(command))}</code></p>'
        if timeout is not None:
            note = f"Auto-denied after {timeout}s (no interactive approval)."
        else:
            note = "Pending operator approval."
        return (
            f'<details type="approval" done="false" name="{title}">\n'
            f"<summary>🔐 Approval requested: {title}</summary>\n"
            f"{body}\n"
            f"<p><em>{html.escape(note)}</em></p>\n"
            f"</details>\n"
        )

    # resolved
    status = data.get("status", "resolved")
    reason = data.get("reason")
    labels = {
        "approved": "Approved", "denied": "Denied",
        "unavailable": "Unavailable", "failed": "Failed",
        "pending": "Pending",
    }
    label = html.escape(labels.get(str(status), str(status)))
    body = f'\n<p><em>{html.escape(str(reason))}</em></p>' if reason else ""
    return (
        f'<details type="approval" done="true" name="{title}">\n'
        f"<summary>🔐 Approval {label}: {title}</summary>\n"
        f"{body}\n"
        f"</details>\n"
    )