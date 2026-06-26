"""Tests for event_mapper.py — Gateway v4 agent events → OWUI SSE chunks."""
import pytest

from event_mapper import (
    _map_final,
    _render_approval,
    _render_command_output,
    _render_item,
    map_agent_events,
)


# ── helpers ────────────────────────────────────────────────────────────────

async def _collect(stream):
    out = []
    async for chunk in stream:
        out.append(chunk)
    return out


async def _aiter(items):
    for i in items:
        yield i


def _assistant(delta_text="", text=None, **extra):
    data = {}
    if delta_text:
        data["delta"] = delta_text
    if text is not None:
        data["text"] = text
    data.update(extra)
    return {"kind": "delta", "stream": "assistant", "data": data}


# ── map_agent_events: assistant streaming ───────────────────────────────────

async def test_assistant_incremental_deltas_stream_then_end_on_final():
    events = [
        _assistant(delta_text="Hel"),
        _assistant(delta_text="lo"),
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert len(chunks) == 3
    assert chunks[0]["choices"][0]["delta"]["content"] == "Hel"
    assert chunks[1]["choices"][0]["delta"]["content"] == "lo"
    assert chunks[2]["choices"][0]["finish_reason"] == "stop"


async def test_assistant_full_text_snapshots_diffed_against_cumulative():
    # Gateway sends cumulative snapshots in data.text — only the suffix
    # beyond already-shown text should be emitted.
    events = [
        _assistant(text="Hello"),
        _assistant(text="Hello world"),
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert chunks[0]["choices"][0]["delta"]["content"] == "Hello"
    assert chunks[1]["choices"][0]["delta"]["content"] == " world"


async def test_assistant_replaceable_non_extending_emits_full_text():
    # A snapshot that does not extend the prior text (true replacement)
    # emits the full text as a best effort.
    events = [
        _assistant(text="Hello"),
        _assistant(text="Goodbye", replaceable=True),
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert chunks[0]["choices"][0]["delta"]["content"] == "Hello"
    assert chunks[1]["choices"][0]["delta"]["content"] == "Goodbye"


# ── map_agent_events: final / unknown ───────────────────────────────────────

async def test_final_error_yields_error_text():
    chunks = await _collect(map_agent_events(_aiter([
        {"kind": "final", "status": "error", "error": "boom"},
    ])))
    assert "[Error: boom]" in chunks[0]["choices"][0]["delta"]["content"]
    assert chunks[0]["choices"][0]["finish_reason"] == "stop"


async def test_unknown_kind_skipped():
    chunks = await _collect(map_agent_events(_aiter([
        {"kind": "mystery"},
        {"kind": "final", "status": "ok"},
    ])))
    assert len(chunks) == 1


# ── map_agent_events: tool / command_output / approval / thinking ───────────

async def test_tool_stream_renders_details_html_not_delta_tool_calls():
    """Critical rule: never emit delta.tool_calls — render HTML instead."""
    events = [
        {"kind": "delta", "stream": "tool", "data": {
            "itemId": "i1", "phase": "start", "kind": "tool",
            "title": "web_search", "status": "running", "toolCallId": "c1",
        }},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    rendered = chunks[0]
    assert isinstance(rendered, str)
    assert not isinstance(rendered, dict)
    assert '<details type="tool_calls"' in rendered
    assert 'name="web_search"' in rendered
    assert 'done="false"' in rendered


async def test_command_output_renders_tool_card():
    events = [
        {"kind": "delta", "stream": "command_output", "data": {
            "itemId": "i1", "phase": "end", "title": "shell",
            "toolCallId": "c1", "output": "hello", "exitCode": 0,
        }},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert isinstance(chunks[0], str)
    assert '<details type="tool_calls"' in chunks[0]
    assert "hello" in chunks[0]
    assert "exit 0" in chunks[0]


async def test_approval_requested_renders_card():
    events = [
        {"kind": "delta", "stream": "approval", "data": {
            "phase": "requested", "status": "pending", "title": "run_shell",
            "kind": "exec", "command": "ls", "timeout": 30,
        }},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert isinstance(chunks[0], str)
    assert '<details type="approval"' in chunks[0]
    assert "run_shell" in chunks[0]
    assert "Auto-denied after 30s" in chunks[0]


async def test_approval_resolved_renders_status():
    events = [
        {"kind": "delta", "stream": "approval", "data": {
            "phase": "resolved", "status": "denied", "title": "run_shell",
            "kind": "exec", "reason": "user denied",
        }},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert isinstance(chunks[0], str)
    assert "Denied" in chunks[0]
    assert "user denied" in chunks[0]


async def test_thinking_stream_emits_status_event():
    calls = []

    async def emitter(evt):
        calls.append(evt)

    events = [
        {"kind": "delta", "stream": "thinking", "data": {"delta": "reasoning..."}},
        {"kind": "final", "status": "ok"},
    ]
    await _collect(map_agent_events(_aiter(events), event_emitter=emitter))
    assert calls and calls[0]["type"] == "status"
    assert "reasoning" in calls[0]["data"]["description"]


async def test_error_stream_yields_text():
    events = [
        {"kind": "delta", "stream": "error", "data": {"message": "oops"}},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    assert chunks[0]["choices"][0]["delta"]["content"] == "oops"


# ── _render_item ────────────────────────────────────────────────────────────

def test_render_item_in_progress():
    s = _render_item({"itemId": "i1", "phase": "start", "kind": "tool",
                     "title": "web_search", "status": "running", "toolCallId": "c1"})
    assert 'done="false"' in s
    assert "Calling: web_search" in s
    assert 'name="web_search"' in s


def test_render_item_completed_includes_summary():
    s = _render_item({"itemId": "i1", "phase": "end", "kind": "tool",
                     "title": "web_search", "status": "completed",
                     "toolCallId": "c1", "summary": "3 hits"})
    assert 'done="true"' in s
    assert "Tool: web_search" in s
    assert "3 hits" in s


def test_render_item_failed_includes_error():
    s = _render_item({"itemId": "i1", "phase": "end", "kind": "tool",
                     "title": "calc", "status": "failed", "toolCallId": "c1",
                     "error": "boom & bust"})
    assert 'done="true"' in s
    assert "boom &amp; bust" in s  # HTML-escaped


# ── _render_command_output ──────────────────────────────────────────────────

def test_render_command_output_end_includes_output_and_exit():
    s = _render_command_output({"itemId": "i1", "phase": "end", "title": "sh",
                                "toolCallId": "c1", "output": "done", "exitCode": 2})
    assert 'done="true"' in s
    assert "done" in s
    assert "exit 2" in s


# ── _render_approval ────────────────────────────────────────────────────────

def test_render_approval_request():
    s = _render_approval({"phase": "requested", "status": "pending",
                          "title": "t", "kind": "exec", "command": "rm -rf", "timeout": 30})
    assert '<details type="approval"' in s
    assert "Approval requested" in s
    assert "Auto-denied after 30s" in s
    assert "rm -rf" in s


def test_render_approval_denied():
    s = _render_approval({"phase": "resolved", "status": "denied",
                          "title": "t", "kind": "exec", "reason": "auto-denied"})
    assert "Denied" in s
    assert "auto-denied" in s


# ── _map_final ──────────────────────────────────────────────────────────────

def test_map_final_ok():
    chunk = _map_final({"status": "ok"})
    assert chunk["choices"][0]["finish_reason"] == "stop"
    assert chunk["choices"][0]["delta"]["content"] == ""


def test_map_final_error_includes_message():
    chunk = _map_final({"status": "error", "error": "boom"})
    assert "[Error: boom]" in chunk["choices"][0]["delta"]["content"]
    assert chunk["choices"][0]["finish_reason"] == "stop"