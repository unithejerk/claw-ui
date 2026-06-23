"""Tests for event_mapper.py — Gateway events → OWUI SSE chunks."""
import json

import pytest

from event_mapper import _map_final, _map_delta, _render_tool_call, _render_approval, map_agent_events


# ── map_agent_events ───────────────────────────────────────────────────────

async def _collect(stream):
    out = []
    async for chunk in stream:
        out.append(chunk)
    return out


async def _aiter(items):
    for i in items:
        yield i


async def test_map_agent_events_streams_deltas_then_ends_on_final():
    events = [
        {"kind": "delta", "delta": {"content": "Hel"}},
        {"kind": "delta", "delta": {"content": "lo"}},
        {"kind": "final", "status": "ok"},
    ]
    chunks = await _collect(map_agent_events(_aiter(events)))
    # Two content deltas + one final stop chunk.
    assert len(chunks) == 3
    assert chunks[0]["choices"][0]["delta"]["content"] == "Hel"
    assert chunks[1]["choices"][0]["delta"]["content"] == "lo"
    assert chunks[2]["choices"][0]["finish_reason"] == "stop"


async def test_map_agent_events_final_error_yields_error_text():
    chunks = await _collect(map_agent_events(_aiter([
        {"kind": "final", "status": "error", "error": "boom"},
    ])))
    assert "[Error: boom]" in chunks[0]["choices"][0]["delta"]["content"]
    assert chunks[0]["choices"][0]["finish_reason"] == "stop"


async def test_map_agent_events_unknown_kind_logged_and_skipped():
    chunks = await _collect(map_agent_events(_aiter([
        {"kind": "mystery"},
        {"kind": "final", "status": "ok"},
    ])))
    # Only the final chunk; unknown kind skipped.
    assert len(chunks) == 1


# ── _map_delta ──────────────────────────────────────────────────────────────

async def test_map_delta_content_yields_sse_delta():
    chunks = await _collect(_map_delta({"delta": {"content": "hi"}}))
    assert chunks[0]["choices"][0]["delta"]["content"] == "hi"
    assert chunks[0]["choices"][0]["finish_reason"] is None


async def test_map_delta_tool_call_renders_details_html_not_delta_tool_calls():
    """Critical rule (#): never emit delta.tool_calls — render HTML instead."""
    chunks = await _collect(_map_delta({
        "delta": {"toolCall": {"id": "c1", "name": "web_search", "arguments": {"q": "x"}}}
    }))
    assert len(chunks) == 1
    rendered = chunks[0]
    # Must be an HTML string, NOT a structured delta.tool_calls dict
    # (which would trigger OWUI's tool-execution retry loop).
    assert isinstance(rendered, str)
    assert not isinstance(rendered, dict)
    assert '<details type="tool_calls"' in rendered
    assert 'name="web_search"' in rendered


async def test_map_delta_thinking_emits_status_event():
    calls = []

    async def emitter(evt):
        calls.append(evt)

    await _collect(_map_delta({"delta": {"thinking": "reasoning..."}}, event_emitter=emitter))
    assert calls and calls[0]["type"] == "status"
    assert "reasoning" in calls[0]["data"]["description"]


async def test_map_delta_approval_request_renders_card():
    chunks = await _collect(_map_delta({
        "delta": {"approval_request": True, "toolName": "run_shell", "arguments": {"cmd": "ls"}, "timeout": 30}
    }))
    assert len(chunks) == 1
    assert isinstance(chunks[0], str)
    assert '<details type="approval"' in chunks[0]
    assert "run_shell" in chunks[0]


# ── _render_tool_call ──────────────────────────────────────────────────────

def test_render_tool_call_in_progress():
    s = _render_tool_call({"id": "c1", "name": "web_search", "arguments": {"q": "x & y"}})
    assert 'done="false"' in s
    assert "Calling: web_search..." in s
    # Arguments are HTML-escaped into the attribute.
    assert "&amp;" in s  # & escaped
    assert 'name="web_search"' in s


def test_render_tool_call_completed_includes_result():
    s = _render_tool_call({"id": "c1", "name": "web_search", "arguments": {}, "result": {"hits": 3}})
    assert 'done="true"' in s
    assert "Tool: web_search" in s
    # Result JSON is HTML-escaped inside the block.
    assert "&quot;hits&quot;: 3" in s


def test_render_tool_call_gateway_shape_callId_input_output():
    s = _render_tool_call({"callId": "c2", "tool": "calc", "input": {"a": 1}, "output": 42})
    assert 'name="calc"' in s
    assert 'done="true"' in s


# ── _render_approval ──────────────────────────────────────────────────────

def test_render_approval_request():
    s = _render_approval({"approval_request": True, "toolName": "t", "arguments": {"x": 1}, "timeout": 30})
    assert '<details type="approval"' in s
    assert "Approval requested" in s
    assert "Auto-denied after 30s" in s


def test_render_approval_denied():
    s = _render_approval({"approval_denied": True, "toolName": "t"})
    assert "Auto-denied" in s


# ── _map_final ─────────────────────────────────────────────────────────────

def test_map_final_ok():
    chunk = _map_final({"status": "ok"})
    assert chunk["choices"][0]["finish_reason"] == "stop"
    assert chunk["choices"][0]["delta"]["content"] == ""


def test_map_final_error_includes_message():
    chunk = _map_final({"status": "error", "error": "boom"})
    assert "[Error: boom]" in chunk["choices"][0]["delta"]["content"]
    assert chunk["choices"][0]["finish_reason"] == "stop"