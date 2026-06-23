"""Tests for the pure helper functions in openclaw_pipe.py."""
import base64

import openclaw_pipe
from openclaw_pipe import (
    _build_session_key,
    _error_chunk,
    _error_stream_generator,
    _extract_file_payloads,
    _extract_messages,
    _extract_model_params,
    _extract_system_prompt,
    _parse_agent_id,
)


# ── _parse_agent_id ────────────────────────────────────────────────────────

def test_parse_agent_id_after_prefix():
    assert _parse_agent_id("openclaw/default", "OpenClaw/") == "default"
    assert _parse_agent_id("openclaw/coding-agent", "OpenClaw/") == "coding-agent"


def test_parse_agent_id_fallback_to_default():
    # No prefix match and no slash → default.
    assert _parse_agent_id("some-other-model", "OpenClaw/") == "default"


def test_parse_agent_id_with_slash_no_prefix():
    # Has a slash but not the configured prefix → take after slash.
    assert _parse_agent_id("vendor/their-model", "OpenClaw/") == "their-model"


# ── _build_session_key ────────────────────────────────────────────────────

def test_session_key_scoped_user_chat_agent():
    key = _build_session_key({"id": "u1"}, {"chat_id": "c1"}, "default")
    assert key == "owui:user:u1:chat:c1:agent:default"


def test_session_key_missing_user_and_chat_returns_none():
    # Only "owui" + agent → too few parts → None.
    assert _build_session_key(None, None, "default") is None


def test_session_key_user_only():
    key = _build_session_key({"id": "u1"}, None, "default")
    assert key == "owui:user:u1:agent:default"


def test_session_key_switching_agent_isolates():
    a = _build_session_key({"id": "u1"}, {"chat_id": "c1"}, "default")
    b = _build_session_key({"id": "u1"}, {"chat_id": "c1"}, "coding")
    assert a != b


# ── _extract_messages ─────────────────────────────────────────────────────

def test_extract_messages_last_only_returns_last_user():
    msgs = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "reply"},
        {"role": "user", "content": "second"},
    ]
    assert _extract_messages({"messages": msgs}, mode="last") == [{"role": "user", "content": "second"}]


def test_extract_messages_last_no_user_falls_back_to_last():
    msgs = [{"role": "assistant", "content": "x"}]
    assert _extract_messages({"messages": msgs}, mode="last") == [{"role": "assistant", "content": "x"}]


def test_extract_messages_full_returns_all():
    msgs = [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"}]
    assert _extract_messages({"messages": msgs}, mode="full") == msgs


# ── _extract_model_params ─────────────────────────────────────────────────

def test_extract_model_params_filters_known_and_skips_none():
    body = {"temperature": 0.7, "max_tokens": 100, "stop": None, "junk": 1}
    params = _extract_model_params(body)
    assert params == {"temperature": 0.7, "max_tokens": 100}
    assert "stop" not in params
    assert "junk" not in params


# ── _extract_system_prompt ────────────────────────────────────────────────

def test_extract_system_prompt_found():
    msgs = [{"role": "user", "content": "hi"}, {"role": "system", "content": "be brief"}]
    assert _extract_system_prompt({"messages": msgs}) == "be brief"


def test_extract_system_prompt_none():
    assert _extract_system_prompt({"messages": [{"role": "user", "content": "hi"}]}) is None


# ── _extract_file_payloads ────────────────────────────────────────────────

def test_extract_file_payloads_none_when_empty():
    assert _extract_file_payloads(None) is None
    assert _extract_file_payloads([]) is None


def test_extract_file_payloads_bytes_b64_encoded():
    out = _extract_file_payloads([{"name": "f.txt", "mimeType": "text/plain", "data": b"hello"}])
    assert out is not None
    assert out[0]["name"] == "f.txt"
    assert out[0]["mimeType"] == "text/plain"
    assert base64.b64decode(out[0]["data"]) == b"hello"


def test_extract_file_payloads_cap_truncates(monkeypatch):
    # Force a tiny cap to verify the truncation guard fires.
    monkeypatch.setattr(openclaw_pipe, "_MAX_FILE_BYTES", 10)
    out = _extract_file_payloads([
        {"name": "a.txt", "data": "0123456789"},   # exactly at cap
        {"name": "b.txt", "data": "x"},             # pushes over cap → break
        {"name": "c.txt", "data": "y"},             # never reached
    ])
    assert out is not None
    assert [p["name"] for p in out] == ["a.txt"]


# ── error helpers ──────────────────────────────────────────────────────────

def test_error_chunk_shape():
    c = _error_chunk("boom")
    assert c["choices"][0]["delta"]["content"] == "boom"
    assert c["choices"][0]["finish_reason"] == "stop"


async def test_error_stream_generator_single_chunk():
    chunks = [c async for c in _error_stream_generator("nope")]
    assert chunks == [_error_chunk("nope")]