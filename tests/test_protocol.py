"""Tests for protocol.py — frame construction, parsing, and auth helpers."""
import json

import pytest

import protocol
from protocol import (
    EventFrame,
    FrameType,
    build_connect,
    build_request,
    generate_idempotency_key,
    generate_request_id,
    parse_connect_challenge,
    parse_frame,
    sign_challenge,
)


# ── build_request ──────────────────────────────────────────────────────────

def test_build_request_basic_shape():
    frame = json.loads(build_request("agents.list", {"x": 1}))
    assert frame["type"] == "req"
    assert frame["method"] == "agents.list"
    assert frame["params"] == {"x": 1}
    assert "id" in frame and len(frame["id"]) == 12
    assert "idempotencyKey" not in frame


def test_build_request_idempotency_key_included_when_given():
    frame = json.loads(build_request("agent", {"a": 1}, idempotency_key="k-1"))
    assert frame["idempotencyKey"] == "k-1"


def test_build_request_uses_provided_request_id():
    frame = json.loads(build_request("agent", {}, request_id="fixed-id"))
    assert frame["id"] == "fixed-id"


# ── parse_frame ────────────────────────────────────────────────────────────

def test_parse_frame_req():
    pf = parse_frame(build_request("agent", {"agentId": "default"}, request_id="abc"))
    assert pf.type is FrameType.REQ
    assert pf.request is not None
    assert pf.request.id == "abc"
    assert pf.request.method == "agent"
    assert pf.request.params == {"agentId": "default"}


def test_parse_frame_res_ok_and_error():
    ok = json.dumps({"type": "res", "id": "r1", "ok": True, "payload": {"status": "ok"}})
    pf = parse_frame(ok)
    assert pf.type is FrameType.RES
    assert pf.response.ok is True
    assert pf.response.payload == {"status": "ok"}

    err = json.dumps({"type": "res", "id": "r1", "ok": False, "error": {"message": "no"}})
    pf = parse_frame(err)
    assert pf.response.ok is False
    assert pf.response.error == {"message": "no"}


def test_parse_frame_event():
    ev = json.dumps({"type": "event", "event": "agent", "payload": {"runId": "r"}, "seq": 7})
    pf = parse_frame(ev)
    assert pf.type is FrameType.EVENT
    assert pf.event.event == "agent"
    assert pf.event.payload == {"runId": "r"}
    assert pf.event.seq == 7


def test_parse_frame_bytes_input():
    pf = parse_frame(build_request("agents.list").encode("utf-8"))
    assert pf.type is FrameType.REQ


def test_parse_frame_unknown_type_raises():
    with pytest.raises(ValueError):
        parse_frame(json.dumps({"type": "bogus", "id": "x"}))


# ── connect challenge ─────────────────────────────────────────────────────

def test_parse_connect_challenge():
    raw = json.dumps({
        "type": "event",
        "event": "connect.challenge",
        "payload": {"nonce": "abc123", "ts": "1700000000"},
    })
    ch = parse_connect_challenge(raw)
    assert ch == {"nonce": "abc123", "ts": "1700000000"}


def test_parse_connect_challenge_wrong_event_raises():
    raw = json.dumps({"type": "event", "event": "agent", "payload": {}})
    with pytest.raises(ValueError):
        parse_connect_challenge(raw)


# ── build_connect ──────────────────────────────────────────────────────────

def test_build_connect_token_only_no_device_signature():
    """Regression guard for the dead-code finding (#4): build_connect must
    not pretend to sign.  It sends token-only auth with no device block."""
    frame = json.loads(build_connect(
        request_id="c1", token="tok",
        challenge_nonce="nonce", challenge_ts="1700000000",
    ))
    assert frame["method"] == "connect"
    assert frame["params"]["auth"] == {"token": "tok"}
    # No device block and no signature field — token-only auth.
    assert "device" not in frame["params"]
    assert "signature" not in frame["params"]["auth"]
    # min/max protocol pinned to v4.
    assert frame["params"]["minProtocol"] == protocol.PROTOCOL_VERSION
    assert frame["params"]["maxProtocol"] == protocol.PROTOCOL_VERSION
    assert frame["params"]["role"] == "operator"


def test_sign_challenge_is_deterministic_hmac():
    """sign_challenge exists but is currently unused (dead code, #4).
    Lock its behaviour in case it gets wired up later."""
    sig_a = sign_challenge("nonce", "1700000000", "secret-token")
    sig_b = sign_challenge("nonce", "1700000000", "secret-token")
    assert sig_a == sig_b
    assert len(sig_a) == 64  # hex-encoded SHA-256
    # Different inputs → different signatures.
    assert sign_challenge("nonce", "1700000000", "other") != sig_a


# ── id generation ──────────────────────────────────────────────────────────

def test_generate_request_id_unique_and_length():
    ids = {generate_request_id() for _ in range(1000)}
    assert len(ids) == 1000  # effectively unique
    assert all(len(i) == 12 for i in ids)


def test_generate_idempotency_key_prefix():
    k = generate_idempotency_key()
    assert k.startswith("owui-")