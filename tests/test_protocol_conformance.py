"""Protocol-v4 conformance tests for the wire frames the Pipe emits.

Unlike the rest of the suite, these tests are an **independent oracle**: they
validate the frames ``build_connect`` / ``build_request`` produce against the
*real* OpenClaw Gateway v4 schema constraints (encoded below from
``openclaw/openclaw`` source), NOT against the Pipe's own assumptions.

This is the layer that was missing.  The internal tests feed the code the
shapes the code expects and therefore pass whether or not those shapes match
the real protocol — which is how wrong method names, a top-level
``idempotencyKey``, and an invalid ``client.id`` all shipped green.  These
tests pin the actual contract so a regression against the v4 schema fails
here regardless of what the Pipe's internal code believes.

Caveat: the constraints below are hand-encoded from a reading of the
OpenClaw TypeBox schemas (``packages/gateway-protocol/src/schema/*.ts``,
``src/protocol/client-info.ts``).  The gold standard remains validating
against the gateway package's emitted JSON schemas or a live Gateway; this
is a static, dependency-free approximation of that.
"""
import json

import pytest

from protocol import (
    PROTOCOL_VERSION,
    build_connect,
    build_request,
    generate_idempotency_key,
    generate_request_id,
)

# ---------------------------------------------------------------------------
# Real v4 constraints (encoded from openclaw/openclaw schemas)
# ---------------------------------------------------------------------------

# src/protocol/client-info.ts — strict enums.
GATEWAY_CLIENT_IDS = {
    "webchat-ui", "openclaw-control-ui", "openclaw-tui", "webchat", "cli",
    "gateway-client", "openclaw-macos", "openclaw-ios", "openclaw-android",
    "node-host", "test", "fingerprint", "openclaw-probe",
}
GATEWAY_CLIENT_MODES = {"webchat", "cli", "ui", "backend", "node", "probe", "test"}

# schema/frames.ts — RequestFrameSchema is additionalProperties:false with
# exactly these top-level fields.  There is NO frame-level idempotencyKey.
REQUEST_FRAME_FIELDS = {"type", "id", "method", "params"}

# schema/frames.ts — ConnectParamsSchema (additionalProperties:false).
CONNECT_PARAMS_FIELDS = {
    "minProtocol", "maxProtocol", "client", "caps", "commands", "permissions",
    "pathEnv", "role", "scopes", "device", "auth", "locale", "userAgent",
}
CONNECT_CLIENT_FIELDS = {
    "id", "displayName", "version", "platform", "deviceFamily",
    "modelIdentifier", "mode", "instanceId",
}
CONNECT_AUTH_FIELDS = {
    "token", "bootstrapToken", "deviceToken", "password",
    "approvalRuntimeToken",
}

# schema/agent.ts — AgentParamsSchema (additionalProperties:false).
# Required: message, idempotencyKey.  The Pipe must NOT send messages /
# systemPrompt / files / tools / OAI model params — none are in this set.
AGENT_PARAMS_FIELDS = {
    "message", "agentId", "provider", "model", "to", "replyTo", "sessionId",
    "sessionKey", "thinking", "deliver", "attachments", "channel",
    "replyChannel", "accountId", "replyAccountId", "threadId", "groupId",
    "groupChannel", "groupSpace", "timeout", "bestEffortDeliver", "lane",
    "cleanupBundleMcpOnRunEnd", "modelRun", "promptMode", "extraSystemPrompt",
    "bootstrapContextMode", "bootstrapContextRunKind", "acpTurnSource",
    "internalRuntimeHandoffId", "execApprovalFollowupExpectedSessionId",
    "internalEvents", "inputProvenance", "suppressPromptPersistence",
    "sessionEffects", "sourceReplyDeliveryMode", "disableMessageTool",
    "voiceWakeTrigger", "idempotencyKey", "label",
}
AGENT_PARAMS_REQUIRED = {"message", "idempotencyKey"}
# Fields old versions of the Pipe sent that the v4 schema rejects.
FORBIDDEN_AGENT_PARAMS = {
    "messages", "systemPrompt", "files", "tools",
    "temperature", "top_p", "max_tokens", "max_completion_tokens", "stop",
    "frequency_penalty", "presence_penalty", "seed", "reasoning_effort",
    "response_format",
}

# schema/exec-approvals.ts — ExecApprovalResolveParamsSchema
# (additionalProperties:false): exactly {id, decision}, both required.
APPROVAL_RESOLVE_FIELDS = {"id", "decision"}

# schema/sessions.ts — SessionsAbortParamsSchema (additionalProperties:false).
SESSIONS_ABORT_FIELDS = {"key", "runId", "agentId"}

# Real method names the Pipe uses (confirmed against the method registry /
# protocol doc).  ``approval.resolve`` and ``approval.requested`` are NOT
# real — the v4 names are ``exec.approval.resolve`` / ``exec.approval.requested``.
REAL_METHODS = {
    "connect": "connect",
    "agent": "agent",
    "agents_list": "agents.list",
    "approval_resolve_exec": "exec.approval.resolve",
    "approval_resolve_plugin": "plugin.approval.resolve",
    "sessions_abort": "sessions.abort",
}


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

def _nonempty_str(v) -> bool:
    return isinstance(v, str) and len(v) > 0


def validate_request_frame(frame: dict) -> None:
    """RequestFrameSchema: exactly {type,id,method,params}, all populated."""
    assert set(frame) == REQUEST_FRAME_FIELDS, (
        f"frame has fields outside RequestFrameSchema "
        f"(additionalProperties:false): {set(frame) - REQUEST_FRAME_FIELDS}"
    )
    assert frame["type"] == "req"
    assert _nonempty_str(frame["id"])
    assert _nonempty_str(frame["method"])
    assert isinstance(frame["params"], dict)


def validate_agent_params(params: dict) -> None:
    """AgentParamsSchema (additionalProperties:false)."""
    keys = set(params)
    extra = keys - AGENT_PARAMS_FIELDS
    assert not extra, f"AgentParamsSchema rejects unknown fields: {extra}"
    forbidden = keys & FORBIDDEN_AGENT_PARAMS
    assert not forbidden, f"AgentParamsSchema rejects legacy fields: {forbidden}"
    missing = AGENT_PARAMS_REQUIRED - keys
    assert not missing, f"AgentParamsSchema requires: {missing}"
    assert _nonempty_str(params["message"]), "message must be a non-empty string"
    assert _nonempty_str(params["idempotencyKey"]), "idempotencyKey required"


def validate_connect_params(params: dict) -> None:
    """ConnectParamsSchema (additionalProperties:false) + client/auth sub-objects."""
    extra = set(params) - CONNECT_PARAMS_FIELDS
    assert not extra, f"ConnectParamsSchema rejects unknown fields: {extra}"
    assert isinstance(params["minProtocol"], int) and params["minProtocol"] >= 1
    assert isinstance(params["maxProtocol"], int) and params["maxProtocol"] >= 1

    client = params["client"]
    assert set(client) <= CONNECT_CLIENT_FIELDS, (
        f"client sub-object rejects: {set(client) - CONNECT_CLIENT_FIELDS}"
    )
    assert client["id"] in GATEWAY_CLIENT_IDS, (
        f"client.id {client['id']!r} not in GatewayClientIdSchema enum"
    )
    assert client["mode"] in GATEWAY_CLIENT_MODES, (
        f"client.mode {client['mode']!r} not in GatewayClientModeSchema enum"
    )
    assert _nonempty_str(client["version"])
    assert _nonempty_str(client["platform"])

    if "caps" in params:
        assert isinstance(params["caps"], list)
        assert all(_nonempty_str(c) for c in params["caps"])
    if "scopes" in params:
        assert isinstance(params["scopes"], list)
        assert all(_nonempty_str(s) for s in params["scopes"])
    if "auth" in params:
        auth = params["auth"]
        assert set(auth) <= CONNECT_AUTH_FIELDS, (
            f"auth sub-object rejects: {set(auth) - CONNECT_AUTH_FIELDS}"
        )


# ---------------------------------------------------------------------------
# Tests: connect frame
# ---------------------------------------------------------------------------

def test_connect_frame_conforms_to_v4_schema():
    frame = json.loads(build_connect(
        request_id="c1", token="tok",
        challenge_nonce="n", challenge_ts="1700000000",
    ))
    validate_request_frame(frame)
    assert frame["method"] == REAL_METHODS["connect"]
    validate_connect_params(frame["params"])
    # operator.approvals is required to resolve agent tool approvals.
    assert "operator.approvals" in frame["params"]["scopes"]
    # tool-events cap is required for the Gateway to direct tool stream events.
    assert "tool-events" in frame["params"]["caps"]
    # Token-only auth: no device block.
    assert "device" not in frame["params"]


def test_connect_client_id_must_be_enum_value():
    """Regression: 'openwebui-pipe' is NOT a valid GatewayClientId."""
    frame = json.loads(build_connect(
        request_id="c1", token="tok",
        challenge_nonce="n", challenge_ts="1700000000",
        client_id="openwebui-pipe",  # the old, invalid value
    ))
    with pytest.raises(AssertionError):
        validate_connect_params(frame["params"])


# ---------------------------------------------------------------------------
# Tests: agent request frame
# ---------------------------------------------------------------------------

def test_agent_request_frame_conforms_to_v4_schema():
    params = {
        "agentId": "default",
        "message": "Hello",
        "sessionKey": "owui:user:u:chat:c:agent:default",
        "extraSystemPrompt": "be brief",
        "attachments": [{"name": "f.txt", "mimeType": "text/plain", "data": "aGk="}],
    }
    frame = json.loads(build_request(
        REAL_METHODS["agent"], params, idempotency_key=generate_idempotency_key(),
    ))
    validate_request_frame(frame)
    assert frame["method"] == REAL_METHODS["agent"]
    # idempotencyKey is a PARAMS field, not a frame-level field.
    assert "idempotencyKey" not in frame, "idempotencyKey must not be frame-level"
    validate_agent_params(frame["params"])


def test_agent_request_with_legacy_fields_is_rejected_by_schema():
    """The v4 schema (additionalProperties:false) rejects the fields old
    versions of the Pipe sent.  This test documents WHY they must not be
    forwarded, so re-adding them fails here."""
    bad_params = {
        "agentId": "default",
        "message": "Hello",
        "idempotencyKey": "k-1",
        "messages": [{"role": "user", "content": "Hello"}],
        "systemPrompt": "be brief",
        "files": [],
        "tools": [],
        "temperature": 0.7,
    }
    with pytest.raises(AssertionError):
        validate_agent_params(bad_params)


def test_oracle_catches_the_frame_level_idempotency_key_bug():
    """Self-check: the oracle MUST reject the exact bug that shipped
    (idempotencyKey at the frame top level, missing from params).  If this
    test ever passes against a frame shaped like the old buggy output, the
    oracle is toothless."""
    buggy_frame = {
        "type": "req",
        "id": "a1",
        "method": "agent",
        "params": {  # missing idempotencyKey
            "agentId": "default",
            "message": "Hello",
        },
        "idempotencyKey": "owui-x",  # at the frame top level — wrong
    }
    # RequestFrameSchema (additionalProperties:false) rejects the extra field.
    with pytest.raises(AssertionError):
        validate_request_frame(buggy_frame)
    # AgentParamsSchema rejects params missing the required idempotencyKey.
    with pytest.raises(AssertionError):
        validate_agent_params(buggy_frame["params"])


# ---------------------------------------------------------------------------
# Tests: approval resolve + sessions abort frames
# ---------------------------------------------------------------------------

def test_exec_approval_resolve_frame_conforms_to_v4_schema():
    frame = json.loads(build_request(
        REAL_METHODS["approval_resolve_exec"],
        {"id": "ap-1", "decision": "deny"},
    ))
    validate_request_frame(frame)
    assert frame["method"] == REAL_METHODS["approval_resolve_exec"]
    # ExecApprovalResolveParamsSchema: exactly {id, decision}, no idempotency key.
    assert set(frame["params"]) == APPROVAL_RESOLVE_FIELDS
    assert _nonempty_str(frame["params"]["id"])
    assert frame["params"]["decision"] in {"allow-once", "allow-always", "deny"}
    assert "idempotencyKey" not in frame["params"]


def test_the_old_approval_resolve_method_name_is_not_real():
    """Regression: 'approval.resolve' is not a real v4 method."""
    # The registry exposes exec.approval.resolve and plugin.approval.resolve
    # only — there is no bare 'approval.resolve'.
    assert "approval.resolve" not in {
        REAL_METHODS["approval_resolve_exec"],
        REAL_METHODS["approval_resolve_plugin"],
    }


def test_sessions_abort_frame_conforms_to_v4_schema():
    frame = json.loads(build_request(
        REAL_METHODS["sessions_abort"],
        {"key": "owui:user:u:chat:c:agent:default"},
    ))
    validate_request_frame(frame)
    assert frame["method"] == REAL_METHODS["sessions_abort"]
    assert set(frame["params"]) <= SESSIONS_ABORT_FIELDS
    # The old code sent {agentId, sessionKey}; 'sessionKey' is not a field.
    assert "sessionKey" not in SESSIONS_ABORT_FIELDS
    assert "idempotencyKey" not in frame["params"]