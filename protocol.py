"""
OpenClaw Gateway Protocol v4 — constants, frame helpers, and auth.

This module implements the wire-level Gateway protocol on top of raw
WebSocket text frames.  Everything here is stdlib-only so it can be
used without installing extra packages beyond what Open WebUI provides.

Reference: https://docs.openclaw.ai/gateway/protocol
"""

from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROTOCOL_VERSION = 4
"""Current Gateway protocol version."""

MAX_PRE_HANDSHAKE_BYTES = 64 * 1024  # 64 KiB
"""Maximum frame size before the connect handshake completes."""

DEFAULT_IDEMPOTENCY_TTL_MS = 30_000
"""Server-side deduplication window (inferred from docs)."""


# ---------------------------------------------------------------------------
# Frame types
# ---------------------------------------------------------------------------

class FrameType(str, Enum):
    """Top-level ``type`` field on every Gateway frame."""

    REQ = "req"
    RES = "res"
    EVENT = "event"


class EventName(str, Enum):
    """Well-known server-push event names."""

    CONNECT_CHALLENGE = "connect.challenge"
    AGENT = "agent"
    CHAT = "chat"
    PRESENCE = "presence"
    HEALTH = "health"
    HEARTBEAT = "heartbeat"
    CRON = "cron"
    SHUTDOWN = "shutdown"
    SESSION_MESSAGE = "session.message"
    SESSION_OPERATION = "session.operation"
    SESSION_TOOL = "session.tool"
    SESSIONS_CHANGED = "sessions.changed"


# ---------------------------------------------------------------------------
# Frame dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RequestFrame:
    """Client-initiated RPC call."""

    id: str
    method: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialise this request to a Gateway ``req`` frame JSON string."""
        return json.dumps({
            "type": FrameType.REQ,
            "id": self.id,
            "method": self.method,
            "params": self.params,
        })


@dataclass
class ResponseFrame:
    """Server response paired to a RequestFrame by matching ``id``."""

    id: str
    ok: bool
    payload: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResponseFrame":
        """Build a :class:`ResponseFrame` from a parsed ``res`` frame dict."""
        return cls(
            id=data["id"],
            ok=data.get("ok", False),
            payload=data.get("payload"),
            error=data.get("error"),
        )


@dataclass
class EventFrame:
    """Server-pushed broadcast event."""

    event: str
    payload: dict[str, Any] = field(default_factory=dict)
    seq: int | None = None
    state_version: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventFrame":
        """Build an :class:`EventFrame` from a parsed ``event`` frame dict."""
        return cls(
            event=data["event"],
            payload=data.get("payload", {}),
            seq=data.get("seq"),
            state_version=data.get("stateVersion"),
        )


@dataclass
class ParsedFrame:
    """Union of the three frame types returned by ``parse_frame``."""

    type: FrameType
    request: RequestFrame | None = None
    response: ResponseFrame | None = None
    event: EventFrame | None = None


# ---------------------------------------------------------------------------
# Frame constructors
# ---------------------------------------------------------------------------

def build_request(
    method: str,
    params: dict[str, Any] | None = None,
    *,
    request_id: str | None = None,
    idempotency_key: str | None = None,
) -> str:
    """Build a JSON ``req`` frame string.

    Side-effecting methods (``send``, ``agent``) should include an
    idempotency key so Gateway can safely deduplicate retries.
    """
    # idempotencyKey is a *params* field (e.g. AgentParamsSchema requires
    # it there), NOT a frame-level field — RequestFrameSchema is
    # additionalProperties:false and has no top-level idempotencyKey, so a
    # frame-level copy would be rejected and the required params copy would
    # be missing.
    frame_params: dict[str, Any] = dict(params or {})
    if idempotency_key:
        frame_params["idempotencyKey"] = idempotency_key
    return json.dumps({
        "type": FrameType.REQ,
        "id": request_id or generate_request_id(),
        "method": method,
        "params": frame_params,
    })


def build_connect(
    *,
    request_id: str,
    token: str,
    challenge_nonce: str,
    challenge_ts: str,
    client_id: str = "gateway-client",
    client_version: str = "1.0.0",
    client_platform: str = "open-webui",
) -> str:
    """Build the ``connect`` request frame for the handshake.

    Uses **token-only auth** — the frame carries ``auth.token`` and no
    device block or challenge signature.  The Gateway accepts this when
    ``gateway.auth.token`` is configured and the client presents no
    conflicting device field.

    Parameters
    ----------
    request_id:
        Correlation ID for the connect request.
    token:
        Gateway operator auth token (sent verbatim in ``auth.token``).
    challenge_nonce, challenge_ts:
        Accepted for signature compatibility but **currently unused** —
        the Pipe does not sign the challenge (see :func:`sign_challenge`).
        Kept so a future device-keypair auth path can consume them without
        changing the call site.
    client_id:
        Gateway client identity.  Must be one of the
        ``GatewayClientIdSchema`` enum values (the schema is a strict
        enum, ``additionalProperties:false``); ``"gateway-client"`` is the
        generic backend-integration identity.
    client_version, client_platform:
        Reported to the Gateway as the client identity.
    """
    return json.dumps({
        "type": FrameType.REQ,
        "id": request_id,
        "method": "connect",
        "params": {
            "minProtocol": PROTOCOL_VERSION,
            "maxProtocol": PROTOCOL_VERSION,
            "client": {
                "id": client_id,
                "version": client_version,
                "platform": client_platform,
                "mode": "backend",
            },
            # caps advertise optional client capabilities to the Gateway.
            # "tool-events" is required for the Gateway to direct agent
            # tool/item stream events to this connection (otherwise the
            # Pipe never sees tool-call activity to render as cards).
            "caps": ["tool-events"],
            "role": "operator",
            # operator.approvals is required to call exec.approval.resolve /
            # plugin.approval.resolve when resolving agent tool approvals.
            "scopes": ["operator.read", "operator.write", "operator.approvals"],
            "auth": {"token": token},
        },
    })


# ---------------------------------------------------------------------------
# Frame parser
# ---------------------------------------------------------------------------

def parse_frame(raw: str | bytes) -> ParsedFrame:
    """Parse a Gateway frame from its JSON wire representation."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    data = json.loads(raw)
    frame_type = FrameType(data["type"])

    if frame_type == FrameType.REQ:
        return ParsedFrame(
            type=frame_type,
            request=RequestFrame(
                id=data["id"],
                method=data["method"],
                params=data.get("params", {}),
            ),
        )
    elif frame_type == FrameType.RES:
        return ParsedFrame(
            type=frame_type,
            response=ResponseFrame.from_dict(data),
        )
    elif frame_type == FrameType.EVENT:
        return ParsedFrame(
            type=frame_type,
            event=EventFrame.from_dict(data),
        )
    else:
        raise ValueError(f"Unknown frame type: {data['type']}")


def parse_connect_challenge(raw: str | bytes) -> dict[str, str]:
    """Extract nonce and timestamp from a ``connect.challenge`` event.

    Returns ``{"nonce": "...", "ts": "..."}``.
    """
    frame = parse_frame(raw)
    if frame.event is None or frame.event.event != EventName.CONNECT_CHALLENGE:
        raise ValueError(
            f"Expected connect.challenge event, got "
            f"{frame.event.event if frame.event else 'non-event'}"
        )
    return {
        "nonce": frame.event.payload["nonce"],
        "ts": str(frame.event.payload.get("ts", "")),
    }


# ---------------------------------------------------------------------------
# Challenge signing
# ---------------------------------------------------------------------------

def sign_challenge(nonce: str, ts: str, token: str) -> str:
    """Produce an HMAC-SHA256 signature over ``nonce:ts`` keyed by *token*.

    .. note::
        **Currently unused.**  The Pipe authenticates with token-only auth
        (see :func:`build_connect`) and never calls this.  It is retained as
        a reference implementation for a possible future challenge-signing
        auth path — do not assume it is wired into the handshake.  The
        standalone ``debug_events.py`` script uses Ed25519 device signing
        instead, not this HMAC path.

    Returns the signature as a 64-character lowercase hex string.
    """
    message = f"{nonce}:{ts}".encode("utf-8")
    key = token.encode("utf-8")
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def _derive_device_id(token: str) -> str:
    """Derive a stable 16-char device identifier from *token* (SHA-256 prefix).

    .. note::
        **Currently unused.**  Token-only auth (see :func:`build_connect`)
        does not send a device identity, so this helper is never called.
        Retained for a possible future device-pairing auth path.
    """
    return hashlib.sha256(f"openwebui-pipe:{token}".encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def generate_request_id() -> str:
    """Return a unique request ID for correlating req/res frames."""
    return uuid.uuid4().hex[:12]


def generate_idempotency_key() -> str:
    """Return an idempotency key for side-effecting RPC methods.

    Gateway uses these for short-lived deduplication so retries are safe.
    """
    return f"owui-{uuid.uuid4().hex}"
