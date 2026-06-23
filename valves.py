"""
Valves configuration schema for the OpenClaw Pipe Function.

These fields appear in the Open WebUI admin UI when configuring the Pipe.
Admins set the Gateway URL, auth token, agent exposure, and resilience
parameters here.
"""

from typing import Literal

from pydantic import BaseModel, Field


class Valves(BaseModel):
    """
    Admin-configurable parameters for the OpenClaw Gateway connection.

    All fields are optional with sensible defaults.  GATEWAY_URL and
    GATEWAY_TOKEN are the only fields that must be set for the Pipe to
    function.
    """

    GATEWAY_URL: str = Field(
        default="ws://127.0.0.1:18789",
        description="WebSocket URL of the OpenClaw Gateway.  Defaults to the "
        "standard loopback bind.  Change this if the Gateway runs on "
        "a different host or port.",
    )

    GATEWAY_TOKEN: str = Field(
        default="",
        description="Authentication token for Gateway operator access.  "
        "Must be a valid Gateway token with operator.read and "
        "operator.write scopes.",
    )

    AGENT_PREFIX: str = Field(
        default="OpenClaw/",
        description="Prefix shown before each agent name in the Open WebUI "
        "model selector.  E.g. 'OpenClaw/default', 'OpenClaw/coding'.",
    )

    AGENT_LIST: str = Field(
        default="__auto__",
        description="Comma-separated list of OpenClaw agent IDs to expose as "
        "models, or '__auto__' to auto-discover via Gateway RPC.  "
        'Examples: "default" or "default,coding,research".',
    )

    REQUEST_TIMEOUT: int = Field(
        default=120,
        ge=10,
        le=600,
        description="Seconds to wait for a single Gateway RPC response, and "
        "per-event in a streaming agent run (each delta must arrive within "
        "this window).  Note: this is NOT a wall-clock cap on a whole "
        "streaming run — a long but continuously-streaming run never times "
        "out.  Gateway will abort the run if it exceeds this.",
    )

    MAX_RECONNECT_ATTEMPTS: int = Field(
        default=5,
        ge=0,
        le=20,
        description="How many times to retry the WebSocket connection before "
        "giving up and returning an error to the user.  0 = no retries.",
    )

    RECONNECT_BASE_DELAY: float = Field(
        default=1.0,
        ge=0.1,
        le=30.0,
        description="Initial backoff delay in seconds for reconnection.  "
        "Doubles on each subsequent attempt (exponential backoff).",
    )

    # ------------------------------------------------------------------
    # Approval
    # ------------------------------------------------------------------

    APPROVAL_MODE: Literal["auto_deny", "auto_approve", "render", "interactive"] = Field(
        default="auto_deny",
        description="How the Pipe handles tool-call approval requests from "
        "the Gateway.  Options: 'auto_deny' (safe — all approvals "
        "rejected), 'auto_approve' (trusted environments — all "
        "approvals granted), 'render' (show approval card in chat "
        "then auto-deny after APPROVAL_TIMEOUT seconds), "
        "'interactive' (prompt user via confirmation dialog; "
        "requires a recent Open WebUI with __event_call__ support).  "
        "Interactive mode falls back to auto-deny when the back-channel "
        "is unavailable.",
    )

    APPROVAL_TIMEOUT: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Seconds before auto-deny in 'render' approval mode.",
    )

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    MESSAGE_MODE: Literal["last", "full"] = Field(
        default="last",
        description="Which messages to forward to the Gateway.  'last' "
        "sends only the newest user message (default — Gateway is "
        "stateful and already has the full conversation in its "
        "session).  'full' sends the entire message history on "
        "every request (useful for stateless backends or debugging).",
    )
