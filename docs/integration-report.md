# Integration Research Report: OpenClaw → Open WebUI

> Date: 2026-06-20
> Status: Complete
> Recommendation: Open WebUI Pipe Function + OpenClaw Gateway RPC

## Research question

What is the best architecture for integrating OpenClaw into Open WebUI when
the OpenAI-compatible HTTP endpoint is not a viable option?

## Open WebUI findings

**Source:** https://docs.openwebui.com/features/extensibility/

### Pipe Functions are the recommended extension point

Pipe Functions are Python plugins that appear as selectable models in the
Open WebUI chat sidebar.  They are the officially recommended replacement for
the legacy Pipelines system.

The Extensibility overview states:

> "If you want to add a model provider, you almost always want a **Pipe
> Function**, not a Pipeline."

The Pipelines GitHub README opens with: **"DO NOT USE PIPELINES!"**

### Pipes are explicitly for non-OpenAI backends

From the goal-to-extension mapping table:

| Goal | Extension |
|---|---|
| "Add a new model or provider to the model selector" | **Pipe Function** |

From the Under the Hood page:

> "A Pipe replaces the entire LLM call. Open WebUI hands the request and
> expects a response back. Nothing in the middleware constrains the response."

### Pipes support streaming, multiple models, and async

- Streaming via `yield` of SSE-compatible dicts
- Multiple models via the `pipes()` manifold method
- Fully async backend — long-running Pipes don't block other users
- Valves for admin-configurable settings
- `__event_emitter__` for progress/status events
- `__user__` and `__metadata__` reserved arguments for session context

**Sources:**
- https://docs.openwebui.com/features/extensibility/plugin/functions/pipe/
- https://docs.openwebui.com/features/extensibility/plugin/development/events/
- https://docs.openwebui.com/features/extensibility/plugin/development/reserved-args/

## OpenClaw findings

**Source:** https://docs.openclaw.ai/gateway/external-apps

### Gateway RPC is the primary integration surface

> "External apps should talk to OpenClaw through the **Gateway protocol** today."

The Gateway exposes a typed WebSocket API serving control-plane clients
(CLI, macOS app, web UI) and peripheral nodes (iOS, Android, headless).

### The OpenAI-compatible endpoint is secondary

From the External Apps page:

| Interface | Status | When to Use |
|---|---|---|
| Gateway protocol (WebSocket) | Ready | **Primary path** for any external app |
| OpenAI-compatible HTTP API | Ready (opt-in) | Compatibility surface for existing OpenAI tooling |

The HTTP API is disabled by default, stateless by default, has coarse auth,
and explicitly warns to "keep this endpoint on loopback/tailnet/private
ingress only."

**Source:** https://docs.openclaw.ai/gateway/openai-http-api

### Channels are messaging platform adapters, not UI backends

Channels connect messaging platforms (WhatsApp, Telegram, Slack, Discord,
Signal, iMessage, IRC, Teams, Matrix) to the Gateway.  They participate in
the full messaging lifecycle.

An LLM chat UI frontend is not a messaging platform — it's an operator
client.  The channel abstraction would degrade all structured UI
capabilities to plain text.

### Nodes are capability hosts, not UI backends

Nodes provide device-local capabilities (camera, screen, location, voice)
that the agent invokes.  The direction is agent → node.  Open WebUI is the
opposite: user → UI → agent.  The roles are reversed.

### Agent runs use a two-stage streaming pattern

1. Immediate acknowledgement: `{status: "accepted", runId: "..."}`
2. Streaming `agent` events with incremental payloads
3. Final result: `{status: "ok"|"error", runId, summary}`

This maps cleanly to Open WebUI's streaming pattern.

**Sources:**
- https://docs.openclaw.ai/concepts/architecture
- https://docs.openclaw.ai/gateway/protocol

## Alternatives compared

### 1. OpenAI-compatible HTTP API
**Verdict:** ❌ Wrong abstraction. Stateless, coarse auth, lossy mapping of
OpenClaw's agent model into OpenAI's chat completion shape. The user already
found it requires major code changes and still doesn't work correctly.

### 2. OpenClaw channel implementation
**Verdict:** ❌ Wrong abstraction. Channels are for messaging platforms.
Treating Open WebUI as a messaging platform would discard all structured UI
capabilities (streaming tokens, tool cards, status events) and reduce them
to plain text.

### 3. Node-style implementation
**Verdict:** ❌ Wrong direction. Nodes are capability hosts that the agent
invokes. Open WebUI invokes the agent — the direction is reversed.

### 4. Separate bridge service
**Verdict:** ⚠️ Overengineering for single-instance. A standalone service adds
deployment complexity. Worth considering only at multi-instance scale.

### 5. Open WebUI Pipe + Gateway RPC (recommended)
**Verdict:** ✅ Correct architecture. Uses each system at its documented
primary extension point. No extra processes. Full streaming fidelity.

## Implementation risks

| Risk | Mitigation |
|---|---|
| No official Gateway client library | Implement protocol directly on `websockets`; small surface area for v1 |
| WebSocket lifecycle inside async Pipe | Lazy connect, shared connection, reconnection logic |
| Gateway handshake complexity | Token-based auth (token-only; no device keypair, challenge not signed) in v1 |
| Streaming format mismatch | `event_mapper.py` isolates all translation |
| Concurrent `pipe()` calls | `asyncio.Future` map for request ID correlation |

## Verdict

**Open WebUI Pipe Function acting as an OpenClaw Gateway RPC client**
because this uses each system at its officially documented primary extension
point — Pipes are the recommended way to add model providers to Open WebUI,
and Gateway RPC is the documented primary path for external applications to
interact with OpenClaw — while the OpenAI-compatible HTTP API is explicitly
a secondary compatibility surface that the user has already found unreliable
in practice.
