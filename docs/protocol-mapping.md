# Gateway Protocol → Open WebUI Concept Mapping

This document maps OpenClaw Gateway Protocol concepts to their Open WebUI
Pipe Function equivalents.  Use it to understand how the two systems'
abstractions fit together and to debug translation issues.

## Frame-level mapping

| Gateway Protocol | Open WebUI Pipe |
|---|---|
| WebSocket text frame (`type: "req"`) | Pipe receives `pipe(body)` call |
| WebSocket text frame (`type: "res"`) | Pipe `yield`s SSE chunk |
| WebSocket text frame (`type: "event"`) | Pipe `yield`s SSE chunk or calls `__event_emitter__` |
| `connect` handshake | Pipe `__init__` + lazy connect on first `pipe()` call |
| Request `id` correlation | `asyncio.Future` map inside `GatewayClient` |
| Idempotency key | `generate_idempotency_key()` for `agent` RPC |

## Request flow

```
OWUI user sends "Hello"
  │
  ▼
body = {
  "model": "openclaw/default",
  "messages": [{"role": "user", "content": "Hello"}],
  "stream": true
}
  │  Pipe.pipe(body)
  ▼
Gateway RPC request (AgentParamsSchema, additionalProperties:false):
{
  "type": "req",
  "id": "a1b2c3d4e5f6",
  "method": "agent",
  "idempotencyKey": "owui-...",
  "params": {
    "agentId": "default",
    "message": "Hello",
    "sessionKey": "owui:user:...:chat:...:agent:default",
    "extraSystemPrompt": "...",     // optional, from system messages
    "attachments": [...]            // optional, from uploaded files
  }
}
```

The agent RPC takes a single `message` string (the Gateway session holds
prior conversation history); OWUI tool definitions and OAI model params are
not forwarded — `AgentParamsSchema` rejects unknown fields.

## Response / streaming mapping

Agent runs stream ``AgentEventPayload`` frames under the ``agent`` event
name, discriminated by ``payload.stream``:

| Gateway event | Open WebUI output |
|---|---|
| `res {ok: true, payload: {status: "accepted", runId}}` | (internal — starts the event stream) |
| `event {event: "agent", payload: {runId, stream: "assistant", data: {delta: "Hel"}}}` | `yield {"choices": [{"delta": {"content": "Hel"}, "finish_reason": null}]}` |
| `event {event: "agent", payload: {runId, stream: "assistant", data: {text: "Hello"}}}` | diffed against cumulative snapshot → suffix delta |
| `event {event: "agent", payload: {runId, stream: "tool", data: {phase, title, status, ...}}}` | `yield "<details type='tool_calls' ...>"` (HTML string) |
| `event {event: "agent", payload: {runId, stream: "command_output", data: {output, exitCode}}}` | `yield "<details type='tool_calls' ...>"` (HTML string) |
| `event {event: "agent", payload: {runId, stream: "thinking", data: {delta}}}` | `__event_emitter__({"type": "status", "data": {...}})` |
| `event {event: "agent", payload: {runId, stream: "approval", data: {phase: "requested", ...}}}` | approval handling (see below) |
| `res {ok: true, payload: {status: "ok", runId, summary}}` | `yield {"choices": [{"delta": {"content": ""}, "finish_reason": "stop"}]}` |
| `res {ok: false, payload: {status: "error", error}}` | `yield {"choices": [{"delta": {"content": "[Error: ...]"}, "finish_reason": "stop"}]}` |

## Tool call rendering

**Critical rule from Open WebUI docs:**

> Do NOT emit `delta.tool_calls` — this triggers Open WebUI's tool-execution
> retry loop (up to 256 iterations).

Instead, the Pipe renders tool calls as HTML `<details>` blocks:

```html
<details type="tool_calls" done="true" id="call_123" name="web_search"
         arguments="{&quot;query&quot;: &quot;...&quot;}">
<summary>Tool: web_search</summary>
Search results here...
</details>
```

Open WebUI recognises `type="tool_calls"` and renders these as expandable
tool-execution cards in the chat UI, without triggering the internal retry
loop.

## Session mapping

| Open WebUI concept | Gateway concept |
|---|---|
| User ID (`__user__["id"]`) | `sessionKey: "owui-user:<id>"` |
| Chat ID (`__metadata__["chat_id"]`) | `sessionKey: "owui-chat:<id>"` (fallback) |
| New chat | Same `sessionKey` → conversation continuity |
| Multiple users | Different `sessionKey` → isolated sessions |

## Status events

| Gateway | Open WebUI `__event_emitter__` |
|---|---|
| Agent start | `{"type": "status", "data": {"description": "OpenClaw/default is thinking...", "done": false}}` |
| Thinking step | `{"type": "status", "data": {"description": "<thinking text>", "done": false}}` |
| Run complete | `{"type": "status", "data": {"description": "OpenClaw/default finished", "done": true}}` |

## Approval mapping

Agent tool approvals travel **in the agent event stream** as
``stream: "approval"`` events (``AgentApprovalEventData``:
``phase`` / ``kind`` / ``status`` / ``title`` / ``approvalId`` /
``command`` …), not as standalone ``exec.approval.requested`` events.
The Pipe resolves them via the ``exec.approval.resolve`` (or
``plugin.approval.resolve``) RPC, which takes ``{id, decision}`` where
``decision`` is ``"allow-once"`` / ``"allow-always"`` / ``"deny"`` and
requires the ``operator.approvals`` scope.

| Gateway event | Pipe resolution | OWUI output |
|---|---|---|
| `event {event: "agent", payload: {runId, stream: "approval", data: {phase: "requested", approvalId, kind, title, command}}}` | `exec.approval.resolve` / `plugin.approval.resolve` with `{id, decision}` | Depends on ``APPROVAL_MODE`` |
| `auto_deny` | `decision: "deny"` immediately | approval card (resolved/denied) |
| `auto_approve` | `decision: "allow-once"` immediately | (silent) |
| `render` | `decision: "deny"` after ``APPROVAL_TIMEOUT`` | approval-request card, then auto-deny |
| `interactive` | Waits for user via ``__event_call__`` confirmation dialog | Browser dialog → approve/deny RPC |

## Auth mapping

| Gateway | Pipe |
|---|---|
| `role: "operator"` | Hardcoded — the Pipe always acts as an operator |
| `scopes: ["operator.read", "operator.write", "operator.approvals"]` | Hardcoded — `operator.approvals` is required to resolve agent tool approvals |
| `caps: ["tool-events"]` | Hardcoded — required for the Gateway to direct agent `tool`/`item` stream events to this connection |
| `auth.token` | `valves.GATEWAY_TOKEN` (set by admin) |
| Device identity | **None** — token-only auth; no device block is sent |
| Challenge signing | **None** — the Pipe sends `auth.token` only and does *not* sign the challenge. The `connect.challenge` event is still consumed (to drain the socket and validate it), but `sign_challenge()` / `_derive_device_id()` in `protocol.py` are currently-unused helpers retained for a possible future device-keypair auth path. `debug_events.py` exercises real Ed25519 device signing; the Pipe does not. |
