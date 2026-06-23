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
Gateway RPC request:
{
  "type": "req",
  "id": "a1b2c3d4e5f6",
  "method": "agent",
  "params": {
    "agentId": "default",
    "messages": [{"role": "user", "content": "Hello"}],
    "sessionKey": "owui-user:user_abc123"
  }
}
```

## Response / streaming mapping

| Gateway event | Open WebUI output |
|---|---|
| `res {ok: true, payload: {status: "accepted", runId: "..."}}` | (internal — starts the event stream) |
| `event {event: "agent", payload: {runId, delta: {content: "Hello"}}}` | `yield {"choices": [{"delta": {"content": "Hello"}, "finish_reason": null}]}` |
| `event {event: "agent", payload: {runId, delta: {toolCall: {...}}}}` | `yield "<details type='tool_calls' ...>"` (HTML string) |
| `event {event: "agent", payload: {runId, thinking: "..."}}` | `__event_emitter__({"type": "status", "data": {...}})` |
| `res {ok: true, payload: {status: "ok", runId, summary: "..."}}` | `yield {"choices": [{"delta": {"content": ""}, "finish_reason": "stop"}]}` |
| `res {ok: false, payload: {status: "error", error: "..."}}` | `yield {"choices": [{"delta": {"content": "[Error: ...]"}, "finish_reason": "stop"}]}` |

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

| Gateway event | Pipe resolution | OWUI output |
|---|---|---|
| `event {event: "approval.requested", payload: {runId, request: {toolName, arguments}}}` | ``approval.resolve`` RPC with approve/deny | Depends on ``APPROVAL_MODE`` |
| `auto_deny` | Denies immediately | ``{"kind": "delta", "delta": {"status": "Auto-denied approval for tool: ..."}}`` |
| `auto_approve` | Approves immediately | (silent) |
| `render` | Denies after ``APPROVAL_TIMEOUT`` | Yields approval card, then auto-deny note |
| `interactive` | Waits for user via ``__event_call__`` confirmation dialog | Browser dialog → approve/deny RPC |

## Auth mapping

| Gateway | Pipe |
|---|---|
| `role: "operator"` | Hardcoded — the Pipe always acts as an operator |
| `scopes: ["operator.read", "operator.write"]` | Hardcoded — minimum needed for chat |
| `auth.token` | `valves.GATEWAY_TOKEN` (set by admin) |
| Device identity | **None** — token-only auth; no device block is sent |
| Challenge signing | **None** — the Pipe sends `auth.token` only and does *not* sign the challenge. The `connect.challenge` event is still consumed (to drain the socket and validate it), but `sign_challenge()` / `_derive_device_id()` in `protocol.py` are currently-unused helpers retained for a possible future device-keypair auth path. `debug_events.py` exercises real Ed25519 device signing; the Pipe does not. |
