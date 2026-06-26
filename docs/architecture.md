# Architecture: OpenClaw → Open WebUI Integration

## Overview

This integration makes OpenClaw agents available as selectable models in the
Open WebUI chat interface.  It uses the **Open WebUI Pipe Function** extension
mechanism on one side and the **OpenClaw Gateway WebSocket RPC protocol** on the
other — the officially documented primary extension point for each system.

```
┌─────────────────────────────────────────────────────────┐
│  Open WebUI                                             │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Browser Chat UI                                  │  │
│  │  Model selector: "OpenClaw/Default", etc.         │  │
│  └──────────────────┬────────────────────────────────┘  │
│                     │ HTTP/SSE (internal)                │
│  ┌──────────────────▼────────────────────────────────┐  │
│  │  Pipe Function (openclaw_pipe.py)                 │  │
│  │                                                   │  │
│  │  pipes() → exposes OpenClaw agents as models      │  │
│  │  pipe()  → translates OWUI requests → Gateway RPC │  │
│  │            translates Gateway events → SSE chunks  │  │
│  │                                                   │  │
│  │  ┌─────────────┐  ┌──────────────┐               │  │
│  │  │ Valves      │  │ GatewayClient│               │  │
│  │  │ (config)    │  │ (WebSocket)  │               │  │
│  │  └─────────────┘  └──────┬───────┘               │  │
│  └───────────────────────────┼───────────────────────┘  │
└──────────────────────────────┼──────────────────────────┘
                               │ WebSocket (Gateway Protocol v4)
                               │ connect handshake → req/res/event
┌──────────────────────────────┼──────────────────────────┐
│  OpenClaw Gateway            │                          │
│  ┌───────────────────────────▼────────────────────────┐  │
│  │  WebSocket Server (ws://127.0.0.1:18789)          │  │
│  │  Operator client authenticated with token          │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                                │
│  ┌──────────────────────▼────────────────────────────┐  │
│  │  Agent Runner                                     │  │
│  │  Think → Act → Observe → Repeat                   │  │
│  │  Context Builder → Planner → Executor → Composer  │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Component map

| File | Role |
|---|---|
| `openclaw_pipe.py` | Pipe Function entry point loaded by Open WebUI |
| `gateway_client.py` | Persistent WebSocket client with RPC, streaming, reconnection, and approval resolution |
| `protocol.py` | Protocol v4 constants, frame constructors/parsers, token-only connect handshake (`sign_challenge` / `_derive_device_id` are currently-unused helpers for a future device-auth path) |
| `event_mapper.py` | Gateway agent events → Open WebUI SSE chunk translation |
| `valves.py` | Pydantic `Valves` schema — admin-configurable settings |
| `telemetry.py` | OpenTelemetry traces + metrics + logs — spans, counters, histogram, gauge, and Python-logging→OTel bridge with trace context. Piggybacks on OWUI's native OTel; degrades to no-ops when OTel is off. |
| `install.py` | Bundler + installer — pushes directly to OWUI via REST API (stdlib-only) |
| `debug_events.py` | Diagnostic script — connects to Gateway and dumps raw frames for debugging |

## Why not the OpenAI-compatible HTTP API?

The Gateway's `/v1/chat/completions` endpoint is:

- **Disabled by default** — must be explicitly enabled
- **Stateless by default** — each request gets a new session unless `user` is
  manually provided
- **Coarse auth** — a valid token grants full operator access
- **Lossy** — OpenClaw's agent model is sessionful and tool-mediated; the
  OpenAI API surface is stateless and single-turn
- **Secondary** — the OpenClaw docs frame it as a "compatibility surface for
  existing OpenAI tooling", not the primary path

The Gateway RPC protocol is the documented primary integration surface for
external applications.  It gives us full access to session management,
streaming agent events, tool visibility, and proper error semantics.

## Why not a channel?

Channels are OpenClaw's abstraction for **messaging platform adapters**
(WhatsApp, Telegram, Slack, Discord, etc.).  They participate in the full
messaging lifecycle: inbound message claims, delivery receipts, typing
indicators, reactions.

Open WebUI is not a messaging platform — it's a chat UI frontend that presents
its own model selector, streaming token display, tool call cards, and session
management.  Treating it as a channel would degrade all of those structured UI
capabilities to plain text, discarding the rich integration Open WebUI
provides.

## Why not a node?

Nodes are **capability hosts** (camera, screen, location, voice) that the
agent invokes.  The invocation direction is agent → node.  Open WebUI is the
opposite: user → Open WebUI → agent.  The roles are reversed.

## Approval flow

When an OpenClaw agent wants to run a tool that requires user approval, the
Gateway sends an ``agent`` event with ``stream: "approval"`` and
``data.phase: "requested"``.  The Pipe resolves it according to
``APPROVAL_MODE`` via the ``exec.approval.resolve`` (or
``plugin.approval.resolve``) RPC, which takes ``{id, decision}`` and
requires the ``operator.approvals`` scope:

```
Gateway ── agent stream="approval" phase="requested" ──► Pipe ──► OWUI browser
                                    │
  auto_deny:    ◄── deny  ──────────┤
  auto_approve: ◄── allow ──────────┤
  render:       ◄── deny after Ns ───┤ (shows card first)
  interactive:  ◄───────────────────► __event_call__ confirmation dialog
                     user clicks         (WebSocket back-channel)
                     Approve or Deny
```

Interactive mode uses Open WebUI's ``__event_call__`` — a bidirectional
WebSocket mechanism that pauses the Pipe mid-stream until the user responds.
Falls back to auto-deny when the back-channel is unavailable (older OWUI,
tab disconnected, non-streaming mode).

## Session mapping

Session keys follow the format ``owui:user:<id>:chat:<id>:agent:<id>``,
scoped so each OWUI chat gets its own Gateway session and switching agents
within a chat isolates context.

```
Open WebUI user + chat + agent ──► owui:user:abc:chat:xyz:agent:default
```

Same chat, same agent → conversation continuity.  New chat or different agent
→ fresh Gateway session.
