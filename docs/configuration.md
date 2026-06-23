# Configuration Guide

## Installing the Pipe

1. In Open WebUI, go to **Workspace → Functions**.
2. Click **+** and select **Pipe**.
3. Paste the contents of `openclaw_pipe.py` (and its companion modules, or
   a single bundled file).
4. Click **Save**.

## Valves reference

All configuration lives in the **Valves** section of the function editor.
Click the gear icon next to the Pipe name.

| Valve | Default | Description |
|---|---|---|
| `GATEWAY_URL` | `ws://127.0.0.1:18789` | WebSocket URL of the OpenClaw Gateway. Change if Gateway runs on a different host or port. |
| `GATEWAY_TOKEN` | _(empty)_ | Authentication token for Gateway operator access. Must be a valid token with `operator.read` and `operator.write` scopes. |
| `AGENT_PREFIX` | `OpenClaw/` | Prefix shown before each agent name in the model selector. |
| `AGENT_LIST` | `__auto__` | Comma-separated agent IDs (e.g. `"default,coding"`) or `__auto__` for discovery via Gateway RPC. |
| `REQUEST_TIMEOUT` | `120` | Maximum seconds to wait for an agent run. Gateway will abort the run if exceeded. Range: 10–600. |
| `MAX_RECONNECT_ATTEMPTS` | `5` | How many times to retry a broken WebSocket connection before returning an error. 0 = no retries. Range: 0–20. |
| `RECONNECT_BASE_DELAY` | `1.0` | Initial backoff delay in seconds. Doubles on each retry (exponential backoff). Range: 0.1–30.0. |

### Approval

| Valve | Default | Description |
|---|---|---|
| `APPROVAL_MODE` | `auto_deny` | How tool-call approvals are resolved. `auto_deny` (safe, all rejected), `auto_approve` (trusted, all granted), `render` (show card, auto-deny after timeout), `interactive` (browser confirmation dialog via `__event_call__`). Interactive falls back to auto-deny on older OWUI. |
| `APPROVAL_TIMEOUT` | `30` | Seconds before auto-deny in `render` mode. Range: 5–300. |

### OpenTelemetry

OpenTelemetry is controlled entirely by Open WebUI's `ENABLE_OTEL`
environment variable.  When enabled, the Pipe automatically joins
OWUI's tracing, metrics, and logging pipelines — no Pipe-level
valves needed.  See [OpenTelemetry tracing](#opentelemetry-tracing)
below for details.

## Gateway setup

The Pipe connects as an **operator** client over the Gateway WebSocket
protocol.  You need:

### 1. Gateway running and reachable

By default the Gateway binds to `127.0.0.1:18789` (loopback).  If Open WebUI
runs on the same host, the default `ws://127.0.0.1:18789` works directly.

If they run on different hosts, you must either:
- Tunnel the Gateway port (SSH, Tailscale, WireGuard)
- Bind the Gateway to a non-loopback interface (**not recommended** — the docs
  warn to keep it on private ingress)

### 2. Auth token

Generate a Gateway token with operator scopes.  Consult the OpenClaw Gateway
runbook for token generation (`openclaw gateway token create` or equivalent).

The token needs at minimum:
- `operator.read`
- `operator.write`

### 3. Firewall

Ensure the Gateway port is reachable from the Open WebUI host.  For same-host
setups no changes are needed.

## Agent configuration

### Static list

Set `AGENT_LIST` to a comma-separated list of agent IDs:

```
default,coding,research
```

Each appears as `OpenClaw/<id>` in the model selector.

### Auto-discovery

Set `AGENT_LIST` to `__auto__` (the default).  The Pipe will call
`agents.list` RPC on first connection and expose whatever agents the Gateway
reports.

## Troubleshooting

### "OpenClaw Gateway unavailable"

- Verify the Gateway is running: `openclaw gateway status` or check the process.
- Verify the WebSocket URL is correct.  The Pipe connects to `ws://...` not `http://...`.
- Check firewall/network between Open WebUI and Gateway.

### "Connect rejected"

- Verify `GATEWAY_TOKEN` is set and valid.
- Check the Gateway logs for auth failures.
- Ensure the token has `operator.read` and `operator.write` scopes.

### "Agent run timed out"

- Increase `REQUEST_TIMEOUT` (max 600s).
- Check if the Gateway is overloaded.
- The agent may be stuck in a tool loop — check Gateway logs.

### No models in selector

- Check `AGENT_LIST` is not empty.
- If using `__auto__`, verify `agents.list` RPC succeeds (check Gateway logs).
- Reload the Open WebUI page after changing Valves.

### Tool calls render as raw HTML

This is expected.  Open WebUI renders `<details type="tool_calls">` blocks
as expandable cards.  If you see raw HTML instead:
- Ensure the Pipe is yielding the HTML string as an SSE chunk (not wrapping
  it in `delta.content`).
- Check that `event_mapper._render_tool_call` is returning a plain string,
  not a dict.

## OpenTelemetry tracing

The Pipe emits all three OpenTelemetry signals — **traces** (spans),
**metrics** (counters, histograms, gauges), and **logs** (Python logging
→ OTel log records with trace context).  It integrates with Open WebUI's
native OTel support — in most cases you don't need to configure anything
extra.

### Setup

The Pipe piggybacks on Open WebUI's native OTel — no Pipe-level config
needed.  When OWUI has `ENABLE_OTEL=true`, the Pipe detects this and
**automatically** joins its pipeline:

- `trace.get_tracer("openclaw-owui-pipe")` + `metrics.get_meter("openclaw-owui-pipe")`
  — both from OWUI's existing global providers.
- Spans and metrics export through OWUI's OTLP pipeline.
- Pipe spans appear as children of OWUI's FastAPI route spans.
- Pipe metrics appear alongside OWUI's HTTP/DB metrics.

To enable OWUI's OTel:

```bash
export ENABLE_OTEL=true
export ENABLE_OTEL_TRACES=true
export ENABLE_OTEL_METRICS=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://your-collector:4317
export OTEL_SERVICE_NAME=open-webui
```

See https://docs.openwebui.com/reference/monitoring/otel/ for full details.

### Traces — span structure

```
openclaw.pipe                          (root — one per chat completion)
├── openclaw.gateway.connect           (on first request or reconnect)
├── openclaw.gateway.request:agent     (the agent RPC call)
│   └── openclaw.gateway.agent_stream  (streaming agent run)
│       ├── event: agent.run.accepted
│       ├── event: agent.delta  (×N)
│       └── event: agent.run.completed
```

### Traces — span attributes

| Span | Attributes |
|---|---|
| `openclaw.pipe` | `gen_ai.system`, `gen_ai.request.model`, `openclaw.agent.id`, `openwebui.user.id`, `openwebui.chat.id` |
| `openclaw.gateway.connect` | `openclaw.gateway.url` |
| `openclaw.gateway.request:*` | `rpc.method`, `rpc.service` |
| `openclaw.gateway.agent_stream` | `openclaw.agent.id`, `openclaw.run.id`, `gen_ai.request.model` |

### Metrics — instruments

| Instrument | Type | Attributes | Description |
|---|---|---|---|
| `openclaw.pipe.requests` | Counter | `openclaw.agent.id`, `status` | Total Pipe requests |
| `openclaw.pipe.duration` | Histogram | `openclaw.agent.id`, `status` | Request duration (seconds) |
| `openclaw.gateway.connections` | UpDownCounter | — | Active Gateway WebSocket connections |
| `openclaw.gateway.rpc.requests` | Counter | `rpc.method`, `status` | Gateway RPC calls |
| `openclaw.agent.stream.events` | Counter | `openclaw.agent.id`, `openclaw.event.kind` | Agent streaming events yielded |

All instruments are **delta-temporality counters** (they report the count
since the last export interval, not cumulative).  Histogram buckets use the
OTel SDK defaults.

### Errors in traces

When a request fails, the affected span is marked `StatusCode.ERROR` and
the exception is recorded with `error.type` and `error.message`.  The
corresponding `openclaw.pipe.requests` counter records `status=error`.

### Logs

The Pipe bridges Python `logging` into the OTel log pipeline.  A
`LoggingHandler` is attached to the `openclaw_pipe` logger hierarchy —
all `logger.info(...)`, `logger.warning(...)`, and `logger.error(...)`
calls from Pipe modules become OTel log records.

| Logger | Level | Example |
|---|---|---|
| `openclaw_pipe` | INFO | `"Gateway connected successfully"` |
| `openclaw_pipe.gateway_client` | INFO | `"Agent run accepted: runId=..."` (carries ``event="agent_run_accepted"``) |
| `openclaw_pipe.telemetry` | INFO | `"Telemetry: piggybacking on Open WebUI OTel"` |
| `openclaw_pipe.event_mapper` | DEBUG | `"Unknown event kind: ..."` (carries ``event="unknown_event_kind"``) |

Logs are **automatically correlated** with the active trace and span via
OTel context propagation — in your tracing backend, clicking on a span
shows all log lines emitted during that span.  No extra plumbing needed.

Operational log entries carry a structured ``event`` field (e.g.
``"agent_run_accepted"``, ``"reconnect_succeeded"``,
``"queue_full_drop"``) and context keys such as ``run_id``,
``agent_id``, ``attempt``, and ``error_type`` in the log record
attributes, making them queryable in the OTel backend without
parsing message text.

`DEBUG`-level logs stay local (not exported) to keep log volume manageable.
Only `INFO`, `WARNING`, `ERROR`, and `CRITICAL` are exported.
