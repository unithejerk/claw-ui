# Agent install instructions

You are installing the OpenClaw Pipe into the user's Open WebUI instance.
Read `install.py` in this repo — it bundles all modules and pushes them
via OWUI's REST API (stdlib-only, no extra deps).

## 1. Gather info (ask one question at a time)

1. **Where does Open WebUI run?**  Same machine as the Gateway?  Different?
   Which machine are you running commands on?

2. **OWUI URL and admin API key.**  The key lives under
   **Settings → Account → API Keys**.  Stop if they don't have one.

3. **Gateway URL.**  Default `ws://127.0.0.1:18789`.  Only change if the
   Gateway is on a different host or port.

4. **Gateway auth token.**  If they don't have one:
   ```bash
   openclaw gateway token create --scopes operator.read,operator.write,operator.approvals
   ```
   If `openclaw` isn't available (Docker/systemd), ask how they manage the
   Gateway and help generate a token through that path.

5. **Agent list.**  `__auto__` for all, or a comma list like
   `default,coding,research`.

6. **Approval mode.**  `auto_deny` (safe), `auto_approve`, or
   `interactive` (browser confirmation dialogs).

## 2. Verify

- `install.py` is in your working directory.  If not, help the user get
  the repo (clone or download).
- `python3` is available.
- No `pip install` needed — bundles stdlib-only, and the Pipe's runtime
  deps (`pydantic`, `websockets`) ship with Open WebUI.

## 3. Install

Substitute every `<PLACEHOLDER>` with actual gathered values:

```bash
python3 install.py \
    --owui-url <OWUI_URL> \
    --owui-key <OWUI_KEY> \
    --valves '{"GATEWAY_URL":"<GATEWAY_URL>","GATEWAY_TOKEN":"<GATEWAY_TOKEN>","APPROVAL_MODE":"<APPROVAL_MODE>","AGENT_LIST":"<AGENT_LIST>"}'
```

If the JSON is awkward (special chars in token), use interactive mode:

```bash
python3 install.py --owui-url <OWUI_URL> --owui-key <OWUI_KEY>
```

It prompts for each valve; Enter accepts defaults.

If the direct install fails (OWUI unreachable from this machine), fall
back:

```bash
python3 install.py -o openclaw_pipe_bundle.py
```

Then tell the user to paste `openclaw_pipe_bundle.py` into Open WebUI at
**Workspace → Functions → + → Pipe → Save**, and set valves manually
via the gear icon.

## 4. Verify

Tell the user to:

1. Open the model selector and look for `OpenClaw/Default` (or their
   configured agents).
2. Send a test message — confirm the agent responds.
3. If it errors: "Gateway unavailable" means the Gateway isn't reachable
   from the OWUI host.  "Connect rejected" means check the token and
   scopes.  "Unknown agent" means check `AGENT_LIST` or Gateway agent
   list.

## 5. Done

Summarize for the user:

- Which agents are available in the selector
- Which approval mode is active
- That valves can be changed anytime at **Workspace → Functions → ⚙️**
  — no restart needed
- That `APPROVAL_MODE=interactive` enables browser confirmation dialogs
  (requires recent Open WebUI)
