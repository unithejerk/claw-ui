#!/usr/bin/env python3
"""
Connect to an OpenClaw Gateway and dump every raw event frame
received during a single agent run.

Usage::

    pip install websockets pynacl
    python3 debug_events.py ws://127.0.0.1:18789 <token> [agent_id] [prompt]
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

# Local protocol helpers (stdlib only for frame parsing)
from protocol import (
    PROTOCOL_VERSION,
    build_request,
    generate_idempotency_key,
    generate_request_id,
    parse_connect_challenge,
    parse_frame,
    FrameType,
)


async def main() -> None:
    """Connect to a Gateway, run one agent, and print every raw frame received.

    A diagnostic tool for inspecting the on-the-wire Gateway protocol
    (challenge, ack, agent events, final result).  Unlike the Pipe, this
    signs the challenge with an Ed25519 device keypair (when ``pynacl`` is
    available) to exercise the full device-auth path.
    """
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <ws://gateway:18789> <token> [agent_id] [prompt]")
        sys.exit(1)

    url = sys.argv[1]
    token = os.environ.get("GATEWAY_TOKEN") or sys.argv[2]
    if token == sys.argv[2]:
        print("⚠️  Token provided via CLI argument — this exposes it in `ps aux`"
              " and shell history.  Consider setting GATEWAY_TOKEN instead.",
              file=sys.stderr)
    agent_id = sys.argv[3] if len(sys.argv) > 3 else "default"
    prompt = sys.argv[4] if len(sys.argv) > 4 else (
        "Search the web for the current Bitcoin price and tell me the result."
    )

    try:
        import websockets
    except ImportError:
        print("❌ 'websockets' not installed. Run: pip install websockets pynacl")
        sys.exit(1)

    # Try pynacl for Ed25519; fall back to no device signing
    try:
        from nacl.signing import SigningKey
        import nacl.encoding
        _HAS_NACL = True
    except ImportError:
        print("⚠️  'pynacl' not installed — device signature may fail.")
        print("   Run: pip install pynacl")
        _HAS_NACL = False

    print(f"Connecting to {url} ...")
    ws = await websockets.connect(url)

    # ── Handshake ──────────────────────────────────────────────────
    print("Waiting for connect.challenge ...")
    raw = await ws.recv()
    challenge = parse_connect_challenge(raw)
    nonce = challenge["nonce"]
    ts = challenge["ts"]
    print(f"  nonce: {nonce[:16]}...  ts: {ts}")

    # Build connect params
    device: dict = {}
    if _HAS_NACL:
        sk = SigningKey.generate()
        vk = sk.verify_key
        public_key_hex = vk.encode(encoder=nacl.encoding.HexEncoder).decode()
        device_id = public_key_hex[:16]
        signed = sk.sign(nonce.encode())
        signature_hex = nacl.encoding.HexEncoder.encode(signed.signature).decode()
        device = {
            "id": device_id,
            "publicKey": public_key_hex,
            "signature": signature_hex,
            "signedAt": int(ts),
            "nonce": nonce,
        }
    else:
        device = {
            "id": "debug-script",
            "publicKey": "",
            "signature": "",
            "signedAt": int(ts),
            "nonce": nonce,
        }

    connect_frame = json.dumps({
        "type": "req",
        "id": generate_request_id(),
        "method": "connect",
        "params": {
            "minProtocol": PROTOCOL_VERSION,
            "maxProtocol": PROTOCOL_VERSION,
            "client": {
                "id": "gateway-client",
                "version": "1.0.0",
                "platform": "open-webui",
                "mode": "cli",
            },
            "role": "operator",
            "scopes": ["operator.read", "operator.write"],
            "auth": {"token": token},
            "device": device,
        },
    })
    await ws.send(connect_frame)

    raw = await ws.recv()
    frame = parse_frame(raw)
    if frame.response and frame.response.ok:
        payload = frame.response.payload or {}
        print(f"  ✅ connected  server={payload.get('server', {}).get('version', '?')}")
        if payload.get("features"):
            print(f"     methods: {', '.join(payload['features'].get('methods', [])[:10])}")
    else:
        err = frame.response.error if frame.response else {}
        print(f"  ❌ connect failed: {json.dumps(err, indent=2)}")
        await ws.close()
        return

    # ── Send agent request ─────────────────────────────────────────
    request_id = generate_request_id()
    agent_frame = build_request(
        "agent",
        {
            "agentId": agent_id,
            "messages": [{"role": "user", "content": prompt}],
        },
        request_id=request_id,
        idempotency_key=generate_idempotency_key(),
    )
    await ws.send(agent_frame)
    print(f"\nSent agent request (id={request_id}): {prompt}\n")
    print("─" * 72)

    # ── Dump every frame ──────────────────────────────────────────
    event_count = 0
    while True:
        raw = await ws.recv()
        frame = parse_frame(raw)

        if frame.type == FrameType.RES and frame.response and frame.response.id == request_id:
            payload = frame.response.payload or {}
            if payload.get("status") == "accepted":
                print(f"[ack] runId={payload.get('runId', '?')}")
                continue
            print(f"\n[final result] {json.dumps(payload, indent=2)}")
            break

        elif frame.type == FrameType.EVENT and frame.event:
            event_count += 1
            name = frame.event.event
            pl = frame.event.payload
            # Summarize known patterns
            if name == "agent":
                delta = pl.get("delta", pl)
                keys = list(delta.keys())
                preview = json.dumps(delta, default=str)[:300]
                print(f"\n── event #{event_count}: {name}  keys={keys}")
                print(f"   {preview}")
            else:
                print(f"\n── event #{event_count}: {name}")
                print(json.dumps(pl, indent=2, default=str)[:500])

        elif frame.type == FrameType.RES and frame.response:
            if not frame.response.ok:
                print(f"\n[error] {json.dumps(frame.response.error, indent=2)}")
                break

    print(f"\n─" * 72)
    print(f"Done. {event_count} events received.")
    await ws.close()


if __name__ == "__main__":
    asyncio.run(main())
