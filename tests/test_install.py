"""Tests for install.py — direct-install toggle, CLI validation, and
bundle generation."""
import subprocess
import sys
from pathlib import Path

import pytest

REPO_DIR = Path(__file__).resolve().parent.parent
INSTALL_PY = REPO_DIR / "install.py"


def _run_install(*args, expect_code=0):
    """Run install.py with *args* and return (rc, stdout, stderr)."""
    p = subprocess.run(
        [sys.executable, str(INSTALL_PY), *args],
        capture_output=True, text=True, timeout=30,
    )
    if expect_code is not None:
        assert p.returncode == expect_code, (
            f"Expected rc {expect_code}, got {p.returncode}\n"
            f"stderr: {p.stderr}"
        )
    return p.returncode, p.stdout, p.stderr


# ── Bundle generation ────────────────────────────────────────────────────────


def test_bundle_check_succeeds():
    """--check exits 0 and produces no stdout bundle."""
    rc, stdout, stderr = _run_install("--check", expect_code=0)
    assert "✅ Bundle:" in stderr
    assert stdout == ""


def test_bundle_output_flag_writes_file(tmp_path):
    """-o writes the bundle to a file."""
    out = tmp_path / "bundle.py"
    rc, stdout, stderr = _run_install("-o", str(out), expect_code=0)
    assert out.exists()
    content = out.read_text()
    assert "OpenClaw Pipe" in content
    assert "from __future__ import annotations" in content


# ── CLI validation ───────────────────────────────────────────────────────────


def test_partial_direct_args_url_only_exits_nonzero():
    """Providing --owui-url without --owui-key fails fast."""
    rc, stdout, stderr = _run_install(
        "--owui-url", "http://localhost:3000", expect_code=2,
    )
    assert "Both --owui-url and --owui-key are required" in stderr


def test_partial_direct_args_key_only_exits_nonzero():
    """Providing --owui-key without --owui-url fails fast."""
    rc, stdout, stderr = _run_install(
        "--owui-key", "sk-fake", expect_code=2,
    )
    assert "Both --owui-url and --owui-key are required" in stderr


def test_both_direct_args_accepted():
    """Providing both --owui-url and --owui-key passes validation (it
    will fail later at the API call, but not at arg parsing)."""
    rc, stdout, stderr = _run_install(
        "--owui-url", "http://localhost:3000",
        "--owui-key", "sk-fake",
        expect_code=1,  # fails at API call, not arg parsing
    )
    assert "Both --owui-url" not in stderr


# ── Toggle / enable-idempotency ──────────────────────────────────────────────


def test_create_or_update_returns_created_true_for_new_function():
    """create_or_update returns (response, True) when function_exists
    returns False (new function)."""
    import install
    recorded_calls = []

    class FakeClient(install.OWUIClient):
        def __init__(self):
            pass  # skip real __init__
        def function_exists(self, fid):
            recorded_calls.append(("exists", fid))
            return False
        def _req(self, method, path, body=None):
            recorded_calls.append(("req", method, path))
            return {"ok": True}

    client = FakeClient()
    _, created = client.create_or_update("test-func", "content")
    assert created is True


def test_create_or_update_returns_created_false_for_existing_function():
    """create_or_update returns (response, False) when function_exists
    returns True (update)."""
    import install
    recorded_calls = []

    class FakeClient(install.OWUIClient):
        def __init__(self):
            pass
        def function_exists(self, fid):
            recorded_calls.append(("exists", fid))
            return True
        def _req(self, method, path, body=None):
            recorded_calls.append(("req", method, path))
            return {"ok": True}

    client = FakeClient()
    _, created = client.create_or_update("test-func", "content")
    assert created is False
