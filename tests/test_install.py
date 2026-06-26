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


def test_bundle_imports_and_runs_in_isolation(tmp_path):
    """The generated bundle must be self-contained: importable with the repo
    NOT on sys.path, and the inlined Pipe must instantiate and answer
    pipes().

    Regression for the bundler bug where each inlined module kept its own
    ``from __future__ import annotations`` mid-file, so the bundle parsed
    (``ast.parse``) but raised ``SyntaxError`` on import.  ``--check`` used
    ``ast.parse`` and missed it; this test imports the bundle for real.
    """
    import textwrap
    out = tmp_path / "bundle.py"
    _run_install("-o", str(out), expect_code=0)

    # Import in a subprocess whose cwd is NOT the repo and whose sys.path
    # excludes the repo, so top-level packages (valves, protocol, …) only
    # resolve from inside the bundle.
    probe = tmp_path / "probe.py"
    probe.write_text(textwrap.dedent("""
        import sys
        assert not any("claw-ui" in p for p in sys.path), "repo must not be on sys.path"
        import importlib.util
        spec = importlib.util.spec_from_file_location("openclaw_bundle", r"{bundle}")
        m = importlib.util.module_from_spec(spec)
        # Register before exec: @dataclass / pydantic resolve the defining
        # module via sys.modules[cls.__module__]; exec_module alone doesn't
        # register it.  This mirrors how OWUI's function loader operates.
        sys.modules["openclaw_bundle"] = m
        spec.loader.exec_module(m)
        p = m.Pipe()
        models = p.pipes()
        assert models and models[0]["id"].startswith("openclaw/")
        print("OK", len(models))
    """).format(bundle=str(out).replace("\\", "\\\\")))
    r = subprocess.run(
        [sys.executable, str(probe)],
        capture_output=True, text=True, timeout=30, cwd=str(tmp_path),
    )
    assert r.returncode == 0, (
        f"isolated bundle import failed:\nstdout:{r.stdout}\nstderr:{r.stderr}"
    )
    assert "OK" in r.stdout


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
