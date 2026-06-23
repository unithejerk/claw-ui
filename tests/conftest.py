"""Pytest configuration — make the repo importable as top-level modules.

The Pipe modules use absolute imports (``from valves import Valves``,
``from protocol import ...``), so the repo directory must be on sys.path.
"""
import sys
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))