"""Tests for valves.py — Pydantic schema, defaults, and bounds."""
import pytest
from pydantic import ValidationError

from valves import Valves


def test_defaults():
    v = Valves()
    assert v.GATEWAY_URL == "ws://127.0.0.1:18789"
    assert v.GATEWAY_TOKEN == ""
    assert v.AGENT_PREFIX == "OpenClaw/"
    assert v.AGENT_LIST == "__auto__"
    assert v.REQUEST_TIMEOUT == 120
    assert v.MAX_RECONNECT_ATTEMPTS == 5
    assert v.RECONNECT_BASE_DELAY == 1.0
    assert v.APPROVAL_MODE == "auto_deny"
    assert v.MESSAGE_MODE == "last"


@pytest.mark.parametrize("field,value", [
    ("REQUEST_TIMEOUT", 9),
    ("REQUEST_TIMEOUT", 601),
    ("MAX_RECONNECT_ATTEMPTS", -1),
    ("MAX_RECONNECT_ATTEMPTS", 21),
    ("RECONNECT_BASE_DELAY", 0.05),
    ("RECONNECT_BASE_DELAY", 30.5),
    ("APPROVAL_TIMEOUT", 4),
    ("APPROVAL_TIMEOUT", 301),
])
def test_bounds_enforced(field, value):
    with pytest.raises(ValidationError):
        Valves(**{field: value})


def test_request_timeout_in_range_accepted():
    assert Valves(REQUEST_TIMEOUT=10).REQUEST_TIMEOUT == 10
    assert Valves(REQUEST_TIMEOUT=600).REQUEST_TIMEOUT == 600


def test_token_and_url_overridable():
    v = Valves(GATEWAY_URL="ws://gw:9000", GATEWAY_TOKEN="abc")
    assert v.GATEWAY_URL == "ws://gw:9000"
    assert v.GATEWAY_TOKEN == "abc"


def test_approval_mode_invalid_rejected():
    with pytest.raises(ValidationError):
        Valves(APPROVAL_MODE="invalid-mode")


def test_message_mode_invalid_rejected():
    with pytest.raises(ValidationError):
        Valves(MESSAGE_MODE="invalid-mode")


@pytest.mark.parametrize("mode", [
    "auto_deny",
    "auto_approve",
    "render",
    "interactive",
])
def test_approval_mode_valid_values_accepted(mode):
    v = Valves(APPROVAL_MODE=mode)
    assert v.APPROVAL_MODE == mode


@pytest.mark.parametrize("mode", [
    "last",
    "full",
])
def test_message_mode_valid_values_accepted(mode):
    v = Valves(MESSAGE_MODE=mode)
    assert v.MESSAGE_MODE == mode