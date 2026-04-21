"""Shared pytest fixtures for the sqlprism test suite."""

import pytest

import sqlprism.core.mcp_tools as _mcp_mod


def _reset_debounce_state():
    """Reset module-level debounce globals between tests."""
    _mcp_mod._reindex_pending.clear()
    for handle in _mcp_mod._reindex_timers.values():
        handle.cancel()
    _mcp_mod._reindex_timers.clear()


@pytest.fixture(autouse=True)
def _reset_mcp_state():
    """Reset global MCP state and debounce state between tests."""
    _mcp_mod._state = None
    _reset_debounce_state()
    yield
    _mcp_mod._state = None
    _reset_debounce_state()
