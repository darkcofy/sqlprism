"""Tests for DuckPGQ graph tools (find_path, find_critical_models, etc.)."""

import pytest

from sqlprism.core.graph import GraphDB


def _build_chain_graph():
    """Create a linear chain: raw.orders -> staging.orders -> int_payments -> marts.revenue.
    Plus an isolated model: model_isolated (no edges).
    """
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")

    raw = db.insert_node(file_id, "table", "raw.orders", "sql")
    stg = db.insert_node(file_id, "table", "staging.orders", "sql")
    intp = db.insert_node(file_id, "table", "int_payments", "sql")
    mart = db.insert_node(file_id, "table", "marts.revenue", "sql")
    db.insert_node(file_id, "table", "model_isolated", "sql")

    db.insert_edge(raw, stg, "references")
    db.insert_edge(stg, intp, "references")
    db.insert_edge(intp, mart, "references")

    db.refresh_property_graph()
    return db


# ── find_path tests ──


def test_find_path_shortest():
    """Shortest path through full chain returns all intermediate nodes."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "marts.revenue")
    assert result["path_found"] is True
    assert result["length"] == 3
    assert result["path"] == ["raw.orders", "staging.orders", "int_payments", "marts.revenue"]
    db.close()


def test_find_path_no_path():
    """No path between disconnected models."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "model_isolated")
    assert result["path_found"] is False
    assert result["path"] == []
    assert result["length"] == 0
    db.close()


def test_find_path_direct():
    """Direct dependency returns 2-element path with length 1."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "staging.orders")
    assert result["path_found"] is True
    assert result["length"] == 1
    assert result["path"] == ["raw.orders", "staging.orders"]
    db.close()


def test_find_path_no_pgq():
    """Returns error dict when DuckPGQ is not installed."""
    db = GraphDB()
    db._has_pgq = False
    result = db.query_find_path("a", "b")
    assert result.keys() == {"error"}
    assert "DuckPGQ" in result["error"]
    db.close()


def test_find_path_reverse_direction():
    """No path in reverse direction (edges are directed)."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("marts.revenue", "raw.orders")
    assert result["path_found"] is False
    db.close()


def test_find_path_nonexistent_node():
    """Nonexistent model returns path_found=False."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("does_not_exist", "raw.orders")
    assert result["path_found"] is False
    assert result["path"] == []
    db.close()


def test_find_path_max_hops_too_short():
    """max_hops shorter than actual path returns no path."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    # Chain is 3 hops, but max_hops=2
    result = db.query_find_path("raw.orders", "marts.revenue", max_hops=2)
    assert result["path_found"] is False
    db.close()


def test_find_path_self():
    """Self-path returns path_found=False (min 1 hop required)."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "raw.orders")
    assert result["path_found"] is False
    db.close()
