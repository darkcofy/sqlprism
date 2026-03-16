"""Tests for DuckPGQ graph tools (find_path, find_critical_models, etc.)."""

import pytest

from sqlprism.core.graph import GraphDB


def _build_chain_graph():
    """Create a linear chain: raw_orders -> stg_orders -> int_payments -> marts_revenue.
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


def test_find_path_shortest():
    db = _build_chain_graph()
    if not db.has_pgq:
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "marts.revenue")
    assert result["path_found"] is True
    assert result["length"] == 3
    assert len(result["path"]) == 4
    assert result["path"][0] == "raw.orders"
    assert result["path"][-1] == "marts.revenue"


def test_find_path_no_path():
    db = _build_chain_graph()
    if not db.has_pgq:
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "model_isolated")
    assert result["path_found"] is False
    assert result["path"] == []
    assert result["length"] == 0


def test_find_path_direct():
    db = _build_chain_graph()
    if not db.has_pgq:
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_path("raw.orders", "staging.orders")
    assert result["path_found"] is True
    assert result["length"] == 1
    assert result["path"] == ["raw.orders", "staging.orders"]


def test_find_path_no_pgq():
    db = GraphDB()
    db._has_pgq = False
    result = db.query_find_path("a", "b")
    assert "error" in result
    assert "DuckPGQ" in result["error"]
