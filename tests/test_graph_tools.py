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


# ── find_critical_models tests ──


def test_find_critical_models_pagerank():
    """PageRank returns ranked models with expected fields."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_critical_models(top_n=5)
    assert "models" in result
    models = result["models"]
    assert len(models) > 0
    for m in models:
        assert "name" in m
        assert "kind" in m
        assert isinstance(m["importance"], float)
        assert isinstance(m["downstream_count"], int)
    # Sorted by importance descending
    importances = [m["importance"] for m in models]
    assert importances == sorted(importances, reverse=True)
    db.close()


def test_find_critical_models_default_top_n():
    """Default top_n returns all models when fewer than 20 exist."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_critical_models()
    assert "models" in result
    # Chain graph has 5 nodes, all should be returned (< 20)
    assert len(result["models"]) == 5
    db.close()


def test_find_critical_models_repo_filter():
    """Repo filter returns only models from the specified repo."""
    db = GraphDB()
    repo_a = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    file_a = db.insert_file(repo_a, "a.sql", "sql", "aaa")
    file_b = db.insert_file(repo_b, "b.sql", "sql", "bbb")

    n_a1 = db.insert_node(file_a, "table", "model_a1", "sql")
    n_a2 = db.insert_node(file_a, "table", "model_a2", "sql")
    n_b1 = db.insert_node(file_b, "table", "model_b1", "sql")

    db.insert_edge(n_a1, n_a2, "references")
    db.insert_edge(n_b1, n_a2, "references")  # cross-repo edge

    db.refresh_property_graph()

    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")

    result = db.query_find_critical_models(repo="repo_a")
    names = {m["name"] for m in result["models"]}
    assert "model_a1" in names or "model_a2" in names
    assert "model_b1" not in names
    db.close()


def test_find_critical_models_no_pgq():
    """Returns error dict when DuckPGQ is not installed."""
    db = GraphDB()
    db._has_pgq = False
    result = db.query_find_critical_models()
    assert result.keys() == {"error"}
    assert "DuckPGQ" in result["error"]
    db.close()
