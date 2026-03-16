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
    """PageRank returns ranked models with expected fields and values."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_critical_models(top_n=5)
    assert "models" in result
    assert result["total_indexed_nodes"] == 5
    models = result["models"]
    assert len(models) == 5
    for m in models:
        assert "name" in m
        assert "kind" in m
        assert isinstance(m["importance"], float)
        assert isinstance(m["direct_dependents"], int)
    # Sorted by importance descending
    importances = [m["importance"] for m in models]
    assert importances == sorted(importances, reverse=True)
    # Verify concrete direct_dependents for known topology
    by_name = {m["name"]: m for m in models}
    # model_isolated has no dependents
    assert by_name["model_isolated"]["direct_dependents"] == 0
    # Chain: raw.orders -> staging.orders -> int_payments -> marts.revenue
    # direct_dependents = models whose edges point TO this node (source_id -> target_id)
    # raw.orders is root source — nothing points to it
    assert by_name["raw.orders"]["direct_dependents"] == 0
    # staging.orders is target of raw.orders
    assert by_name["staging.orders"]["direct_dependents"] == 1
    # int_payments is target of staging.orders
    assert by_name["int_payments"]["direct_dependents"] == 1
    # marts.revenue is target of int_payments
    assert by_name["marts.revenue"]["direct_dependents"] == 1
    db.close()


def test_find_critical_models_default_top_n():
    """Default top_n returns all models when fewer than 20 exist."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_critical_models()
    assert "models" in result
    assert len(result["models"]) == 5
    db.close()


def test_find_critical_models_top_n_truncation():
    """top_n=2 returns exactly 2 models."""
    db = _build_chain_graph()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not installed")
    result = db.query_find_critical_models(top_n=2)
    assert len(result["models"]) == 2
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
    assert names == {"model_a1", "model_a2"}
    assert result["total_indexed_nodes"] == 2
    # model_a2 is referenced by both n_a1 and n_b1 (cross-repo)
    by_name = {m["name"]: m for m in result["models"]}
    assert by_name["model_a2"]["direct_dependents"] == 2
    assert by_name["model_a1"]["direct_dependents"] == 0
    db.close()


def test_find_critical_models_no_pgq():
    """Returns error dict when DuckPGQ is not installed."""
    db = GraphDB()
    db._has_pgq = False
    result = db.query_find_critical_models()
    assert result.keys() == {"error"}
    assert "DuckPGQ" in result["error"]
    db.close()


# ── detect_cycles tests ──


def test_detect_cycles_dag():
    """DAG with no cycles returns has_cycles=False."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "dag.sql", "sql", "abc")

    a = db.insert_node(file_id, "table", "a", "sql")
    b = db.insert_node(file_id, "table", "b", "sql")
    c = db.insert_node(file_id, "table", "c", "sql")
    d = db.insert_node(file_id, "table", "d", "sql")

    db.insert_edge(a, b, "references")
    db.insert_edge(b, c, "references")
    db.insert_edge(c, d, "references")

    result = db.query_detect_cycles()
    assert result["has_cycles"] is False
    assert result["cycles"] == []
    assert result["total_nodes_in_scope"] == 4
    db.close()


def test_detect_cycles_simple():
    """Simple cycle a->b->c->a is detected."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "cycle.sql", "sql", "abc")

    a = db.insert_node(file_id, "table", "a", "sql")
    b = db.insert_node(file_id, "table", "b", "sql")
    c = db.insert_node(file_id, "table", "c", "sql")

    db.insert_edge(a, b, "references")
    db.insert_edge(b, c, "references")
    db.insert_edge(c, a, "references")

    result = db.query_detect_cycles()
    assert result["has_cycles"] is True
    assert result["total_nodes_in_scope"] == 3
    assert len(result["cycles"]) == 1
    cycle = result["cycles"][0]
    assert cycle["length"] == 3
    assert len(cycle["path"]) == 4
    assert cycle["path"][0] == cycle["path"][-1]
    assert set(cycle["path"][:3]) == {"a", "b", "c"}
    db.close()


def test_detect_cycles_multiple():
    """Two independent cycles are both detected with correct content."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "multi.sql", "sql", "abc")

    a = db.insert_node(file_id, "table", "a", "sql")
    b = db.insert_node(file_id, "table", "b", "sql")
    c = db.insert_node(file_id, "table", "c", "sql")
    d = db.insert_node(file_id, "table", "d", "sql")
    e = db.insert_node(file_id, "table", "e", "sql")

    # Cycle 1: a -> b -> a (length 2)
    db.insert_edge(a, b, "references")
    db.insert_edge(b, a, "references")

    # Cycle 2: c -> d -> e -> c (length 3)
    db.insert_edge(c, d, "references")
    db.insert_edge(d, e, "references")
    db.insert_edge(e, c, "references")

    result = db.query_detect_cycles()
    assert result["has_cycles"] is True
    assert len(result["cycles"]) == 2
    # Verify cycle lengths and node sets
    lengths = {cy["length"] for cy in result["cycles"]}
    assert lengths == {2, 3}
    node_sets = {frozenset(cy["path"][:-1]) for cy in result["cycles"]}
    assert node_sets == {frozenset({"a", "b"}), frozenset({"c", "d", "e"})}
    db.close()


def test_detect_cycles_repo_filter():
    """Repo filter isolates cycles to the specified repo."""
    db = GraphDB()
    repo_a = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    file_a = db.insert_file(repo_a, "a.sql", "sql", "aaa")
    file_b = db.insert_file(repo_b, "b.sql", "sql", "bbb")

    # DAG in repo_a
    a1 = db.insert_node(file_a, "table", "a1", "sql")
    a2 = db.insert_node(file_a, "table", "a2", "sql")
    db.insert_edge(a1, a2, "references")

    # Cycle in repo_b
    b1 = db.insert_node(file_b, "table", "b1", "sql")
    b2 = db.insert_node(file_b, "table", "b2", "sql")
    db.insert_edge(b1, b2, "references")
    db.insert_edge(b2, b1, "references")

    result_a = db.query_detect_cycles(repo="repo_a")
    assert result_a["has_cycles"] is False
    assert result_a["total_nodes_in_scope"] == 2

    result_b = db.query_detect_cycles(repo="repo_b")
    assert result_b["has_cycles"] is True
    assert len(result_b["cycles"]) == 1
    assert set(result_b["cycles"][0]["path"][:-1]) == {"b1", "b2"}
    db.close()


def test_detect_cycles_cross_repo_no_leak():
    """Cross-repo edges don't produce false-positive cycles in filtered results."""
    db = GraphDB()
    repo_a = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    file_a = db.insert_file(repo_a, "a.sql", "sql", "aaa")
    file_b = db.insert_file(repo_b, "b.sql", "sql", "bbb")

    # Cross-repo "cycle": a1 (repo_a) -> b1 (repo_b) -> a1 (repo_a)
    a1 = db.insert_node(file_a, "table", "a1", "sql")
    b1 = db.insert_node(file_b, "table", "b1", "sql")
    db.insert_edge(a1, b1, "references")
    db.insert_edge(b1, a1, "references")

    # Neither repo should see a cycle when filtered
    assert db.query_detect_cycles(repo="repo_a")["has_cycles"] is False
    assert db.query_detect_cycles(repo="repo_b")["has_cycles"] is False
    # But unfiltered should see it
    assert db.query_detect_cycles()["has_cycles"] is True
    db.close()


def test_detect_cycles_max_length_boundary():
    """Cycle longer than max_cycle_length is not detected."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "long.sql", "sql", "abc")

    # Build a cycle of length 4: a -> b -> c -> d -> a
    nodes = []
    for name in ["a", "b", "c", "d"]:
        nodes.append(db.insert_node(file_id, "table", name, "sql"))
    for i in range(len(nodes)):
        db.insert_edge(nodes[i], nodes[(i + 1) % len(nodes)], "references")

    # max_cycle_length=3 should miss the length-4 cycle
    result_short = db.query_detect_cycles(max_cycle_length=3)
    assert result_short["has_cycles"] is False

    # max_cycle_length=4 should find it
    result_long = db.query_detect_cycles(max_cycle_length=4)
    assert result_long["has_cycles"] is True
    assert result_long["cycles"][0]["length"] == 4
    db.close()


def test_detect_cycles_no_pgq_not_required():
    """Cycle detection works without DuckPGQ (CTE-only approach)."""
    db = GraphDB()
    db._has_pgq = False
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    file_id = db.insert_file(repo_id, "cycle.sql", "sql", "abc")

    a = db.insert_node(file_id, "table", "a", "sql")
    b = db.insert_node(file_id, "table", "b", "sql")
    db.insert_edge(a, b, "references")
    db.insert_edge(b, a, "references")

    result = db.query_detect_cycles()
    assert result["has_cycles"] is True
    assert len(result["cycles"]) == 1
    db.close()
