"""Tests for the DuckDB graph storage layer."""

import os
import tempfile
import threading

from sqlprism.core.graph import GraphDB, _read_file_lines


def test_init_creates_schema():
    db = GraphDB()  # in-memory
    status = db.get_index_status()
    assert status["schema_version"] == "1.0"
    db.close()


def test_repo_crud():
    db = GraphDB()
    repo_id = db.upsert_repo("test-repo", "/tmp/test")
    assert repo_id > 0

    # Upsert same name returns same id
    same_id = db.upsert_repo("test-repo", "/tmp/test")
    assert same_id == repo_id
    db.close()


def test_file_and_node_insertion():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    assert node_id > 0

    # Should be findable
    found = db.resolve_node("orders", "table", repo_id)
    assert found == node_id
    db.close()


def test_phantom_nodes():
    db = GraphDB()
    phantom_id = db.get_or_create_phantom("unknown_table", "table", "sql")
    assert phantom_id > 0

    # Same phantom returns same id
    same_id = db.get_or_create_phantom("unknown_table", "table", "sql")
    assert same_id == phantom_id
    db.close()


def test_edge_insertion_and_query():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    source_id = db.insert_node(file_id, "query", "my_query", "sql")
    target_id = db.insert_node(file_id, "table", "orders", "sql")
    db.insert_edge(source_id, target_id, "references", "FROM clause")

    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) == 1
    assert refs["inbound"][0]["name"] == "my_query"
    assert refs["inbound"][0]["relationship"] == "references"
    db.close()


def test_search():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    db.insert_node(file_id, "table", "orders", "sql")
    db.insert_node(file_id, "table", "order_items", "sql")
    db.insert_node(file_id, "table", "customers", "sql")

    results = db.query_search("order")
    assert results["total_count"] == 2
    names = {m["name"] for m in results["matches"]}
    assert "orders" in names
    assert "order_items" in names
    db.close()


def test_delete_file_data_cascades():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    db.insert_edge(node_id, node_id, "self_ref")

    db.delete_file_data(repo_id, "query.sql")

    # Node should be gone
    assert db.resolve_node("orders", "table") is None
    db.close()


def test_delete_repo_cascades():
    """delete_repo removes all associated data (task 2.1)."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    db.insert_edge(node_id, node_id, "self_ref")
    db.insert_column_usage(node_id, "orders", "id", "select", file_id)
    db.insert_column_lineage(file_id, "orders", "id", 0, 0, "id", "raw_orders")

    db.delete_repo(repo_id)

    # Everything should be gone
    assert db.resolve_node("orders", "table") is None
    result = db.query_column_lineage(output_node="orders", column="id")
    assert result["total_count"] == 0
    status = db.get_index_status()
    assert len(status["repos"]) == 0
    assert status["totals"]["files"] == 0
    db.close()


def test_search_by_schema():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    db.insert_node(file_id, "table", "orders", "sql", metadata={"schema": "staging"}, schema="staging")
    db.insert_node(file_id, "table", "orders", "sql", metadata={"schema": "production"}, schema="production")
    db.insert_node(file_id, "table", "customers", "sql", metadata={"schema": "staging"}, schema="staging")

    # Search with schema filter
    results = db.query_search("order", schema="staging")
    assert results["total_count"] == 1
    assert results["matches"][0]["name"] == "orders"

    # Search all schemas
    results = db.query_search("order")
    assert results["total_count"] == 2

    # Schema filter with no match
    results = db.query_search("order", schema="archive")
    assert results["total_count"] == 0
    db.close()


def test_resolve_node_with_schema():
    """resolve_node with schema disambiguates same name+kind in different schemas."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    staging_id = db.insert_node(file_id, "table", "orders", "sql", metadata={"schema": "staging"}, schema="staging")
    prod_id = db.insert_node(file_id, "table", "orders", "sql", metadata={"schema": "production"}, schema="production")

    # Without schema, returns whichever comes first (non-deterministic but non-None)
    assert db.resolve_node("orders", "table", repo_id) is not None

    # With schema, returns the correct one
    assert db.resolve_node("orders", "table", repo_id, schema="staging") == staging_id
    assert db.resolve_node("orders", "table", repo_id, schema="production") == prod_id

    # Schema that doesn't exist returns None
    assert db.resolve_node("orders", "table", repo_id, schema="archive") is None

    # Cross-repo (no repo_id) also works with schema
    assert db.resolve_node("orders", "table", schema="staging") == staging_id
    assert db.resolve_node("orders", "table", schema="production") == prod_id
    db.close()


def test_column_lineage_storage():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    db.insert_column_lineage(file_id, "dim_users", "created_date", 0, "created_date", "dim_users", "CAST(created_at AS DATE)")
    db.insert_column_lineage(file_id, "dim_users", "created_date", 1, "created_at", "users")

    # Query by output node
    result = db.query_column_lineage(output_node="dim_users", column="created_date")
    assert result["total_count"] == 1
    chain = result["chains"][0]
    assert chain["output_column"] == "created_date"
    assert len(chain["hops"]) == 2
    assert chain["hops"][0]["expression"] == "CAST(created_at AS DATE)"
    assert chain["hops"][1]["table"] == "users"

    # Query by source table
    result = db.query_column_lineage(table="users", column="created_at")
    assert result["total_count"] == 1
    db.close()


def test_write_transaction_commit():
    """write_transaction: commit makes data visible."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "orders", "sql")

    # Data should be visible after commit
    found = db.resolve_node("orders", "table", repo_id)
    assert found == node_id
    db.close()


def test_write_transaction_rollback():
    """write_transaction: exception triggers rollback."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    # Insert a file+node outside transaction
    file_id = db.insert_file(repo_id, "base.sql", "sql", "base123")
    db.insert_node(file_id, "table", "base_table", "sql")

    # Start a write_transaction, insert, then raise to trigger rollback
    try:
        with db.write_transaction():
            db.insert_file(repo_id, "query.sql", "sql", "abc123")
            # Force a rollback via exception
            raise ValueError("deliberate rollback")
    except ValueError:
        pass

    # Rolled-back data should not be visible
    assert db.resolve_node("orders", "table", repo_id) is None
    # Pre-existing data should still be there
    assert db.resolve_node("base_table", "table", repo_id) is not None
    db.close()


def test_phantom_cleanup():
    """Phantom nodes are repointed and deleted when real counterparts exist."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    # Create a phantom node
    phantom_id = db.get_or_create_phantom("orders", "table", "sql")

    # Create an edge pointing to the phantom
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    query_id = db.insert_node(file_id, "query", "my_query", "sql")
    db.insert_edge(query_id, phantom_id, "references")

    # Now create a real node for orders
    real_id = db.insert_node(file_id, "table", "orders", "sql")

    # Run cleanup
    cleaned = db.cleanup_phantoms()
    assert cleaned == 1

    # Edge should now point to the real node
    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) == 1
    assert refs["inbound"][0]["name"] == "my_query"

    # Phantom should be gone
    phantom_check = db.conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]
    ).fetchone()[0]
    assert phantom_check == 0
    db.close()


def test_cleanup_orphaned_phantoms():
    """Phantom nodes with no edges are deleted by cleanup_phantoms()."""
    db = GraphDB()

    # Create a phantom node with no edges
    phantom_id = db.get_or_create_phantom("orphan_table", "table", "sql")

    # Verify it exists
    count = db.conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]
    ).fetchone()[0]
    assert count == 1

    # Run cleanup
    cleaned = db.cleanup_phantoms()
    assert cleaned == 1

    # Phantom should be gone
    count = db.conn.execute(
        "SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]
    ).fetchone()[0]
    assert count == 0
    db.close()


def test_column_usage_query():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "query", "my_query", "sql")
    db.insert_column_usage(node_id, "orders", "customer_id", "where", file_id)
    db.insert_column_usage(node_id, "orders", "customer_id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "total", "select", file_id)

    result = db.query_column_usage("orders")
    assert result["total_count"] == 3
    assert result["summary"]["where"] == 1
    assert result["summary"]["select"] == 2

    # Filter by column
    result = db.query_column_usage("orders", column="customer_id")
    assert result["total_count"] == 2

    # Filter by usage type
    result = db.query_column_usage("orders", usage_type="where")
    assert result["total_count"] == 1
    db.close()


# ── P3.2: LIMIT/pagination ──


def test_query_column_usage_limit():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "query", "my_query", "sql")
    for i in range(10):
        db.insert_column_usage(node_id, "orders", f"col_{i}", "select", file_id)

    result = db.query_column_usage("orders", limit=3)
    assert len(result["usage"]) == 3
    db.close()


def test_query_references_limit():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    target_id = db.insert_node(file_id, "table", "orders", "sql")
    for i in range(10):
        src = db.insert_node(file_id, "query", f"q_{i}", "sql")
        db.insert_edge(src, target_id, "references")

    result = db.query_references("orders", kind="table", limit=3)
    assert len(result["inbound"]) == 3
    db.close()


def test_query_trace_limit():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    root = db.insert_node(file_id, "table", "root", "sql")
    for i in range(10):
        child = db.insert_node(file_id, "query", f"q_{i}", "sql")
        db.insert_edge(root, child, "references")

    result = db.query_trace("root", kind="table", direction="downstream", limit=3)
    assert len(result["paths"]) == 3
    db.close()


# ── P3.4: ILIKE wildcard escaping ──


def test_search_escapes_wildcards():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    db.insert_node(file_id, "table", "my_table", "sql")
    db.insert_node(file_id, "table", "my%table", "sql")

    # Searching for literal "%" should only match the one with %
    result = db.query_search("%")
    assert result["total_count"] == 1
    assert result["matches"][0]["name"] == "my%table"
    db.close()


def test_search_escapes_underscore():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    db.insert_node(file_id, "table", "my_table", "sql")
    db.insert_node(file_id, "table", "myXtable", "sql")

    # Searching for literal "_" should only match names with actual underscore
    # Both match because "my_table" contains literal "_" and "myXtable" doesn't
    result = db.query_search("_")
    # "my_table" has a literal underscore, "myXtable" doesn't
    names = {m["name"] for m in result["matches"]}
    assert "my_table" in names
    # Without escape, myXtable would also match "_" as wildcard — with escape it shouldn't
    assert "myXtable" not in names
    db.close()


# ── P4.1: Batch DuckDB inserts ──


def test_insert_nodes_batch():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    rows = [
        (file_id, "table", "orders", "sql", None, None, None, None),
        (file_id, "table", "customers", "sql", None, None, None, None),
        (file_id, "cte", "recent", "sql", None, None, '{"parent_query": "q"}', None),
    ]
    ids = db.insert_nodes_batch(rows)
    assert len(ids) == 3
    assert all(isinstance(i, int) for i in ids)
    assert len(set(ids)) == 3  # all unique

    # Nodes should be findable
    assert db.resolve_node("orders", "table", repo_id) == ids[0]
    assert db.resolve_node("customers", "table", repo_id) == ids[1]
    db.close()


def test_insert_nodes_batch_empty():
    db = GraphDB()
    assert db.insert_nodes_batch([]) == []
    db.close()


def test_insert_edges_batch():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    n1 = db.insert_node(file_id, "query", "q", "sql")
    n2 = db.insert_node(file_id, "table", "orders", "sql")
    n3 = db.insert_node(file_id, "table", "customers", "sql")

    db.insert_edges_batch([
        (n1, n2, "references", "FROM clause", None),
        (n1, n3, "references", "JOIN clause", None),
    ])

    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) == 1
    refs2 = db.query_references("customers", kind="table")
    assert len(refs2["inbound"]) == 1
    db.close()


def test_insert_column_usage_batch():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "query", "my_query", "sql")

    db.insert_column_usage_batch([
        (node_id, "orders", "id", "select", file_id, None, None),
        (node_id, "orders", "total", "where", file_id, None, "total > 100"),
        (node_id, "customers", "name", "select", file_id, "customer_name", None),
    ])

    result = db.query_column_usage("orders")
    assert result["total_count"] == 2
    db.close()


def test_insert_column_lineage_batch():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Each row: (file_id, output_node, output_column, chain_index, hop_index, hop_column, hop_table, hop_expression)
    db.insert_column_lineage_batch([
        (file_id, "dim_users", "created_date", 0, 0, "created_date", "dim_users", "CAST(created_at AS DATE)"),
        (file_id, "dim_users", "created_date", 0, 1, "created_at", "users", None),
        (file_id, "dim_users", "user_name", 0, 0, "user_name", "dim_users", None),
        (file_id, "dim_users", "user_name", 0, 1, "name", "users", None),
    ])

    result = db.query_column_lineage(output_node="dim_users")
    assert result["total_count"] == 2
    db.close()


def test_column_lineage_limit_by_chain_count():
    """LIMIT should count chains, not individual hop rows (task 1.7)."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Insert 3 chains, each with 3 hops
    for chain_idx in range(3):
        col = f"col_{chain_idx}"
        for hop_idx in range(3):
            db.insert_column_lineage(
                file_id, "dim_users", col, chain_idx,
                hop_idx, f"src_{hop_idx}", f"table_{hop_idx}",
            )

    # Limit=2 should return exactly 2 chains with all their hops
    result = db.query_column_lineage(output_node="dim_users", limit=2)
    assert len(result["chains"]) == 2
    assert result["total_count"] == 3  # true total, not page size
    for chain in result["chains"]:
        assert len(chain["hops"]) == 3  # all hops present, not truncated

    # Limit=10 should return all 3 chains
    result = db.query_column_lineage(output_node="dim_users", limit=10)
    assert len(result["chains"]) == 3
    assert result["total_count"] == 3
    db.close()


def test_column_lineage_limit_preserves_full_chains():
    """LIMIT truncates by chain count, never mid-chain (task 5.8).

    When limit=1, only the first chain is returned but ALL its hops are
    present — the limit never slices individual hops out of a chain.
    """
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Insert 5 chains with varying hop counts (2, 4, 3, 1, 5)
    hop_counts = [2, 4, 3, 1, 5]
    for chain_idx, n_hops in enumerate(hop_counts):
        col = f"col_{chain_idx}"
        for hop_idx in range(n_hops):
            db.insert_column_lineage(
                file_id, "wide_table", col, chain_idx,
                hop_idx, f"src_{hop_idx}", f"tbl_{hop_idx}",
            )

    # Limit=1: one chain, all its hops intact
    result = db.query_column_lineage(output_node="wide_table", limit=1)
    assert len(result["chains"]) == 1
    assert result["total_count"] == 5  # true total, not page size
    assert len(result["chains"][0]["hops"]) == hop_counts[0]

    # Limit=3: three chains, each with the correct hop count
    result = db.query_column_lineage(output_node="wide_table", limit=3)
    assert len(result["chains"]) == 3
    assert result["total_count"] == 5
    for chain in result["chains"]:
        col_name = chain["output_column"]
        idx = int(col_name.split("_")[1])
        assert len(chain["hops"]) == hop_counts[idx], (
            f"chain {col_name} has {len(chain['hops'])} hops, expected {hop_counts[idx]}"
        )
    db.close()


# ── P4.0: Schema catalog ──


def test_get_table_columns():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "query", "my_query", "sql")

    db.insert_column_usage(node_id, "orders", "id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "total", "select", file_id)
    db.insert_column_usage(node_id, "customers", "name", "select", file_id)
    db.insert_column_usage(node_id, "orders", "*", "select", file_id)  # should be excluded

    schema = db.get_table_columns(repo_id)
    assert "orders" in schema
    assert "id" in schema["orders"]
    assert "total" in schema["orders"]
    assert "*" not in schema["orders"]
    assert "customers" in schema
    assert "name" in schema["customers"]
    db.close()


def test_get_table_columns_empty():
    db = GraphDB()
    schema = db.get_table_columns()
    assert schema == {}
    db.close()


# ── P4.3: Cached file reads ──


def test_read_file_lines_cache():
    """_read_file_lines caches results for the same path."""
    from sqlprism.core.graph import _read_file_lines

    # Clear cache to avoid interference from other tests
    _read_file_lines.cache_clear()

    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write("SELECT 1;\nSELECT 2;\n")
        f.flush()
        path = f.name

    try:
        lines1 = _read_file_lines(path)
        lines2 = _read_file_lines(path)
        assert lines1 is lines2  # same object from cache
        assert lines1 == ("SELECT 1;", "SELECT 2;")
        info = _read_file_lines.cache_info()
        assert info.hits >= 1
    finally:
        os.unlink(path)
        _read_file_lines.cache_clear()


def test_query_trace_both_sums_depth_summary():
    """direction='both' sums depth_summary counts instead of overwriting."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Create a hub node with both upstream and downstream neighbours at depth 1
    hub = db.insert_node(file_id, "table", "hub", "sql")
    # Two downstream nodes (hub -> child edges)
    c1 = db.insert_node(file_id, "query", "child_1", "sql")
    c2 = db.insert_node(file_id, "query", "child_2", "sql")
    db.insert_edge(hub, c1, "references")
    db.insert_edge(hub, c2, "references")
    # Three upstream nodes (parent -> hub edges)
    for i in range(3):
        p = db.insert_node(file_id, "table", f"parent_{i}", "sql")
        db.insert_edge(p, hub, "references")

    result = db.query_trace("hub", kind="table", direction="both")

    # downstream has 2 nodes at depth 1, upstream has 3 nodes at depth 1
    assert result["depth_summary"][1] == 5  # 2 + 3, not overwritten
    assert len(result["downstream"]) == 2
    assert len(result["upstream"]) == 3
    db.close()


def test_clear_snippet_cache():
    """GraphDB.clear_snippet_cache() clears the _read_file_lines lru_cache."""
    from sqlprism.core.graph import _read_file_lines

    db = GraphDB()
    # Populate the cache with a dummy call
    _read_file_lines.cache_clear()
    _read_file_lines("/nonexistent/path/for/test.sql")
    assert _read_file_lines.cache_info().misses >= 1

    db.clear_snippet_cache()
    info = _read_file_lines.cache_info()
    assert info.hits == 0 and info.misses == 0 and info.currsize == 0
    db.close()


# ── 5.5: Concurrent access ──


def test_concurrent_sequential_access():
    """Verify RLock serialises access — sequential calls from different threads work.

    Note: DuckDB in-memory connections have internal thread-safety issues when
    two threads call execute() truly concurrently (even with our RLock), so we
    test sequential hand-off instead of simultaneous access.
    """
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    result_holder: list[dict] = []

    def insert_from_thread():
        for i in range(5):
            db.insert_node(file_id, "table", f"threaded_table_{i}", "sql")

    t = threading.Thread(target=insert_from_thread)
    t.start()
    t.join()

    # Now read from main thread — data from other thread should be visible
    result = db.query_search("threaded_table", limit=50)
    assert result["total_count"] == 5
    db.close()


# ── 5.6: _read_snippet with real files ──


def test_read_snippet_with_real_file():
    """_read_snippet returns correct lines from a real SQL file on disk."""
    _read_file_lines.cache_clear()
    sql_content = "\n".join([f"-- line {i}" for i in range(1, 11)])
    # 10 lines: "-- line 1" through "-- line 10"

    with tempfile.TemporaryDirectory() as tmpdir:
        sql_path = os.path.join(tmpdir, "example.sql")
        with open(sql_path, "w") as f:
            f.write(sql_content)

        db = GraphDB()
        repo_id = db.upsert_repo("snippet-test", tmpdir)
        file_id = db.insert_file(repo_id, "example.sql", "sql", "snap1")
        # Node on lines 4-6
        db.insert_node(
            file_id, "table", "my_snippet_table", "sql",
            line_start=4, line_end=6,
        )

        # Use query_search with include_snippets=True
        result = db.query_search("my_snippet_table", include_snippets=True)
        assert result["total_count"] == 1
        match = result["matches"][0]
        assert "snippet" in match

        snippet = match["snippet"]
        # Context padding = 2 lines by default, so lines 2..8 should appear
        assert "-- line 2" in snippet  # context before
        assert "-- line 4" in snippet  # node start
        assert "-- line 6" in snippet  # node end
        assert "-- line 8" in snippet  # context after

        db.close()
    _read_file_lines.cache_clear()


def test_read_snippet_context_padding():
    """_read_snippet context_lines pads before and after the target lines."""
    _read_file_lines.cache_clear()
    sql_content = "\n".join([f"-- line {i}" for i in range(1, 21)])

    with tempfile.TemporaryDirectory() as tmpdir:
        sql_path = os.path.join(tmpdir, "padded.sql")
        with open(sql_path, "w") as f:
            f.write(sql_content)

        db = GraphDB()
        repo_id = db.upsert_repo("pad-test", tmpdir)
        file_id = db.insert_file(repo_id, "padded.sql", "sql", "snap2")
        # Node on line 10 only
        db.insert_node(
            file_id, "table", "padded_table", "sql",
            line_start=10, line_end=10,
        )

        # Test directly: context_lines=2 means lines 8..12
        snippet = db._read_snippet("pad-test", "padded.sql", 10, 10, context_lines=2)
        assert snippet is not None
        assert "-- line 8" in snippet
        assert "-- line 9" in snippet
        assert "-- line 10" in snippet
        assert "-- line 11" in snippet
        assert "-- line 12" in snippet
        # Lines outside the window should not appear
        assert "-- line 7" not in snippet
        assert "-- line 13" not in snippet

        db.close()
    _read_file_lines.cache_clear()


def test_read_snippet_nonexistent_file():
    """_read_snippet returns None for a file that does not exist."""
    _read_file_lines.cache_clear()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDB()
        repo_id = db.upsert_repo("ghost-test", tmpdir)

        result = db._read_snippet("ghost-test", "no_such_file.sql", 1, 1)
        assert result is None

        db.close()
    _read_file_lines.cache_clear()


def test_read_snippet_none_inputs():
    """_read_snippet returns None when file_path or line_start is None."""
    db = GraphDB()
    db.upsert_repo("test", "/tmp/test")

    assert db._read_snippet("test", None, 1, 1) is None
    assert db._read_snippet("test", "file.sql", None, None) is None
    db.close()


# ── Phase 1: Thread-safety and concurrency ──


def test_concurrent_read_during_write_transaction():
    """Reads from thread B succeed while thread A holds a write transaction (1.2a)."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "a.sql", "sql", "h1")
    db.insert_node(file_id, "table", "existing", "sql")

    read_result = []
    read_error = []
    barrier = threading.Barrier(2, timeout=5)

    def writer():
        with db.write_transaction():
            # Insert inside transaction but don't commit yet
            db.insert_node(file_id, "table", "new_table", "sql")
            barrier.wait()  # signal reader to go
            import time; time.sleep(0.1)  # hold transaction open briefly

    def reader():
        barrier.wait()  # wait for writer to be mid-transaction
        try:
            result = db._execute_read(
                "SELECT COUNT(*) FROM nodes WHERE name = 'existing'"
            ).fetchone()
            read_result.append(result[0])
        except Exception as e:
            read_error.append(e)

    t_write = threading.Thread(target=writer)
    t_read = threading.Thread(target=reader)
    t_write.start()
    t_read.start()
    t_write.join(timeout=10)
    t_read.join(timeout=10)

    assert not read_error, f"Read during write raised: {read_error}"
    assert read_result[0] >= 1, "Reader should see pre-transaction data"
    db.close()


def test_in_transaction_thread_local():
    """_tlocal.in_transaction is per-thread — thread B doesn't see thread A's flag (1.5c)."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "a.sql", "sql", "h1")
    db.insert_node(file_id, "table", "t1", "sql")

    thread_b_saw_transaction = []
    barrier = threading.Barrier(2, timeout=5)

    def writer():
        with db.write_transaction():
            barrier.wait()
            import time; time.sleep(0.1)

    def reader():
        barrier.wait()
        # Thread B should NOT see in_transaction=True
        thread_b_saw_transaction.append(
            getattr(db._tlocal, "in_transaction", False)
        )
        # And reads should use cursor path (not main connection)
        result = db._execute_read("SELECT COUNT(*) FROM nodes").fetchone()
        assert result[0] >= 1

    t_write = threading.Thread(target=writer)
    t_read = threading.Thread(target=reader)
    t_write.start()
    t_read.start()
    t_write.join(timeout=10)
    t_read.join(timeout=10)

    assert thread_b_saw_transaction == [False], \
        "Thread B should not see thread A's in_transaction flag"
    db.close()


def test_query_column_lineage_total_count_is_true_total():
    """total_count should reflect all matching chains, not the page size."""
    db = GraphDB()
    repo_id = db.upsert_repo("lineage-repo", "/tmp/lineage")
    file_id = db.insert_file(repo_id, "model.sql", "sql", "aaa111")

    # Insert 5 distinct lineage chains (each with 1 hop)
    for i in range(5):
        db.insert_column_lineage(
            file_id=file_id,
            output_node=f"output_table_{i}",
            output_column="col_a",
            hop_index=0,
            hop_column="src_col",
            hop_table="source_table",
            chain_index=0,
        )

    # Query with limit=2 — should return 2 chains but total_count=5
    result = db.query_column_lineage(column="col_a", limit=2, offset=0)
    assert len(result["chains"]) == 2
    assert result["total_count"] == 5

    # Query with offset=3 — should return 2 chains but total_count still 5
    result2 = db.query_column_lineage(column="col_a", limit=10, offset=3)
    assert len(result2["chains"]) == 2
    assert result2["total_count"] == 5
    db.close()


def test_cleanup_phantoms_removes_phantom_only_referenced_by_phantoms():
    """Phantoms whose only inbound edges come from other phantoms should be cleaned up."""
    db = GraphDB()
    repo_id = db.upsert_repo("phantom-repo", "/tmp/phantom")
    file_id = db.insert_file(repo_id, "real.sql", "sql", "bbb222")

    # Create a real node
    real_node_id = db.insert_node(file_id, "table", "real_table", "sql")

    # Create phantom A — will be referenced by a real node (should survive)
    phantom_a_id = db.get_or_create_phantom("phantom_a", "table", "sql")
    # Create phantom B — will only be referenced by another phantom (should be cleaned up)
    phantom_b_id = db.get_or_create_phantom("phantom_b", "table", "sql")
    # Create phantom C — source phantom that references phantom B
    phantom_c_id = db.get_or_create_phantom("phantom_c", "table", "sql")

    # Edge: real_node -> phantom_a (phantom_a has a real inbound edge)
    db.insert_edge(real_node_id, phantom_a_id, "references")
    # Edge: phantom_c -> phantom_b (phantom_b only has phantom inbound edges)
    db.insert_edge(phantom_c_id, phantom_b_id, "references")

    # First pass cleans stale phantoms (B has only phantom inbound, C is stale source)
    cleaned1 = db.cleanup_phantoms()
    # Second pass cleans any newly-orphaned phantoms
    cleaned2 = db.cleanup_phantoms()
    cleaned = cleaned1 + cleaned2

    # phantom_b and phantom_c should be cleaned up
    assert cleaned >= 2

    # phantom_a should still exist
    row = db._execute_read(
        "SELECT node_id FROM nodes WHERE node_id = ?", [phantom_a_id]
    ).fetchone()
    assert row is not None, "phantom_a should survive (has real inbound edge)"

    # phantom_b should be gone
    row_b = db._execute_read(
        "SELECT node_id FROM nodes WHERE node_id = ?", [phantom_b_id]
    ).fetchone()
    assert row_b is None, "phantom_b should be deleted (only phantom inbound edges)"

    # phantom_c should be gone (no inbound edges at all, orphaned after B's edges removed
    # or was already identified as stale/orphaned)
    row_c = db._execute_read(
        "SELECT node_id FROM nodes WHERE node_id = ?", [phantom_c_id]
    ).fetchone()
    assert row_c is None, "phantom_c should be deleted (no real inbound edges)"
    db.close()
