"""Tests for the DuckDB graph storage layer."""

import os
import tempfile
import threading

from sqlprism.core.graph import INDEX_SQL, SCHEMA_SQL, GraphDB, _read_file_lines


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


def test_migration_adds_repo_type_column():
    """repos table has a repo_type column with default 'sql'."""
    db = GraphDB()
    cols = db.conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'repos' AND column_name = 'repo_type'"
    ).fetchone()
    assert cols is not None
    assert cols[0] == "repo_type"
    assert cols[1] == "VARCHAR"

    # Verify DDL-level default by inserting without repo_type
    db.conn.execute(
        "INSERT INTO repos (name, path) VALUES ('raw-repo', '/tmp/raw')"
    )
    row = db.conn.execute(
        "SELECT repo_type FROM repos WHERE name = 'raw-repo'"
    ).fetchone()
    assert row[0] == "sql"
    db.close()


def test_migration_existing_repos_default_sql():
    """Repos created without explicit repo_type default to 'sql' and data is preserved."""
    db = GraphDB()
    db.upsert_repo("default-repo", "/tmp/default")
    row = db.conn.execute(
        "SELECT name, path, repo_type FROM repos WHERE name = 'default-repo'"
    ).fetchone()
    assert row[0] == "default-repo"
    assert row[1] == "/tmp/default"
    assert row[2] == "sql"
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
    db.insert_column_lineage(file_id, "orders", "id", 0, "id", "raw_orders")

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
    db.insert_node(
        file_id,
        "table",
        "orders",
        "sql",
        metadata={"schema": "staging"},
        schema="staging",
    )
    db.insert_node(
        file_id,
        "table",
        "orders",
        "sql",
        metadata={"schema": "production"},
        schema="production",
    )
    db.insert_node(
        file_id,
        "table",
        "customers",
        "sql",
        metadata={"schema": "staging"},
        schema="staging",
    )

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

    staging_id = db.insert_node(
        file_id,
        "table",
        "orders",
        "sql",
        metadata={"schema": "staging"},
        schema="staging",
    )
    prod_id = db.insert_node(
        file_id,
        "table",
        "orders",
        "sql",
        metadata={"schema": "production"},
        schema="production",
    )

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

    db.insert_column_lineage(
        file_id,
        "dim_users",
        "created_date",
        0,
        "created_date",
        "dim_users",
        "CAST(created_at AS DATE)",
    )
    db.insert_column_lineage(
        file_id,
        "dim_users",
        "created_date",
        1,
        "created_at",
        "users",
    )

    # Query by output node
    result = db.query_column_lineage(
        output_node="dim_users",
        column="created_date",
    )
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
    db.insert_node(file_id, "table", "orders", "sql")

    # Run cleanup
    cleaned = db.cleanup_phantoms()
    assert cleaned == 1

    # Edge should now point to the real node
    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) == 1
    assert refs["inbound"][0]["name"] == "my_query"

    # Phantom should be gone
    phantom_check = db.conn.execute("SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]).fetchone()[0]
    assert phantom_check == 0
    db.close()


def test_cleanup_orphaned_phantoms():
    """Phantom nodes with no edges are deleted by cleanup_phantoms()."""
    db = GraphDB()

    # Create a phantom node with no edges
    phantom_id = db.get_or_create_phantom("orphan_table", "table", "sql")

    # Verify it exists
    count = db.conn.execute("SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]).fetchone()[0]
    assert count == 1

    # Run cleanup
    cleaned = db.cleanup_phantoms()
    assert cleaned == 1

    # Phantom should be gone
    count = db.conn.execute("SELECT COUNT(*) FROM nodes WHERE node_id = ?", [phantom_id]).fetchone()[0]
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

    db.insert_edges_batch(
        [
            (n1, n2, "references", "FROM clause", None),
            (n1, n3, "references", "JOIN clause", None),
        ]
    )

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

    db.insert_column_usage_batch(
        [
            (node_id, "orders", "id", "select", file_id, None, None),
            (node_id, "orders", "total", "where", file_id, None, "total > 100"),
            (node_id, "customers", "name", "select", file_id, "customer_name", None),
        ]
    )

    result = db.query_column_usage("orders")
    assert result["total_count"] == 2
    db.close()


def test_insert_column_lineage_batch():
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Each row: (file_id, output_node, output_column, chain_index,
    #            hop_index, hop_column, hop_table, hop_expression)
    db.insert_column_lineage_batch(
        [
            (
                file_id,
                "dim_users",
                "created_date",
                0,
                0,
                "created_date",
                "dim_users",
                "CAST(created_at AS DATE)",
            ),
            (
                file_id,
                "dim_users",
                "created_date",
                0,
                1,
                "created_at",
                "users",
                None,
            ),
            (file_id, "dim_users", "user_name", 0, 0, "user_name", "dim_users", None),
            (file_id, "dim_users", "user_name", 0, 1, "name", "users", None),
        ]
    )

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
                file_id,
                "dim_users",
                col,
                hop_idx,
                f"src_{hop_idx}",
                f"table_{hop_idx}",
                chain_index=chain_idx,
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
                file_id,
                "wide_table",
                col,
                hop_idx,
                f"src_{hop_idx}",
                f"tbl_{hop_idx}",
                chain_index=chain_idx,
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

    import os
    import tempfile

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
            file_id,
            "table",
            "my_snippet_table",
            "sql",
            line_start=4,
            line_end=6,
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
            file_id,
            "table",
            "padded_table",
            "sql",
            line_start=10,
            line_end=10,
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
        db.upsert_repo("ghost-test", tmpdir)

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
            import time

            time.sleep(0.1)  # hold transaction open briefly

    def reader():
        barrier.wait()  # wait for writer to be mid-transaction
        try:
            result = db._execute_read("SELECT COUNT(*) FROM nodes WHERE name = 'existing'").fetchone()
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
            import time

            time.sleep(0.1)

    def reader():
        barrier.wait()
        # Thread B should NOT see in_transaction=True
        thread_b_saw_transaction.append(getattr(db._tlocal, "in_transaction", False))
        # And reads should use cursor path (not main connection)
        result = db._execute_read("SELECT COUNT(*) FROM nodes").fetchone()
        assert result[0] >= 1

    t_write = threading.Thread(target=writer)
    t_read = threading.Thread(target=reader)
    t_write.start()
    t_read.start()
    t_write.join(timeout=10)
    t_read.join(timeout=10)

    assert thread_b_saw_transaction == [False], "Thread B should not see thread A's in_transaction flag"
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
    row = db._execute_read("SELECT node_id FROM nodes WHERE node_id = ?", [phantom_a_id]).fetchone()
    assert row is not None, "phantom_a should survive (has real inbound edge)"

    # phantom_b should be gone
    row_b = db._execute_read("SELECT node_id FROM nodes WHERE node_id = ?", [phantom_b_id]).fetchone()
    assert row_b is None, "phantom_b should be deleted (only phantom inbound edges)"

    # phantom_c should be gone (no inbound edges at all, orphaned after B's edges removed
    # or was already identified as stale/orphaned)
    row_c = db._execute_read("SELECT node_id FROM nodes WHERE node_id = ?", [phantom_c_id]).fetchone()
    assert row_c is None, "phantom_c should be deleted (no real inbound edges)"
    db.close()


# ── v1.1: columns table ──


def test_columns_table_created_fresh_db():
    """columns table exists with correct structure on a fresh database."""
    db = GraphDB()
    cols = db._execute_read(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'columns' ORDER BY ordinal_position"
    ).fetchall()
    col_names = [c[0] for c in cols]
    assert "column_id" in col_names
    assert "node_id" in col_names
    assert "column_name" in col_names
    assert "data_type" in col_names
    assert "position" in col_names
    assert "source" in col_names
    assert "description" in col_names

    # Verify sequence exists via catalog
    seq = db._execute_read(
        "SELECT sequence_name FROM duckdb_sequences() WHERE sequence_name = 'seq_column_id'"
    ).fetchone()
    assert seq is not None

    # Verify indexes exist
    indexes = db._execute_read(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'columns'"
    ).fetchall()
    idx_names = {r[0] for r in indexes}
    assert "idx_columns_node" in idx_names
    assert "idx_columns_name" in idx_names
    db.close()


def test_columns_table_migration_existing_db():
    """columns table is created on an existing DB without affecting other tables."""
    db = GraphDB()
    # Insert data into all existing tables
    repo_id = db.upsert_repo("migration-test", "/tmp/migration")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    node_id2 = db.insert_node(file_id, "query", "my_query", "sql")
    db.insert_edge(node_id2, node_id, "references", "FROM clause")

    # Re-init (simulates opening an existing DB with new schema)
    db.conn.execute(SCHEMA_SQL)
    db.conn.execute(INDEX_SQL)

    # All existing data should be intact across all tables
    assert db.resolve_node("orders", "table", repo_id) == node_id
    assert db._execute_read("SELECT COUNT(*) FROM repos").fetchone()[0] >= 1
    assert db._execute_read("SELECT COUNT(*) FROM files").fetchone()[0] >= 1
    assert db._execute_read("SELECT COUNT(*) FROM edges").fetchone()[0] >= 1

    # columns table should exist
    count = db._execute_read(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'columns'"
    ).fetchone()[0]
    assert count == 1
    db.close()


def test_columns_migration_idempotent():
    """Running schema creation twice does not error or lose data."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")

    # Insert column data
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
        (node_id, "status", "TEXT", 1, "definition", "Order status"),
    ])

    # Re-run schema (idempotent)
    db.conn.execute(SCHEMA_SQL)
    db.conn.execute(INDEX_SQL)

    # Column data should be preserved
    rows = db.conn.execute(
        "SELECT column_name, data_type FROM columns WHERE node_id = ? ORDER BY position",
        [node_id],
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("order_id", "INT")
    assert rows[1] == ("status", "TEXT")
    db.close()


def test_insert_columns_batch_upsert():
    """insert_columns_batch upserts on (node_id, column_name) conflict."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")

    # Initial insert
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
    ])

    # Upsert with new description
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "schema_yml", "Primary key"),
    ])

    rows = db.conn.execute(
        "SELECT source, description FROM columns WHERE node_id = ? AND column_name = 'order_id'",
        [node_id],
    ).fetchall()
    assert len(rows) == 1
    assert rows[0] == ("schema_yml", "Primary key")
    db.close()


def test_insert_columns_batch_empty():
    """insert_columns_batch with empty list returns 0."""
    db = GraphDB()
    count = db.insert_columns_batch([])
    assert count == 0
    db.close()


def test_delete_repo_cascades_columns():
    """delete_repo removes column definitions for the repo's nodes."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
        (node_id, "status", "TEXT", 1, "definition", None),
    ])

    # Verify columns exist
    count = db.conn.execute("SELECT COUNT(*) FROM columns").fetchone()[0]
    assert count == 2

    db.delete_repo(repo_id)

    # Columns should be gone
    count = db.conn.execute("SELECT COUNT(*) FROM columns").fetchone()[0]
    assert count == 0
    db.close()


def test_delete_file_data_cascades_columns():
    """delete_file_data removes column definitions for the file's nodes."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
    ])

    db.delete_file_data(repo_id, "query.sql")

    count = db.conn.execute("SELECT COUNT(*) FROM columns").fetchone()[0]
    assert count == 0
    db.close()


def test_insert_columns_batch_coalesce_description():
    """Upsert preserves existing description when new value is NULL."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")

    # First insert with description from schema_yml
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "schema_yml", "Primary key"),
    ])

    # Re-insert from DDL parse (no description)
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
    ])

    row = db.conn.execute(
        "SELECT source, description FROM columns WHERE node_id = ? AND column_name = 'order_id'",
        [node_id],
    ).fetchone()
    # Source updates (definition wins), but description is preserved via COALESCE
    assert row[0] == "definition"
    assert row[1] == "Primary key"
    db.close()


def test_insert_columns_batch_coalesce_data_type():
    """Upsert preserves existing data_type when new value is NULL."""
    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql")

    # First insert with type from DDL
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
    ])

    # Re-insert from inferred source (no type)
    db.insert_columns_batch([
        (node_id, "order_id", None, 0, "inferred", None),
    ])

    row = db.conn.execute(
        "SELECT data_type, source FROM columns WHERE node_id = ? AND column_name = 'order_id'",
        [node_id],
    ).fetchone()
    # data_type preserved via COALESCE, source updates
    assert row[0] == "INT"
    assert row[1] == "inferred"
    db.close()


# ── DuckPGQ property graph tests ──


def test_duckpgq_init_success():
    """DuckPGQ installs/loads automatically and the property graph is queryable."""
    import pytest

    db = GraphDB()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")

    # Property graph must be queryable — no try/except, failures are real bugs
    result = db._execute_read(
        "FROM GRAPH_TABLE (sqlprism_graph MATCH (n:nodes) COLUMNS (n.node_id)) LIMIT 1"
    ).fetchall()
    assert isinstance(result, list)

    db.close()


def test_duckpgq_init_fallback():
    """When DuckPGQ install fails, has_pgq is False and refresh is a no-op."""
    from unittest.mock import patch

    original_init_pgq = GraphDB._init_pgq

    def _failing_init_pgq(self):
        # Simulate DuckPGQ not being available by patching _execute_write
        # to fail on LOAD duckpgq
        original_execute = self._execute_write

        def _reject_pgq(sql, params=None):
            if "duckpgq" in sql.lower():
                raise RuntimeError("Extension not available")
            return original_execute(sql, params)

        with patch.object(self, "_execute_write", new=_reject_pgq):
            original_init_pgq(self)

    with patch.object(GraphDB, "_init_pgq", _failing_init_pgq):
        db = GraphDB()

    assert db.has_pgq is False
    # refresh_property_graph should be a safe no-op
    db.refresh_property_graph()
    db.close()


def test_duckpgq_refresh_after_reindex():
    """Property graph reflects newly inserted nodes after refresh."""
    import pytest

    db = GraphDB()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")

    # Insert test data
    repo_id = db.upsert_repo("pgq-test", "/tmp/pgq")
    file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
    src_id = db.insert_node(file_id, "table", "orders", "sql")
    tgt_id = db.insert_node(file_id, "table", "customers", "sql")
    db.insert_edge(src_id, tgt_id, "references")

    # Refresh property graph so it picks up the new rows
    db.refresh_property_graph()

    # Query must succeed — no try/except fallback
    result = db._execute_read(
        "FROM GRAPH_TABLE (sqlprism_graph MATCH (n:nodes) COLUMNS (n.name)) LIMIT 10"
    ).fetchall()
    names = [r[0] for r in result]
    assert "orders" in names
    assert "customers" in names

    db.close()


def test_duckpgq_tools_check_flag():
    """has_pgq property returns a bool usable for feature gating."""
    import pytest

    db = GraphDB()
    assert isinstance(db.has_pgq, bool)
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")
    assert db.has_pgq is True
    db.close()


# ── Trace dispatch and CTE parity tests ──


def _build_trace_graph():
    """Create a graph with known topology for trace tests.

    Topology (downstream flow):
        raw_orders -> stg_orders -> marts_revenue
        raw_orders -> dim_customers

    Edges follow source -> target for downstream traversal:
        raw_orders  -> stg_orders
        stg_orders  -> marts_revenue
        raw_orders  -> dim_customers
    """
    db = GraphDB()
    repo_id = db.upsert_repo("trace-repo", "/tmp/trace")
    file_id = db.insert_file(repo_id, "models.sql", "sql", "trace123")

    raw_orders = db.insert_node(file_id, "table", "raw_orders", "sql")
    stg_orders = db.insert_node(file_id, "table", "stg_orders", "sql")
    marts_revenue = db.insert_node(file_id, "table", "marts_revenue", "sql")
    dim_customers = db.insert_node(file_id, "table", "dim_customers", "sql")

    # Edges follow source->target for downstream traversal
    # raw_orders -> stg_orders -> marts_revenue, raw_orders -> dim_customers
    db.insert_edge(raw_orders, stg_orders, "references")
    db.insert_edge(stg_orders, marts_revenue, "references")
    db.insert_edge(raw_orders, dim_customers, "references")

    return db


def _assert_trace_structure(result):
    """Verify trace result has expected top-level keys."""
    assert "root" in result
    assert "paths" in result
    assert "depth_summary" in result
    assert "repos_affected" in result


def test_trace_deps_duckpgq():
    """PGQ trace from raw_orders downstream finds all dependants."""
    import pytest

    db = _build_trace_graph()
    db.refresh_property_graph()

    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")

    result = db.query_trace("raw_orders", direction="downstream", max_depth=3)

    _assert_trace_structure(result)
    names = {p["name"] for p in result["paths"]}
    assert names == {"stg_orders", "marts_revenue", "dim_customers"}
    db.close()


def test_trace_deps_cte_fallback():
    """CTE fallback finds the same dependants when PGQ is disabled."""
    db = _build_trace_graph()
    db._has_pgq = False

    result = db.query_trace("raw_orders", direction="downstream", max_depth=3)

    _assert_trace_structure(result)
    names = {p["name"] for p in result["paths"]}
    assert names == {"stg_orders", "marts_revenue", "dim_customers"}

    # CTE provides real per-hop depth
    depths = {p["name"]: p["depth"] for p in result["paths"]}
    assert depths["stg_orders"] == 1
    assert depths["dim_customers"] == 1
    assert depths["marts_revenue"] == 2
    db.close()


def test_pr_impact_duckpgq_multi_root():
    """PGQ trace from multiple roots finds expected downstream models."""
    import pytest

    db = _build_trace_graph()
    db.refresh_property_graph()

    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")

    # Trace from raw_orders — should find all three dependants
    result_raw = db.query_trace("raw_orders", direction="downstream")
    _assert_trace_structure(result_raw)
    names_raw = {p["name"] for p in result_raw["paths"]}
    assert names_raw == {"stg_orders", "marts_revenue", "dim_customers"}

    # Trace from stg_orders — should find only marts_revenue
    result_stg = db.query_trace("stg_orders", direction="downstream")
    names_stg = {p["name"] for p in result_stg["paths"]}
    assert names_stg == {"marts_revenue"}
    db.close()


def test_pr_impact_cte_fallback():
    """CTE path with exclude_edges filters out excluded edges and their dependants."""
    db = _build_trace_graph()
    db._has_pgq = False

    # Exclude the edge raw_orders -> stg_orders
    result = db.query_trace(
        "raw_orders",
        direction="downstream",
        exclude_edges={("raw_orders", "stg_orders")},
    )

    _assert_trace_structure(result)
    names = {p["name"] for p in result["paths"]}
    assert names == {"dim_customers"}
    db.close()


def test_pr_impact_exclude_edges_forces_cte():
    """exclude_edges forces CTE dispatch even when PGQ is available."""
    import pytest

    db = _build_trace_graph()
    db.refresh_property_graph()

    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ not available in this environment")

    # PGQ is available but exclude_edges should force CTE
    result = db.query_trace(
        "raw_orders",
        direction="downstream",
        exclude_edges={("raw_orders", "stg_orders")},
    )

    _assert_trace_structure(result)
    names = {p["name"] for p in result["paths"]}
    # CTE with exclusion: stg_orders and marts_revenue unreachable
    assert names == {"dim_customers"}
    db.close()


def test_trace_max_depth_boundary():
    """max_depth=1 excludes models beyond 1 hop."""
    db = _build_trace_graph()
    db._has_pgq = False  # Use CTE for reliable depth

    result = db.query_trace("raw_orders", direction="downstream", max_depth=1)

    _assert_trace_structure(result)
    names = {p["name"] for p in result["paths"]}
    # Only depth-1 nodes: stg_orders and dim_customers (not marts_revenue at depth 2)
    assert names == {"stg_orders", "dim_customers"}
    assert result["depth_summary"] == {1: 2}
    db.close()


def _build_cte_alias_graph():
    """Simulate a dbt-compiled model whose file stem is also a CTE alias.

    Mirrors jaffle-mesh finance/orders.sql where ``orders`` exists as:
      - query (file stem, the CREATE wrapper)
      - table (the CREATEd target)
      - cte (the first CTE in the WITH clause that reads from stg_orders)

    Edges mirror the SQL parser's convention
    (``source_name = file_stem``, ``target_name = referenced_table``):

      orders (query)      -[defines]->        orders (table)       # CREATE wrap
      orders (query)      -[references]->     stg_orders (table)   # orders.sql FROM stg_orders
      orders (cte)        -[cte_references]-> stg_orders (table)
      order_items (query) -[references]->     orders (table)       # a separate model reads orders
    """
    db = GraphDB()
    repo_id = db.upsert_repo("cte-alias-repo", "/tmp/cte-alias")
    orders_file = db.insert_file(repo_id, "orders.sql", "sql", "cte-alias-123")
    items_file = db.insert_file(repo_id, "order_items.sql", "sql", "items-456")

    orders_query = db.insert_node(orders_file, "query", "orders", "sql")
    orders_table = db.insert_node(orders_file, "table", "orders", "sql")
    orders_cte = db.insert_node(orders_file, "cte", "orders", "sql")
    stg_orders = db.insert_node(orders_file, "table", "stg_orders", "sql")
    items_query = db.insert_node(items_file, "query", "order_items", "sql")

    db.insert_edge(orders_query, orders_table, "defines", "CREATE statement")
    db.insert_edge(orders_query, stg_orders, "references")
    db.insert_edge(orders_cte, stg_orders, "cte_references")
    db.insert_edge(items_query, orders_table, "references")
    return db


def test_trace_prefers_table_root_over_cte():
    """Issue #122: query_trace without kind picks table root and produces clean paths.

    Covers the full BDD acceptance criteria from the issue in a single test:
    given ``orders`` exists as table/query/cte, the reported root is the table
    and ``orders`` never appears in its own downstream paths — while the
    legitimate downstream target ``stg_orders`` still surfaces so the filter
    is proven surgical, not wholesale.
    """
    db = _build_cte_alias_graph()

    result = db.query_trace("orders", direction="downstream", max_depth=3)

    _assert_trace_structure(result)
    assert result["root"] == {"name": "orders", "kind": "table"}
    names = {p["name"] for p in result["paths"]}
    assert "orders" not in names
    assert "stg_orders" in names
    db.close()


def test_trace_excludes_self_via_defines_edge():
    """Issue #122: orders does not appear in its own downstream trace; real refs do."""
    db = _build_cte_alias_graph()

    result = db.query_trace("orders", direction="downstream", max_depth=5)

    names = {p["name"] for p in result["paths"]}
    # orders itself must not appear — the defines edge is identity, not dataflow,
    # and a query-local CTE alias must not pull the CREATE target back in.
    assert "orders" not in names
    # ...and a legitimate downstream target still surfaces — proves the filter
    # is surgical rather than wholesale.
    assert "stg_orders" in names
    # No path should carry the 'defines' relationship after filtering.
    assert all(p["relationship"] != "defines" for p in result["paths"])
    db.close()


def test_trace_upstream_excludes_self_via_defines_edge():
    """Issue #122: upstream trace also filters the defines edge without collateral damage."""
    db = _build_cte_alias_graph()

    result = db.query_trace("orders", direction="upstream", max_depth=5)

    names = {p["name"] for p in result["paths"]}
    assert "orders" not in names
    # The order_items model reads orders via a real references edge — that must
    # still surface when tracing upstream from orders (table).
    assert "order_items" in names
    assert all(p["relationship"] != "defines" for p in result["paths"])
    db.close()


def test_trace_both_direction_excludes_self_via_defines_edge():
    """direction='both' splits paths and applies the defines filter to each side."""
    db = _build_cte_alias_graph()

    result = db.query_trace("orders", direction="both", max_depth=3)

    assert result["root"] == {"name": "orders", "kind": "table"}
    downstream_names = {p["name"] for p in result["downstream"]}
    upstream_names = {p["name"] for p in result["upstream"]}
    assert "orders" not in downstream_names
    assert "orders" not in upstream_names
    assert "stg_orders" in downstream_names
    assert "order_items" in upstream_names
    db.close()


def test_references_excludes_defines_edge():
    """Issue #122: query_references skips defines edges in both directions."""
    db = _build_cte_alias_graph()

    result = db.query_references("orders", kind="table", include_snippets=False)

    inbound_rels = {e["relationship"] for e in result["inbound"]}
    outbound_rels = {e["relationship"] for e in result["outbound"]}
    assert "defines" not in inbound_rels
    assert "defines" not in outbound_rels
    # Non-defines inbound references still surface (order_items model reads orders)
    inbound_names = {e["name"] for e in result["inbound"]}
    assert "order_items" in inbound_names
    db.close()


def test_references_outbound_filters_defines_not_real_refs():
    """query_references outbound filter drops defines but keeps real references.

    Resolves to the ``orders (query)`` node — which has both a ``defines`` edge
    to ``orders (table)`` and a ``references`` edge to ``stg_orders``. Without
    this coverage the outbound filter could be silently removed and
    ``test_references_excludes_defines_edge`` would still pass vacuously
    (``orders (table)`` has no outbound edges in the fixture).
    """
    db = _build_cte_alias_graph()

    result = db.query_references("orders", kind="query", include_snippets=False)

    outbound = result["outbound"]
    out_names = {e["name"] for e in outbound}
    out_rels = {e["relationship"] for e in outbound}
    # Real references survive
    assert "stg_orders" in out_names
    assert "references" in out_rels
    # The defines target (the CREATE'd orders table) must not leak through
    assert "orders" not in out_names
    assert "defines" not in out_rels
    db.close()


def test_trace_explicit_cte_kind_still_works():
    """Asking for kind='cte' explicitly traces from the CTE node."""
    db = _build_cte_alias_graph()

    result = db.query_trace("orders", kind="cte", direction="downstream", max_depth=3)

    _assert_trace_structure(result)
    assert result["root"] == {"name": "orders", "kind": "cte"}
    # The CTE references stg_orders via cte_references — still surfaces.
    names = {p["name"] for p in result["paths"]}
    assert "stg_orders" in names
    db.close()


def test_trace_falls_back_to_cte_when_no_real_node():
    """When kind is unspecified and only a CTE matches the name, fall back to it.

    Without the fallback, ``query_trace("my_cte")`` would return an empty root
    indistinguishable from "not indexed" — callers would have no signal that
    the name actually exists in the graph.
    """
    db = GraphDB()
    repo_id = db.upsert_repo("cte-only-repo", "/tmp/cte-only")
    file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "cte-only-123")

    # No table/view/query named 'scratch' — only the CTE alias exists.
    db.insert_node(file_id, "cte", "scratch", "sql")
    pipeline = db.insert_node(file_id, "query", "pipeline", "sql")
    # No edges are needed for this assertion; root resolution is what matters.

    result = db.query_trace("scratch", direction="downstream")

    assert result["root"] == {"name": "scratch", "kind": "cte"}
    # Not strictly needed but confirms the structure is well-formed
    _assert_trace_structure(result)
    _ = pipeline  # silence unused
    db.close()


def test_trace_prefers_table_root_over_subquery():
    """subquery aliases behave like CTEs and must never win as a trace root."""
    db = GraphDB()
    repo_id = db.upsert_repo("subq-repo", "/tmp/subq")
    file_id = db.insert_file(repo_id, "orders.sql", "sql", "subq-123")

    # Insert the subquery first so ordering depends on kind rank, not insertion.
    db.insert_node(file_id, "subquery", "orders", "sql")
    table_id = db.insert_node(file_id, "table", "orders", "sql")
    stg = db.insert_node(file_id, "table", "stg_orders", "sql")
    # Table points downstream to stg so the trace has something to find from root.
    db.insert_edge(table_id, stg, "references")

    result = db.query_trace("orders", direction="downstream", max_depth=3)

    assert result["root"] == {"name": "orders", "kind": "table"}
    names = {p["name"] for p in result["paths"]}
    assert "stg_orders" in names
    db.close()


def test_resolve_node_fallback_prefers_table_over_cte():
    """Issue #122: kind-relaxed fallback ranks table > view > query; aliases last."""
    db = GraphDB()
    repo_id = db.upsert_repo("rank-repo", "/tmp/rank")
    file_id = db.insert_file(repo_id, "orders.sql", "sql", "rank-123")

    # Insert aliases first to prove ordering is not insertion-order dependent.
    db.insert_node(file_id, "cte", "orders", "sql")
    db.insert_node(file_id, "subquery", "orders", "sql")
    query_id = db.insert_node(file_id, "query", "orders", "sql")
    view_id = db.insert_node(file_id, "view", "orders", "sql")
    table_id = db.insert_node(file_id, "table", "orders", "sql")

    # Requested kind 'source' doesn't exist here, forcing the kind-relaxed
    # secondary rank to decide: table > view > query; cte and subquery last.
    assert db.resolve_node("orders", "source", repo_id) == table_id
    # Cross-repo fallback path (no repo_id) follows the same ordering.
    assert db.resolve_node("orders", "source") == table_id
    # Requested kind takes precedence over the secondary rank.
    assert db.resolve_node("orders", "view", repo_id) == view_id
    assert db.resolve_node("orders", "query", repo_id) == query_id
    db.close()


def test_resolve_node_fallback_respects_schema_filter():
    """Kind-relaxed fallback honors the schema qualifier in both branches."""
    db = GraphDB()
    repo_id = db.upsert_repo("schema-rank-repo", "/tmp/schema-rank")
    file_id = db.insert_file(repo_id, "orders.sql", "sql", "schema-rank-123")

    # Two real nodes named 'orders' in distinct schemas plus a same-name CTE
    # with no schema. The fallback must prefer the schema-qualified table.
    staging_table = db.insert_node(
        file_id, "table", "orders", "sql", schema="staging"
    )
    prod_table = db.insert_node(
        file_id, "table", "orders", "sql", schema="production"
    )
    db.insert_node(file_id, "cte", "orders", "sql")

    # Same-repo schema-qualified fallback
    assert (
        db.resolve_node("orders", "source", repo_id, schema="staging")
        == staging_table
    )
    # Cross-repo schema-qualified fallback
    assert (
        db.resolve_node("orders", "source", schema="production")
        == prod_table
    )
    db.close()


def test_inserts_into_file_stem_collision_no_self_loop():
    """#127: ``INSERT INTO target SELECT FROM src`` in a file whose stem
    collides with ``target`` does NOT resurrect the self-loop class bug #122
    fixed for the ``defines`` edge.

    The SQL parser emits ``file_stem (query) -[inserts_into]-> target (table)``
    for INSERT statements (src/sqlprism/languages/sql.py:410). When the file
    stem equals the target, both endpoints share the same name — the same
    shape that bit ``defines`` before #126. Exploratory coverage confirmed
    the bug resurfaced via ``inserts_into``, so ``_trace_cte`` and
    ``query_references`` widen the filter: ``inserts_into`` is dropped from
    traversal when source.name == target.name. The filter is narrow on
    purpose — legitimate cross-table INSERT dataflow (where the file stem
    and the target differ) is still traversed.
    """
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("inserts-collide-repo", "/tmp/inserts-collide")
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "inserts-123")

        orders_query = db.insert_node(file_id, "query", "orders", "sql")
        orders_table = db.insert_node(file_id, "table", "orders", "sql")
        raw_orders = db.insert_node(file_id, "table", "raw_orders", "sql")

        # Mirrors what SqlParser emits for `INSERT INTO orders SELECT * FROM raw_orders`
        # in a file named orders.sql.
        db.insert_edge(orders_query, orders_table, "inserts_into")
        db.insert_edge(orders_query, raw_orders, "inserts_into")

        # Cover every direction — the filter lives in both CTE arms of
        # _trace_cte and both branches of query_references, so regressions
        # could surface in any of them.
        up = db.query_trace("orders", direction="upstream", max_depth=5)
        down = db.query_trace("orders", direction="downstream", max_depth=5)
        both = db.query_trace("orders", direction="both", max_depth=5)

        for label, paths in (
            ("upstream", up["paths"]),
            ("downstream", down["paths"]),
            ("both.downstream", both["downstream"]),
            ("both.upstream", both["upstream"]),
        ):
            names = {p["name"] for p in paths}
            assert "orders" not in names, (
                f"inserts_into self-loop leaked into {label} trace: {names}"
            )

        # query_references outbound — the sibling filter site. Resolving
        # 'orders' by kind='query' makes the outbound edges the self-loop
        # (to orders(table)) and the real cross-table (to raw_orders).
        refs = db.query_references("orders", kind="query", include_snippets=False)
        out_names = {e["name"] for e in refs["outbound"]}
        assert "orders" not in out_names, (
            f"inserts_into self-loop leaked into outbound references: {out_names}"
        )
        assert "raw_orders" in out_names, "real cross-table inserts_into missing"
    finally:
        db.close()


def test_inserts_into_cross_table_dataflow_still_traced():
    """Complementary to the narrow ``inserts_into`` filter: when the file stem
    does NOT collide with the INSERT target, the ``inserts_into`` edge still
    participates in both ``query_references`` and ``_trace_cte`` — the filter
    only drops the self-loop shape, not legitimate cross-table dataflow.
    """
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("inserts-ok-repo", "/tmp/inserts-ok")
        file_id = db.insert_file(repo_id, "build_orders.sql", "sql", "inserts-ok-1")

        # Source kind is 'query' (not file-stem-colliding with the target).
        builder_query = db.insert_node(file_id, "query", "build_orders", "sql")
        target_table = db.insert_node(file_id, "table", "dim_orders", "sql")

        db.insert_edge(builder_query, target_table, "inserts_into")

        # query_references path
        refs = db.query_references("dim_orders", kind="table", include_snippets=False)
        inbound_names = {e["name"] for e in refs["inbound"]}
        assert "build_orders" in inbound_names, (
            "non-colliding inserts_into must still surface via references; "
            "the filter is intentionally narrow to the self-loop shape only"
        )

        # _trace_cte path (upstream from the table — builder_query should appear)
        trace = db.query_trace("dim_orders", direction="upstream", max_depth=3)
        trace_names = {p["name"] for p in trace["paths"]}
        assert "build_orders" in trace_names, (
            "non-colliding inserts_into must still surface via trace"
        )
    finally:
        db.close()

