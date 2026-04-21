"""Tests for the indexer orchestrator."""



from sqlprism.core.mcp_tools import _compute_structural_diff
from sqlprism.types import (
    ColumnUsageResult,
    EdgeResult,
    NodeResult,
    ParseResult,
)

# ── P6.2: Integration — full reindex cycle ──


def test_full_reindex_cycle(tmp_path):
    """Full reindex cycle: create files, index, modify, re-index, verify."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    # Setup repo directory with SQL files
    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)")
    (repo_dir / "customers.sql").write_text("CREATE TABLE customers (id INT, name TEXT)")
    (repo_dir / "report.sql").write_text(
        "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id"
    )

    db = GraphDB()
    indexer = Indexer(db)

    # First reindex
    stats = indexer.reindex_repo("test", str(repo_dir))
    assert stats["files_scanned"] == 3
    assert stats["files_added"] == 3
    assert stats["nodes_added"] > 0
    assert stats["edges_added"] > 0

    # Verify data is in the DB
    status = db.get_index_status()
    assert status["totals"]["files"] == 3
    assert status["totals"]["nodes"] > 0

    # Search works
    results = db.query_search("orders")
    assert results["total_count"] >= 1

    # References work
    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) >= 1

    # Second reindex (no changes)
    stats2 = indexer.reindex_repo("test", str(repo_dir))
    assert stats2["files_changed"] == 0
    assert stats2["files_added"] == 0
    assert stats2["files_removed"] == 0

    # Add a new file
    (repo_dir / "summary.sql").write_text("SELECT COUNT(*) FROM orders")

    # Third reindex (detect addition)
    stats3 = indexer.reindex_repo("test", str(repo_dir))
    assert stats3["files_added"] == 1
    assert stats3["files_changed"] == 0

    # Verify total file count grew
    status2 = db.get_index_status()
    assert status2["totals"]["files"] == 4

    # Modify a file
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL, status TEXT)")

    # Fourth reindex (detect change)
    stats4 = indexer.reindex_repo("test", str(repo_dir))
    assert stats4["files_changed"] == 1

    # Delete a file
    (repo_dir / "summary.sql").unlink()

    # Fifth reindex (detect deletion)
    stats5 = indexer.reindex_repo("test", str(repo_dir))
    assert stats5["files_removed"] == 1

    db.close()


def test_reindex_with_dialect(tmp_path):
    """Reindex with dialect applies case normalization."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "pgrepo"
    repo_dir.mkdir()
    (repo_dir / "query.sql").write_text("SELECT ID, Name FROM Orders")

    db = GraphDB()
    indexer = Indexer(db)
    stats = indexer.reindex_repo("pg", str(repo_dir), dialect="postgres")
    assert stats["files_added"] == 1

    # Postgres should lowercase identifiers
    results = db.query_search("orders")
    assert results["total_count"] >= 1

    db.close()


def test_reindex_with_dialect_overrides(tmp_path):
    """Dialect overrides apply per-path dialect selection."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "multidialect"
    repo_dir.mkdir()
    (repo_dir / "athena").mkdir()
    (repo_dir / "athena" / "query.sql").write_text("SELECT id FROM orders")
    (repo_dir / "starrocks").mkdir()
    (repo_dir / "starrocks" / "query.sql").write_text("SELECT id FROM orders")

    db = GraphDB()
    indexer = Indexer(db)
    stats = indexer.reindex_repo(
        "multi",
        str(repo_dir),
        dialect="postgres",
        dialect_overrides={"athena/": "athena", "starrocks/": "starrocks"},
    )
    assert stats["files_added"] == 2
    db.close()


def test_reindex_skips_jinja_templated_sql(tmp_path):
    """Files containing Jinja markers are skipped, not parsed."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "jinja_repo"
    repo_dir.mkdir()
    (repo_dir / "clean.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    (repo_dir / "expr_template.sql").write_text(
        "CREATE OR REPLACE VIEW {{ project }}.analytics.orders AS SELECT * FROM raw.orders"
    )
    (repo_dir / "block_template.sql").write_text(
        "SELECT {% for col in columns %}{{ col }},{% endfor %} 1 FROM t"
    )

    db = GraphDB()
    indexer = Indexer(db)

    stats = indexer.reindex_repo("jinja", str(repo_dir))
    assert stats["files_scanned"] == 3
    assert stats["files_added"] == 3
    assert stats["files_skipped_jinja"] == 2
    # Only clean.sql contributes nodes
    nodes = db.query_search("orders")
    assert nodes["total_count"] >= 1
    # No nodes from the Jinja files (e.g. no `analytics` view, no template CTE names)
    raw_nodes = db.query_search("analytics")
    assert raw_nodes["total_count"] == 0

    # Second reindex — skipped files have checksums recorded, so nothing is re-read
    stats2 = indexer.reindex_repo("jinja", str(repo_dir))
    assert stats2["files_added"] == 0
    assert stats2["files_changed"] == 0
    assert stats2["files_skipped_jinja"] == 0

    db.close()


# ── P6.3: MCP tool + integration tests ──


def test_mcp_search_integration():
    """MCP search tool queries GraphDB correctly."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    db.insert_node(file_id, "table", "dim_users", "sql")
    db.insert_node(file_id, "table", "fact_orders", "sql")
    db.insert_node(file_id, "view", "v_active_users", "sql")

    # Search by pattern
    results = db.query_search("user")
    assert results["total_count"] == 2
    names = {m["name"] for m in results["matches"]}
    assert "dim_users" in names
    assert "v_active_users" in names

    # Filter by kind
    results = db.query_search("user", kind="table")
    assert results["total_count"] == 1
    assert results["matches"][0]["name"] == "dim_users"

    db.close()


def test_mcp_find_column_usage_integration():
    """MCP column usage tool returns correct results with transforms."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "query", "report", "sql")

    db.insert_column_usage(node_id, "orders", "id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "amount", "where", file_id, transform="amount > 100")
    db.insert_column_usage(
        node_id,
        "orders",
        "created_at",
        "select",
        file_id,
        alias="order_date",
        transform="CAST(created_at AS DATE)",
    )

    result = db.query_column_usage("orders")
    assert result["total_count"] == 3
    assert result["summary"]["select"] == 2
    assert result["summary"]["where"] == 1

    # Find specific column with transform
    result = db.query_column_usage("orders", column="created_at")
    assert result["total_count"] == 1
    assert result["usage"][0]["transform"] == "CAST(created_at AS DATE)"
    assert result["usage"][0]["alias"] == "order_date"

    db.close()


def test_mcp_trace_integration():
    """MCP trace tool follows multi-hop chains."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Build a chain: raw_orders -> stg_orders -> dim_orders -> report
    # Edge direction: source references target (source depends on target)
    raw = db.insert_node(file_id, "table", "raw_orders", "sql")
    stg = db.insert_node(file_id, "table", "stg_orders", "sql")
    dim = db.insert_node(file_id, "table", "dim_orders", "sql")
    report = db.insert_node(file_id, "query", "report", "sql")

    # downstream trace follows source→target edges, so build: raw→stg→dim→report
    db.insert_edge(raw, stg, "feeds")
    db.insert_edge(stg, dim, "feeds")
    db.insert_edge(dim, report, "feeds")

    # Trace downstream from raw_orders
    result = db.query_trace("raw_orders", kind="table", direction="downstream", max_depth=3)
    assert result["root"]["name"] == "raw_orders"
    names = {p["name"] for p in result["paths"]}
    assert "stg_orders" in names
    assert "dim_orders" in names
    assert "report" in names

    # Trace upstream from report (follows target→source)
    result = db.query_trace("report", kind="query", direction="upstream", max_depth=3)
    names = {p["name"] for p in result["paths"]}
    assert "dim_orders" in names

    db.close()


def test_mcp_column_lineage_integration():
    """MCP column lineage tool returns correct chain data."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # created_date traced through: dim_users -> base -> users.created_at
    db.insert_column_lineage(
        file_id,
        "dim_users",
        "created_date",
        0,
        "created_date",
        "dim_users",
        "CAST(created_at AS DATE)",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "dim_users",
        "created_date",
        1,
        "created_at",
        "base",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "dim_users",
        "created_date",
        2,
        "created_at",
        "users",
        chain_index=0,
    )

    result = db.query_column_lineage(output_node="dim_users", column="created_date")
    assert result["total_count"] == 1
    chain = result["chains"][0]
    assert len(chain["hops"]) == 3
    assert chain["hops"][0]["expression"] == "CAST(created_at AS DATE)"
    assert chain["hops"][2]["table"] == "users"

    # Search by source table
    result = db.query_column_lineage(table="users", column="created_at")
    assert result["total_count"] == 1

    db.close()


def test_structural_diff_added_and_removed():
    """Structural diff detects added and removed nodes."""
    old = {
        "a.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="old_table")],
        )
    }
    new = {
        "a.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="new_table")],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert any(n["name"] == "new_table" for n in diff["nodes_added"])
    assert any(n["name"] == "old_table" for n in diff["nodes_removed"])


def test_structural_diff_edge_changes():
    """Structural diff detects edge additions and removals."""
    edge_old = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="orders",
        target_kind="table",
        relationship="references",
    )
    edge_new = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="customers",
        target_kind="table",
        relationship="references",
    )
    old = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            edges=[edge_old],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            edges=[edge_new],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert any(e["target"] == "customers" for e in diff["edges_added"])
    assert any(e["target"] == "orders" for e in diff["edges_removed"])


def test_structural_diff_column_usage_changes():
    """Structural diff detects column usage additions and removals."""
    col_old = ColumnUsageResult(
        node_name="q",
        node_kind="query",
        table_name="orders",
        column_name="id",
        usage_type="select",
    )
    col_new = ColumnUsageResult(
        node_name="q",
        node_kind="query",
        table_name="orders",
        column_name="amount",
        usage_type="where",
    )
    old = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            column_usage=[col_old],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            column_usage=[col_new],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert len(diff["columns_added"]) == 1
    assert diff["columns_added"][0]["column"] == "amount"
    assert len(diff["columns_removed"]) == 1
    assert diff["columns_removed"][0]["column"] == "id"


# ── P6.3: Phantom node accumulation over reindex cycles ──


def test_phantom_nodes_stable_across_reindex_cycles(tmp_path):
    """Phantom node count doesn't grow unboundedly over repeated reindex cycles."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "phantom_repo"
    repo_dir.mkdir()
    (repo_dir / "report.sql").write_text("SELECT id, name FROM orders WHERE active = 1")

    db = GraphDB()
    indexer = Indexer(db)

    # First reindex
    indexer.reindex_repo("test", str(repo_dir))
    status1 = db.get_index_status()
    phantom_count_1 = status1["phantom_nodes"]

    # Second reindex (no changes) — phantom count shouldn't grow
    indexer.reindex_repo("test", str(repo_dir))
    status2 = db.get_index_status()
    assert status2["phantom_nodes"] == phantom_count_1

    # Third reindex (no changes) — still stable
    indexer.reindex_repo("test", str(repo_dir))
    status3 = db.get_index_status()
    assert status3["phantom_nodes"] == phantom_count_1

    db.close()


def test_phantom_cleanup_graph_layer():
    """Phantom nodes are cleaned up when real counterparts appear (graph layer)."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    # Create phantom node
    phantom_id = db.get_or_create_phantom("orders", "table", "sql")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")
    query_id = db.insert_node(file_id, "query", "report", "sql")
    db.insert_edge(query_id, phantom_id, "references")

    assert db.get_index_status()["phantom_nodes"] >= 1

    # Now create a real node with the same name+kind
    db.insert_node(file_id, "table", "orders", "sql")
    cleaned = db.cleanup_phantoms()
    assert cleaned >= 1

    # Phantom should be gone, edge repointed
    assert db.get_index_status()["phantom_nodes"] == 0
    refs = db.query_references("orders", kind="table")
    assert len(refs["inbound"]) == 1

    db.close()


def test_noise_nodes_filtered_from_trace():
    """dbt test nodes and CTEs are excluded from trace results."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    file_a = db.insert_file(repo_id, "stg_orders.sql", "sql", "aaa")
    stg_id = db.insert_node(file_a, "query", "stg_orders", "sql")

    # Real downstream model
    file_b = db.insert_file(repo_id, "orders.sql", "sql", "bbb")
    orders_id = db.insert_node(file_b, "query", "orders", "sql")
    db.insert_edge(stg_id, orders_id, "references")

    # dbt test node (should be filtered)
    file_c = db.insert_file(repo_id, "test_not_null.sql", "sql", "ccc")
    test_id = db.insert_node(file_c, "table", "not_null_stg_orders_order_id", "sql")
    db.insert_edge(stg_id, test_id, "references")

    # CTE node (should be filtered)
    cte_id = db.insert_node(file_b, "cte", "joined", "sql")
    db.insert_edge(stg_id, cte_id, "references")

    db.refresh_property_graph()
    result = db.query_trace("stg_orders", direction="downstream", max_depth=3)
    names = [p["name"] for p in result["paths"]]

    assert "orders" in names
    assert "not_null_stg_orders_order_id" not in names
    assert "joined" not in names

    db.close()


def test_noise_nodes_filtered_from_references():
    """dbt test nodes are excluded from query_references results."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    file_a = db.insert_file(repo_id, "stg_orders.sql", "sql", "aaa")
    stg_id = db.insert_node(file_a, "query", "stg_orders", "sql")

    # Real inbound ref
    file_b = db.insert_file(repo_id, "orders.sql", "sql", "bbb")
    orders_id = db.insert_node(file_b, "query", "orders", "sql")
    db.insert_edge(orders_id, stg_id, "references")

    # dbt test inbound ref (should be filtered)
    file_c = db.insert_file(repo_id, "test.sql", "sql", "ccc")
    test_id = db.insert_node(file_c, "table", "unique_stg_orders_order_id", "sql")
    db.insert_edge(test_id, stg_id, "references")

    refs = db.query_references("stg_orders")
    inbound_names = [r["name"] for r in refs["inbound"]]

    assert "orders" in inbound_names
    assert "unique_stg_orders_order_id" not in inbound_names

    db.close()


def test_merge_duplicate_nodes():
    """Stub 'table' nodes are merged into defining 'query' nodes in the same repo."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    # File A defines a query node "orders"
    file_a = db.insert_file(repo_id, "orders.sql", "sql", "aaa")
    query_id = db.insert_node(file_a, "query", "orders", "sql")

    # File B references "orders" as a table (stub node created in same repo)
    file_b = db.insert_file(repo_id, "report.sql", "sql", "bbb")
    stub_id = db.insert_node(file_b, "table", "orders", "sql")
    report_id = db.insert_node(file_b, "query", "report", "sql")
    db.insert_edge(report_id, stub_id, "references")

    # Add outbound edges from the real query node
    file_c = db.insert_file(repo_id, "customers.sql", "sql", "ccc")
    customers_id = db.insert_node(file_c, "query", "customers", "sql")
    db.insert_edge(query_id, customers_id, "references")

    # Before merge: report -> stub (dead end), query_id -> customers
    merged = db.merge_duplicate_nodes()
    assert merged >= 1

    # After merge: report -> query_id -> customers (traversable)
    refs = db.query_references("orders")
    assert len(refs["inbound"]) == 1
    assert refs["inbound"][0]["name"] == "report"
    assert len(refs["outbound"]) == 1
    assert refs["outbound"][0]["name"] == "customers"

    db.close()


def test_merge_duplicate_nodes_schema_mismatch():
    """Stub with schema='sushi' merges into defining node with schema=NULL."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")

    # Defining query node with schema=NULL (sqlmesh model)
    file_a = db.insert_file(repo_id, "orders.sql", "sql", "aaa")
    db.insert_node(file_a, "query", "orders", "sql")

    # Stub table node with schema='sushi' (from qualified reference)
    file_b = db.insert_file(repo_id, "report.sql", "sql", "bbb")
    stub_id = db.insert_node(file_b, "table", "orders", "sql")
    # Set schema on the stub
    db._execute_write("UPDATE nodes SET schema = 'sushi' WHERE node_id = ?", [stub_id])
    report_id = db.insert_node(file_b, "query", "report", "sql")
    db.insert_edge(report_id, stub_id, "references")

    merged = db.merge_duplicate_nodes()
    assert merged >= 1

    # After merge: report should reference the query node
    refs = db.query_references("orders")
    assert len(refs["inbound"]) == 1
    assert refs["inbound"][0]["name"] == "report"

    db.close()


# ── 5.5: Column usage dropped counter integration test ──


def test_column_usage_dropped_counter(tmp_path):
    """Column usage referencing an unresolvable node increments column_usage_dropped (2.11)."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import ColumnUsageResult, NodeResult, ParseResult

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", str(tmp_path))

    # Create a ParseResult with column_usage referencing a node that doesn't exist
    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="real_table")],
        column_usage=[
            # This references "ghost_node" which is NOT in the nodes list
            ColumnUsageResult(
                node_name="ghost_node",
                node_kind="query",
                table_name="real_table",
                column_name="id",
                usage_type="select",
            ),
        ],
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "column_usage_dropped": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "test.sql", "sql", "abc")
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    assert stats["column_usage_dropped"] > 0, f"Expected drops but got: {stats}"
    db.close()


# ── v1.2: Task 1.1 — Cross-file edge persistence after incremental reindex ──


def test_cross_file_edges_survive_incremental_reindex(tmp_path):
    """Edges from file B → file A's nodes survive when file A is re-indexed.

    This is the CRITICAL bug where incremental reindex silently lost
    cross-file edges, causing the graph to degrade over time.
    """
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "cross_edge_repo"
    repo_dir.mkdir()

    # File A defines a table
    (repo_dir / "a_orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    # File B references file A's table
    (repo_dir / "b_report.sql").write_text("SELECT id, amount FROM orders WHERE amount > 100")

    db = GraphDB()
    indexer = Indexer(db)

    # First index — both files
    stats1 = indexer.reindex_repo("test", str(repo_dir))
    assert stats1["files_added"] == 2

    # Verify B → orders edge exists
    refs = db.query_references("orders", kind="table")
    inbound_before = len(refs["inbound"])
    assert inbound_before >= 1, "File B should reference orders"

    # Now modify ONLY file A (the target of B's edge)
    (repo_dir / "a_orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL, status TEXT)")

    # Re-index — only file A should change
    stats2 = indexer.reindex_repo("test", str(repo_dir))
    assert stats2["files_changed"] == 1
    assert stats2["files_added"] == 0

    # CRITICAL: B's edge to orders must still exist
    refs_after = db.query_references("orders", kind="table")
    inbound_after = len(refs_after["inbound"])
    assert inbound_after >= 1, "Cross-file edge from B → orders should survive incremental reindex of A"

    db.close()


# ── v1.2: Task 1.2 — Subquery alias column_usage no longer dropped ──


def test_subquery_alias_column_usage_not_dropped(tmp_path):
    """Column usage for subquery aliases should resolve to subquery nodes."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "subquery_repo"
    repo_dir.mkdir()
    (repo_dir / "query.sql").write_text("SELECT x.id FROM (SELECT id FROM orders WHERE id > 0) x")

    db = GraphDB()
    indexer = Indexer(db)
    stats = indexer.reindex_repo("test", str(repo_dir))

    # Subquery alias 'x' should now have a node, so column_usage should resolve
    assert stats["column_usage_dropped"] == 0, "Subquery alias column_usage should not be dropped"

    db.close()


# ── v1.2: Task 1.3 — Schema-qualified node_id_map collision ──


def test_schema_qualified_nodes_no_collision(tmp_path):
    """staging.orders and production.orders should not collide in node_id_map."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "schema_repo"
    repo_dir.mkdir()
    (repo_dir / "query.sql").write_text("SELECT s.id FROM staging.orders s JOIN production.orders p ON s.id = p.id")

    db = GraphDB()
    indexer = Indexer(db)
    indexer.reindex_repo("test", str(repo_dir))

    # Both schema-qualified orders nodes should exist
    results = db.query_search("orders")
    assert results["total_count"] == 2, "staging.orders and production.orders should be separate nodes"

    db.close()


# ── v1.2: Task 1.5 — CTE dedup across statements ──


def test_cte_dedup_across_statements():
    """Same CTE name in two statements should produce one node, not two."""
    from sqlprism.languages.sql import SqlParser

    parser = SqlParser()
    result = parser.parse(
        "test.sql",
        """
        WITH base AS (SELECT 1 AS id) SELECT * FROM base;
        WITH base AS (SELECT 2 AS id) SELECT * FROM base;
    """,
    )

    cte_nodes = [n for n in result.nodes if n.kind == "cte" and n.name == "base"]
    assert len(cte_nodes) == 1, f"Expected 1 CTE node for 'base', got {len(cte_nodes)}"
