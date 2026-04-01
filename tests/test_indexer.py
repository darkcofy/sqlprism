"""Tests for the indexer orchestrator."""

import importlib.metadata

import pytest

from sqlprism.core.indexer import _resolve_dialect
from sqlprism.core.mcp_tools import _compute_structural_diff
from sqlprism.languages.sqlmesh import _validate_command
from sqlprism.types import (
    ColumnDefResult,
    ColumnUsageResult,
    EdgeResult,
    NodeResult,
    ParseResult,
)


def test_version_string():
    """Verify package version is 1.2.0."""
    version = importlib.metadata.version("sqlprism")
    assert version == "1.2.0"


def test_resolve_dialect_no_overrides():
    assert _resolve_dialect("models/foo.sql", "athena", None) == "athena"
    assert _resolve_dialect("models/foo.sql", None, None) is None


def test_resolve_dialect_prefix_override():
    overrides = {
        "starrocks/": "starrocks",
        "athena/": "athena",
    }
    assert _resolve_dialect("starrocks/models/foo.sql", "postgres", overrides) == "starrocks"
    assert _resolve_dialect("athena/queries/bar.sql", "postgres", overrides) == "athena"
    # No match falls back to default
    assert _resolve_dialect("other/baz.sql", "postgres", overrides) == "postgres"


def test_resolve_dialect_glob_override():
    overrides = {
        "**/*_sr.sql": "starrocks",
        "legacy/*.sql": "mysql",
    }
    assert _resolve_dialect("models/fact_orders_sr.sql", None, overrides) == "starrocks"
    assert _resolve_dialect("legacy/old_query.sql", None, overrides) == "mysql"
    assert _resolve_dialect("models/normal.sql", "athena", overrides) == "athena"


# ── P2.2: Command injection validation ──


def test_validate_command_allowed():
    """Valid commands pass validation."""
    _validate_command("uv run python", allowed_keywords={"python", "sqlmesh", "uv"})
    _validate_command("python", allowed_keywords={"python", "sqlmesh", "uv"})
    _validate_command("/usr/bin/python3", allowed_keywords={"python3"})
    _validate_command("uv run dbt", allowed_keywords={"dbt", "uv", "uvx"})
    _validate_command("uvx --with dbt-starrocks dbt", allowed_keywords={"dbt", "uv", "uvx"})


def test_validate_command_rejects_shell_metachar():
    """Commands with shell metacharacters are rejected."""
    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python; rm -rf /", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python | cat", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("$(whoami)", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python & bg", allowed_keywords={"python", "uv"})


def test_validate_command_rejects_unknown_base():
    """Commands with unrecognized base command are rejected."""
    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("rm -rf /", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("curl http://evil.com", allowed_keywords={"python", "uv"})


def test_validate_command_rejects_substring_bypass():
    """Commands that contain an allowed keyword as a substring are rejected."""
    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("pythonmalicious", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("mypython", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("/usr/bin/uvxploit", allowed_keywords={"uv", "uvx"})


def test_validate_command_rejects_empty():
    """Empty command is rejected."""
    with pytest.raises(ValueError, match="Empty command"):
        _validate_command("", allowed_keywords={"python"})


# ── P2.4: Checksum rendered models by content not path ──


def test_checksum_parse_result_content_based():
    """Checksum should change when parse result content changes."""
    from sqlprism.core.indexer import _checksum_parse_result
    from sqlprism.types import NodeResult, ParseResult

    r1 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="orders")])
    r2 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="orders")])
    r3 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="customers")])

    # Same content → same checksum
    assert _checksum_parse_result(r1) == _checksum_parse_result(r2)
    # Different content → different checksum
    assert _checksum_parse_result(r1) != _checksum_parse_result(r3)


# ── P3.3: Fix nodes_modified false positives ──


def test_structural_diff_unchanged_nodes_not_modified():
    """Nodes that exist in both old and new with same edges/columns should NOT be modified."""
    edge = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="orders",
        target_kind="table",
        relationship="references",
    )
    col = ColumnUsageResult(
        node_name="q",
        node_kind="query",
        table_name="orders",
        column_name="id",
        usage_type="select",
    )
    old = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="orders"), NodeResult(kind="query", name="q")],
            edges=[edge],
            column_usage=[col],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="orders"), NodeResult(kind="query", name="q")],
            edges=[edge],
            column_usage=[col],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert diff["nodes_added"] == []
    assert diff["nodes_removed"] == []
    assert diff["nodes_modified"] == []


def test_structural_diff_detects_actual_modification():
    """Nodes with changed edges/columns should show as modified."""
    old_edge = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="orders",
        target_kind="table",
        relationship="references",
    )
    new_edge = EdgeResult(
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
            edges=[old_edge],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            edges=[new_edge],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert len(diff["nodes_modified"]) == 1
    assert diff["nodes_modified"][0]["name"] == "q"


# ── P5.1: Shared utils ──


def test_parse_dotenv_matching_quotes():
    """parse_dotenv strips matching quotes, not mismatched ones."""
    import os
    import tempfile
    from pathlib import Path

    from sqlprism.languages.utils import parse_dotenv

    content = (
        "SIMPLE=hello\n"
        'DOUBLE_QUOTED="world"\n'
        "SINGLE_QUOTED='value'\n"
        'PARTIAL_QUOTE="not closed\n'
        "EMPTY=\n"
        "# comment\n"
        'STARTS_WITH_QUOTE="abc\n'
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write(content)
        f.flush()
        path = Path(f.name)

    try:
        result = parse_dotenv(path)
        assert result["SIMPLE"] == "hello"
        assert result["DOUBLE_QUOTED"] == "world"
        assert result["SINGLE_QUOTED"] == "value"
        assert result["EMPTY"] == ""
        # Mismatched quote should NOT be stripped
        assert result["PARTIAL_QUOTE"] == '"not closed'
        assert result["STARTS_WITH_QUOTE"] == '"abc'
    finally:
        os.unlink(path)


def test_find_venv_dir_fallback():
    """find_venv_dir falls back to project_path when no .venv found."""
    import tempfile
    from pathlib import Path

    from sqlprism.languages.utils import find_venv_dir

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "deep" / "project"
        p.mkdir(parents=True)
        assert find_venv_dir(p) == p


# ── P5.4: chain_index ──


def test_chain_index_disambiguates_multi_path():
    """Multiple lineage chains for same output column get distinct chain_index values."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Two chains for the same output column (e.g. COALESCE(a.x, b.x) AS x)
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=0,
        hop_column="x",
        hop_table="v",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=1,
        hop_column="x",
        hop_table="a",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=0,
        hop_column="x",
        hop_table="v",
        chain_index=1,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=1,
        hop_column="x",
        hop_table="b",
        chain_index=1,
    )

    result = db.query_column_lineage(output_node="v", column="x")
    assert result["total_count"] == 2
    chain_indices = {c["chain_index"] for c in result["chains"]}
    assert chain_indices == {0, 1}
    db.close()


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


# ── v1.2: Task 2.2 — upsert_repo updates path ──


def test_upsert_repo_updates_path():
    """upsert_repo should update the stored path when repo is moved."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id1 = db.upsert_repo("myrepo", "/old/path")
    repo_id2 = db.upsert_repo("myrepo", "/new/path")

    assert repo_id1 == repo_id2, "Should return same repo_id"

    # Path should be updated
    row = db._execute_read("SELECT path FROM repos WHERE repo_id = ?", [repo_id1]).fetchone()
    assert row[0] == "/new/path", "Path should be updated to new location"

    db.close()


# ── v1.2: Task 1.4 — INSERT...SELECT alias resolution ──


def test_insert_select_resolves_table_aliases():
    """INSERT...SELECT should resolve table aliases to real table names."""
    from sqlprism.languages.sql import SqlParser

    parser = SqlParser()
    result = parser.parse(
        "test.sql",
        """
        INSERT INTO target (col_a, col_b)
        SELECT o.id, o.amount
        FROM orders o
    """,
    )

    # Column usage for the INSERT should reference 'orders', not 'o'
    insert_usage = [cu for cu in result.column_usage if cu.usage_type == "insert"]
    for cu in insert_usage:
        assert cu.table_name == "orders", f"Expected table_name='orders', got '{cu.table_name}' (alias not resolved)"


# ── 5.9/5.10: Edge case tests ──


def test_get_git_info_non_git_directory(tmp_path):
    """_get_git_info returns (None, None) for a non-git directory."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    # tmp_path is not a git repo
    db = GraphDB()
    indexer = Indexer(db)
    commit, branch = indexer._get_git_info(tmp_path)
    assert commit is None
    assert branch is None
    db.close()


def test_checksum_parse_result_stability():
    """Same ParseResult input produces the same checksum across calls."""
    from sqlprism.core.indexer import _checksum_parse_result

    result = ParseResult(
        language="sql",
        nodes=[
            NodeResult(kind="table", name="orders"),
            NodeResult(kind="query", name="report"),
        ],
        edges=[
            EdgeResult(
                source_name="report",
                source_kind="query",
                target_name="orders",
                target_kind="table",
                relationship="references",
            ),
        ],
        column_usage=[
            ColumnUsageResult(
                node_name="report",
                node_kind="query",
                table_name="orders",
                column_name="id",
                usage_type="select",
            ),
        ],
    )

    checksum1 = _checksum_parse_result(result)
    checksum2 = _checksum_parse_result(result)
    checksum3 = _checksum_parse_result(result)

    assert checksum1 == checksum2 == checksum3
    # Sanity: it's a hex string
    assert len(checksum1) == 64
    int(checksum1, 16)  # valid hex


# ── Issue #11: reindex_files() and _resolve_file_repo() ──


def test_reindex_files_plain_sql(tmp_path):
    """reindex_files() parses a plain SQL file and inserts into the graph."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT o.id, o.amount FROM orders o")

    db = GraphDB()
    indexer = Indexer(db)

    # Register the repo first
    db.upsert_repo("test", str(repo_dir), repo_type="sql")

    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["reindexed"] == 1
    assert stats["skipped"] == 0
    assert stats["errors"] == []

    # Verify data is in the DB with specific checks
    results = db.query_search("orders")
    assert results["total_count"] >= 1
    match = results["matches"][0]
    assert match["name"] == "orders"
    assert "report.sql" in match["file"]
    db.close()


def test_reindex_files_unknown_repo_skipped(tmp_path):
    """reindex_files() silently skips files not under any configured repo."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    # File outside any repo
    orphan = tmp_path / "orphan.sql"
    orphan.write_text("SELECT 1")

    db = GraphDB()
    indexer = Indexer(db)

    stats = indexer.reindex_files(paths=[str(orphan)])
    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0

    detail = stats["details"][0]
    assert detail["reason"] == "no matching repo"
    db.close()


def test_reindex_files_deleted_file_cleans_graph(tmp_path):
    """reindex_files() removes graph data when file has been deleted."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("CREATE TABLE report (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    # First, do a full reindex so data exists
    indexer.reindex_repo("test", str(repo_dir))
    status = db.get_index_status()
    assert status["totals"]["files"] == 1

    # Now delete the file and call reindex_files with the deleted path
    sql_file.unlink()
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["deleted"] == 1

    # Verify data was cleaned
    status = db.get_index_status()
    assert status["totals"]["files"] == 0
    db.close()


def test_reindex_files_unchanged_checksum_skipped(tmp_path):
    """reindex_files() skips files whose content hasn't changed."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT 1 FROM orders")

    db = GraphDB()
    indexer = Indexer(db)

    # First reindex via full reindex
    indexer.reindex_repo("test", str(repo_dir))

    # Now reindex_files — file unchanged
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0

    detail = stats["details"][0]
    assert detail["reason"] == "unchanged"
    db.close()


def test_reindex_files_groups_by_repo(tmp_path):
    """reindex_files() groups files by repo and processes each correctly."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    (repo_a / "a.sql").write_text("CREATE TABLE alpha (id INT)")
    (repo_b / "b.sql").write_text("CREATE TABLE beta (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    db.upsert_repo("repo_a", str(repo_a), repo_type="sql")
    db.upsert_repo("repo_b", str(repo_b), repo_type="sql")

    stats = indexer.reindex_files(paths=[
        str(repo_a / "a.sql"),
        str(repo_b / "b.sql"),
    ])
    assert stats["reindexed"] == 2
    assert stats["skipped"] == 0

    # Both tables are searchable
    assert db.query_search("alpha")["total_count"] >= 1
    assert db.query_search("beta")["total_count"] >= 1
    db.close()


def test_resolve_file_repo_deepest_match(tmp_path):
    """_resolve_file_repo picks the deepest matching repo for nested repos."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    parent = tmp_path / "project"
    child = parent / "subdir"
    child.mkdir(parents=True)

    db = GraphDB()
    indexer = Indexer(db)

    db.upsert_repo("parent", str(parent), repo_type="sql")
    db.upsert_repo("child", str(child), repo_type="dbt")

    all_repos = db.get_all_repos()

    file_path = child / "model.sql"
    resolved = indexer._resolve_file_repo(file_path, all_repos)
    assert resolved is not None
    repo_id, repo_name, repo_path, repo_type = resolved
    assert repo_name == "child"
    assert repo_type == "dbt"

    # A file in parent but not in child resolves to parent
    parent_file = parent / "query.sql"
    resolved2 = indexer._resolve_file_repo(parent_file, all_repos)
    assert resolved2 is not None
    assert resolved2[1] == "parent"
    assert resolved2[3] == "sql"
    db.close()


def test_reindex_files_non_sql_skipped(tmp_path):
    """reindex_files() skips non-SQL files."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    py_file = repo_dir / "script.py"
    py_file.write_text("print('hello')")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("test", str(repo_dir), repo_type="sql")

    stats = indexer.reindex_files(paths=[str(py_file)])
    assert stats["skipped"] == 1
    assert stats["details"][0]["reason"] == "not a SQL file"
    db.close()


def test_reindex_files_changed_content(tmp_path):
    """reindex_files() re-parses a file whose content changed since last index."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("CREATE TABLE report (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    # Initial full reindex
    indexer.reindex_repo("test", str(repo_dir))
    assert db.query_search("report")["total_count"] >= 1

    # Modify the file
    sql_file.write_text("CREATE TABLE report (id INT, name TEXT, amount DECIMAL)")

    # reindex_files should detect the change and re-parse
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["reindexed"] == 1
    assert stats["skipped"] == 0

    # Verify the new content is actually in the graph —
    # the updated DDL has 3 columns vs 1, so node count should reflect the change
    status = db.get_index_status()
    assert status["totals"]["files"] == 1  # still 1 file
    assert status["totals"]["nodes"] >= 1  # node exists
    db.close()


def test_reindex_files_dbt_model(tmp_path):
    """reindex_files() compiles a dbt model via render_models() and inserts the result."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_project"
    repo_dir.mkdir()
    model_file = repo_dir / "models" / "stg_orders.sql"
    model_file.parent.mkdir()
    model_file.write_text("SELECT id, amount FROM raw_orders")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_dbt", str(repo_dir), repo_type="dbt")

    # Mock render_models to return a ParseResult without running dbt
    mock_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="stg_orders", metadata={"schema": "staging"})],
        edges=[EdgeResult(
            source_name="stg_orders", source_kind="table",
            target_name="raw_orders", target_kind="table",
            relationship="references",
        )],
    )

    with patch.object(indexer.dbt_renderer, "render_models", return_value={"staging/stg_orders.sql": mock_result}):
        stats = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_dbt": {"project_path": str(repo_dir), "repo_type": "dbt"}},
        )

    assert stats["reindexed"] == 1
    assert stats["errors"] == []

    # Verify data in the graph
    results = db.query_search("stg_orders")
    assert results["total_count"] >= 1
    db.close()


def test_reindex_files_sqlmesh_model(tmp_path):
    """reindex_files() renders a sqlmesh model via render_models() and inserts the result."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "sqlmesh_project"
    repo_dir.mkdir()
    model_file = repo_dir / "models" / "model_a.sql"
    model_file.parent.mkdir()
    model_file.write_text("SELECT id FROM source_table")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_sm", str(repo_dir), repo_type="sqlmesh")

    # Mock render_models to return a ParseResult without running sqlmesh
    mock_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
        edges=[EdgeResult(
            source_name="model_a", source_kind="table",
            target_name="source_table", target_kind="table",
            relationship="references",
        )],
    )

    renderer = indexer.get_sqlmesh_renderer("athena")
    with patch.object(renderer, "render_models", return_value={'"db"."schema"."model_a"': mock_result}):
        stats = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_sm": {
                "project_path": str(repo_dir),
                "repo_type": "sqlmesh",
                "dialect": "athena",
            }},
        )

    assert stats["reindexed"] == 1
    assert stats["errors"] == []

    # Verify data in the graph
    results = db.query_search("model_a")
    assert results["total_count"] >= 1
    db.close()


def test_reindex_files_dbt_delete_finds_stored_path(tmp_path):
    """Deleting a dbt model finds and removes the stored compiled-dir path."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_project"
    repo_dir.mkdir()

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("my_dbt", str(repo_dir), repo_type="dbt")

    # Simulate a previously indexed dbt model stored as "staging/stg_orders.sql"
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "staging/stg_orders.sql", "sql", "abc123")
        db.insert_nodes_batch([(file_id, "table", "stg_orders", "sql", 1, 1, None, "staging")])

    assert db.get_index_status()["totals"]["files"] == 1

    # The filesystem path is models/stg_orders.sql — now deleted
    deleted_path = repo_dir / "models" / "stg_orders.sql"

    stats = indexer.reindex_files(
        paths=[str(deleted_path)],
        repo_configs={"my_dbt": {"project_path": str(repo_dir), "repo_type": "dbt"}},
    )

    assert stats["deleted"] >= 1
    assert db.get_index_status()["totals"]["files"] == 0
    db.close()


def test_reindex_files_empty_input():
    """reindex_files() with empty paths returns zeroed stats."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    stats = indexer.reindex_files(paths=[])
    assert stats["reindexed"] == 0
    assert stats["skipped"] == 0
    assert stats["deleted"] == 0
    assert stats["errors"] == []
    assert stats["details"] == []
    db.close()


# ── Issue #14: Additional indexer unit and integration tests ──


def test_resolve_file_repo_various_layouts(tmp_path):
    """_resolve_file_repo maps files to deepest matching repo; unmatched → None."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    parent = tmp_path / "project" / "a"
    child = parent / "sub"
    child.mkdir(parents=True)

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("parent", str(parent), repo_type="sql")
    db.upsert_repo("child", str(child), repo_type="dbt")

    all_repos = db.get_all_repos()

    # File in child → resolves to child (deepest)
    resolved = indexer._resolve_file_repo(child / "model.sql", all_repos)
    assert resolved is not None
    repo_id, repo_name, repo_path, repo_type = resolved
    assert repo_name == "child"
    assert repo_type == "dbt"

    # File in parent but not in child → resolves to parent
    resolved2 = indexer._resolve_file_repo(parent / "query.sql", all_repos)
    assert resolved2 is not None
    _, repo_name2, _, repo_type2 = resolved2
    assert repo_name2 == "parent"
    assert repo_type2 == "sql"

    # Deeply nested file under child → still resolves to child
    deep_file = child / "nested" / "deep" / "model.sql"
    resolved3 = indexer._resolve_file_repo(deep_file, all_repos)
    assert resolved3 is not None
    assert resolved3[1] == "child"

    # File completely outside all repos → None
    outside = tmp_path / "elsewhere" / "orphan.sql"
    resolved4 = indexer._resolve_file_repo(outside, all_repos)
    assert resolved4 is None

    db.close()


def test_reindex_checksum_skip_unchanged(tmp_path):
    """reindex_files() skips unchanged files — parser.parse is NOT called."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT id, amount FROM orders")

    db = GraphDB()
    indexer = Indexer(db)

    # Full reindex first
    indexer.reindex_repo("test", str(repo_dir))

    # Patch the parser to track calls.
    # get_parser(None) returns the same cached instance that _reindex_sql_files uses
    # because this repo has no dialect config, so file_dialect resolves to None.
    parser = indexer.get_parser(None)
    with patch.object(parser, "parse", wraps=parser.parse) as mock_parse:
        stats = indexer.reindex_files(paths=[str(sql_file)])

    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0
    mock_parse.assert_not_called()
    db.close()


def test_reindex_plain_sql_updates_graph(tmp_path):
    """reindex_files() updates nodes and edges when plain SQL content changes."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT id FROM orders")

    db = GraphDB()
    indexer = Indexer(db)

    # Initial full reindex
    indexer.reindex_repo("test", str(repo_dir))

    # Verify initial state — orders is referenced
    results = db.query_search("orders")
    assert results["total_count"] >= 1

    # Now change the SQL to reference a different table
    sql_file.write_text("SELECT id, name FROM customers JOIN payments ON customers.id = payments.customer_id")

    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["reindexed"] == 1

    # Verify graph updated: customers should be searchable now
    results = db.query_search("customers")
    assert results["total_count"] >= 1

    # payments should also appear
    results = db.query_search("payments")
    assert results["total_count"] >= 1

    # Verify stale node (orders) is no longer file-backed — delete + re-insert worked
    stale = db.query_search("orders")
    file_backed = [m for m in stale["matches"] if m.get("file")]
    assert len(file_backed) == 0

    db.close()


def test_reindex_deleted_file_cleans_graph(tmp_path):
    """reindex_files() removes all graph data when a file is deleted."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("CREATE TABLE report (id INT, name TEXT)")

    db = GraphDB()
    indexer = Indexer(db)

    # Index the file
    indexer.reindex_repo("test", str(repo_dir))
    status = db.get_index_status()
    assert status["totals"]["files"] == 1
    assert status["totals"]["nodes"] >= 1

    # Delete the file
    sql_file.unlink()

    # Reindex the deleted path
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["deleted"] == 1

    # Verify everything is cleaned up
    status = db.get_index_status()
    assert status["totals"]["files"] == 0
    # Real (file-backed) nodes are removed; only phantoms may remain.
    # Use status["phantom_nodes"] (no default) so a KeyError surfaces schema changes.
    real_nodes = status["totals"]["nodes"] - status["phantom_nodes"]
    assert real_nodes == 0

    # Search for the table name should return no file-backed matches
    results = db.query_search("report")
    file_backed = [m for m in results["matches"] if m.get("file")]
    assert len(file_backed) == 0

    db.close()


def test_reindex_file_outside_repos_skipped(tmp_path):
    """reindex_files() skips files outside all configured repos with reason."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    outside_file = tmp_path / "elsewhere" / "orphan.sql"
    outside_file.parent.mkdir()
    outside_file.write_text("SELECT 1")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("test", str(repo_dir), repo_type="sql")

    stats = indexer.reindex_files(paths=[str(outside_file)])
    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0

    detail = stats["details"][0]
    assert detail["path"] == str(outside_file)
    assert "no matching repo" in detail["reason"]

    db.close()


def test_insert_parse_result_stores_columns():
    """_insert_parse_result stores ColumnDefResult entries in the columns table."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")

    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="orders")],
        columns=[
            ColumnDefResult(
                node_name="orders", column_name="order_id",
                data_type="INT", position=0, source="definition",
            ),
            ColumnDefResult(
                node_name="orders", column_name="status",
                data_type="TEXT", position=1, source="definition",
            ),
        ],
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "columns_added": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    # Verify node_id matches the orders node
    orders_node_id = db._execute_read(
        "SELECT node_id FROM nodes WHERE name = 'orders'"
    ).fetchone()[0]

    rows = db._execute_read(
        "SELECT node_id, column_name, data_type, position, source FROM columns ORDER BY position"
    ).fetchall()

    assert len(rows) == 2
    assert rows[0][0] == orders_node_id
    assert rows[0][1] == "order_id"
    assert rows[0][2] == "INT"
    assert rows[0][3] == 0
    assert rows[0][4] == "definition"
    assert rows[1][0] == orders_node_id
    assert rows[1][1] == "status"
    assert rows[1][2] == "TEXT"
    assert rows[1][3] == 1
    assert stats["columns_added"] == 2

    db.close()


def test_columns_multiple_sources_merged():
    """Column definitions from multiple sources are merged with upsert semantics."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")

    # First: insert columns via _insert_parse_result (source="definition")
    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="orders")],
        columns=[
            ColumnDefResult(
                node_name="orders", column_name="order_id",
                data_type="INT", position=0, source="definition",
            ),
            ColumnDefResult(
                node_name="orders", column_name="status",
                data_type="TEXT", position=1, source="definition",
            ),
        ],
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "columns_added": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    # Get the node_id for orders
    node_row = db._execute_read(
        "SELECT node_id FROM nodes WHERE name = 'orders'"
    ).fetchone()
    node_id = node_row[0]

    # Second: insert from schema_yml source with description
    db.insert_columns_batch([
        (node_id, "order_id", None, None, "schema_yml", "The unique order identifier"),
        (node_id, "status", None, None, "schema_yml", "Current order status"),
    ])

    rows = db._execute_read(
        "SELECT column_name, data_type, source, description FROM columns WHERE node_id = ? ORDER BY column_name",
        [node_id],
    ).fetchall()

    assert len(rows) == 2
    # Build dict for robust lookup regardless of ordering
    by_name = {r[0]: r for r in rows}
    # Upsert: schema_yml wins for source, data_type preserved via COALESCE
    assert by_name["order_id"][1] == "INT"  # data_type preserved from definition
    assert by_name["order_id"][2] == "schema_yml"  # source updated
    assert by_name["order_id"][3] == "The unique order identifier"
    assert by_name["status"][1] == "TEXT"  # data_type preserved from definition
    assert by_name["status"][2] == "schema_yml"  # source updated
    assert by_name["status"][3] == "Current order status"

    db.close()


def test_get_table_columns_prefers_columns_table():
    """get_table_columns returns types from columns table over column_usage."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "orders", "sql")

    # Insert real types in columns table
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
        (node_id, "amount", "VARCHAR", 1, "definition", None),
    ])

    # Also insert column_usage for the same columns
    db.insert_column_usage(node_id, "orders", "order_id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "amount", "select", file_id)

    schema = db.get_table_columns(repo_id)

    assert "orders" in schema
    assert schema["orders"]["order_id"] == "INT"
    assert schema["orders"]["amount"] == "VARCHAR"

    db.close()


def test_get_table_columns_fallback_column_usage():
    """get_table_columns falls back to column_usage with TEXT type when columns table is empty."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "orders", "sql")

    # Only insert column_usage — no columns table entries
    db.insert_column_usage(node_id, "orders", "order_id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "status", "where", file_id)

    schema = db.get_table_columns(repo_id)

    assert "orders" in schema
    assert schema["orders"]["order_id"] == "TEXT"
    assert schema["orders"]["status"] == "TEXT"

    db.close()


def test_get_table_columns_merged_result():
    """get_table_columns merges columns table and column_usage, preferring real types."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "orders", "sql")

    # 3 columns in columns table with real types
    db.insert_columns_batch([
        (node_id, "order_id", "INT", 0, "definition", None),
        (node_id, "amount", "DECIMAL", 1, "definition", None),
        (node_id, "status", "VARCHAR", 2, "definition", None),
    ])

    # 1 additional column only in column_usage
    db.insert_column_usage(node_id, "orders", "order_id", "select", file_id)
    db.insert_column_usage(node_id, "orders", "created_at", "select", file_id)

    schema = db.get_table_columns(repo_id)

    assert "orders" in schema
    assert len(schema["orders"]) == 4
    # Columns from columns table have real types
    assert schema["orders"]["order_id"] == "INT"
    assert schema["orders"]["amount"] == "DECIMAL"
    assert schema["orders"]["status"] == "VARCHAR"
    # Column from usage only has TEXT
    assert schema["orders"]["created_at"] == "TEXT"

    db.close()


def test_columns_batch_insert():
    """Batch insert of 50+ column definitions works correctly."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "wide_table.sql", "sql", "abc123")

    num_columns = 55
    col_defs = [
        ColumnDefResult(
            node_name="wide_table",
            column_name=f"col_{i}",
            data_type="TEXT",
            position=i,
            source="definition",
        )
        for i in range(num_columns)
    ]

    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="wide_table")],
        columns=col_defs,
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "columns_added": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    rows = db._execute_read("SELECT COUNT(*) FROM columns").fetchone()
    assert rows[0] == num_columns

    assert stats["columns_added"] == 55

    db.close()


def test_column_def_unresolved_node_skipped():
    """Column definitions for unresolved node names are skipped with warning."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "ghost.sql", "sql", "abc123")

    # ColumnDefResult references a node that doesn't exist anywhere
    result = ParseResult(
        language="sql",
        nodes=[],  # no nodes — ghost_table won't be found
        columns=[
            ColumnDefResult(
                node_name="ghost_table", column_name="col_a",
                data_type="INT", position=0, source="definition",
            ),
        ],
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "columns_added": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    assert stats["columns_added"] == 0
    rows = db._execute_read("SELECT COUNT(*) FROM columns").fetchone()
    assert rows[0] == 0

    db.close()


def test_column_def_null_data_type_returns_text():
    """Column with data_type=None is returned as TEXT by get_table_columns."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "inferred.sql", "sql", "abc123")

    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="inferred_table")],
        columns=[
            ColumnDefResult(
                node_name="inferred_table", column_name="unknown_col",
                data_type=None, position=0, source="inferred",
            ),
        ],
    )

    stats = {
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
        "columns_added": 0,
        "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    assert stats["columns_added"] == 1

    schema = db.get_table_columns(repo_id)
    assert "inferred_table" in schema
    assert schema["inferred_table"]["unknown_col"] == "TEXT"

    db.close()


# ── v1.2: conventions and semantic_tags schema ──


def test_conventions_table_exists():
    """conventions table is created on database init with correct types."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    rows = db.conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'conventions' ORDER BY ordinal_position"
    ).fetchall()
    col_map = {r[0]: r[1] for r in rows}
    assert col_map["convention_id"] == "INTEGER"
    assert col_map["repo_id"] == "INTEGER"
    assert col_map["layer"] == "VARCHAR"
    assert col_map["convention_type"] == "VARCHAR"
    assert col_map["payload"] == "JSON"
    assert col_map["confidence"] == "FLOAT"
    assert col_map["source"] == "VARCHAR"
    assert col_map["model_count"] == "INTEGER"
    db.close()


def test_semantic_tags_table_exists():
    """semantic_tags table is created on database init with correct types."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    rows = db.conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'semantic_tags' ORDER BY ordinal_position"
    ).fetchall()
    col_map = {r[0]: r[1] for r in rows}
    assert col_map["tag_id"] == "INTEGER"
    assert col_map["repo_id"] == "INTEGER"
    assert col_map["tag_name"] == "VARCHAR"
    assert col_map["node_id"] == "INTEGER"
    assert col_map["confidence"] == "FLOAT"
    assert col_map["source"] == "VARCHAR"
    db.close()


def test_schema_migration_preserves_data():
    """New tables created alongside existing schema without affecting data."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    # Insert data into existing tables
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "model.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql", 1, 10, schema="public")

    # Verify existing data still accessible across multiple tables
    result = db.conn.execute("SELECT name FROM nodes WHERE node_id = ?", [node_id]).fetchone()
    assert result[0] == "orders"
    assert db.conn.execute("SELECT COUNT(*) FROM repos").fetchone()[0] == 1
    assert db.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1

    # Verify new tables exist and are empty
    assert db.conn.execute("SELECT COUNT(*) FROM conventions").fetchone()[0] == 0
    assert db.conn.execute("SELECT COUNT(*) FROM semantic_tags").fetchone()[0] == 0
    db.close()


def test_convention_id_auto_increment():
    """Sequences auto-increment IDs for conventions and semantic_tags."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "model.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql", 1, 10, schema="public")

    # Insert conventions — IDs should be 1 and 2 in fresh DB
    db.conn.execute(
        "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
        "VALUES (?, 'staging', 'naming', '{\"pattern\": \"stg_{entity}\"}', 0.9, 'inferred', 10)",
        [repo_id],
    )
    db.conn.execute(
        "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
        "VALUES (?, 'marts', 'naming', '{\"pattern\": \"{entity}\"}', 0.8, 'inferred', 5)",
        [repo_id],
    )
    ids = db.conn.execute("SELECT convention_id FROM conventions ORDER BY convention_id").fetchall()
    assert ids == [(1,), (2,)]

    # Insert semantic tag — ID should be 1 in fresh DB
    db.conn.execute(
        "INSERT INTO semantic_tags (repo_id, tag_name, node_id, confidence, source) "
        "VALUES (?, 'customer', ?, 0.85, 'inferred')",
        [repo_id, node_id],
    )
    tag_id = db.conn.execute("SELECT tag_id FROM semantic_tags").fetchone()[0]
    assert tag_id == 1
    db.close()


def test_conventions_unique_constraints():
    """Unique constraints prevent duplicate conventions and tags."""
    import duckdb

    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "model.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql", 1, 10, schema="public")

    # Insert initial rows
    db.conn.execute(
        "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
        "VALUES (?, 'staging', 'naming', '{\"pattern\": \"stg_{entity}\"}', 0.9, 'inferred', 10)",
        [repo_id],
    )
    db.conn.execute(
        "INSERT INTO semantic_tags (repo_id, tag_name, node_id, confidence, source) "
        "VALUES (?, 'customer', ?, 0.85, 'inferred')",
        [repo_id, node_id],
    )

    # Duplicate convention (same repo_id, layer, convention_type) must fail
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
            "VALUES (?, 'staging', 'naming', '{}', 0.5, 'override', 10)",
            [repo_id],
        )

    # Duplicate tag (same tag_name, node_id) must fail
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO semantic_tags (repo_id, tag_name, node_id, confidence, source) "
            "VALUES (?, 'customer', ?, 0.9, 'explicit')",
            [repo_id, node_id],
        )
    db.close()


def test_conventions_check_constraints():
    """CHECK constraints enforce valid confidence range and enum values."""
    import duckdb

    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "model.sql", "sql", "abc123")
    node_id = db.insert_node(file_id, "table", "orders", "sql", 1, 10, schema="public")

    # Confidence out of range on conventions
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
            "VALUES (?, 'staging', 'naming', '{}', 1.5, 'inferred', 10)",
            [repo_id],
        )

    # Invalid convention_type
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
            "VALUES (?, 'staging', 'invalid_type', '{}', 0.5, 'inferred', 10)",
            [repo_id],
        )

    # Invalid source on conventions
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO conventions (repo_id, layer, convention_type, payload, confidence, source, model_count) "
            "VALUES (?, 'staging', 'naming', '{}', 0.5, 'bad_source', 10)",
            [repo_id],
        )

    # Confidence out of range on semantic_tags
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO semantic_tags (repo_id, tag_name, node_id, confidence, source) "
            "VALUES (?, 'customer', ?, -0.1, 'inferred')",
            [repo_id, node_id],
        )

    # Invalid source on semantic_tags
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO semantic_tags (repo_id, tag_name, node_id, confidence, source) "
            "VALUES (?, 'customer', ?, 0.5, 'bad_source')",
            [repo_id, node_id],
        )
    db.close()


def test_reindex_sqlmesh_batch_transaction_rollback(tmp_path, monkeypatch):
    """If a model insertion fails mid-batch, no partial data remains."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import NodeResult, ParseResult

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    # Two fake rendered models — second one will cause an error
    good_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
    )
    bad_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_b")],
    )

    rendered = {
        '"catalog"."schema"."model_a"': good_result,
        '"catalog"."schema"."model_b"': bad_result,
    }

    # Mock render_project to return our fake data
    with patch.object(indexer, "get_sqlmesh_renderer") as mock_renderer:
        mock_renderer.return_value.render_project.return_value = rendered

        # Make _insert_parse_result fail on the second call
        original_insert = indexer._insert_parse_result
        call_count = 0

        def failing_insert(result, file_id, repo_id, stats):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("Simulated insertion failure")
            return original_insert(result, file_id, repo_id, stats)

        with patch.object(indexer, "_insert_parse_result", side_effect=failing_insert):
            with pytest.raises(RuntimeError, match="Simulated insertion failure"):
                indexer.reindex_sqlmesh("test_repo", tmp_path)

    # Verify nothing was committed — no files or nodes in the repo
    repo_id_row = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
    ).fetchone()
    assert repo_id_row is not None, "Repo row should exist (upsert_repo runs before transaction)"
    files = db._execute_read(
        "SELECT count(*) FROM files WHERE repo_id = ?", [repo_id_row[0]]
    ).fetchone()
    assert files[0] == 0, "Partial file data should not exist after rollback"
    nodes = db._execute_read(
        "SELECT count(*) FROM nodes WHERE file_id IN "
        "(SELECT file_id FROM files WHERE repo_id = ?)", [repo_id_row[0]]
    ).fetchone()
    assert nodes[0] == 0, "Partial node data should not exist after rollback"


def test_reindex_sqlmesh_checksum_skip(tmp_path):
    """Unchanged models are skipped on re-run."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import NodeResult, ParseResult

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    result_a = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
    )
    rendered = {'"catalog"."schema"."model_a"': result_a}

    with patch.object(indexer, "get_sqlmesh_renderer") as mock_renderer:
        mock_renderer.return_value.render_project.return_value = rendered

        # First run — indexes everything
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1
        assert stats1.get("models_skipped", 0) == 0

        # Second run — same rendered output, should skip
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 1
        assert stats2["nodes_added"] == 0


def test_reindex_sqlmesh_checksum_changed(tmp_path):
    """Changed models are re-indexed."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import NodeResult, ParseResult

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    result_v1 = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
    )
    result_v2 = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a"), NodeResult(kind="view", name="model_a_view")],
    )

    with patch.object(indexer, "get_sqlmesh_renderer") as mock_renderer:
        # First run
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_v1,
        }
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1

        # Second run — different parse result (different checksum)
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_v2,
        }
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 0
        assert stats2["nodes_added"] == 2  # re-inserted both nodes

        # Verify graph has exactly 2 nodes, not duplicates
        repo_id = db._execute_read(
            "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
        ).fetchone()[0]
        node_count = db._execute_read(
            "SELECT count(*) FROM nodes WHERE file_id IN "
            "(SELECT file_id FROM files WHERE repo_id = ?)", [repo_id]
        ).fetchone()[0]
        assert node_count == 2


def test_reindex_sqlmesh_stale_model_deleted(tmp_path):
    """Models removed from the project are deleted from the graph."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import NodeResult, ParseResult

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    result_a = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
    )
    result_b = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_b")],
    )

    with patch.object(indexer, "get_sqlmesh_renderer") as mock_renderer:
        # First run — two models
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_a,
            '"catalog"."schema"."model_b"': result_b,
        }
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 2

        # Second run — model_b removed
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_a,
        }
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_removed"] == 1
        assert stats2["models_skipped"] == 1  # model_a unchanged

    # Verify only model_a remains in graph
    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
    ).fetchone()[0]
    files = db._execute_read(
        "SELECT path FROM files WHERE repo_id = ?", [repo_id]
    ).fetchall()
    assert len(files) == 1
    assert files[0][0] == "catalog/schema/model_a.sql"


def test_reindex_sqlmesh_new_model_indexed(tmp_path):
    """New models are always indexed even when existing models are skipped."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.types import NodeResult, ParseResult

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    result_a = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
    )
    result_b = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_b")],
    )

    with patch.object(indexer, "get_sqlmesh_renderer") as mock_renderer:
        # First run — one model
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_a,
        }
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1

        # Second run — add model_b, model_a unchanged
        mock_renderer.return_value.render_project.return_value = {
            '"catalog"."schema"."model_a"': result_a,
            '"catalog"."schema"."model_b"': result_b,
        }
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 1  # model_a
        assert stats2["nodes_added"] == 1  # model_b is new

    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
    ).fetchone()[0]
    file_count = db._execute_read(
        "SELECT count(*) FROM files WHERE repo_id = ?", [repo_id]
    ).fetchone()[0]
    assert file_count == 2
