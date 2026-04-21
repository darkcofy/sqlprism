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
