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


def _mock_render_raw(indexer, raw_models, column_schemas=None):
    """Helper: mock render_project_raw on the sqlmesh renderer."""
    from unittest.mock import MagicMock, patch

    mock_renderer = MagicMock()
    mock_renderer.render_project_raw.return_value = (raw_models, column_schemas or {})
    # Preserve real parse methods for actual sqlglot parsing
    from sqlprism.languages.sqlmesh import SqlMeshRenderer
    real = SqlMeshRenderer()
    mock_renderer._parse_models_sequential = real._parse_models_sequential
    mock_renderer._parse_models_parallel = real._parse_models_parallel
    return patch.object(indexer, "get_sqlmesh_renderer", return_value=mock_renderer)


def test_reindex_sqlmesh_batch_transaction_rollback(tmp_path, monkeypatch):
    """If a model insertion fails mid-batch, no partial data remains."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    raw_models = {
        '"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1",
        '"catalog"."schema"."model_b"': "SELECT 2 AS id FROM raw.t2",
    }

    with _mock_render_raw(indexer, raw_models):
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
    """Unchanged models are skipped on re-run (same schema catalog)."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    raw_models = {'"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1"}

    with _mock_render_raw(indexer, raw_models):
        # First run — indexes everything
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1
        assert stats1["nodes_added"] >= 1
        assert stats1.get("models_skipped", 0) == 0

        # Second run — same rendered SQL + same schema catalog → skip
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 1
        assert stats2["nodes_added"] == 0


def test_reindex_sqlmesh_checksum_changed(tmp_path):
    """Changed models are re-indexed."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    sql_v1 = {'"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1"}
    sql_v2 = {'"catalog"."schema"."model_a"': "SELECT 1 AS id, 'x' AS name FROM raw.t1 JOIN raw.t2 ON t1.id = t2.id"}

    with _mock_render_raw(indexer, sql_v1):
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1

    # Touch a file to invalidate the source fingerprint
    (tmp_path / "changed.sql").write_text("-- changed")

    # Second run — different SQL (simulates model change)
    with _mock_render_raw(indexer, sql_v2):
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 0
        assert stats2["nodes_added"] >= 1

    # Verify no duplicates in graph
    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
    ).fetchone()[0]
    file_count = db._execute_read(
        "SELECT count(*) FROM files WHERE repo_id = ?", [repo_id]
    ).fetchone()[0]
    assert file_count == 1


def test_reindex_sqlmesh_stale_model_deleted(tmp_path):
    """Models removed from the project are deleted from the graph."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    two_models = {
        '"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1",
        '"catalog"."schema"."model_b"': "SELECT 2 AS id FROM raw.t2",
    }
    one_model = {'"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1"}

    with _mock_render_raw(indexer, two_models):
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 2

    # Touch a file to invalidate the source fingerprint
    (tmp_path / "changed.sql").write_text("-- removed model")

    # Second run — model_b removed
    with _mock_render_raw(indexer, one_model):
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
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    one_model = {'"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1"}
    two_models = {
        '"catalog"."schema"."model_a"': "SELECT 1 AS id FROM raw.t1",
        '"catalog"."schema"."model_b"': "SELECT 2 AS id FROM raw.t2",
    }

    with _mock_render_raw(indexer, one_model):
        stats1 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats1["models_rendered"] == 1

    # Touch a file to invalidate the source fingerprint
    (tmp_path / "new_model.sql").write_text("-- new model added")

    # Second run — add model_b, model_a unchanged
    with _mock_render_raw(indexer, two_models):
        stats2 = indexer.reindex_sqlmesh("test_repo", tmp_path)
        assert stats2["models_skipped"] == 1  # model_a
        assert stats2["nodes_added"] >= 1  # model_b is new

    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["test_repo"]
    ).fetchone()[0]
    file_count = db._execute_read(
        "SELECT count(*) FROM files WHERE repo_id = ?", [repo_id]
    ).fetchone()[0]
    assert file_count == 2


def test_reindex_repo_bigquery_dialect(tmp_path):
    """BigQuery files with backtick identifiers should parse when dialect is set."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    repo_dir = tmp_path / "bq_repo"
    repo_dir.mkdir()
    (repo_dir / "clients_daily.sql").write_text(
        "CREATE OR REPLACE VIEW `myproject.dataset.clients_daily` AS\n"
        "SELECT client_id, submission_date FROM `myproject.dataset.raw_clients`"
    )
    (repo_dir / "sessions.sql").write_text(
        "SELECT session_id, client_id FROM `myproject.dataset.sessions_v1`"
    )

    stats = indexer.reindex_repo("bq-test", str(repo_dir), dialect="bigquery")

    assert stats["nodes_added"] >= 2
    assert stats["edges_added"] >= 1
    assert len(stats["parse_errors"]) == 0


# ── Issue #120: cross-repo schema catalog & SELECT * lineage ──


def test_get_cross_repo_columns_merges_other_repos(tmp_path):
    """Cross-repo catalog layers current repo on top of sibling-repo columns.

    Ensures that when indexing repo B, schema lookups can resolve a table
    defined in already-indexed repo A — without clobbering anything in B.
    """
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    repo_a = tmp_path / "repo_a"
    repo_a.mkdir()
    (repo_a / "upstream.sql").write_text(
        "CREATE TABLE upstream (customer_id INT, status VARCHAR(20))"
    )
    indexer.reindex_repo("repo_a", str(repo_a))

    repo_b = tmp_path / "repo_b"
    repo_b.mkdir()
    (repo_b / "local.sql").write_text(
        "CREATE TABLE local_t (id INT)"
    )
    indexer.reindex_repo("repo_b", str(repo_b))

    b_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["repo_b"]
    ).fetchone()[0]

    catalog = db.get_cross_repo_columns(b_id)
    assert "upstream" in catalog, "upstream from sibling repo should be visible"
    assert "customer_id" in catalog["upstream"]
    assert "local_t" in catalog, "current repo columns remain present"


def test_get_cross_repo_columns_current_repo_wins(tmp_path):
    """When two repos define the same table, the *current* repo's type wins.

    The docstring promises local definitions take precedence over cross-repo
    siblings; pin that guarantee so a future refactor can't silently invert
    the overlay order.
    """
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    sibling = tmp_path / "sibling"
    sibling.mkdir()
    (sibling / "shared.sql").write_text(
        "CREATE TABLE shared_t (id VARCHAR(64))"  # sibling has VARCHAR
    )
    indexer.reindex_repo("sibling", str(sibling))

    current = tmp_path / "current"
    current.mkdir()
    (current / "shared.sql").write_text(
        "CREATE TABLE shared_t (id BIGINT)"  # current has BIGINT
    )
    indexer.reindex_repo("current", str(current))

    current_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["current"]
    ).fetchone()[0]

    catalog = db.get_cross_repo_columns(current_id)
    assert "BIGINT" in catalog["shared_t"]["id"].upper(), \
        f"current repo's BIGINT should win over sibling's VARCHAR, got {catalog['shared_t']}"


def test_reindex_sqlmesh_select_star_lineage_single_repo(tmp_path):
    """SELECT * through a CTE resolves on a fresh index using column_schemas."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    raw_models = {
        '"demo"."stg_orders"': "SELECT customer_id, order_id FROM raw.orders",
        '"demo"."orders"': (
            "WITH wrapped AS (SELECT * FROM stg_orders) "
            "SELECT customer_id FROM wrapped"
        ),
    }
    column_schemas = {
        '"demo"."stg_orders"': {"customer_id": "INT", "order_id": "INT"},
    }

    with _mock_render_raw(indexer, raw_models, column_schemas):
        indexer.reindex_sqlmesh("demo", tmp_path)

    # Pull column_lineage for the orders model: it should trace customer_id
    # back to stg_orders.customer_id (not just the table, but the exact column
    # — a regression resolving to ``*`` would otherwise pass).
    rows = db._execute_read(
        "SELECT DISTINCT hop_table, hop_column "
        "FROM column_lineage cl "
        "JOIN files f ON cl.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND cl.output_node = ? AND cl.output_column = ?",
        ["demo", "orders", "customer_id"],
    ).fetchall()
    hops = {(r[0], r[1]) for r in rows}
    assert ("stg_orders", "customer_id") in hops, \
        f"expected (stg_orders, customer_id) in chain, got {hops}"


def test_reindex_sqlmesh_cross_repo_select_star_lineage_via_sql_upstream(tmp_path):
    """Downstream sqlmesh repo resolves SELECT * through a plain-SQL sibling.

    Covers the cross-repo catalog plumbing. NOTE: upstream is plain SQL with
    explicit CREATE TABLE column types — the supported path today. The true
    dbt→dbt and sqlmesh→sqlmesh mesh shapes require the column-persistence
    fixes tracked in #124 / #125; an xfail companion below pins that gap.
    """
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    upstream_path = tmp_path / "platform"
    upstream_path.mkdir()
    (upstream_path / "stg_orders.sql").write_text(
        "CREATE TABLE stg_orders (customer_id INT, order_id INT)"
    )
    indexer.reindex_repo("platform", str(upstream_path))

    downstream_path = tmp_path / "finance"
    downstream_path.mkdir()
    downstream_models = {
        '"finance"."orders"': (
            "WITH orders AS (SELECT * FROM stg_orders) "
            "SELECT customer_id FROM orders"
        ),
    }
    with _mock_render_raw(indexer, downstream_models, {}):
        indexer.reindex_sqlmesh("finance", downstream_path)

    rows = db._execute_read(
        "SELECT DISTINCT hop_table, hop_column "
        "FROM column_lineage cl "
        "JOIN files f ON cl.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND cl.output_node = ? AND cl.output_column = ?",
        ["finance", "orders", "customer_id"],
    ).fetchall()
    hops = {(r[0], r[1]) for r in rows}
    assert ("stg_orders", "customer_id") in hops, \
        f"expected (stg_orders, customer_id) in chain, got {hops}"


def test_reindex_sqlmesh_cross_repo_select_star_lineage_true_mesh(tmp_path):
    """True sqlmesh→sqlmesh mesh — upstream column persistence (#124) carries
    types into the downstream schema catalog via the graph."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    upstream_path = tmp_path / "platform"
    upstream_path.mkdir()
    upstream_models = {
        '"platform"."stg_orders"': "SELECT customer_id, order_id FROM raw.orders",
    }
    upstream_schemas = {
        '"platform"."stg_orders"': {"customer_id": "INT", "order_id": "INT"},
    }
    with _mock_render_raw(indexer, upstream_models, upstream_schemas):
        indexer.reindex_sqlmesh("platform", upstream_path)

    downstream_path = tmp_path / "finance"
    downstream_path.mkdir()
    downstream_models = {
        '"finance"."orders"': (
            "WITH orders AS (SELECT * FROM stg_orders) "
            "SELECT customer_id FROM orders"
        ),
    }
    with _mock_render_raw(indexer, downstream_models, {}):
        indexer.reindex_sqlmesh("finance", downstream_path)

    rows = db._execute_read(
        "SELECT DISTINCT hop_table, hop_column "
        "FROM column_lineage cl "
        "JOIN files f ON cl.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND cl.output_node = ? AND cl.output_column = ?",
        ["finance", "orders", "customer_id"],
    ).fetchall()
    hops = {(r[0], r[1]) for r in rows}
    assert ("stg_orders", "customer_id") in hops, \
        f"expected (stg_orders, customer_id) in chain, got {hops}"

    # Upstream INT types must persist cross-repo — catches the regression
    # where columns fall back to TEXT from the column_usage path.
    upstream_repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["platform"]
    ).fetchone()[0]
    catalog = db.get_table_columns(upstream_repo_id)
    assert catalog.get("stg_orders", {}).get("customer_id") == "INT", \
        f"expected INT type to survive, got {catalog.get('stg_orders')}"


def test_reindex_dbt_cross_repo_select_star_lineage_true_mesh(tmp_path):
    """True dbt→sqlmesh mesh — upstream dbt column names reach the downstream
    schema catalog, so SELECT * through a CTE expands and lineage resolves."""
    import json
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    # ── Upstream: minimal dbt project with a single staging model ──
    upstream = tmp_path / "platform"
    upstream.mkdir()
    (upstream / "dbt_project.yml").write_text("name: platform\n")
    (upstream / ".venv").mkdir()
    compiled = upstream / "target" / "compiled" / "platform" / "models" / "staging"
    compiled.mkdir(parents=True)
    (compiled / "stg_orders.sql").write_text(
        "SELECT customer_id, order_id FROM raw.orders"
    )
    manifest = {
        "nodes": {
            "model.platform.stg_orders": {
                "resource_type": "model",
                "package_name": "platform",
                "name": "stg_orders",
                "path": "staging/stg_orders.sql",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
    }
    (upstream / "target" / "manifest.json").write_text(json.dumps(manifest))
    # schema.yml declares column types — #125 gap: this isn't persisted today.
    (upstream / "models" / "staging").mkdir(parents=True)
    (upstream / "models" / "staging" / "schema.yml").write_text(
        "version: 2\nmodels:\n"
        "  - name: stg_orders\n"
        "    columns:\n"
        "      - name: customer_id\n        data_type: INT\n"
        "      - name: order_id\n        data_type: INT\n"
    )
    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(repo_name="platform", project_path=str(upstream), dialect="duckdb")

    # ── Downstream: sqlmesh repo selecting * from the dbt model ──
    downstream_path = tmp_path / "finance"
    downstream_path.mkdir()
    downstream_models = {
        '"finance"."orders"': (
            "WITH orders AS (SELECT * FROM stg_orders) "
            "SELECT customer_id FROM orders"
        ),
    }
    with _mock_render_raw(indexer, downstream_models, {}):
        indexer.reindex_sqlmesh("finance", downstream_path)

    rows = db._execute_read(
        "SELECT DISTINCT hop_table, hop_column "
        "FROM column_lineage cl "
        "JOIN files f ON cl.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND cl.output_node = ? AND cl.output_column = ?",
        ["finance", "orders", "customer_id"],
    ).fetchall()
    hops = {(r[0], r[1]) for r in rows}
    assert ("stg_orders", "customer_id") in hops, \
        f"expected (stg_orders, customer_id) in chain, got {hops}"

    # Mirror the sqlmesh sibling's upstream-type assertion — the dbt mesh
    # path must persist schema.yml types into the graph, not fall back to
    # TEXT via column_usage. This is the exact regression #125 addresses.
    upstream_repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["platform"]
    ).fetchone()[0]
    catalog = db.get_table_columns(upstream_repo_id)
    assert catalog.get("stg_orders", {}).get("customer_id") == "INT", \
        f"expected INT from schema.yml to survive, got {catalog.get('stg_orders')}"


async def test_trace_column_lineage_mcp_tool_traverses_select_star(tmp_path):
    """AC #1: the MCP ``trace_column_lineage`` tool (not just the raw table)
    returns hops through the SELECT * CTE — exercising the public interface
    users actually call.
    """
    from sqlprism.core import mcp_tools
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer
    from sqlprism.core.mcp_tools import TraceColumnLineageInput, trace_column_lineage

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    upstream_path = tmp_path / "platform"
    upstream_path.mkdir()
    (upstream_path / "stg_orders.sql").write_text(
        "CREATE TABLE stg_orders (customer_id INT, order_id INT)"
    )
    indexer.reindex_repo("platform", str(upstream_path))

    downstream_path = tmp_path / "finance"
    downstream_path.mkdir()
    downstream_models = {
        '"finance"."orders"': (
            "WITH orders AS (SELECT * FROM stg_orders) "
            "SELECT customer_id FROM orders"
        ),
    }
    with _mock_render_raw(indexer, downstream_models, {}):
        indexer.reindex_sqlmesh("finance", downstream_path)

    # Point the MCP tool at our in-memory graph for this invocation.
    prior_state = mcp_tools._state
    mcp_tools._state = mcp_tools._ServerState(graph=db, indexer=indexer, config={})
    try:
        result = await trace_column_lineage(TraceColumnLineageInput(
            table="orders", column="customer_id"
        ))
    finally:
        mcp_tools._state = prior_state

    # Result shape is implementation-defined; just verify it serializes to
    # something containing the expected upstream column somewhere in the
    # traversal. Flatten via repr so we're robust to dict/list nesting.
    payload = repr(result)
    assert "stg_orders" in payload, f"stg_orders missing from trace: {payload}"
    assert "customer_id" in payload, f"customer_id missing from trace: {payload}"


def test_reindex_sqlmesh_populates_columns_table(tmp_path, caplog):
    """sqlmesh column_schemas land in the `columns` table under source='sqlmesh_schema'."""
    import logging

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    raw_models = {
        '"platform"."stg_orders"': "SELECT customer_id, order_id FROM raw.orders",
    }
    column_schemas = {
        '"platform"."stg_orders"': {"customer_id": "INT", "order_id": "INT"},
    }

    with caplog.at_level(logging.WARNING, logger="sqlprism.core.indexer"):
        with _mock_render_raw(indexer, raw_models, column_schemas):
            indexer.reindex_sqlmesh("platform", tmp_path)

    rows = db._execute_read(
        "SELECT n.name, c.column_name, c.data_type, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? ORDER BY c.column_name",
        ["platform"],
    ).fetchall()

    assert rows == [
        ("stg_orders", "customer_id", "INT", "sqlmesh_schema"),
        ("stg_orders", "order_id", "INT", "sqlmesh_schema"),
    ]

    assert not any(
        "Column def skipped: cannot resolve node" in rec.getMessage()
        for rec in caplog.records
    ), f"unexpected skip warning(s): {[r.getMessage() for r in caplog.records]}"

    db.close()


def test_sqlmesh_columns_visible_in_schema_catalog(tmp_path):
    """GraphDB.get_table_columns returns real sqlmesh types, not the TEXT fallback."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    raw_models = {
        '"platform"."stg_orders"': "SELECT customer_id, order_id FROM raw.orders",
    }
    column_schemas = {
        '"platform"."stg_orders"': {"customer_id": "INT", "order_id": "INT"},
    }

    with _mock_render_raw(indexer, raw_models, column_schemas):
        indexer.reindex_sqlmesh("platform", tmp_path)

    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["platform"]
    ).fetchone()[0]

    catalog = db.get_table_columns(repo_id)

    assert "stg_orders" in catalog, f"stg_orders missing from catalog: {catalog}"
    assert catalog["stg_orders"].get("customer_id") == "INT", \
        f"expected customer_id=INT, got {catalog['stg_orders']}"
    assert catalog["stg_orders"].get("order_id") == "INT"

    db.close()


def test_insert_parse_result_resolves_query_kind_for_columns():
    """A kind='query' node is a valid target for a ColumnDefResult insert."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sqlmesh")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "stg_orders.sql", "sql", "abc")

    result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="query", name="stg_orders")],
        columns=[
            ColumnDefResult(
                node_name="stg_orders", column_name="customer_id",
                data_type="INT", position=0, source="sqlmesh_schema",
            ),
        ],
    )
    stats = {
        "nodes_added": 0, "edges_added": 0, "column_usage_added": 0,
        "columns_added": 0, "lineage_chains": 0,
    }

    with db.write_transaction():
        indexer._insert_parse_result(result, file_id, repo_id, stats)

    rows = db._execute_read(
        "SELECT n.name, n.kind, c.column_name, c.data_type, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id"
    ).fetchall()
    assert rows == [("stg_orders", "query", "customer_id", "INT", "sqlmesh_schema")]
    assert stats["columns_added"] == 1

    db.close()


def test_insert_parse_result_strips_qualified_column_def_names():
    """Fully-qualified ColumnDefResult.node_name resolves to an existing base-name node."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB(":memory:")
    indexer = Indexer(db)
    repo_id = db.upsert_repo("platform", "/tmp/platform", repo_type="sqlmesh")

    with db.write_transaction():
        file_id = db.insert_file(repo_id, "stg_orders.sql", "sql", "abc")

    # First pass: create the node under the bare base name.
    seed = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="query", name="stg_orders")],
    )
    seed_stats = {
        "nodes_added": 0, "edges_added": 0, "column_usage_added": 0,
        "columns_added": 0, "lineage_chains": 0,
    }
    with db.write_transaction():
        indexer._insert_parse_result(seed, file_id, repo_id, seed_stats)

    # Second pass: ColumnDefResult uses the fully-qualified form.
    with db.write_transaction():
        file_id2 = db.insert_file(repo_id, "stg_orders_cols.sql", "sql", "def")
    defs = ParseResult(
        language="sql",
        columns=[
            ColumnDefResult(
                node_name='"platform"."stg_orders"', column_name="customer_id",
                data_type="INT", position=0, source="sqlmesh_schema",
            ),
        ],
    )
    stats = {
        "nodes_added": 0, "edges_added": 0, "column_usage_added": 0,
        "columns_added": 0, "lineage_chains": 0,
    }
    with db.write_transaction():
        indexer._insert_parse_result(defs, file_id2, repo_id, stats)

    node_id = db._execute_read(
        "SELECT node_id FROM nodes WHERE name = 'stg_orders' AND kind = 'query'"
    ).fetchone()[0]
    rows = db._execute_read(
        "SELECT node_id, column_name, data_type, source FROM columns"
    ).fetchall()
    assert rows == [(node_id, "customer_id", "INT", "sqlmesh_schema")]
    assert stats["columns_added"] == 1

    db.close()
