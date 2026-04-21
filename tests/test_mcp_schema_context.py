"""Tests for the get_schema, check_impact, and get_context MCP tools (graph query layer)."""

import asyncio

import pytest

import sqlprism.core.mcp_tools as _mcp_mod
from sqlprism.core.mcp_tools import (
    GetSchemaInput,
    configure,
    get_schema,
)


# ── get_schema (query_schema) tests ──


def test_get_schema_with_columns():
    """query_schema returns columns with correct types, positions, and sources."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "orders", "sql")
        db.insert_columns_batch([
            (node_id, "order_id", "INT", 0, "definition", None),
            (node_id, "status", "TEXT", 1, "definition", None),
            (node_id, "amount", "DECIMAL", 2, "definition", None),
        ])

    result = db.query_schema("orders")

    assert result["name"] == "orders"
    assert result["kind"] == "table"
    assert result["file"] == "orders.sql"
    assert result["repo"] == "test"
    assert len(result["columns"]) == 3

    cols_by_name = {c["name"]: c for c in result["columns"]}
    assert cols_by_name["order_id"]["type"] == "INT"
    assert cols_by_name["order_id"]["position"] == 0
    assert cols_by_name["status"]["type"] == "TEXT"
    assert cols_by_name["status"]["position"] == 1
    assert cols_by_name["amount"]["type"] == "DECIMAL"
    assert cols_by_name["amount"]["position"] == 2
    for col in result["columns"]:
        assert col["source"] == "definition"
        assert col["description"] is None

    db.close()


def test_get_schema_dbt_descriptions():
    """query_schema merges dbt schema_yml descriptions with definition types."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("dbt_proj", "/tmp/dbt", repo_type="dbt")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "stg_orders.sql", "sql", "def456")
        node_id = db.insert_node(file_id, "table", "stg_orders", "sql")
        # First pass: definition columns with types but no descriptions
        db.insert_columns_batch([
            (node_id, "order_id", "INT", 0, "definition", None),
            (node_id, "status", "TEXT", 1, "definition", None),
        ])
        # Second pass: schema_yml upsert adds descriptions (types left as None
        # so COALESCE preserves the original type from definition)
        db.insert_columns_batch([
            (node_id, "order_id", None, None, "schema_yml", "Primary key for orders"),
            (node_id, "status", None, None, "schema_yml", "Current order status"),
        ])

    result = db.query_schema("stg_orders")

    assert len(result["columns"]) == 2
    cols_by_name = {c["name"]: c for c in result["columns"]}
    # Types preserved from definition pass
    assert cols_by_name["order_id"]["type"] == "INT"
    assert cols_by_name["status"]["type"] == "TEXT"
    # Descriptions added from schema_yml pass
    assert cols_by_name["order_id"]["description"] == "Primary key for orders"
    assert cols_by_name["status"]["description"] == "Current order status"
    # Source updated to schema_yml by upsert
    assert cols_by_name["order_id"]["source"] == "schema_yml"
    assert cols_by_name["status"]["source"] == "schema_yml"

    db.close()


def test_get_schema_unknown_model():
    """query_schema returns error dict for a nonexistent table."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    db.upsert_repo("test", "/tmp/test", repo_type="sql")

    result = db.query_schema("nonexistent_table")

    assert list(result.keys()) == ["error"]
    assert "nonexistent_table" in result["error"]

    db.close()


def test_get_schema_repo_filter():
    """query_schema filters results by repo name."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_a_id = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b_id = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    with db.write_transaction():
        file_a = db.insert_file(repo_a_id, "orders.sql", "sql", "aaa")
        db.insert_node(file_a, "table", "orders", "sql")
        file_b = db.insert_file(repo_b_id, "orders.sql", "sql", "bbb")
        db.insert_node(file_b, "table", "orders", "sql")

    result_a = db.query_schema("orders", repo="repo_a")
    assert result_a["name"] == "orders"
    assert result_a["repo"] == "repo_a"
    assert result_a["columns"] == []

    result_b = db.query_schema("orders", repo="repo_b")
    assert result_b["name"] == "orders"
    assert result_b["repo"] == "repo_b"
    assert result_b["columns"] == []

    db.close()


def test_get_schema_upstream_downstream():
    """query_schema returns upstream and downstream dependencies."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "xyz789")
        raw_id = db.insert_node(file_id, "table", "raw_orders", "sql")
        stg_id = db.insert_node(file_id, "table", "staging_orders", "sql")
        mart_id = db.insert_node(file_id, "table", "marts_revenue", "sql")
        # staging_orders references raw_orders
        db.insert_edge(stg_id, raw_id, "references")
        # marts_revenue references staging_orders
        db.insert_edge(mart_id, stg_id, "references")

    result = db.query_schema("staging_orders")

    assert len(result["upstream"]) == 1
    assert result["upstream"][0]["name"] == "raw_orders"

    assert len(result["downstream"]) == 1
    assert result["downstream"][0]["name"] == "marts_revenue"

    db.close()


def test_get_schema_ambiguous_no_repo_filter():
    """query_schema without repo filter returns first match with matches count."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_a_id = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b_id = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    with db.write_transaction():
        file_a = db.insert_file(repo_a_id, "orders.sql", "sql", "aaa")
        db.insert_node(file_a, "table", "orders", "sql")
        file_b = db.insert_file(repo_b_id, "orders.sql", "sql", "bbb")
        db.insert_node(file_b, "table", "orders", "sql")

    result = db.query_schema("orders")

    # Should return a result (not error), with ambiguity indicator
    assert "error" not in result
    assert result["name"] == "orders"
    assert result["matches"] == 2

    db.close()


def test_get_schema_null_data_type_returns_unknown():
    """query_schema returns UNKNOWN for columns with NULL data_type."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "inferred.sql", "sql", "abc123")
        node_id = db.insert_node(file_id, "table", "inferred_table", "sql")
        db.insert_columns_batch([
            (node_id, "known_col", "INT", 0, "definition", None),
            (node_id, "unknown_col", None, 1, "inferred", None),
        ])

    result = db.query_schema("inferred_table")

    cols_by_name = {c["name"]: c for c in result["columns"]}
    assert cols_by_name["known_col"]["type"] == "INT"
    assert cols_by_name["unknown_col"]["type"] == "UNKNOWN"

    db.close()


def test_get_schema_mcp_tool_integration(tmp_path):
    """get_schema MCP tool returns schema via async-to-thread bridge."""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, name TEXT);")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    state = _mcp_mod._state
    assert state is not None
    graph = state.graph

    # Manually insert a node with columns (reindex would parse but we want control)
    repo_id = graph.upsert_repo("test", str(repo_dir), repo_type="sql")
    with graph.write_transaction():
        file_id = graph.insert_file(repo_id, "orders.sql", "sql", "abc123")
        node_id = graph.insert_node(file_id, "table", "orders", "sql")
        graph.insert_columns_batch([
            (node_id, "id", "INT", 0, "definition", None),
            (node_id, "name", "TEXT", 1, "definition", None),
        ])

    result = asyncio.run(get_schema(GetSchemaInput(name="orders")))

    assert result["name"] == "orders"
    assert result["kind"] == "table"
    assert result["repo"] == "test"
    assert len(result["columns"]) == 2

    graph.close()


# ── check_impact (query_check_impact) tests ──


def test_check_impact_remove_column_breaking():
    """Removing a column used in SELECT is a breaking change."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        db.insert_column_usage(ds1_id, "staging.orders", "total_amount", "select", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "remove_column", "column": "total_amount"}],
    )

    impact = result["impacts"][0]
    breaking_models = [b["model"] for b in impact["breaking"]]
    assert "marts.revenue" in breaking_models
    breaking_entry = next(b for b in impact["breaking"] if b["model"] == "marts.revenue")
    assert "select" in breaking_entry["usage_types"]

    db.close()


def test_check_impact_remove_column_warning():
    """Removing a column used only in WHERE is a warning (not breaking)."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "int_orders", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        db.insert_column_usage(ds1_id, "staging.orders", "status", "where", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "remove_column", "column": "status"}],
    )

    impact = result["impacts"][0]
    warning_models = [w["model"] for w in impact["warnings"]]
    breaking_models = [b["model"] for b in impact["breaking"]]
    assert "int_orders" in warning_models
    assert "int_orders" not in breaking_models
    warning_entry = next(w for w in impact["warnings"] if w["model"] == "int_orders")
    assert "where" in warning_entry["usage_types"]

    db.close()


def test_check_impact_remove_unused_safe():
    """Removing a column not referenced by any downstream model is safe."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        # marts.revenue uses a different column, not internal_note
        db.insert_column_usage(ds1_id, "staging.orders", "total_amount", "select", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "remove_column", "column": "internal_note"}],
    )

    impact = result["impacts"][0]
    assert impact["breaking"] == []
    assert impact["warnings"] == []
    safe_models = [s["model"] for s in impact["safe"]]
    assert "marts.revenue" in safe_models

    db.close()


def test_check_impact_rename_column():
    """Renaming a column used in SELECT by 2 downstream models breaks both."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        ds2_id = db.insert_node(file_id, "query", "marts.orders_summary", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        db.insert_edge(ds2_id, src_id, "references")
        db.insert_column_usage(ds1_id, "staging.orders", "order_id", "select", file_id)
        db.insert_column_usage(ds2_id, "staging.orders", "order_id", "select", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "rename_column", "old": "order_id", "new": "id"}],
    )

    impact = result["impacts"][0]
    breaking_models = {b["model"] for b in impact["breaking"]}
    assert "marts.revenue" in breaking_models
    assert "marts.orders_summary" in breaking_models
    # Verify usage_types are reported
    for b in impact["breaking"]:
        assert "select" in b["usage_types"]

    db.close()


def test_check_impact_add_column_safe():
    """Adding a new column is always safe for all downstream models."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        ds2_id = db.insert_node(file_id, "query", "marts.orders_summary", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        db.insert_edge(ds2_id, src_id, "references")
        db.insert_column_usage(ds1_id, "staging.orders", "order_id", "select", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "add_column", "column": "new_field"}],
    )

    impact = result["impacts"][0]
    assert impact["breaking"] == []
    assert impact["warnings"] == []
    safe_models = {s["model"] for s in impact["safe"]}
    assert "marts.revenue" in safe_models
    assert "marts.orders_summary" in safe_models

    db.close()


def test_check_impact_multiple_changes():
    """Multiple changes are analyzed independently with correct summary totals."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        ds2_id = db.insert_node(file_id, "query", "marts.orders_summary", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        db.insert_edge(ds2_id, src_id, "references")
        # ds1 uses total_amount (SELECT) and order_id (SELECT)
        db.insert_column_usage(ds1_id, "staging.orders", "total_amount", "select", file_id)
        db.insert_column_usage(ds1_id, "staging.orders", "order_id", "select", file_id)
        # ds2 uses order_id (SELECT)
        db.insert_column_usage(ds2_id, "staging.orders", "order_id", "select", file_id)

    changes = [
        {"action": "remove_column", "column": "total_amount"},  # breaking for ds1, safe for ds2
        {"action": "rename_column", "old": "order_id", "new": "id"},  # breaking for ds1 & ds2
        {"action": "add_column", "column": "new_field"},  # safe for all
    ]
    result = db.query_check_impact("staging.orders", changes)

    assert result["changes_analyzed"] == 3
    assert len(result["impacts"]) == 3

    # Change 0: remove total_amount — ds1 breaking, ds2 safe
    imp0 = result["impacts"][0]
    assert imp0["change"]["action"] == "remove_column"
    assert len(imp0["breaking"]) == 1
    assert imp0["breaking"][0]["model"] == "marts.revenue"
    assert len(imp0["safe"]) == 1

    # Change 1: rename order_id — both ds1 and ds2 breaking
    imp1 = result["impacts"][1]
    assert imp1["change"]["action"] == "rename_column"
    breaking_models = {b["model"] for b in imp1["breaking"]}
    assert "marts.revenue" in breaking_models
    assert "marts.orders_summary" in breaking_models

    # Change 2: add new_field — all safe
    imp2 = result["impacts"][2]
    assert imp2["change"]["action"] == "add_column"
    assert imp2["breaking"] == []
    assert imp2["warnings"] == []
    assert len(imp2["safe"]) == 2

    # Summary totals
    summary = result["summary"]
    assert summary["total_breaking"] == 3  # 1 (remove) + 2 (rename)
    assert summary["total_warnings"] == 0
    assert summary["total_safe"] == 3  # 1 (remove) + 0 (rename) + 2 (add)

    db.close()


def test_check_impact_mixed_breaking_and_warning_usage():
    """A model with both SELECT and WHERE usage on the same column is classified as breaking."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "pipeline.sql", "sql", "abc123")
        src_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        ds1_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        db.insert_edge(ds1_id, src_id, "references")
        # Same column used in both SELECT (breaking) and WHERE (warning)
        db.insert_column_usage(ds1_id, "staging.orders", "amount", "select", file_id)
        db.insert_column_usage(ds1_id, "staging.orders", "amount", "where", file_id)

    result = db.query_check_impact(
        "staging.orders",
        [{"action": "remove_column", "column": "amount"}],
    )

    impact = result["impacts"][0]
    # Should be breaking (SELECT takes precedence), NOT in warnings
    assert len(impact["breaking"]) == 1
    assert impact["breaking"][0]["model"] == "marts.revenue"
    assert "select" in impact["breaking"][0]["usage_types"]
    assert "where" in impact["breaking"][0]["usage_types"]
    assert impact["warnings"] == []

    db.close()


def test_check_impact_nonexistent_model():
    """check_impact for a model not in the index returns model_found=False."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    db.upsert_repo("test", "/tmp/test", repo_type="sql")

    result = db.query_check_impact(
        "nonexistent_model",
        [{"action": "remove_column", "column": "col"}],
    )

    assert result["model_found"] is False
    assert result["changes_analyzed"] == 1
    assert result["impacts"] == []
    assert result["summary"]["total_breaking"] == 0

    db.close()


def test_check_impact_excludes_defining_query_when_file_stem_differs():
    """#127: downstream discovery must exclude the file-stem `query` node
    that defines the target table even when it has a different name.

    Today ``query_check_impact`` resolves ``node_rows`` by name only, so a
    colliding file stem hides the ``defines`` edge via the NOT IN clause
    by accident. When the file stem and the target table differ (e.g. a
    ``build_orders.sql`` whose CREATE target is ``staging.orders``),
    ``node_rows`` only contains the table — and without the explicit
    defines filter the defining query surfaces as a phantom consumer. The
    filter also covers every ``remove_column`` / ``rename_column`` change
    bucket, not just ``add_column``.
    """
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "build_orders.sql", "sql", "abc")
        target_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        definer_id = db.insert_node(file_id, "query", "build_orders", "sql")
        real_consumer_id = db.insert_node(file_id, "query", "marts.revenue", "sql")
        db.insert_edge(definer_id, target_id, "defines", "CREATE statement")
        db.insert_edge(real_consumer_id, target_id, "references")
        db.insert_column_usage(real_consumer_id, "staging.orders", "amount", "select", file_id)

    try:
        # add_column: every downstream surfaces in safe — definer must not
        add_result = db.query_check_impact(
            "staging.orders",
            [{"action": "add_column", "column": "new_field"}],
        )
        safe_models = {s["model"] for s in add_result["impacts"][0]["safe"]}
        assert "build_orders" not in safe_models, (
            f"defining query node leaked into safe consumers: {safe_models}"
        )
        assert "marts.revenue" in safe_models

        # remove_column: breaking classification must also exclude the definer
        # (a regression where the filter only applied to the add_column branch
        # would pass the assertion above but fail here).
        rm_result = db.query_check_impact(
            "staging.orders",
            [{"action": "remove_column", "column": "amount"}],
        )
        impact = rm_result["impacts"][0]
        surfaced = {x["model"] for x in impact["breaking"] + impact["warnings"] + impact["safe"]}
        assert "build_orders" not in surfaced, (
            f"defining query leaked into remove_column impact: {surfaced}"
        )
        assert "marts.revenue" in {b["model"] for b in impact["breaking"]}
    finally:
        db.close()


def test_check_impact_column_change_validation():
    """ColumnChange validator rejects missing required fields."""
    import pytest

    from sqlprism.core.mcp_tools import ColumnChange

    # remove_column without column
    with pytest.raises(Exception, match="requires 'column'"):
        ColumnChange(action="remove_column")

    # rename_column without old
    with pytest.raises(Exception, match="requires both"):
        ColumnChange(action="rename_column", new="id")

    # rename_column without new
    with pytest.raises(Exception, match="requires both"):
        ColumnChange(action="rename_column", old="order_id")

    # Valid cases should work
    ColumnChange(action="remove_column", column="col")
    ColumnChange(action="add_column", column="col")
    ColumnChange(action="rename_column", old="a", new="b")


# ── get_context (query_context) tests ──


def test_get_context_full():
    """query_context returns model metadata, columns, deps, and column_usage_summary."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        stg_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        raw_id = db.insert_node(file_id, "table", "raw_orders", "sql")
        mart_id = db.insert_node(file_id, "table", "marts.revenue", "sql")
        # Columns on staging.orders
        db.insert_columns_batch([
            (stg_id, "order_id", "INT", 0, "definition", None),
            (stg_id, "amount", "DECIMAL", 1, "definition", None),
            (stg_id, "status", "TEXT", 2, "definition", None),
        ])
        # Edges: staging.orders -> raw_orders, marts.revenue -> staging.orders
        db.insert_edge(stg_id, raw_id, "references")
        db.insert_edge(mart_id, stg_id, "references")
        # Column usage: marts.revenue uses staging.orders columns
        db.insert_column_usage(mart_id, "staging.orders", "order_id", "select", file_id)
        db.insert_column_usage(mart_id, "staging.orders", "order_id", "join_on", file_id)
        db.insert_column_usage(mart_id, "staging.orders", "amount", "select", file_id)
        db.insert_column_usage(mart_id, "staging.orders", "amount", "group_by", file_id)

    result = db.query_context("staging.orders")

    # Model metadata
    assert result["model"]["name"] == "staging.orders"
    assert result["model"]["kind"] == "table"
    assert result["model"]["file"] == "orders.sql"
    assert result["model"]["repo"] == "test"
    # Columns
    assert len(result["columns"]) == 3
    # Upstream / downstream
    upstream_names = [u["name"] for u in result["upstream"]]
    downstream_names = [d["name"] for d in result["downstream"]]
    assert "raw_orders" in upstream_names
    assert "marts.revenue" in downstream_names
    # Column usage summary
    cus = result["column_usage_summary"]
    assert set(cus["most_used_columns"]) == {"order_id", "amount"}
    assert "order_id" in cus["downstream_join_keys"]
    assert "amount" in cus["downstream_aggregations"]
    # Snippet is None (no real file on disk)
    assert result["snippet"] is None

    db.close()


def test_get_context_no_pgq():
    """query_context omits graph_metrics when DuckPGQ is disabled."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        db.insert_node(file_id, "table", "staging.orders", "sql")

    db._has_pgq = False
    result = db.query_context("staging.orders")

    assert "graph_metrics" not in result
    # All other sections present
    assert "model" in result
    assert "columns" in result
    assert "upstream" in result
    assert "downstream" in result
    assert "column_usage_summary" in result
    assert "snippet" in result

    db.close()


def test_get_context_with_pgq():
    """query_context includes graph_metrics when DuckPGQ pagerank succeeds."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    if not db.has_pgq:
        db.close()
        pytest.skip("DuckPGQ extension not available")

    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "orders.sql", "sql", "abc123")
        stg_id = db.insert_node(file_id, "table", "staging.orders", "sql")
        mart_id = db.insert_node(file_id, "table", "marts.revenue", "sql")
        db.insert_edge(mart_id, stg_id, "references")

    db.refresh_property_graph()
    result = db.query_context("staging.orders")

    # DuckPGQ is available and graph refreshed — graph_metrics must be present
    assert "graph_metrics" in result
    gm = result["graph_metrics"]
    assert isinstance(gm["importance"], (float, type(None)))
    assert gm["downstream_count"] == 1

    db.close()


def test_get_context_no_columns():
    """query_context handles models with no columns and no column_usage."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test", repo_type="sql")
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "procs.sql", "sql", "abc123")
        proc_id = db.insert_node(file_id, "table", "legacy_proc", "sql")
        raw_id = db.insert_node(file_id, "table", "raw_data", "sql")
        mart_id = db.insert_node(file_id, "table", "marts.report", "sql")
        db.insert_edge(proc_id, raw_id, "references")
        db.insert_edge(mart_id, proc_id, "references")

    result = db.query_context("legacy_proc")

    assert result["columns"] == []
    cus = result["column_usage_summary"]
    assert cus["most_used_columns"] == []
    assert cus["downstream_join_keys"] == []
    assert cus["downstream_aggregations"] == []
    # Upstream and downstream still populated
    upstream_names = [u["name"] for u in result["upstream"]]
    downstream_names = [d["name"] for d in result["downstream"]]
    assert "raw_data" in upstream_names
    assert "marts.report" in downstream_names

    db.close()


def test_get_context_unknown_model():
    """query_context returns error dict for a nonexistent model."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    db.upsert_repo("test", "/tmp/test", repo_type="sql")

    result = db.query_context("nonexistent")

    assert "error" in result
    assert "model" not in result

    db.close()


def test_get_context_repo_filter():
    """query_context with repo filter disambiguates same-named models."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_a_id = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b_id = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    with db.write_transaction():
        file_a = db.insert_file(repo_a_id, "orders.sql", "sql", "aaa")
        node_a = db.insert_node(file_a, "table", "orders", "sql")
        db.insert_columns_batch([(node_a, "col_a", "INT", 0, "definition", None)])

        file_b = db.insert_file(repo_b_id, "orders.sql", "sql", "bbb")
        node_b = db.insert_node(file_b, "table", "orders", "sql")
        db.insert_columns_batch([(node_b, "col_b", "TEXT", 0, "definition", None)])

        # Column usage in repo_a only
        ds_id = db.insert_node(file_a, "query", "downstream_a", "sql")
        db.insert_edge(ds_id, node_a, "references")
        db.insert_column_usage(ds_id, "orders", "col_a", "select", file_a)

    result_a = db.query_context("orders", repo="repo_a")
    assert result_a["model"]["repo"] == "repo_a"
    col_names = [c["name"] for c in result_a["columns"]]
    assert "col_a" in col_names
    assert "col_b" not in col_names

    result_b = db.query_context("orders", repo="repo_b")
    assert result_b["model"]["repo"] == "repo_b"
    col_names_b = [c["name"] for c in result_b["columns"]]
    assert "col_b" in col_names_b

    db.close()
