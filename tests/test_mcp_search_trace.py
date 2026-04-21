"""Async end-to-end tests for search, find_references, find_column_usage,
index_status, trace_dependencies, and trace_column_lineage MCP tools."""

import asyncio

import pytest
from pydantic import ValidationError

from sqlprism.core.mcp_tools import (
    FindColumnUsageInput,
    FindReferencesInput,
    SearchInput,
    TraceColumnLineageInput,
    TraceDependenciesInput,
    configure,
    find_column_usage,
    find_references,
    index_status,
    search,
    trace_column_lineage,
    trace_dependencies,
)

# ── 5.1: Async end-to-end MCP tool test ──


def test_search_tool_end_to_end(tmp_path):
    """Async search tool returns results after configure + reindex."""

    repo_dir = tmp_path / "mcp_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)")
    (repo_dir / "report.sql").write_text("SELECT o.id, o.amount FROM orders o WHERE o.amount > 100")

    # Configure the MCP server module
    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    # Reindex via the indexer (same pattern as integration tests)
    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Call the async search tool end-to-end
    result = asyncio.run(search(SearchInput(pattern="orders")))
    assert result["total_count"] >= 1
    names = {m["name"] for m in result["matches"]}
    assert "orders" in names


def test_find_references_tool_end_to_end(tmp_path):
    """Async find_references tool returns results after configure + reindex."""
    repo_dir = tmp_path / "mcp_ref_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    (repo_dir / "summary.sql").write_text("SELECT COUNT(*) FROM orders")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(find_references(FindReferencesInput(name="orders", kind="table")))
    assert len(result["inbound"]) >= 1


def test_find_column_usage_tool_end_to_end(tmp_path):
    """Async find_column_usage tool returns results after configure + reindex."""
    repo_dir = tmp_path / "mcp_col_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    (repo_dir / "report.sql").write_text("SELECT o.id, o.amount FROM orders o WHERE o.amount > 50")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(find_column_usage(FindColumnUsageInput(table="orders")))
    assert result["total_count"] >= 1


# ── 5.2: Async end-to-end MCP tool tests for trace_dependencies, etc. ──


def test_index_status_returns_expected_shape(tmp_path):
    """index_status returns dict with repos, totals, phantom_nodes, schema_version."""
    repo_dir = tmp_path / "status_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(index_status())
    assert "repos" in result
    assert "totals" in result
    assert "phantom_nodes" in result
    assert result["schema_version"] == "1.0"
    assert isinstance(result["repos"], list)
    assert len(result["repos"]) == 1
    assert result["repos"][0]["name"] == "test"
    assert result["totals"]["files"] >= 1
    assert result["totals"]["nodes"] >= 1


def test_index_status_empty_index(tmp_path):
    """index_status works on a freshly configured but empty index."""
    repo_dir = tmp_path / "empty_repo"
    repo_dir.mkdir()

    configure(
        db_path=":memory:",
        repos={"empty": str(repo_dir)},
    )

    result = asyncio.run(index_status())
    assert result["repos"][0]["name"] == "empty"
    assert result["totals"]["files"] == 0
    assert result["totals"]["nodes"] == 0
    assert result["totals"]["edges"] == 0


def test_trace_dependencies_downstream(tmp_path):
    """trace_dependencies follows downstream references from a table."""
    repo_dir = tmp_path / "trace_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    (repo_dir / "summary.sql").write_text("CREATE VIEW order_summary AS SELECT COUNT(*) AS cnt FROM orders")
    (repo_dir / "report.sql").write_text("SELECT cnt FROM order_summary WHERE cnt > 10")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # summary.sql defines order_summary and references orders.
    # Downstream from "summary" should find both.
    result = asyncio.run(
        trace_dependencies(TraceDependenciesInput(name="summary", direction="downstream", max_depth=3))
    )
    assert result["root"] is not None
    assert result["root"]["name"] == "summary"
    assert len(result["paths"]) >= 1
    downstream_names = {p["name"] for p in result["paths"]}
    assert "orders" in downstream_names or "order_summary" in downstream_names


def test_trace_dependencies_upstream(tmp_path):
    """trace_dependencies follows upstream references and skips defines edges.

    Exercises two MCP-integration invariants in one repo:
      * A regular `references` edge (summary(query) -> orders(table)) surfaces
        upstream from `orders`.
      * The `defines` edge (summary(query) -> order_summary(view)) does NOT
        surface upstream from `order_summary`, directly guarding the fix for
        issue #122 at the MCP tool layer.
    """
    repo_dir = tmp_path / "trace_up_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    (repo_dir / "summary.sql").write_text("CREATE VIEW order_summary AS SELECT COUNT(*) AS cnt FROM orders")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Upstream from orders (table) traverses the real references edge:
    # summary(query) -[references]-> orders(table).
    result = asyncio.run(
        trace_dependencies(TraceDependenciesInput(name="orders", direction="upstream", max_depth=3))
    )
    assert result["root"] is not None
    assert len(result["paths"]) >= 1
    upstream_names = {p["name"] for p in result["paths"]}
    assert "summary" in upstream_names
    assert all(p["relationship"] != "defines" for p in result["paths"])

    # Upstream from order_summary (view) would previously surface summary(query)
    # via the defines edge. With the #122 fix that edge is filtered, so no
    # path should appear via that relationship.
    result_view = asyncio.run(
        trace_dependencies(
            TraceDependenciesInput(name="order_summary", direction="upstream", max_depth=3)
        )
    )
    assert all(p["relationship"] != "defines" for p in result_view["paths"])
    view_upstream_names = {p["name"] for p in result_view["paths"]}
    assert "summary" not in view_upstream_names


def test_trace_dependencies_not_found(tmp_path):
    """trace_dependencies returns empty result for unknown entity."""
    repo_dir = tmp_path / "trace_empty_repo"
    repo_dir.mkdir()

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    result = asyncio.run(trace_dependencies(TraceDependenciesInput(name="nonexistent_table", direction="downstream")))
    assert result["root"] is None
    assert result["paths"] == []


def test_trace_dependencies_input_validation():
    """TraceDependenciesInput validates max_depth range."""
    with pytest.raises(ValidationError):
        TraceDependenciesInput(name="x", max_depth=0)
    with pytest.raises(ValidationError):
        TraceDependenciesInput(name="x", max_depth=7)
    # Boundary values are accepted
    inp = TraceDependenciesInput(name="x", max_depth=1)
    assert inp.max_depth == 1
    inp = TraceDependenciesInput(name="x", max_depth=6)
    assert inp.max_depth == 6


def test_trace_dependencies_input_defaults():
    """TraceDependenciesInput has correct defaults."""
    inp = TraceDependenciesInput(name="orders")
    assert inp.direction == "downstream"
    assert inp.max_depth == 3
    assert inp.kind is None
    assert inp.repo is None
    assert inp.include_snippets is False
    assert inp.limit == 100


def test_trace_column_lineage_end_to_end(tmp_path):
    """trace_column_lineage returns lineage chains through CTEs."""
    repo_dir = tmp_path / "lineage_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)")
    (repo_dir / "report.sql").write_text("WITH base AS (SELECT id, amount FROM orders) SELECT id, amount FROM base")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(trace_column_lineage(TraceColumnLineageInput(table="orders", column="amount")))
    assert "chains" in result
    assert "total_count" in result


def test_trace_column_lineage_no_match(tmp_path):
    """trace_column_lineage returns empty chains when no lineage exists."""
    repo_dir = tmp_path / "lineage_empty_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(trace_column_lineage(TraceColumnLineageInput(table="nonexistent_table", column="foo")))
    assert result["chains"] == []
    assert result["total_count"] == 0


def test_trace_column_lineage_empty_params():
    """trace_column_lineage with no filters returns empty result."""
    configure(
        db_path=":memory:",
        repos={"test": "/tmp/dummy"},
    )

    result = asyncio.run(trace_column_lineage(TraceColumnLineageInput()))
    assert result["chains"] == []
    assert result["total_count"] == 0


def test_trace_column_lineage_input_defaults():
    """TraceColumnLineageInput has correct defaults."""
    inp = TraceColumnLineageInput()
    assert inp.table is None
    assert inp.column is None
    assert inp.output_node is None
    assert inp.repo is None
    assert inp.limit == 100
    assert inp.offset == 0
