"""Tests for MCP tool endpoints — Pydantic validation, field aliases, async end-to-end."""

import asyncio
import subprocess
from unittest.mock import patch

import pytest

import sqlprism.core.mcp_tools as _mcp_mod
from sqlprism.core.mcp_tools import (
    FindColumnUsageInput,
    FindReferencesInput,
    GetSchemaInput,
    PrImpactInput,
    ReindexFilesInput,
    ReindexInput,
    SearchInput,
    TraceColumnLineageInput,
    TraceDependenciesInput,
    _enqueue_reindex,
    _flush_reindex,
    configure,
    find_column_usage,
    find_references,
    get_schema,
    index_status,
    pr_impact,
    reindex,
    reindex_files,
    search,
    trace_column_lineage,
    trace_dependencies,
)


def _reset_debounce_state():
    """Reset module-level debounce globals between tests."""
    _mcp_mod._reindex_pending.clear()
    for handle in _mcp_mod._reindex_timers.values():
        handle.cancel()
    _mcp_mod._reindex_timers.clear()


@pytest.fixture(autouse=True)
def _reset_mcp_state():
    """Reset global MCP state and debounce state between tests."""
    _mcp_mod._state = None
    _reset_debounce_state()
    yield
    _mcp_mod._state = None
    _reset_debounce_state()

# ── 5.1: Pydantic validation and field aliases ──


def test_search_input_schema_alias():
    """SearchInput accepts 'schema' as field alias for sql_schema."""
    inp = SearchInput(pattern="orders", schema="staging")
    assert inp.sql_schema == "staging"


def test_search_input_sql_schema_direct():
    """SearchInput accepts sql_schema directly too (populate_by_name)."""
    inp = SearchInput(pattern="orders", sql_schema="public")
    assert inp.sql_schema == "public"


def test_find_references_input_schema_alias():
    """FindReferencesInput accepts 'schema' as field alias for sql_schema."""
    inp = FindReferencesInput(name="orders", schema="staging")
    assert inp.sql_schema == "staging"


def test_find_references_input_sql_schema_direct():
    """FindReferencesInput accepts sql_schema directly too."""
    inp = FindReferencesInput(name="orders", sql_schema="production")
    assert inp.sql_schema == "production"


def test_configure_sets_repo_type_from_config(tmp_path):
    """configure() stores repo_type based on config section (repos/dbt_repos/sqlmesh_repos)."""
    from sqlprism.core.graph import GraphDB

    db_path = str(tmp_path / "test.duckdb")
    repos = {
        "sql_repo": {"path": str(tmp_path / "sql"), "repo_type": "sql"},
        "dbt_repo": {"path": str(tmp_path / "dbt"), "repo_type": "dbt"},
        "sm_repo": {"path": str(tmp_path / "sm"), "repo_type": "sqlmesh"},
    }
    configure(db_path=db_path, repos=repos)

    graph = GraphDB(db_path)
    rows = graph.conn.execute(
        "SELECT name, repo_type FROM repos ORDER BY name"
    ).fetchall()
    result = {r[0]: r[1] for r in rows}
    assert result == {"dbt_repo": "dbt", "sm_repo": "sqlmesh", "sql_repo": "sql"}
    graph.close()


def test_search_input_validation_limit_too_low():
    """SearchInput rejects limit < 1."""
    with pytest.raises(Exception):
        SearchInput(pattern="x", limit=0)


def test_search_input_validation_limit_too_high():
    """SearchInput rejects limit > 100."""
    with pytest.raises(Exception):
        SearchInput(pattern="x", limit=200)


def test_search_input_validation_limit_boundary():
    """SearchInput accepts boundary values for limit."""
    inp_min = SearchInput(pattern="x", limit=1)
    assert inp_min.limit == 1
    inp_max = SearchInput(pattern="x", limit=100)
    assert inp_max.limit == 100


def test_find_references_input_validation_limit():
    """FindReferencesInput rejects limit out of range."""
    with pytest.raises(Exception):
        FindReferencesInput(name="x", limit=0)
    with pytest.raises(Exception):
        FindReferencesInput(name="x", limit=501)


def test_find_column_usage_input_validation_limit():
    """FindColumnUsageInput rejects limit out of range."""
    with pytest.raises(Exception):
        FindColumnUsageInput(table="x", limit=0)
    with pytest.raises(Exception):
        FindColumnUsageInput(table="x", limit=501)


def test_search_input_defaults():
    """SearchInput has correct defaults."""
    inp = SearchInput(pattern="orders")
    assert inp.kind is None
    assert inp.sql_schema is None
    assert inp.repo is None
    assert inp.limit == 20
    assert inp.include_snippets is True


def test_find_references_input_defaults():
    """FindReferencesInput has correct defaults."""
    inp = FindReferencesInput(name="orders")
    assert inp.kind is None
    assert inp.sql_schema is None
    assert inp.direction == "both"
    assert inp.limit == 100


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
    """trace_dependencies follows upstream references."""
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

    # Edge: summary(query) -[defines]-> order_summary(view)
    # Upstream from order_summary finds the defining query node.
    result = asyncio.run(
        trace_dependencies(TraceDependenciesInput(name="order_summary", direction="upstream", max_depth=3))
    )
    assert result["root"] is not None
    assert len(result["paths"]) >= 1
    upstream_names = {p["name"] for p in result["paths"]}
    assert "summary" in upstream_names


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
    with pytest.raises(Exception):
        TraceDependenciesInput(name="x", max_depth=0)
    with pytest.raises(Exception):
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


# ── pr_impact end-to-end tests ──


@pytest.fixture
def git_repo(tmp_path):
    """Create a git repo with SQL files and commits for pr_impact testing."""
    repo_dir = tmp_path / "test_repo"
    repo_dir.mkdir()

    # Init git repo
    subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    # Create initial SQL files on default branch
    models_dir = repo_dir / "models"
    models_dir.mkdir()

    (models_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL);")
    (models_dir / "customers.sql").write_text("CREATE TABLE customers (id INT, name TEXT);")
    (models_dir / "order_summary.sql").write_text(
        "CREATE VIEW order_summary AS SELECT c.name, SUM(o.amount) as total "
        "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name;"
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    # Get the base commit
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    base_commit = result.stdout.strip()

    # Make changes on a new branch
    subprocess.run(
        ["git", "checkout", "-b", "feature"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    # Modify orders.sql — add a new column
    (models_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL, status TEXT);"
    )

    # Add a new model
    (models_dir / "refunds.sql").write_text(
        "CREATE VIEW refunds AS SELECT o.id, o.amount FROM orders o WHERE o.status = 'refunded';"
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "add refunds"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    return {"path": repo_dir, "base_commit": base_commit}


def _configure_and_index_git_repo(git_repo_info):
    """Helper: configure MCP state with git repo and reindex."""
    from sqlprism.core.mcp_tools import _get_indexer

    repo_path = git_repo_info["path"]
    configure(
        db_path=":memory:",
        repos={"test": str(repo_path)},
    )
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_path))


def test_pr_impact_basic(git_repo):
    """pr_impact reports changed files, structural diff, and blast radius."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(pr_impact(PrImpactInput(base_commit=git_repo["base_commit"], repo="test")))

    # files_changed should include the modified and newly added files
    assert "files_changed" in result
    changed = result["files_changed"]
    assert len(changed) >= 2
    changed_basenames = [f.rsplit("/", 1)[-1] for f in changed]
    assert "orders.sql" in changed_basenames
    assert "refunds.sql" in changed_basenames

    # structural_diff should have nodes_added (refunds) and nodes_modified (orders)
    diff = result["structural_diff"]
    added_names = {n["name"] for n in diff["nodes_added"]}
    assert "refunds" in added_names

    modified_names = {n["name"] for n in diff["nodes_modified"]}
    assert "orders" in modified_names

    # blast_radius should be populated
    assert "blast_radius" in result
    br = result["blast_radius"]
    assert br.get("total_affected_nodes", 0) >= 1


def test_pr_impact_no_changes(git_repo):
    """pr_impact with HEAD as base_commit (no diff) returns empty result."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(pr_impact(PrImpactInput(base_commit="HEAD", repo="test")))

    assert result["files_changed"] == []
    assert result["structural_diff"] == {}
    assert result["blast_radius"] == {}


def test_pr_impact_structural_diff_correctness(git_repo):
    """Structural diff correctly identifies added/removed/modified nodes and edges."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(pr_impact(PrImpactInput(base_commit=git_repo["base_commit"], repo="test")))

    diff = result["structural_diff"]

    # refunds view is new
    added_names = {n["name"] for n in diff["nodes_added"]}
    assert "refunds" in added_names

    # orders table is modified (new status column)
    modified_names = {n["name"] for n in diff["nodes_modified"]}
    assert "orders" in modified_names

    # No nodes should be removed (we didn't delete any files)
    removed_names = {n["name"] for n in diff["nodes_removed"]}
    assert "orders" not in removed_names
    assert "customers" not in removed_names

    # New edges should have been added for the refunds view referencing orders
    edges_added = diff["edges_added"]
    refund_edges = [e for e in edges_added if e["source"] == "refunds" or e["target"] == "refunds"]
    assert len(refund_edges) >= 1

    # columns_added should include the new status column usage
    cols_added = diff["columns_added"]
    status_cols = [c for c in cols_added if c["column"] == "status"]
    assert len(status_cols) >= 1


def test_pr_impact_delta_mode(git_repo):
    """pr_impact delta mode returns newly_affected and no_longer_affected."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(
        pr_impact(
            PrImpactInput(
                base_commit=git_repo["base_commit"],
                repo="test",
                compare_mode="delta",
            )
        )
    )

    br = result["blast_radius"]
    assert br["compare_mode"] == "delta"
    assert "head_total" in br
    assert "base_total" in br
    assert "delta" in br
    assert "newly_affected" in br
    assert "no_longer_affected" in br
    assert "unchanged_affected" in br
    assert isinstance(br["newly_affected"], list)
    assert isinstance(br["no_longer_affected"], list)
    # The refunds view is new — its downstream impact is net-new
    assert br["head_total"] >= br["base_total"]


def test_pr_impact_absolute_mode(git_repo):
    """pr_impact returns the standard backward-compatible response format."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(
        pr_impact(
            PrImpactInput(
                base_commit=git_repo["base_commit"],
                repo="test",
                compare_mode="absolute",
            )
        )
    )

    # Verify all top-level keys are present
    assert "files_changed" in result
    assert "structural_diff" in result
    assert "blast_radius" in result

    # Verify structural_diff has all expected sub-keys
    diff = result["structural_diff"]
    for key in (
        "nodes_added",
        "nodes_removed",
        "nodes_modified",
        "edges_added",
        "edges_removed",
        "columns_added",
        "columns_removed",
    ):
        assert key in diff, f"Missing key '{key}' in structural_diff"
        assert isinstance(diff[key], list)

    # Verify blast_radius shape
    br = result["blast_radius"]
    assert "transitively_affected" in br
    assert "repos_affected" in br
    assert "total_affected_nodes" in br


def test_pr_impact_invalid_repo(tmp_path):
    """pr_impact with a non-existent repo raises ValueError."""
    repo_dir = tmp_path / "dummy"
    repo_dir.mkdir()

    configure(
        db_path=":memory:",
        repos={"real_repo": str(repo_dir)},
    )

    # Requesting a repo that doesn't exist raises ValueError from _resolve_repo_config
    with pytest.raises(ValueError, match="not found"):
        asyncio.run(pr_impact(PrImpactInput(base_commit="HEAD", repo="nonexistent_repo")))


def test_pr_impact_deleted_file(tmp_path):
    """pr_impact detects removed nodes when a SQL file is deleted."""
    repo_dir = tmp_path / "del_repo"
    repo_dir.mkdir()

    subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL);")
    (repo_dir / "legacy.sql").write_text("CREATE VIEW legacy_report AS SELECT id FROM orders;")

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    subprocess.run(
        ["git", "checkout", "-b", "cleanup"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    (repo_dir / "legacy.sql").unlink()
    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "remove legacy"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    from sqlprism.core.mcp_tools import _get_indexer

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(pr_impact(PrImpactInput(base_commit=base, repo="test")))

    diff = result["structural_diff"]
    removed_names = {n["name"] for n in diff["nodes_removed"]}
    assert "legacy_report" in removed_names


# ── Phase 1: Reindex thread-safety tests (1.1d-f) ──


def _reset_reindex_state():
    """Reset module-level reindex globals between tests."""
    import sqlprism.core.mcp_tools as m

    m._reindex_task = None
    m._reindex_status = {"state": "idle"}


def test_reindex_idempotency_guard(tmp_path):
    """Calling reindex twice returns in_progress for the second call (1.1d)."""
    _reset_reindex_state()
    repo_dir = tmp_path / "idem_repo"
    repo_dir.mkdir()
    # Use many files to ensure reindex takes long enough
    for i in range(50):
        (repo_dir / f"model_{i}.sql").write_text(f"CREATE TABLE t{i} (id INT); SELECT * FROM t{i};")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    async def _run():
        r1 = await reindex(ReindexInput())
        assert r1["status"] == "started"
        # Immediately call again — task should still be running
        r2 = await reindex(ReindexInput())
        assert r2["status"] == "in_progress"

        import sqlprism.core.mcp_tools as m

        if m._reindex_task:
            await m._reindex_task

    asyncio.run(_run())
    _reset_reindex_state()


def test_reindex_completes_and_index_status_shows_result(tmp_path):
    """Background reindex completes; index_status shows completed state (1.1e)."""
    _reset_reindex_state()
    repo_dir = tmp_path / "complete_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    async def _run():
        r = await reindex(ReindexInput())
        assert r["status"] == "started"

        # Wait for background task to complete
        import sqlprism.core.mcp_tools as m

        if m._reindex_task:
            await m._reindex_task

        status = await index_status()
        return status

    status = asyncio.run(_run())
    assert "last_reindex" in status
    last = status["last_reindex"]
    assert last["state"] == "completed", f"Expected completed, got: {last}"
    assert "result" in last

    _reset_reindex_state()


def test_reindex_failure_shows_in_status(tmp_path):
    """Background reindex failure is captured in index_status (1.1f)."""
    _reset_reindex_state()
    repo_dir = tmp_path / "fail_repo"
    repo_dir.mkdir()

    # Configure with a repo that doesn't exist in config to trigger error
    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    async def _run():
        import sqlprism.core.mcp_tools as m

        # Monkey-patch indexer to raise
        original_reindex = m._get_indexer().reindex_repo

        def _exploding(*args, **kwargs):
            raise RuntimeError("boom")

        m._get_indexer().reindex_repo = _exploding
        try:
            r = await reindex(ReindexInput())
            assert r["status"] == "started"

            if m._reindex_task:
                await m._reindex_task

            status = await index_status()
            return status
        finally:
            m._get_indexer().reindex_repo = original_reindex

    status = asyncio.run(_run())
    assert "last_reindex" in status
    assert status["last_reindex"]["state"] == "failed"
    assert "boom" in status["last_reindex"]["error"]

    _reset_reindex_state()


# ── Phase 1: PR Impact delta mode note (1.3c) ──


def test_pr_impact_delta_mode_includes_note(git_repo):
    """Delta mode response includes a note about the approximation (1.3c)."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(
        pr_impact(
            PrImpactInput(
                base_commit=git_repo["base_commit"],
                repo="test",
                compare_mode="delta",
            )
        )
    )

    br = result["blast_radius"]
    assert "note" in br
    assert "removed edges" in br["note"]


# ── Phase 1: Pydantic Literal validation (1.6d) ──


def test_compare_mode_rejects_invalid():
    """PrImpactInput rejects invalid compare_mode values (1.6a)."""
    with pytest.raises(Exception):
        PrImpactInput(base_commit="HEAD", compare_mode="relative")


def test_find_references_direction_rejects_invalid():
    """FindReferencesInput rejects invalid direction values (1.6b)."""
    with pytest.raises(Exception):
        FindReferencesInput(name="x", direction="upstream")


def test_trace_dependencies_direction_rejects_invalid():
    """TraceDependenciesInput rejects invalid direction values (1.6c)."""
    with pytest.raises(Exception):
        TraceDependenciesInput(name="x", direction="inbound")


# ── reindex_files tool and debounce tests ──


def test_mcp_reindex_files_single(tmp_path):
    """reindex_files reindexes a modified SQL file via debounce."""
    repo_dir = tmp_path / "rf_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    # A query that will reference the new column after modification
    report_file = repo_dir / "report.sql"
    report_file.write_text("SELECT id, amount FROM orders")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Verify 'status' column is NOT present before modification
    graph = _mcp_mod._state.graph
    col_before = graph.query_column_usage(table="orders", column="status")
    assert col_before["total_count"] == 0

    # Modify the SQL file and the report to reference the new column
    sql_file.write_text("CREATE TABLE orders (id INT, amount DECIMAL, status TEXT)")
    report_file.write_text("SELECT id, amount, status FROM orders")

    async def _run():
        result = await reindex_files(
            ReindexFilesInput(paths=[str(sql_file), str(report_file)])
        )
        assert result["accepted"] == 2
        # Wait for debounce to fire (0.5s for sql + margin)
        await asyncio.sleep(1.0)

    asyncio.run(_run())

    # Verify the updated schema was indexed — status column should now appear
    col_after = graph.query_column_usage(table="orders", column="status")
    assert col_after["total_count"] >= 1, (
        "Expected 'status' column usage after reindex_files update"
    )


def test_mcp_reindex_files_filters_non_sql(tmp_path):
    """reindex_files skips non-SQL files and only accepts SQL ones."""
    repo_dir = tmp_path / "filter_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "model.sql"
    sql_file.write_text("CREATE TABLE model (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    async def _run():
        result = await reindex_files(
            ReindexFilesInput(
                paths=[str(sql_file), str(repo_dir / "readme.md"), str(repo_dir / "config.yml")]
            )
        )
        assert result["accepted"] == 1
        assert result["skipped"] == 2
        return result

    asyncio.run(_run())


def test_debounce_batches_plain_sql(tmp_path):
    """Enqueuing multiple SQL files batches them and flushes after debounce."""
    repo_dir = tmp_path / "batch_repo"
    repo_dir.mkdir()
    for name in ("a.sql", "b.sql", "c.sql"):
        (repo_dir / name).write_text(f"CREATE TABLE {name[0]} (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    async def _run():
        await _enqueue_reindex("test", "sql", [str(repo_dir / "a.sql")])
        await _enqueue_reindex("test", "sql", [str(repo_dir / "b.sql")])
        await _enqueue_reindex("test", "sql", [str(repo_dir / "c.sql")])

        # All 3 should be pending
        assert len(_mcp_mod._reindex_pending["test"]) == 3

        # Wait for debounce to fire (0.5s + margin)
        await asyncio.sleep(1.0)

        # Pending should be empty after flush
        assert len(_mcp_mod._reindex_pending.get("test", [])) == 0

    asyncio.run(_run())


def test_debounce_batches_dbt_sqlmesh(tmp_path):
    """dbt debounce batches 5 models saved within the debounce window."""
    repo_dir = tmp_path / "dbt_batch_repo"
    repo_dir.mkdir()

    configure(db_path=":memory:", repos={"dbt_test": {"path": str(repo_dir), "repo_type": "dbt"}})

    flush_calls = []

    async def mock_flush(repo_name):
        flush_calls.append(repo_name)
        # Still drain the pending list like real flush does
        _mcp_mod._reindex_pending.pop(repo_name, None)
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with (
            patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush),
            patch.object(_mcp_mod, "_DEBOUNCE_RENDERED", 0.2),
        ):
            # Enqueue 5 distinct paths (BDD: "5 models saved within 2s")
            for i in range(5):
                await _enqueue_reindex("dbt_test", "dbt", [f"/models/model_{i}.sql"])

            # All 5 should be pending
            assert len(_mcp_mod._reindex_pending["dbt_test"]) == 5

            # Sleep 0.1s — less than 0.2s debounce, timer not fired yet
            await asyncio.sleep(0.1)
            assert len(flush_calls) == 0

            # Sleep another 0.2s — now past the 0.2s debounce
            await asyncio.sleep(0.2)
            assert len(flush_calls) == 1

    asyncio.run(_run())


def test_debounce_timer_resets(tmp_path):
    """Enqueuing a second file resets the debounce timer."""
    repo_dir = tmp_path / "reset_repo"
    repo_dir.mkdir()

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    flush_calls = []

    async def mock_flush(repo_name):
        flush_calls.append(list(_mcp_mod._reindex_pending.get(repo_name, [])))
        _mcp_mod._reindex_pending.pop(repo_name, None)
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush):
            await _enqueue_reindex("test", "sql", ["/a.sql"])
            await asyncio.sleep(0.3)

            # Enqueue second file — resets the 0.5s timer
            await _enqueue_reindex("test", "sql", ["/b.sql"])
            await asyncio.sleep(0.3)

            # Timer hasn't fired yet (only 0.3s since reset)
            assert len(flush_calls) == 0

            # Wait another 0.3s — now 0.6s since last enqueue, timer should fire
            await asyncio.sleep(0.3)
            assert len(flush_calls) == 1
            # Both files should be in the batch
            assert "/a.sql" in flush_calls[0]
            assert "/b.sql" in flush_calls[0]

    asyncio.run(_run())


def test_debounce_deduplicates_paths(tmp_path):
    """Duplicate paths are deduplicated when flush fires."""
    repo_dir = tmp_path / "dedup_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "model.sql"
    sql_file.write_text("CREATE TABLE model (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    captured_paths = []
    original_reindex_files = _mcp_mod._state.indexer.reindex_files

    def capture_reindex_files(paths, **kwargs):
        captured_paths.extend(paths)
        return original_reindex_files(paths=paths, **kwargs)

    async def _run():
        with patch.object(_mcp_mod._state.indexer, "reindex_files", side_effect=capture_reindex_files):
            # Enqueue same path twice
            await _enqueue_reindex("test", "sql", [str(sql_file)])
            await _enqueue_reindex("test", "sql", [str(sql_file)])

            # Pending has 2 entries (pre-dedup)
            assert len(_mcp_mod._reindex_pending["test"]) == 2

            # Wait for flush
            await asyncio.sleep(1.0)

            # Flush should have deduplicated
            assert captured_paths.count(str(sql_file)) == 1

    asyncio.run(_run())


def test_mcp_reindex_files_not_configured():
    """reindex_files returns error when server is not configured."""
    # _reset_mcp_state fixture already sets _state = None

    async def _run():
        result = await reindex_files(ReindexFilesInput(paths=["some.sql"]))
        assert "error" in result
        assert "not configured" in result["error"].lower()

    asyncio.run(_run())


def test_reindex_files_waits_for_lock(tmp_path):
    """_flush_reindex blocks on _reindex_lock and completes after release."""
    repo_dir = tmp_path / "lock_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Enqueue a file so flush has something to do
    _mcp_mod._reindex_pending["test"] = [str(sql_file)]

    flush_done = False

    async def _run():
        nonlocal flush_done

        # Acquire the lock first
        await _mcp_mod._reindex_lock.acquire()

        # Start flush in background — should block on the lock
        flush_task = asyncio.create_task(_flush_reindex("test"))

        # Give it a moment to attempt acquiring the lock
        await asyncio.sleep(0.1)
        assert not flush_task.done(), "flush should be blocked on the lock"

        # Release the lock
        _mcp_mod._reindex_lock.release()

        # Now flush should complete
        await asyncio.wait_for(flush_task, timeout=5.0)
        flush_done = True

    asyncio.run(_run())
    assert flush_done


def test_debounce_batches_rapid_enqueues(tmp_path):
    """5 rapid enqueues produce exactly one flush containing all 5 files."""
    repo_dir = tmp_path / "batch5_repo"
    repo_dir.mkdir()
    paths = []
    for i in range(5):
        f = repo_dir / f"model_{i}.sql"
        f.write_text(f"CREATE TABLE t{i} (id INT)")
        paths.append(str(f))

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    flushed_paths = []
    flush_call_count = 0

    async def mock_flush(repo_name):
        nonlocal flush_call_count
        flush_call_count += 1
        flushed_paths.extend(_mcp_mod._reindex_pending.pop(repo_name, []))
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with (
            patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush),
            patch.object(_mcp_mod, "_DEBOUNCE_SQL", 0.05),
        ):
            for p in paths:
                await _enqueue_reindex("test", "sql", [p])

            assert len(_mcp_mod._reindex_pending["test"]) == 5

            # Wait for debounce to fire (0.05s + margin)
            await asyncio.sleep(0.3)

        # Exactly one flush should have run, containing all 5 paths
        assert flush_call_count == 1
        assert len(flushed_paths) == 5
        for p in paths:
            assert p in flushed_paths

    asyncio.run(_run())


def test_reindex_concurrent_waits_for_lock(tmp_path):
    """_flush_reindex waits for _reindex_lock, then updates the graph after release.

    Differs from test_reindex_files_waits_for_lock by verifying the graph is
    actually updated after the lock is released (not just that the task completes).
    """
    repo_dir = tmp_path / "concurrent_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Verify initial state — no 'status' column
    graph = _mcp_mod._state.graph
    col_before = graph.query_column_usage(table="orders", column="status")
    assert col_before["total_count"] == 0

    # Modify the file and stage it for flush
    sql_file.write_text("CREATE TABLE orders (id INT, status TEXT)")
    _mcp_mod._reindex_pending["test"] = [str(sql_file)]

    flush_completed = False

    async def _run():
        nonlocal flush_completed

        # Recreate the lock on the current event loop — prior tests may have
        # bound it to a different loop via asyncio.run().
        _mcp_mod._reindex_lock = asyncio.Lock()

        # Simulate a full reindex holding the lock
        await _mcp_mod._reindex_lock.acquire()

        flush_task = asyncio.create_task(_flush_reindex("test"))

        await asyncio.sleep(0.2)
        assert not flush_task.done(), "flush should be blocked waiting for the lock"

        _mcp_mod._reindex_lock.release()

        await asyncio.wait_for(flush_task, timeout=5.0)
        flush_completed = True

    asyncio.run(_run())
    assert flush_completed

    # Verify the graph was actually updated after lock release
    status = graph.get_index_status()
    assert status["totals"]["files"] == 1


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
