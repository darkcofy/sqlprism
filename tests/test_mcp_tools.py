"""Tests for MCP tool endpoints — Pydantic validation, field aliases, async end-to-end."""

import asyncio
import subprocess

import pytest

from sqlprism.core.mcp_tools import (
    FindColumnUsageInput,
    FindReferencesInput,
    PrImpactInput,
    ReindexInput,
    SearchInput,
    TraceColumnLineageInput,
    TraceDependenciesInput,
    configure,
    find_column_usage,
    find_references,
    index_status,
    pr_impact,
    reindex,
    search,
    trace_column_lineage,
    trace_dependencies,
)

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
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "mcp_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)"
    )
    (repo_dir / "report.sql").write_text(
        "SELECT o.id, o.amount FROM orders o WHERE o.amount > 100"
    )

    # Configure the MCP server module
    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    # Reindex via the indexer (same pattern as integration tests)
    from sqlprism.core.mcp_tools import _get_graph, _get_indexer
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
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )
    (repo_dir / "summary.sql").write_text(
        "SELECT COUNT(*) FROM orders"
    )

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
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )
    (repo_dir / "report.sql").write_text(
        "SELECT o.id, o.amount FROM orders o WHERE o.amount > 50"
    )

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(find_column_usage(FindColumnUsageInput(table="orders")))
    assert result["total_count"] >= 1


# ── 5.2: Async end-to-end MCP tool tests for trace_dependencies, trace_column_lineage, index_status ──


def test_index_status_returns_expected_shape(tmp_path):
    """index_status returns dict with repos, totals, phantom_nodes, schema_version."""
    repo_dir = tmp_path / "status_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )

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
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )
    (repo_dir / "summary.sql").write_text(
        "CREATE VIEW order_summary AS SELECT COUNT(*) AS cnt FROM orders"
    )
    (repo_dir / "report.sql").write_text(
        "SELECT cnt FROM order_summary WHERE cnt > 10"
    )

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Edges go: summary -[references]-> orders, so "upstream" from orders
    # finds the queries that reference it.
    result = asyncio.run(trace_dependencies(
        TraceDependenciesInput(name="orders", direction="upstream", max_depth=3)
    ))
    assert result["root"] is not None
    assert result["root"]["name"] == "orders"
    assert len(result["paths"]) >= 1
    upstream_names = {p["name"] for p in result["paths"]}
    assert "summary" in upstream_names


def test_trace_dependencies_upstream(tmp_path):
    """trace_dependencies follows upstream references."""
    repo_dir = tmp_path / "trace_up_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )
    (repo_dir / "summary.sql").write_text(
        "CREATE VIEW order_summary AS SELECT COUNT(*) AS cnt FROM orders"
    )

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Edge: summary(query) -[defines]-> order_summary(view)
    # Upstream from order_summary finds the defining query node.
    result = asyncio.run(trace_dependencies(
        TraceDependenciesInput(name="order_summary", direction="upstream", max_depth=3)
    ))
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

    result = asyncio.run(trace_dependencies(
        TraceDependenciesInput(name="nonexistent_table", direction="downstream")
    ))
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
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL)"
    )
    (repo_dir / "report.sql").write_text(
        "WITH base AS (SELECT id, amount FROM orders) "
        "SELECT id, amount FROM base"
    )

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(trace_column_lineage(
        TraceColumnLineageInput(table="orders", column="amount")
    ))
    assert "chains" in result
    assert "total_count" in result


def test_trace_column_lineage_no_match(tmp_path):
    """trace_column_lineage returns empty chains when no lineage exists."""
    repo_dir = tmp_path / "lineage_empty_repo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL)"
    )

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )

    from sqlprism.core.mcp_tools import _get_indexer
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(trace_column_lineage(
        TraceColumnLineageInput(table="nonexistent_table", column="foo")
    ))
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
        cwd=repo_dir, check=True, capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir, check=True, capture_output=True,
    )

    # Create initial SQL files on default branch
    models_dir = repo_dir / "models"
    models_dir.mkdir()

    (models_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, customer_id INT, amount DECIMAL);"
    )
    (models_dir / "customers.sql").write_text(
        "CREATE TABLE customers (id INT, name TEXT);"
    )
    (models_dir / "order_summary.sql").write_text(
        "CREATE VIEW order_summary AS SELECT c.name, SUM(o.amount) as total "
        "FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name;"
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo_dir, check=True, capture_output=True,
    )

    # Get the base commit
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    )
    base_commit = result.stdout.strip()

    # Make changes on a new branch
    subprocess.run(
        ["git", "checkout", "-b", "feature"],
        cwd=repo_dir, check=True, capture_output=True,
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
        cwd=repo_dir, check=True, capture_output=True,
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

    result = asyncio.run(
        pr_impact(PrImpactInput(base_commit=git_repo["base_commit"], repo="test"))
    )

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

    result = asyncio.run(
        pr_impact(PrImpactInput(base_commit="HEAD", repo="test"))
    )

    assert result["files_changed"] == []
    assert result["structural_diff"] == {}
    assert result["blast_radius"] == {}


def test_pr_impact_structural_diff_correctness(git_repo):
    """Structural diff correctly identifies added/removed/modified nodes and edges."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(
        pr_impact(PrImpactInput(base_commit=git_repo["base_commit"], repo="test"))
    )

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
    refund_edges = [
        e for e in edges_added
        if e["source"] == "refunds" or e["target"] == "refunds"
    ]
    assert len(refund_edges) >= 1

    # columns_added should include the new status column usage
    cols_added = diff["columns_added"]
    status_cols = [c for c in cols_added if c["column"] == "status"]
    assert len(status_cols) >= 1


def test_pr_impact_delta_mode(git_repo):
    """pr_impact delta mode returns newly_affected and no_longer_affected."""
    _configure_and_index_git_repo(git_repo)

    result = asyncio.run(
        pr_impact(PrImpactInput(
            base_commit=git_repo["base_commit"], repo="test", compare_mode="delta",
        ))
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
        pr_impact(PrImpactInput(
            base_commit=git_repo["base_commit"], repo="test", compare_mode="absolute",
        ))
    )

    # Verify all top-level keys are present
    assert "files_changed" in result
    assert "structural_diff" in result
    assert "blast_radius" in result

    # Verify structural_diff has all expected sub-keys
    diff = result["structural_diff"]
    for key in ("nodes_added", "nodes_removed", "nodes_modified",
                "edges_added", "edges_removed",
                "columns_added", "columns_removed"):
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
        asyncio.run(
            pr_impact(PrImpactInput(base_commit="HEAD", repo="nonexistent_repo"))
        )


def test_pr_impact_deleted_file(tmp_path):
    """pr_impact detects removed nodes when a SQL file is deleted."""
    repo_dir = tmp_path / "del_repo"
    repo_dir.mkdir()

    subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test.com"],
        cwd=repo_dir, check=True, capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir, check=True, capture_output=True,
    )

    (repo_dir / "orders.sql").write_text(
        "CREATE TABLE orders (id INT, amount DECIMAL);"
    )
    (repo_dir / "legacy.sql").write_text(
        "CREATE VIEW legacy_report AS SELECT id FROM orders;"
    )

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo_dir, check=True, capture_output=True,
    )
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_dir, check=True, capture_output=True, text=True,
    ).stdout.strip()

    subprocess.run(
        ["git", "checkout", "-b", "cleanup"],
        cwd=repo_dir, check=True, capture_output=True,
    )
    (repo_dir / "legacy.sql").unlink()
    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "remove legacy"],
        cwd=repo_dir, check=True, capture_output=True,
    )

    from sqlprism.core.mcp_tools import _get_indexer

    configure(
        db_path=":memory:",
        repos={"test": str(repo_dir)},
    )
    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    result = asyncio.run(
        pr_impact(PrImpactInput(base_commit=base, repo="test"))
    )

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
        (repo_dir / f"model_{i}.sql").write_text(
            f"CREATE TABLE t{i} (id INT); SELECT * FROM t{i};"
        )

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
        pr_impact(PrImpactInput(
            base_commit=git_repo["base_commit"], repo="test", compare_mode="delta",
        ))
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
