"""Tests for the pr_impact MCP tool, reindex thread-safety, and related Pydantic Literal validation."""

import asyncio
import subprocess

import pytest
from pydantic import ValidationError

from sqlprism.core.mcp_tools import (
    FindReferencesInput,
    PrImpactInput,
    ReindexInput,
    TraceDependenciesInput,
    configure,
    index_status,
    pr_impact,
    reindex,
)


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

        m._get_indexer().reindex_repo = _exploding  # type: ignore[invalid-assignment]
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
    with pytest.raises(ValidationError):
        PrImpactInput(base_commit="HEAD", compare_mode="relative")  # type: ignore[invalid-argument-type]


def test_find_references_direction_rejects_invalid():
    """FindReferencesInput rejects invalid direction values (1.6b)."""
    with pytest.raises(ValidationError):
        FindReferencesInput(name="x", direction="upstream")  # type: ignore[invalid-argument-type]


def test_trace_dependencies_direction_rejects_invalid():
    """TraceDependenciesInput rejects invalid direction values (1.6c)."""
    with pytest.raises(ValidationError):
        TraceDependenciesInput(name="x", direction="inbound")  # type: ignore[invalid-argument-type]
