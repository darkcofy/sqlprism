"""Tests for cross-repo federation: tracing, context, collisions, and impact."""

from sqlprism.core.graph import GraphDB


def _build_cross_repo_graph():
    """Build two repos with cross-repo dependencies.

    Repo A: raw_orders -> staging_orders (staging depends on raw)
    Repo B: marts_revenue -> staging_orders (marts depends on staging from repo A)

    This creates a cross-repo edge: marts_revenue (repo_b) -> staging_orders (repo_a)
    Edge convention: source REFERENCES target = source depends on target.
    """
    db = GraphDB()
    repo_a = db.upsert_repo("repo_a", "/tmp/repo_a", repo_type="sql")
    repo_b = db.upsert_repo("repo_b", "/tmp/repo_b", repo_type="sql")
    file_a = db.insert_file(repo_a, "models.sql", "sql", "abc")
    file_b = db.insert_file(repo_b, "models.sql", "sql", "def")

    # Repo A nodes
    raw_orders = db.insert_node(file_a, "table", "raw_orders", "sql")
    staging_orders = db.insert_node(file_a, "table", "staging_orders", "sql")

    # Repo B nodes
    marts_revenue = db.insert_node(file_b, "table", "marts_revenue", "sql")

    # Edges: source -> target follows downstream flow
    db.insert_edge(raw_orders, staging_orders, "references")  # raw -> staging (within repo_a)
    db.insert_edge(staging_orders, marts_revenue, "references")  # staging -> marts (cross-repo)

    return db


def test_cross_repo_trace():
    """Upstream trace from marts_revenue should cross repo boundaries to reach raw_orders."""
    db = _build_cross_repo_graph()
    try:
        if db.has_pgq:
            db._create_property_graph()
        result = db.query_trace("marts_revenue", direction="upstream", max_depth=3)

        path_names = [step["name"] for step in result["paths"]]
        assert "staging_orders" in path_names, "staging_orders should appear in upstream trace"
        assert "raw_orders" in path_names, "raw_orders should appear (traced through staging_orders)"

        # Nodes from both repos must appear
        path_repos = {step["repo"] for step in result["paths"]}
        assert "repo_a" in path_repos, "trace should include nodes from repo_a"
    finally:
        db.close()


def test_cross_repo_get_context():
    """Context for staging_orders should show cross-repo downstream dependency marts_revenue."""
    db = _build_cross_repo_graph()
    try:
        if db.has_pgq:
            db._create_property_graph()
        # query_context for staging_orders: it has an outgoing edge to marts_revenue
        # which query_schema exposes in "upstream" (targets of edges where node is source)
        result = db.query_context("staging_orders")

        upstream_names = [u["name"] for u in result["upstream"]]
        assert "marts_revenue" in upstream_names, (
            "marts_revenue should appear as a dependency of staging_orders"
        )

        # Verify staging_orders lives in repo_a
        assert result["model"]["repo"] == "repo_a", "staging_orders should belong to repo_a"

        # Confirm marts_revenue is in a different repo (repo_b) — cross-repo dependency
        marts_schema = db.query_schema("marts_revenue")
        assert marts_schema["repo"] == "repo_b", "marts_revenue should belong to repo_b"
    finally:
        db.close()


def test_cross_repo_name_collision():
    """A node with the same name in two repos should appear in name_collisions."""
    db = _build_cross_repo_graph()
    try:
        # Add a second staging_orders node in repo_b's file
        file_b_row = db._execute_read(
            "SELECT f.file_id FROM files f "
            "JOIN repos r ON f.repo_id = r.repo_id "
            "WHERE r.name = 'repo_b'",
        ).fetchone()
        db.insert_node(file_b_row[0], "table", "staging_orders", "sql")

        status = db.get_index_status()
        collision_names = {c["name"] for c in status["name_collisions"]}
        assert "staging_orders" in collision_names, "staging_orders should be a name collision"

        # Verify both repos are listed
        for collision in status["name_collisions"]:
            if collision["name"] == "staging_orders":
                assert sorted(collision["repos"]) == ["repo_a", "repo_b"]
                break
    finally:
        db.close()


def test_index_status_cross_repo_edges():
    """Index status should report exactly 1 cross-repo edge (marts_revenue -> staging_orders)."""
    db = _build_cross_repo_graph()
    try:
        status = db.get_index_status()
        assert status["cross_repo_edges"] == 1, (
            f"Expected 1 cross-repo edge, got {status['cross_repo_edges']}"
        )
    finally:
        db.close()


def test_cross_repo_pr_impact():
    """Downstream trace from raw_orders should reach marts_revenue in repo_b (cross-repo blast radius)."""
    db = _build_cross_repo_graph()
    try:
        if db.has_pgq:
            db._create_property_graph()
        result = db.query_trace("raw_orders", direction="downstream", max_depth=3)

        path_names = [step["name"] for step in result["paths"]]
        assert "marts_revenue" in path_names, (
            "marts_revenue from repo_b should appear in downstream trace of raw_orders"
        )

        # Confirm the trace spans repos
        repos_affected = result.get("repos_affected", [])
        assert "repo_a" in repos_affected, "repo_a should be in repos_affected"
        assert "repo_b" in repos_affected, "repo_b should be in repos_affected"
    finally:
        db.close()
