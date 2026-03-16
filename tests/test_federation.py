"""Tests for cross-repo federation: tracing, context, collisions, and impact."""

from sqlprism.core.graph import GraphDB


def _build_cross_repo_graph():
    """Build two repos with cross-repo dependencies.

    Data flow: raw_orders -> staging_orders -> marts_revenue
    Edge convention: insert_edge(source, target) = source depends on target.

    Repo A: staging_orders depends on raw_orders
    Repo B: marts_revenue depends on staging_orders (cross-repo)

    query_trace direction semantics:
    - "upstream" follows inbound edges (what depends on this node = downstream impact)
    - "downstream" follows outbound edges (what this node depends on = upstream deps)
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

    # Edges: source REFERENCES target = source depends on target
    db.insert_edge(staging_orders, raw_orders, "references")     # staging depends on raw (within repo_a)
    db.insert_edge(marts_revenue, staging_orders, "references")  # marts depends on staging (cross-repo)

    return db, file_b


def test_cross_repo_trace():
    """Trace what marts_revenue depends on — should cross into repo_a."""
    db, _ = _build_cross_repo_graph()
    try:
        # "downstream" in query_trace = follow outbound edges = what this node depends on
        result = db.query_trace("marts_revenue", direction="downstream", max_depth=3)

        path_names = [step["name"] for step in result["paths"]]
        assert "staging_orders" in path_names
        assert "raw_orders" in path_names

        # Both repos should appear
        path_repos = {step["repo"] for step in result["paths"]}
        assert "repo_a" in path_repos
    finally:
        db.close()


def test_cross_repo_get_context():
    """Context for marts_revenue shows staging_orders (repo_a) as upstream dependency."""
    db, _ = _build_cross_repo_graph()
    try:
        result = db.query_context("marts_revenue")

        # marts_revenue depends on staging_orders → staging_orders is upstream
        upstream_names = [u["name"] for u in result["upstream"]]
        assert "staging_orders" in upstream_names

        # Verify repos
        assert result["model"]["repo"] == "repo_b"
        staging_schema = db.query_schema("staging_orders")
        assert staging_schema["repo"] == "repo_a"
    finally:
        db.close()


def test_cross_repo_name_collision():
    """Same (name, kind) in two repos appears in name_collisions."""
    db, file_b = _build_cross_repo_graph()
    try:
        # Add a second staging_orders table in repo_b
        db.insert_node(file_b, "table", "staging_orders", "sql")

        status = db.get_index_status()
        collision_names = {c["name"] for c in status["name_collisions"]}
        assert "staging_orders" in collision_names

        for collision in status["name_collisions"]:
            if collision["name"] == "staging_orders":
                assert collision["kind"] == "table"
                assert sorted(collision["repos"]) == ["repo_a", "repo_b"]
                break
    finally:
        db.close()


def test_index_status_cross_repo_edges():
    """Index status reports exactly 1 cross-repo edge (marts_revenue -> staging_orders)."""
    db, _ = _build_cross_repo_graph()
    try:
        status = db.get_index_status()
        assert status["cross_repo_edges"] == 1
    finally:
        db.close()


def test_cross_repo_pr_impact():
    """Downstream impact from raw_orders reaches marts_revenue in repo_b.

    Uses query_trace as a proxy for PR impact — the full pr_impact tool
    requires git operations that are complex to mock in unit tests.
    "upstream" in query_trace follows inbound edges = downstream impact.
    """
    db, _ = _build_cross_repo_graph()
    try:
        # "upstream" = follow inbound edges = what depends on this node
        result = db.query_trace("raw_orders", direction="upstream", max_depth=3)

        path_names = [step["name"] for step in result["paths"]]
        assert "staging_orders" in path_names
        assert "marts_revenue" in path_names

        # Confirm the trace spans repos
        repos_affected = result["repos_affected"]
        assert "repo_a" in repos_affected
        assert "repo_b" in repos_affected
    finally:
        db.close()


def test_single_repo_no_cross_repo_edges():
    """Single-repo graph has zero cross-repo edges and no name collisions."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("only_repo", "/tmp/only", repo_type="sql")
        file_id = db.insert_file(repo_id, "models.sql", "sql", "abc")

        a = db.insert_node(file_id, "table", "a", "sql")
        b = db.insert_node(file_id, "table", "b", "sql")
        db.insert_edge(a, b, "references")

        status = db.get_index_status()
        assert status["cross_repo_edges"] == 0
        assert status["name_collisions"] == []
    finally:
        db.close()
