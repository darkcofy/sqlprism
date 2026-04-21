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


def _build_jaffle_mesh_graph():
    """Simulate a dbt-mesh graph where each consumer file holds a local *shadow*
    node for every cross-project ref it emits — reproducing the on-disk state
    produced by indexing jaffle-mesh (issue #131).

    Topology:
        platform.stg_order_items     ← real producer
        finance.order_items.sql      defines order_items, refs stg_order_items (shadow in finance)
        finance.orders.sql           defines orders, refs order_items (shadow in finance)
        marketing.customers.sql      defines customers, refs orders AND order_items (shadows in marketing)

    Each consumer file stores a local node for the ref target; the edge from
    that file's file-level query node points at the *local* shadow, never at
    the defining node in another repo. Pre-fix, trace from the producer
    terminates inside finance and never reaches marketing.
    """
    db = GraphDB()
    platform = db.upsert_repo("platform", "/tmp/platform", repo_type="dbt")
    finance = db.upsert_repo("finance", "/tmp/finance", repo_type="dbt")
    marketing = db.upsert_repo("marketing", "/tmp/marketing", repo_type="dbt")

    # platform: the real producer. File-level query node whose stem matches.
    platform_file = db.insert_file(platform, "models/stg_order_items.sql", "sql", "h1")
    db.insert_node(platform_file, "query", "stg_order_items", "sql")

    # finance/order_items.sql → refs stg_order_items (local shadow)
    fin_oi_file = db.insert_file(finance, "models/order_items.sql", "sql", "h2")
    fin_oi = db.insert_node(fin_oi_file, "query", "order_items", "sql")
    fin_stg_shadow = db.insert_node(fin_oi_file, "table", "stg_order_items", "sql")
    db.insert_edge(fin_oi, fin_stg_shadow, "references")

    # finance/orders.sql → refs order_items (local shadow)
    fin_o_file = db.insert_file(finance, "models/orders.sql", "sql", "h3")
    fin_o = db.insert_node(fin_o_file, "query", "orders", "sql")
    fin_oi_shadow = db.insert_node(fin_o_file, "table", "order_items", "sql")
    db.insert_edge(fin_o, fin_oi_shadow, "references")

    # marketing/customers.sql → refs orders + order_items (both shadows,
    # lifting the cross-project deps into the marketing graph only)
    mkt_c_file = db.insert_file(marketing, "models/customers.sql", "sql", "h4")
    mkt_c = db.insert_node(mkt_c_file, "query", "customers", "sql")
    mkt_o_shadow = db.insert_node(mkt_c_file, "table", "orders", "sql")
    mkt_oi_shadow = db.insert_node(mkt_c_file, "table", "order_items", "sql")
    db.insert_edge(mkt_c, mkt_o_shadow, "references")
    db.insert_edge(mkt_c, mkt_oi_shadow, "references")

    return db


def test_cross_repo_trace_through_shadow_nodes():
    """Upstream trace from a producer must walk through consumer-repo shadows.

    Regression for #131: marketing/customers.sql references finance.orders via
    a local shadow node `orders` in marketing. Pre-fix, the walk from
    stg_order_items stopped inside finance. Post-fix, same-name shadows
    teleport the walk into marketing so customers shows up.
    """
    db = _build_jaffle_mesh_graph()
    try:
        result = db.query_trace("stg_order_items", direction="upstream", max_depth=5)

        path_names = [step["name"] for step in result["paths"]]
        assert "order_items" in path_names
        assert "orders" in path_names
        assert "customers" in path_names, (
            f"expected marketing.customers to surface via shadow hop, got {path_names}"
        )

        assert "finance" in result["repos_affected"]
        assert "marketing" in result["repos_affected"]
    finally:
        db.close()


def test_cross_repo_check_impact_finds_shadow_consumers():
    """check_impact on a producer must report consumers that only touch a shadow.

    Finance's order_items references stg_order_items via a local shadow; marketing's
    customers references orders via a local shadow. Pre-fix, filtering by the
    producer's repo (platform) missed downstream consumers entirely.
    """
    db = _build_jaffle_mesh_graph()
    try:
        result = db.query_check_impact(
            "stg_order_items",
            [{"action": "add_column", "column": "new_col"}],
            repo="platform",
        )

        assert result["model_found"] is True
        safe_names = {entry["model"] for entry in result["impacts"][0]["safe"]}
        assert "order_items" in safe_names, (
            f"expected finance.order_items to appear via shadow edge, got {safe_names}"
        )
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
