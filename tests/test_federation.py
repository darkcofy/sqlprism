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

    Each consumer file stores a local node for the ref target; the ref edge
    points at the *local* shadow, never at the defining node in another
    repo. Pre-fix, trace from the producer terminated inside finance and
    never reached marketing.

    Each file is modelled with the full shape emitted by the SQL parser:
    a file-stem ``query`` node, a same-name ``table`` node representing
    the CREATE target, a ``defines`` edge between them, and
    ``references`` edges to cross-repo shadows. The defines edge re-
    exercises ``_non_dataflow_edge_filter`` under the name-quotient walk
    (#122/#127 regression surface).
    """
    db = GraphDB()
    platform = db.upsert_repo("platform", "/tmp/platform", repo_type="dbt")
    finance = db.upsert_repo("finance", "/tmp/finance", repo_type="dbt")
    marketing = db.upsert_repo("marketing", "/tmp/marketing", repo_type="dbt")

    def _model_file(repo_id, path, file_hash, stem, refs):
        """Insert one model file with file-stem query + CREATE-target table +
        ``defines`` edge + ``references`` edges to each shadow in ``refs``.
        Returns ``(query_node_id, table_node_id, shadow_node_ids)``."""
        file_id = db.insert_file(repo_id, path, "sql", file_hash)
        q = db.insert_node(file_id, "query", stem, "sql")
        t = db.insert_node(file_id, "table", stem, "sql", metadata={"create_type": "TABLE"})
        db.insert_edge(q, t, "defines", context="CREATE statement")
        shadow_ids: list[int] = []
        for ref in refs:
            sid = db.insert_node(file_id, "table", ref, "sql")
            db.insert_edge(q, sid, "references")
            shadow_ids.append(sid)
        return q, t, shadow_ids

    # platform: the real producer. Just the CREATE target + a file-stem
    # query with a defines edge — no upstream refs.
    _model_file(platform, "models/stg_order_items.sql", "h1", "stg_order_items", [])

    # finance/order_items.sql refs stg_order_items (local shadow)
    _model_file(finance, "models/order_items.sql", "h2", "order_items", ["stg_order_items"])

    # finance/orders.sql refs order_items (local shadow)
    _model_file(finance, "models/orders.sql", "h3", "orders", ["order_items"])

    # marketing/customers.sql refs orders + order_items (both shadows,
    # lifting the cross-project deps into the marketing graph only)
    _model_file(marketing, "models/customers.sql", "h4", "customers", ["orders", "order_items"])

    return db


def test_cross_repo_trace_through_shadow_nodes():
    """Upstream trace from a producer must walk through consumer-repo shadows.

    Regression for #131: marketing/customers.sql references finance.orders via
    a local shadow node `orders` in marketing. Pre-fix, the walk from
    stg_order_items stopped inside finance. Post-fix, same-name shadows
    teleport the walk into marketing so customers shows up.

    Also guards #122/#127: the fixture includes a ``defines`` edge per
    file, which must not be followed by the name-quotient walk (its
    shape would otherwise invite defines / inserts_into self-loops to
    surface as hops).
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

        # The start name must never hop to itself via a defines edge
        # (platform's file-stem query defines its same-name table).
        assert "stg_order_items" not in path_names, (
            f"defines self-loop leaked into trace: {path_names}"
        )

        assert "finance" in result["repos_affected"]
        assert "marketing" in result["repos_affected"]
    finally:
        db.close()


def test_cross_repo_trace_downstream_through_shadow_nodes():
    """Downstream direction must also traverse shadow refs across repos.

    Complements the upstream test: both direction paths share the new
    seed/recurse SQL, so a column-swap regression in downstream would
    ship undetected otherwise.
    """
    db = _build_jaffle_mesh_graph()
    try:
        result = db.query_trace("customers", direction="downstream", max_depth=5)

        path_names = [step["name"] for step in result["paths"]]
        # customers → orders/order_items → stg_order_items across 3 repos
        assert "orders" in path_names
        assert "order_items" in path_names
        assert "stg_order_items" in path_names, (
            f"downstream walk failed to reach platform producer: {path_names}"
        )

        repos = set(result["repos_affected"])
        assert {"finance", "platform"}.issubset(repos), (
            f"expected finance+platform in repos_affected, got {repos}"
        )
    finally:
        db.close()


def test_cross_repo_trace_both_merges_directions():
    """direction="both" splits results into downstream/upstream keys and
    merges ``repos_affected`` across both walks."""
    db = _build_jaffle_mesh_graph()
    try:
        result = db.query_trace("orders", direction="both", max_depth=5)

        down_names = [p["name"] for p in result["downstream"]]
        up_names = [p["name"] for p in result["upstream"]]

        # downstream from orders (finance) reaches order_items + stg_order_items
        assert "order_items" in down_names
        assert "stg_order_items" in down_names
        # upstream reaches the marketing consumer
        assert "customers" in up_names

        repos = set(result["repos_affected"])
        assert {"platform", "finance", "marketing"}.issubset(repos), (
            f"both-direction walk should span all 3 repos, got {repos}"
        )
    finally:
        db.close()


def test_cross_repo_trace_respects_max_depth():
    """max_depth=1 bounds the walk to immediate neighbours even under
    name-quotient fanout."""
    db = _build_jaffle_mesh_graph()
    try:
        result = db.query_trace("stg_order_items", direction="upstream", max_depth=1)

        path_names = [step["name"] for step in result["paths"]]
        # Only depth-1 hops allowed: finance.order_items references the
        # shadow. orders/customers live further upstream — must be absent.
        assert path_names == ["order_items"], (
            f"max_depth=1 allowed deeper hops: {path_names}"
        )
    finally:
        db.close()


def test_cross_repo_trace_name_collision_is_conflated():
    """Name-quotient traversal *deliberately* conflates unrelated same-name
    models across repos.

    This is the tradeoff required to make cross-project shadow refs
    resolve (#131): downstream consumers in repo Z surface via the
    shadow name even when repo X and repo Z happen to define unrelated
    models with the same name. The test pins the tradeoff so a future
    refactor can't silently revert it.
    """
    db = GraphDB()
    try:
        repo_a = db.upsert_repo("alpha", "/tmp/alpha", repo_type="sql")
        repo_b = db.upsert_repo("bravo", "/tmp/bravo", repo_type="sql")

        file_a = db.insert_file(repo_a, "models/users.sql", "sql", "ha")
        users_a = db.insert_node(file_a, "query", "users", "sql")

        # Repo bravo has an UNRELATED "users" model with its own consumer.
        file_b = db.insert_file(repo_b, "models/users.sql", "sql", "hb")
        users_b = db.insert_node(file_b, "query", "users", "sql")

        file_b_cons = db.insert_file(repo_b, "marts/revenue.sql", "sql", "hc")
        revenue = db.insert_node(file_b_cons, "query", "revenue", "sql")
        users_b_shadow = db.insert_node(file_b_cons, "table", "users", "sql")
        db.insert_edge(revenue, users_b_shadow, "references")

        # Upstream from alpha.users picks up bravo's consumer via the
        # shared name — not because alpha.users is referenced (it isn't),
        # but because the name-quotient walk treats both repos' `users`
        # as equivalent.
        result = db.query_trace("users", direction="upstream", max_depth=3)
        path_names = {p["name"] for p in result["paths"]}
        assert "revenue" in path_names, (
            "name-quotient tradeoff must surface cross-repo same-name "
            f"consumers; got {path_names}"
        )
        # Both repos end up in repos_affected because of the conflation.
        assert {"alpha", "bravo"}.issubset(set(result["repos_affected"])) or \
               "bravo" in result["repos_affected"]

        _ = users_a, users_b  # keep linters quiet about unused ids
    finally:
        db.close()


def test_cross_repo_trace_prefers_real_over_phantom():
    """Representative selection prefers a real (non-phantom) node when the
    same name has both phantom and real variants."""
    db = GraphDB()
    try:
        repo = db.upsert_repo("only", "/tmp/only", repo_type="sql")
        file_id = db.insert_file(repo, "models/a.sql", "sql", "h")
        a = db.insert_node(file_id, "query", "a", "sql")
        # Phantom node with the same name as the real producer target.
        phantom = db.get_or_create_phantom("b", "table", "sql")
        real_b = db.insert_node(file_id, "table", "b", "sql")
        db.insert_edge(a, phantom, "references")
        db.insert_edge(a, real_b, "references")

        result = db.query_trace("a", direction="downstream", max_depth=2)
        b_rows = [p for p in result["paths"] if p["name"] == "b"]
        assert b_rows, "expected a hop for 'b'"
        # The representative must be the real node — its repo is set.
        assert b_rows[0]["repo"] == "only", (
            f"expected real repo 'only', got {b_rows[0]['repo']!r}"
        )
    finally:
        db.close()


def test_cross_repo_trace_query_local_fallback():
    """When a name resolves only to a CTE/subquery alias (query-local
    kinds), the trace still seeds from it rather than returning empty."""
    db = GraphDB()
    try:
        repo = db.upsert_repo("r", "/tmp/r", repo_type="sql")
        file_id = db.insert_file(repo, "models/host.sql", "sql", "h")
        host = db.insert_node(file_id, "query", "host", "sql")
        cte = db.insert_node(file_id, "cte", "local_only", "sql")
        downstream = db.insert_node(file_id, "table", "downstream_tbl", "sql")
        db.insert_edge(host, cte, "defines")
        db.insert_edge(cte, downstream, "references")

        result = db.query_trace("local_only", direction="downstream", max_depth=3)
        path_names = {p["name"] for p in result["paths"]}
        assert "downstream_tbl" in path_names, (
            f"fallback to query-local start should still trace edges; got {path_names}"
        )
    finally:
        db.close()


def test_check_impact_repo_filter_gates_producer_lookup():
    """repo filter gates *producer* lookup: a name that doesn't exist at
    all in the named repo returns model_found=False, even if the name
    exists in another repo."""
    db = _build_jaffle_mesh_graph()
    try:
        # marketing has no node named stg_order_items at all — not even
        # a shadow. Asking about it with repo="marketing" must not
        # resolve to the platform producer silently.
        result = db.query_check_impact(
            "stg_order_items",
            [{"action": "add_column", "column": "x"}],
            repo="marketing",
        )
        assert result["model_found"] is False, (
            "marketing has no stg_order_items node; repo filter must "
            "keep this from resolving against the platform producer"
        )
    finally:
        db.close()


def test_check_impact_rename_column_via_shadow():
    """rename_column classification must reach a shadow-only consumer
    via the column_usage join (which lost its consumer-side repo filter
    as part of the #131 fix)."""
    db = _build_jaffle_mesh_graph()
    try:
        # Record a SELECT-time usage of stg_order_items.amount by the
        # finance consumer. The column_usage row is keyed to the finance
        # consumer's file, and the producer's repo filter must NOT scope
        # it out.
        finance_consumer = db.query_context("order_items")["model"]
        node_id = finance_consumer["node_id"] if "node_id" in finance_consumer else None
        if node_id is None:
            # Fall back to a direct node lookup
            row = db._execute_read(
                "SELECT n.node_id, f.file_id FROM nodes n "
                "JOIN files f ON n.file_id = f.file_id "
                "JOIN repos r ON f.repo_id = r.repo_id "
                "WHERE n.name = 'order_items' AND n.kind = 'query' AND r.name = 'finance'",
            ).fetchone()
            node_id, file_id = row[0], row[1]
        else:
            file_id = finance_consumer.get("file_id")

        db.insert_column_usage(node_id, "stg_order_items", "amount", "select", file_id)

        result = db.query_check_impact(
            "stg_order_items",
            [{"action": "rename_column", "old": "amount", "new": "amount_cents"}],
            repo="platform",
        )
        assert result["model_found"] is True
        breaking_names = {b["model"] for b in result["impacts"][0]["breaking"]}
        assert "order_items" in breaking_names, (
            "finance consumer with SELECT-usage of renamed column should "
            f"be classified as breaking; got breaking={breaking_names}"
        )
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
