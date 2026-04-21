"""Tests for semantic tag inference, tag confidence, storage, and tag query methods."""

from sqlprism.core.conventions import ConventionEngine
from sqlprism.core.graph import GraphDB
from sqlprism.core.indexer import Indexer


def _setup_models_with_edges(db, models, edges):
    """Helper: create a repo with models and edges for clustering tests.

    Args:
        db: GraphDB instance.
        models: List of (file_path, model_name) tuples.
        edges: List of (source_name, target_name) tuples representing references.

    Returns:
        (repo_id, name_to_node_id mapping).
    """
    repo_id = db.upsert_repo("test", "/tmp/test")
    name_to_id = {}
    for i, (path, name) in enumerate(models):
        file_id = db.insert_file(repo_id, path, "sql", f"checksum_{i}")
        node_id = db.insert_node(file_id, "table", name, "sql", 1, 10)
        name_to_id[name] = node_id

    for src_name, tgt_name in edges:
        db.insert_edge(name_to_id[src_name], name_to_id[tgt_name], "references")

    return repo_id, name_to_id


def _setup_tagged_repo(db, repo_name, repo_path, models, tags):
    """Helper: create a repo with models and semantic tags.

    Args:
        db: GraphDB instance.
        repo_name: Name for the repo.
        repo_path: Path for the repo.
        models: List of (file_path, model_name) tuples.
        tags: List of (model_name, tag_name, confidence) tuples.

    Returns:
        (repo_id, name_to_node_id mapping).
    """
    repo_id = db.upsert_repo(repo_name, repo_path)
    name_to_id = {}
    for i, (path, name) in enumerate(models):
        file_id = db.insert_file(repo_id, path, "sql", f"ck_{repo_name}_{i}")
        node_id = db.insert_node(file_id, "table", name, "sql", 1, 10)
        name_to_id[name] = node_id

    tag_rows = [
        {
            "tag_name": tag_name,
            "node_id": name_to_id[model_name],
            "confidence": confidence,
            "source": "inferred",
        }
        for model_name, tag_name, confidence in tags
    ]
    db.upsert_tags(repo_id, tag_rows)
    return repo_id, name_to_id


# ── Semantic tag inference ──


def test_clustering_shared_refs():
    """Models sharing upstream refs are grouped into the same cluster."""
    db = GraphDB()
    try:
        # Need >= 5 models with refs for clustering.
        # 3 customer models + 2 filler models all reference stg_users.
        models = [
            ("models/marts/customer_ltv.sql", "marts.customer_ltv"),
            ("models/marts/customer_segments.sql", "marts.customer_segments"),
            ("models/intermediate/int_customer_orders.sql", "int_customer_orders"),
            ("models/marts/customer_activity.sql", "marts.customer_activity"),
            ("models/marts/customer_churn.sql", "marts.customer_churn"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("marts.customer_ltv", "stg_users"),
            ("marts.customer_segments", "stg_users"),
            ("int_customer_orders", "stg_users"),
            ("marts.customer_activity", "stg_users"),
            ("marts.customer_churn", "stg_users"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)

        # All 5 models that reference stg_users should be in the same cluster
        customer_ids = {
            name_to_id["marts.customer_ltv"],
            name_to_id["marts.customer_segments"],
            name_to_id["int_customer_orders"],
            name_to_id["marts.customer_activity"],
            name_to_id["marts.customer_churn"],
        }
        # All customer models should have the same tag
        customer_tags = {t.tag_name for t in tags if t.node_id in customer_ids}
        assert len(customer_tags) == 1, (
            f"Expected all customer models in one cluster, got tags: {customer_tags}"
        )
    finally:
        db.close()


def test_clustering_no_overlap():
    """Models with no shared references end up in separate clusters."""
    db = GraphDB()
    try:
        # Two disjoint groups of 3 models each, sharing refs only within
        # their group.  6 models with refs total (>= 5 threshold).
        models = [
            # Group A: revenue models → all ref stg_payments
            ("models/marts/revenue_daily.sql", "revenue_daily"),
            ("models/marts/revenue_monthly.sql", "revenue_monthly"),
            ("models/marts/revenue_summary.sql", "revenue_summary"),
            ("models/staging/stg_payments.sql", "stg_payments"),
            # Group B: customer models → all ref stg_users
            ("models/marts/customer_ltv.sql", "customer_ltv"),
            ("models/marts/customer_segments.sql", "customer_segments"),
            ("models/marts/customer_churn.sql", "customer_churn"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("revenue_daily", "stg_payments"),
            ("revenue_monthly", "stg_payments"),
            ("revenue_summary", "stg_payments"),
            ("customer_ltv", "stg_users"),
            ("customer_segments", "stg_users"),
            ("customer_churn", "stg_users"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)

        # Should produce two distinct tag names — one per group
        revenue_ids = {
            name_to_id["revenue_daily"],
            name_to_id["revenue_monthly"],
            name_to_id["revenue_summary"],
        }
        customer_ids = {
            name_to_id["customer_ltv"],
            name_to_id["customer_segments"],
            name_to_id["customer_churn"],
        }
        revenue_tags = {t.tag_name for t in tags if t.node_id in revenue_ids}
        customer_tags = {t.tag_name for t in tags if t.node_id in customer_ids}
        assert len(revenue_tags) == 1, f"Expected one tag for revenue group, got {revenue_tags}"
        assert len(customer_tags) == 1, f"Expected one tag for customer group, got {customer_tags}"
        assert revenue_tags != customer_tags, (
            f"Groups should have different tags: revenue={revenue_tags}, customer={customer_tags}"
        )
    finally:
        db.close()


def test_auto_label_token_frequency():
    """Auto-labeling picks the most frequent token after stripping prefixes."""
    db = GraphDB()
    try:
        # All models have "customer" in the name after prefix stripping.
        models = [
            ("models/marts/customer_ltv.sql", "marts.customer_ltv"),
            ("models/marts/customer_segments.sql", "marts.customer_segments"),
            ("models/intermediate/int_customer_orders.sql", "int_customer_orders"),
            ("models/staging/stg_customer_events.sql", "stg_customer_events"),
            ("models/marts/customer_churn.sql", "marts.customer_churn"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("marts.customer_ltv", "stg_users"),
            ("marts.customer_segments", "stg_users"),
            ("int_customer_orders", "stg_users"),
            ("stg_customer_events", "stg_users"),
            ("marts.customer_churn", "stg_users"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)

        # The cluster containing the customer models should be labeled "customer"
        customer_ids = {
            name_to_id["marts.customer_ltv"],
            name_to_id["marts.customer_segments"],
            name_to_id["int_customer_orders"],
            name_to_id["stg_customer_events"],
            name_to_id["marts.customer_churn"],
        }
        tag_names = {t.tag_name for t in tags if t.node_id in customer_ids}
        assert "customer" in tag_names, (
            f"Expected 'customer' tag, got: {tag_names}"
        )
    finally:
        db.close()


def test_description_signal_boost():
    """Column description containing the tag name boosts confidence by +0.1."""
    db = GraphDB()
    try:
        # Use a model whose name does NOT contain "customer" so the name
        # bonus is absent, leaving room for the description boost to be
        # observable (confidence is capped at 1.0).
        models = [
            ("models/marts/customer_ltv.sql", "marts.customer_ltv"),
            ("models/marts/customer_segments.sql", "marts.customer_segments"),
            ("models/intermediate/int_customer_orders.sql", "int_customer_orders"),
            ("models/staging/stg_customer_events.sql", "stg_customer_events"),
            ("models/marts/loyalty_score.sql", "marts.loyalty_score"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("marts.customer_ltv", "stg_users"),
            ("marts.customer_segments", "stg_users"),
            ("int_customer_orders", "stg_users"),
            ("stg_customer_events", "stg_users"),
            ("marts.loyalty_score", "stg_users"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)

        # First run: no column descriptions → baseline confidence
        engine = ConventionEngine(db, repo_id)
        tags_no_desc = engine.infer_semantic_tags(threshold=0.5)

        # Pick a model whose name does NOT contain the tag token, so name
        # bonus is 0 and baseline is lower.
        target_id = name_to_id["marts.loyalty_score"]
        baseline = next(
            (t.confidence for t in tags_no_desc if t.node_id == target_id), None
        )
        assert baseline is not None, "Expected a tag for marts.loyalty_score"

        # Add a column with description mentioning the tag name ("customer")
        # via the columns table (where dbt schema.yml descriptions land).
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source, description) "
            "VALUES (?, 'score', 'FLOAT', 1, 'definition', ?)",
            [target_id, "Customer loyalty and retention score"],
        )

        # Second run: with column description → boosted confidence
        tags_with_desc = engine.infer_semantic_tags(threshold=0.5)
        boosted = next(
            (t.confidence for t in tags_with_desc if t.node_id == target_id), None
        )
        assert boosted is not None, "Expected a tag for marts.loyalty_score after boost"
        assert boosted >= baseline + 0.1 - 0.01, (
            f"Expected confidence boost of +0.1: baseline={baseline}, boosted={boosted}"
        )
    finally:
        db.close()


# ── Tag confidence and edge cases ──


def test_tag_confidence_core_member():
    """Core cluster member with name token match gets ~0.95 confidence."""
    db = GraphDB()
    try:
        # All 5 "customer_*" models reference the SAME two upstream sources.
        # Since they all share identical ref sets, pairwise Jaccard = 1.0,
        # so avg_sim = 1.0 → base = 0.85. Name contains "customer" (the tag
        # token) → +0.10. Total = 0.95.
        models = [
            ("models/marts/customer_ltv.sql", "customer_ltv"),
            ("models/marts/customer_segments.sql", "customer_segments"),
            ("models/marts/customer_activity.sql", "customer_activity"),
            ("models/marts/customer_churn.sql", "customer_churn"),
            ("models/marts/customer_health.sql", "customer_health"),
            ("models/staging/stg_users.sql", "stg_users"),
            ("models/staging/stg_orders.sql", "stg_orders"),
        ]
        edges = [
            ("customer_ltv", "stg_users"),
            ("customer_ltv", "stg_orders"),
            ("customer_segments", "stg_users"),
            ("customer_segments", "stg_orders"),
            ("customer_activity", "stg_users"),
            ("customer_activity", "stg_orders"),
            ("customer_churn", "stg_users"),
            ("customer_churn", "stg_orders"),
            ("customer_health", "stg_users"),
            ("customer_health", "stg_orders"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)

        # All 5 customer models share identical refs → Jaccard = 1.0
        # base = 0.85 (core), name match bonus = 0.10 → 0.95
        for model_name in [
            "customer_ltv", "customer_segments", "customer_activity",
            "customer_churn", "customer_health",
        ]:
            tag = next(
                (t for t in tags if t.node_id == name_to_id[model_name]), None
            )
            assert tag is not None, f"Expected tag for {model_name}"
            assert tag.confidence == 0.95, (
                f"{model_name}: expected 0.95, got {tag.confidence}"
            )
    finally:
        db.close()


def test_tag_confidence_edge_member():
    """Edge cluster member just above Jaccard threshold gets ~0.60 confidence."""
    db = GraphDB()
    try:
        # Build a cluster where 4 "core" models share refs {A, B, C, D, E}
        # and 1 "edge" model shares only a carefully chosen subset so that
        # its average Jaccard to the 4 core members is ~0.52.
        #
        # Core models ref: {src_a, src_b, src_c, src_d, src_e}   (5 refs)
        # Edge model ref:  {src_a, src_b, src_c, src_f, src_g, src_h}  (6 refs)
        # Jaccard(edge, core) = |{a,b,c}| / |{a,b,c,d,e,f,g,h}| = 3/8 = 0.375
        #
        # That's below 0.5. We need ~0.52 average.
        # Let edge ref = {src_a, src_b, src_c, src_d, src_f}   (5 refs)
        # Jaccard(edge, core) = |{a,b,c,d}| / |{a,b,c,d,e,f}| = 4/6 ≈ 0.667
        # That's too high.
        #
        # We want avg_sim ≈ 0.52. With threshold=0.5, the clustering must
        # merge them. Use individual ref variation among core members.
        #
        # Strategy: 4 core models each ref {A,B,C} plus one unique ref each.
        # Edge model refs {A, X, Y} — shares only A with cores.
        # Jaccard(edge, core_i) = |{A}| / |{A,B,C,unique_i,X,Y}| = 1/6 ≈ 0.17
        # Too low — won't cluster.
        #
        # Better strategy: use a low threshold and construct carefully.
        # All 5 models need to end up in one cluster. Use threshold=0.3.
        #
        # 4 core models ref exactly {A, B, C, D}
        # 1 edge model refs {A, B, E, F, G}
        # Jaccard(edge, core) = 2/7 ≈ 0.286 — still too low for 0.3.
        #
        # Simplest: use threshold=0.5. Make 5 core models with identical refs
        # plus 1 edge model that has ~0.52 Jaccard with each core.
        # Core refs: {1,2,3,4,5,6,7,8,9,10} (10 refs)
        # Edge refs: {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19} (19 refs)
        # Jaccard = 10/19 ≈ 0.526 ✓
        #
        # We need 6+ models with refs for clustering (>= 5).
        # 5 core + 1 edge = 6 models with refs. Plus source nodes.

        # Create source nodes (no refs themselves)
        sources = [(f"models/staging/src_{i}.sql", f"src_{i}") for i in range(19)]
        # Core models: all ref src_0..src_9
        core_models = [
            (f"models/marts/customer_{c}.sql", f"customer_{c}")
            for c in ["ltv", "segments", "activity", "churn", "health"]
        ]
        # Edge model: refs src_0..src_9 plus src_10..src_18 (19 total refs)
        edge_model = [("models/marts/loyalty_score.sql", "loyalty_score")]

        models = sources + core_models + edge_model
        edges = []
        # Core models each ref src_0..src_9
        for _, core_name in core_models:
            for i in range(10):
                edges.append((core_name, f"src_{i}"))
        # Edge model refs src_0..src_18
        for i in range(19):
            edges.append(("loyalty_score", f"src_{i}"))

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)

        # The edge model "loyalty_score" should have avg Jaccard to cores ≈ 0.526
        # base = 0.60 + (0.526 - 0.5) * 0.5 = 0.60 + 0.013 ≈ 0.613
        # No name match (tag is "customer", model is "loyalty_score") → 0.61
        edge_tag = next(
            (t for t in tags if t.node_id == name_to_id["loyalty_score"]), None
        )
        assert edge_tag is not None, "Expected tag for loyalty_score"
        # Edge member: lower than core members, but above threshold
        core_tag = next(
            (t for t in tags if t.node_id == name_to_id["customer_ltv"]), None
        )
        assert core_tag is not None, "Expected tag for customer_ltv (core member)"
        assert edge_tag.confidence < core_tag.confidence, (
            f"Edge ({edge_tag.confidence}) should be < core ({core_tag.confidence})"
        )
        assert edge_tag.confidence >= 0.55, (
            f"Edge member should have confidence >= 0.55, got {edge_tag.confidence}"
        )
    finally:
        db.close()


def test_tag_storage_upsert():
    """Tags from infer_semantic_tags can be stored and read back via GraphDB."""
    db = GraphDB()
    try:
        # Set up a repo with enough models for clustering
        models = [
            ("models/marts/customer_ltv.sql", "customer_ltv"),
            ("models/marts/customer_segments.sql", "customer_segments"),
            ("models/marts/customer_activity.sql", "customer_activity"),
            ("models/marts/customer_churn.sql", "customer_churn"),
            ("models/marts/customer_health.sql", "customer_health"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("customer_ltv", "stg_users"),
            ("customer_segments", "stg_users"),
            ("customer_activity", "stg_users"),
            ("customer_churn", "stg_users"),
            ("customer_health", "stg_users"),
        ]

        repo_id, _name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags(threshold=0.5)
        assert len(tags) > 0, "Expected at least one tag assignment"

        # Store via upsert_tags
        tag_dicts = [
            {
                "tag_name": t.tag_name,
                "node_id": t.node_id,
                "confidence": t.confidence,
                "source": t.source,
            }
            for t in tags
        ]
        count = db.upsert_tags(repo_id, tag_dicts)
        assert count == len(tags)

        # Read back via get_tags
        stored = db.get_tags(repo_id)
        assert len(stored) == len(tags)

        # Each stored tag has correct fields
        for row in stored:
            assert row["tag_name"] != ""
            assert row["node_id"] > 0
            assert row["confidence"] > 0.0
            assert row["source"] == "inferred"
            assert row["node_name"] != ""

        # Verify specific (repo_id, tag_name, node_id) combos match
        stored_set = {(r["tag_name"], r["node_id"]) for r in stored}
        expected_set = {(t.tag_name, t.node_id) for t in tags}
        assert stored_set == expected_set

        # Test UPDATE path: modify confidence and upsert again
        for td in tag_dicts:
            td["confidence"] = 0.50
        db.upsert_tags(repo_id, tag_dicts)
        updated = db.get_tags(repo_id)
        assert len(updated) == len(tags), "Row count should not change on update"
        for row in updated:
            assert row["confidence"] == 0.50, (
                f"Expected updated confidence 0.50, got {row['confidence']}"
            )
    finally:
        db.close()


def test_tag_stability_no_flap():
    """A tagged model stays tagged when similarity drops but stays above threshold."""
    db = GraphDB()
    try:
        # Run 1: all 5 models share refs {A, B} → high similarity, all tagged.
        models = [
            ("models/marts/customer_ltv.sql", "customer_ltv"),
            ("models/marts/customer_segments.sql", "customer_segments"),
            ("models/marts/customer_activity.sql", "customer_activity"),
            ("models/marts/customer_churn.sql", "customer_churn"),
            ("models/marts/customer_health.sql", "customer_health"),
            ("models/staging/stg_users.sql", "stg_users"),
            ("models/staging/stg_orders.sql", "stg_orders"),
            ("models/staging/stg_extra.sql", "stg_extra"),
        ]
        edges = [
            ("customer_ltv", "stg_users"),
            ("customer_ltv", "stg_orders"),
            ("customer_segments", "stg_users"),
            ("customer_segments", "stg_orders"),
            ("customer_activity", "stg_users"),
            ("customer_activity", "stg_orders"),
            ("customer_churn", "stg_users"),
            ("customer_churn", "stg_orders"),
            ("customer_health", "stg_users"),
            ("customer_health", "stg_orders"),
        ]

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags_run1 = engine.infer_semantic_tags(threshold=0.5)

        target_id = name_to_id["customer_health"]
        tag_run1 = next(
            (t for t in tags_run1 if t.node_id == target_id), None
        )
        assert tag_run1 is not None, "Expected tag for customer_health in run 1"
        assert tag_run1.confidence >= 0.8, (
            f"Expected high confidence in run 1, got {tag_run1.confidence}"
        )

        # Run 2: add an extra edge to customer_health to slightly change its
        # ref set, but it still shares stg_users + stg_orders with others.
        # Jaccard({users, orders, extra}, {users, orders}) = 2/3 ≈ 0.67 > 0.5
        db.insert_edge(
            name_to_id["customer_health"], name_to_id["stg_extra"], "references"
        )

        tags_run2 = engine.infer_semantic_tags(
            threshold=0.5, existing_tags=tags_run1
        )

        tag_run2 = next(
            (t for t in tags_run2 if t.node_id == target_id), None
        )
        assert tag_run2 is not None, (
            "customer_health should remain tagged after minor graph change"
        )
        # Tag name should be preserved
        assert tag_run2.tag_name == tag_run1.tag_name
    finally:
        db.close()


def test_clustering_skip_small_repo():
    """Repos with fewer than 5 models skip clustering and return empty."""
    db = GraphDB()
    try:
        # Only 3 models with refs — below the minimum of 5
        models = [
            ("models/marts/revenue.sql", "revenue"),
            ("models/marts/customers.sql", "customers"),
            ("models/marts/orders.sql", "orders"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        edges = [
            ("revenue", "stg_users"),
            ("customers", "stg_users"),
            ("orders", "stg_users"),
        ]

        repo_id, _name_to_id = _setup_models_with_edges(db, models, edges)
        engine = ConventionEngine(db, repo_id)
        tags = engine.infer_semantic_tags()

        # Only 3 models have refs, which is < 5 → clustering skipped
        assert tags == []
    finally:
        db.close()


def test_tags_computed_after_reindex():
    """Semantic tags are computed and stored after reindex alongside conventions.

    Uses direct graph setup + _run_convention_inference to test the
    integration path, since the SQL parser creates separate phantom
    nodes per file (no shared target node_ids for Jaccard).
    """
    db = GraphDB()
    try:
        indexer = Indexer(db)

        # Set up a repo with 5+ models sharing a common ref (same as unit tests)
        models = [
            ("models/marts/customer_ltv.sql", "customer_ltv"),
            ("models/marts/customer_segments.sql", "customer_segments"),
            ("models/marts/customer_activity.sql", "customer_activity"),
            ("models/marts/customer_churn.sql", "customer_churn"),
            ("models/marts/customer_health.sql", "customer_health"),
            ("models/staging/stg_users.sql", "stg_users"),
        ]
        repo_id, _name_to_id = _setup_models_with_edges(db, models, [
            ("customer_ltv", "stg_users"),
            ("customer_segments", "stg_users"),
            ("customer_activity", "stg_users"),
            ("customer_churn", "stg_users"),
            ("customer_health", "stg_users"),
        ])

        # Call the reindex integration path that runs both conventions + tags
        indexer._run_convention_inference(repo_id, project_path="/tmp/test")

        # Tags should have been computed and stored
        tags = db.get_tags(repo_id)
        assert len(tags) > 0, "Expected semantic tags to be stored after reindex"
        assert all(t["confidence"] > 0.0 for t in tags)
        assert all(t["source"] == "inferred" for t in tags)
    finally:
        db.close()


# ── Tag query methods ──


def test_search_by_tag_ranked():
    """query_search_by_tag returns models in descending confidence order."""
    db = GraphDB()
    try:
        models = [
            ("marts/customer_ltv.sql", "customer_ltv"),
            ("marts/customer_segments.sql", "customer_segments"),
            ("marts/customer_churn.sql", "customer_churn"),
            ("marts/customer_health.sql", "customer_health"),
        ]
        tags = [
            ("customer_ltv", "customer", 0.90),
            ("customer_segments", "customer", 0.85),
            ("customer_churn", "customer", 0.70),
            ("customer_health", "customer", 0.65),
        ]
        _setup_tagged_repo(db, "test", "/tmp/test", models, tags)

        result = db.query_search_by_tag(tag="customer")

        assert result["tag"] == "customer"
        assert result["total"] == 4
        assert len(result["models"]) == 4

        confidences = [m["confidence"] for m in result["models"]]
        expected = [0.90, 0.85, 0.70, 0.65]
        assert len(confidences) == len(expected)
        for actual, exp in zip(confidences, expected, strict=False):
            assert abs(actual - exp) < 0.01, f"Expected ~{exp}, got {actual}"

        for m in result["models"]:
            assert "node_name" in m
            assert "confidence" in m
            assert "source" in m
    finally:
        db.close()


def test_search_by_tag_min_confidence():
    """query_search_by_tag with min_confidence filters out low-confidence models."""
    db = GraphDB()
    try:
        models = [
            ("marts/customer_ltv.sql", "customer_ltv"),
            ("marts/customer_segments.sql", "customer_segments"),
            ("marts/customer_churn.sql", "customer_churn"),
            ("marts/customer_health.sql", "customer_health"),
        ]
        tags = [
            ("customer_ltv", "customer", 0.90),
            ("customer_segments", "customer", 0.85),
            ("customer_churn", "customer", 0.70),
            ("customer_health", "customer", 0.45),
        ]
        _setup_tagged_repo(db, "test", "/tmp/test", models, tags)

        result = db.query_search_by_tag(tag="customer", min_confidence=0.5)

        assert result["total"] == 3
        assert len(result["models"]) == 3
        assert all(m["confidence"] >= 0.5 for m in result["models"])
        assert not any(m["node_name"] == "customer_health" for m in result["models"])
    finally:
        db.close()


def test_search_by_tag_unknown():
    """query_search_by_tag for nonexistent tag returns empty with suggestion."""
    db = GraphDB()
    try:
        models = [("marts/revenue.sql", "revenue")]
        tags = [("revenue", "finance", 0.80)]
        _setup_tagged_repo(db, "test", "/tmp/test", models, tags)

        result = db.query_search_by_tag(tag="nonexistent")

        assert result["total"] == 0
        assert result["models"] == []
        assert "suggestion" in result
        assert isinstance(result["suggestion"], str)
    finally:
        db.close()


def test_list_tags_all():
    """query_list_tags returns all tags with model_count and avg_confidence."""
    db = GraphDB()
    try:
        models = [
            ("marts/customer_ltv.sql", "customer_ltv"),
            ("marts/customer_segments.sql", "customer_segments"),
            ("marts/customer_churn.sql", "customer_churn"),
            ("marts/customer_health.sql", "customer_health"),
            ("marts/order_summary.sql", "order_summary"),
            ("marts/order_daily.sql", "order_daily"),
            ("marts/order_weekly.sql", "order_weekly"),
        ]
        tags = [
            ("customer_ltv", "customer", 0.90),
            ("customer_segments", "customer", 0.85),
            ("customer_churn", "customer", 0.70),
            ("customer_health", "customer", 0.65),
            ("order_summary", "order", 0.88),
            ("order_daily", "order", 0.80),
            ("order_weekly", "order", 0.72),
        ]
        _setup_tagged_repo(db, "test", "/tmp/test", models, tags)

        result = db.query_list_tags()

        assert "tags" in result
        assert len(result["tags"]) == 2

        tag_map = {t["tag_name"]: t for t in result["tags"]}
        assert "customer" in tag_map
        assert "order" in tag_map

        assert tag_map["customer"]["model_count"] == 4
        assert tag_map["order"]["model_count"] == 3

        assert abs(tag_map["customer"]["avg_confidence"] - 0.775) < 0.01
        assert abs(tag_map["order"]["avg_confidence"] - 0.80) < 0.01
    finally:
        db.close()


def test_list_tags_empty():
    """query_list_tags on repo with no tags returns empty with suggestion."""
    db = GraphDB()
    try:
        db.upsert_repo("test", "/tmp/test")

        result = db.query_list_tags()

        assert result["tags"] == []
        assert "suggestion" in result
        assert isinstance(result["suggestion"], str)
    finally:
        db.close()


def test_tags_repo_filter():
    """Tag queries filter by repo when repo parameter is provided."""
    db = GraphDB()
    try:
        # Set up project_a
        models_a = [
            ("marts/customer_ltv.sql", "customer_ltv"),
            ("marts/customer_segments.sql", "customer_segments"),
        ]
        tags_a = [
            ("customer_ltv", "customer", 0.90),
            ("customer_segments", "customer", 0.85),
        ]
        _setup_tagged_repo(db, "project_a", "/tmp/project_a", models_a, tags_a)

        # Set up project_b
        models_b = [
            ("marts/order_summary.sql", "order_summary"),
            ("marts/order_daily.sql", "order_daily"),
        ]
        tags_b = [
            ("order_summary", "order", 0.88),
            ("order_daily", "order", 0.80),
        ]
        _setup_tagged_repo(db, "project_b", "/tmp/project_b", models_b, tags_b)

        # list_tags filtered to project_a
        result_tags = db.query_list_tags(repo="project_a")
        tag_names = {t["tag_name"] for t in result_tags["tags"]}
        assert "customer" in tag_names
        assert "order" not in tag_names

        # search_by_tag filtered to project_a
        result_search = db.query_search_by_tag(tag="customer", repo="project_a")
        assert result_search["total"] == 2
        assert all(m["node_name"].startswith("customer_") for m in result_search["models"])

        # search_by_tag for tag that only exists in project_b, filtered to project_a
        result_empty = db.query_search_by_tag(tag="order", repo="project_a")
        assert result_empty["total"] == 0
    finally:
        db.close()


def test_search_by_tag_repo_not_found():
    """query_search_by_tag returns error when repo name doesn't exist."""
    db = GraphDB()
    try:
        result = db.query_search_by_tag(tag="customer", repo="nonexistent")
        assert "error" in result
        assert "nonexistent" in result["error"]
    finally:
        db.close()


def test_list_tags_repo_not_found():
    """query_list_tags returns error when repo name doesn't exist."""
    db = GraphDB()
    try:
        result = db.query_list_tags(repo="nonexistent")
        assert "error" in result
        assert "nonexistent" in result["error"]
    finally:
        db.close()
