"""Tests for the convention inference engine."""

from sqlprism.core.conventions import ConventionEngine, Layer
from sqlprism.core.graph import GraphDB


def _setup_repo(db: GraphDB, file_paths: list[tuple[str, str]]) -> int:
    """Helper: create a repo and populate with models at given file paths."""
    repo_id = db.upsert_repo("test", "/tmp/test")
    for i, (path, name) in enumerate(file_paths):
        file_id = db.insert_file(repo_id, path, "sql", f"checksum_{i}")
        db.insert_node(file_id, "table", name, "sql", 1, 10)
    return repo_id


# ── Layer detection ──


def test_detect_layers_standard_dbt():
    """Detect layers from standard dbt directory structure (models/staging/, models/marts/)."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("models/staging/stg_orders.sql", "stg_orders"),
            ("models/staging/stg_payments.sql", "stg_payments"),
            ("models/staging/stg_customers.sql", "stg_customers"),
            ("models/marts/revenue.sql", "revenue"),
            ("models/marts/customers.sql", "customers"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        names = {ly.name for ly in layers}
        assert "staging" in names
        assert "marts" in names

        staging = next(ly for ly in layers if ly.name == "staging")
        assert staging.model_count == 3
        assert "stg_orders" in staging.model_names

        marts = next(ly for ly in layers if ly.name == "marts")
        assert marts.model_count == 2
    finally:
        db.close()


def test_detect_layers_flat_dirs():
    """Detect layers from flat directory structure (staging/, marts/)."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("staging/stg_orders.sql", "stg_orders"),
            ("staging/stg_payments.sql", "stg_payments"),
            ("marts/revenue.sql", "revenue"),
            ("marts/customers.sql", "customers"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        names = {ly.name for ly in layers}
        assert "staging" in names
        assert "marts" in names

        staging = next(ly for ly in layers if ly.name == "staging")
        assert staging.model_count == 2
        marts = next(ly for ly in layers if ly.name == "marts")
        assert marts.model_count == 2
    finally:
        db.close()


def test_detect_layers_nested_domains():
    """Handle nested domain directories like models/finance/staging/."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("models/finance/staging/stg_invoices.sql", "stg_invoices"),
            ("models/finance/staging/stg_payments.sql", "stg_payments"),
            ("models/marketing/staging/stg_campaigns.sql", "stg_campaigns"),
            ("models/marketing/staging/stg_emails.sql", "stg_emails"),
            ("models/finance/marts/revenue.sql", "revenue"),
            ("models/marketing/marts/conversions.sql", "conversions"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        names = {ly.name for ly in layers}
        # Should collapse repeated sub-layers across domains
        assert "staging" in names
        staging = next(ly for ly in layers if ly.name == "staging")
        assert staging.model_count == 4

        assert "marts" in names
        marts = next(ly for ly in layers if ly.name == "marts")
        assert marts.model_count == 2
    finally:
        db.close()


def test_detect_layers_skip_small():
    """Skip layers with fewer than 2 models."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("models/staging/stg_orders.sql", "stg_orders"),
            ("models/staging/stg_payments.sql", "stg_payments"),
            ("models/archive/old_model.sql", "old_model"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        names = {ly.name for ly in layers}
        assert "staging" in names
        assert "archive" not in names
    finally:
        db.close()


def test_layer_confidence_scaling():
    """Confidence scales with model count: >=10->0.9, >=5->0.8, <5->0.6."""
    db = GraphDB()
    try:
        models = [
            (f"models/staging/stg_{i}.sql", f"stg_{i}")
            for i in range(12)
        ] + [
            (f"models/intermediate/int_{i}.sql", f"int_{i}")
            for i in range(6)
        ] + [
            (f"models/marts/mart_{i}.sql", f"mart_{i}")
            for i in range(3)
        ]
        repo_id = _setup_repo(db, models)

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        layer_map = {ly.name: ly for ly in layers}
        assert layer_map["staging"].confidence == 0.9   # 12 models
        assert layer_map["intermediate"].confidence == 0.8  # 6 models
        assert layer_map["marts"].confidence == 0.6     # 3 models
    finally:
        db.close()


def test_layer_confidence_boundaries():
    """Confidence boundary values: exactly 10->0.9, exactly 5->0.8."""
    db = GraphDB()
    try:
        models = [
            (f"models/staging/stg_{i}.sql", f"stg_{i}")
            for i in range(10)
        ] + [
            (f"models/intermediate/int_{i}.sql", f"int_{i}")
            for i in range(5)
        ]
        repo_id = _setup_repo(db, models)

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        layer_map = {ly.name: ly for ly in layers}
        assert layer_map["staging"].confidence == 0.9   # exactly 10
        assert layer_map["intermediate"].confidence == 0.8  # exactly 5
    finally:
        db.close()


def test_detect_layers_single_layer():
    """Single-layer repo (all models in one dir) detects the layer."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("staging/stg_orders.sql", "stg_orders"),
            ("staging/stg_payments.sql", "stg_payments"),
            ("staging/stg_customers.sql", "stg_customers"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        assert len(layers) == 1
        assert layers[0].name == "staging"
        assert layers[0].model_count == 3
    finally:
        db.close()


def test_detect_layers_root_level_files():
    """Models at repo root (no directory) returns empty layers."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("orders.sql", "orders"),
            ("payments.sql", "payments"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        assert layers == []
    finally:
        db.close()


def test_detect_layers_subdirs_not_domain_nested():
    """Subdirs within a layer (staging/by_source/) stay as one layer."""
    db = GraphDB()
    try:
        repo_id = _setup_repo(db, [
            ("models/staging/by_source/stg_orders.sql", "stg_orders"),
            ("models/staging/by_source/stg_payments.sql", "stg_payments"),
            ("models/staging/manual/stg_overrides.sql", "stg_overrides"),
            ("models/marts/revenue.sql", "revenue"),
            ("models/marts/customers.sql", "customers"),
        ])

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()

        names = {ly.name for ly in layers}
        assert "staging" in names
        assert "marts" in names
        # Should NOT create domain-nested keys like staging/by_source
        assert all("/" not in ly.name for ly in layers)

        staging = next(ly for ly in layers if ly.name == "staging")
        assert staging.model_count == 3
    finally:
        db.close()


# ── Naming pattern inference ──


def test_naming_pattern_clear_prefix():
    """Infer naming pattern with clear prefix like stg_."""
    db = GraphDB()
    try:
        # infer_naming_pattern is pure — repo_id unused, but use real setup
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        names = [
            "stg_stripe_payments",
            "stg_shopify_orders",
            "stg_stripe_refunds",
            "stg_postgres_users",
            "stg_github_repos",
            "stg_slack_messages",
        ]
        result = engine.infer_naming_pattern(names)

        assert result.pattern.startswith("stg_")
        # 6 names, all match prefix → 6/6 = 1.0 (above <5 cap)
        assert result.confidence == 1.0
        assert result.matching_count == 6
        assert result.exceptions == []
    finally:
        db.close()


def test_naming_pattern_mixed_styles():
    """Infer naming pattern with mixed styles (no clear prefix)."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        names = [
            "customer_ltv",
            "customer_segments",
            "order_summary",
            "revenue_daily",
            "churn_prediction",
        ]
        result = engine.infer_naming_pattern(names)

        assert result.pattern != ""
        assert 0.0 < result.confidence < 0.9
        assert result.total_count == 5
        assert result.matching_count > 0
    finally:
        db.close()


def test_naming_pattern_exceptions():
    """Report exceptions -- models that don't match the inferred pattern."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        names = [
            "stg_stripe_payments",
            "stg_shopify_orders",
            "stg_stripe_refunds",
            "stg_postgres_users",
            "stg_github_repos",
            "stg_slack_messages",
            "stg_jira_issues",
            "stg_aws_costs",
            "stg_gcp_billing",
            "stg_azure_resources",
            "legacy_users",  # exception
        ]
        result = engine.infer_naming_pattern(names)

        assert result.pattern.startswith("stg_")
        assert "legacy_users" in result.exceptions
        assert result.matching_count == 10
        assert result.total_count == 11
        # confidence ~ 10/11 = 0.91
        assert result.confidence >= 0.85
    finally:
        db.close()


def test_naming_pattern_small_layer():
    """Small layer gets low confidence (capped at 0.6)."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        names = ["stg_orders", "stg_payments"]
        result = engine.infer_naming_pattern(names)

        assert result.confidence == 0.6
    finally:
        db.close()


def test_naming_pattern_confidence_cap_boundary():
    """Confidence cap applies at <5, not at 5 — 5 names should be uncapped."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        # Exactly 5 names, all matching prefix → uncapped at 1.0
        names = ["stg_a", "stg_b", "stg_c", "stg_d", "stg_e"]
        result = engine.infer_naming_pattern(names)
        assert result.confidence == 1.0  # 5 names, not capped

        # 4 names → capped at 0.6
        names_small = ["stg_a", "stg_b", "stg_c", "stg_d"]
        result_small = engine.infer_naming_pattern(names_small)
        assert result_small.confidence == 0.6  # 4 < 5, capped
    finally:
        db.close()


def test_naming_pattern_empty_input():
    """Empty model list returns zero-confidence empty pattern."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")
        engine = ConventionEngine(db, repo_id)
        result = engine.infer_naming_pattern([])

        assert result.pattern == ""
        assert result.confidence == 0.0
        assert result.matching_count == 0
        assert result.total_count == 0
    finally:
        db.close()


# ── Reference rules ──


def _setup_layered_repo_with_edges(db: GraphDB) -> tuple[int, dict[str, int]]:
    """Set up a repo with staging/marts layers and edges between them.

    Returns (repo_id, {model_name: node_id}).
    """
    repo_id = db.upsert_repo("test", "/tmp/test")
    nodes = {}
    models = [
        ("models/raw/raw_orders.sql", "raw_orders"),
        ("models/raw/raw_payments.sql", "raw_payments"),
        ("models/staging/stg_orders.sql", "stg_orders"),
        ("models/staging/stg_payments.sql", "stg_payments"),
        ("models/staging/stg_customers.sql", "stg_customers"),
        ("models/marts/revenue.sql", "revenue"),
        ("models/marts/customers.sql", "customers"),
    ]
    for i, (path, name) in enumerate(models):
        fid = db.insert_file(repo_id, path, "sql", f"ck_{i}")
        nid = db.insert_node(fid, "table", name, "sql", 1, 10)
        nodes[name] = nid
    return repo_id, nodes


def test_reference_rules_clean_flow():
    """Detect clean layer-to-layer reference rules."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)

        # staging references raw only
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
        db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")
        db.insert_edge(nodes["stg_customers"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()
        rules = engine.infer_reference_rules(layers)

        staging_rule = next(
            (r for r in rules if r.source_layer == "staging"), None
        )
        assert staging_rule is not None
        assert staging_rule.allowed_targets == ["raw"]
        assert staging_rule.confidence == 1.0
    finally:
        db.close()


def test_reference_rules_mixed_targets():
    """Detect mixed reference patterns with confidence < 1.0."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)

        # marts references staging (2x) and raw (1x) — mixed
        db.insert_edge(nodes["revenue"], nodes["stg_orders"], "references")
        db.insert_edge(nodes["customers"], nodes["stg_customers"], "references")
        db.insert_edge(nodes["revenue"], nodes["raw_payments"], "references")

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()
        rules = engine.infer_reference_rules(layers)

        marts_rule = next(
            (r for r in rules if r.source_layer == "marts"), None
        )
        assert marts_rule is not None
        assert "staging" in marts_rule.allowed_targets
        assert "raw" in marts_rule.allowed_targets
        assert marts_rule.target_distribution["staging"] > marts_rule.target_distribution["raw"]
        assert marts_rule.confidence < 1.0
    finally:
        db.close()


def test_reference_rules_cross_layer_violation():
    """Cross-layer reference shows up in distribution with lower confidence."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)

        # staging references raw (2x) + one staging→marts violation
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
        db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")
        db.insert_edge(nodes["stg_customers"], nodes["revenue"], "references")

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()
        rules = engine.infer_reference_rules(layers)

        staging_rule = next(
            (r for r in rules if r.source_layer == "staging"), None
        )
        assert staging_rule is not None
        # 2/3 to raw, 1/3 to marts → confidence = round(2/3, 2) = 0.67
        assert 0.60 <= staging_rule.confidence <= 0.70
        assert "raw" in staging_rule.allowed_targets
        assert "marts" in staging_rule.allowed_targets
        assert staging_rule.target_distribution["raw"] > staging_rule.target_distribution["marts"]
    finally:
        db.close()


# ── Common columns ──


def _setup_repo_with_columns(db: GraphDB) -> tuple[int, Layer]:
    """Set up a repo with staging layer and column definitions."""
    repo_id = db.upsert_repo("test", "/tmp/test")
    model_names = []
    node_ids = []
    file_ids = []
    for i, name in enumerate(["stg_orders", "stg_payments", "stg_customers", "stg_users", "stg_events"]):
        fid = db.insert_file(repo_id, f"models/staging/{name}.sql", "sql", f"ck_{i}")
        nid = db.insert_node(fid, "table", name, "sql", 1, 10)
        model_names.append(name)
        node_ids.append(nid)
        file_ids.append(fid)

    layer = Layer(
        name="staging",
        path_pattern="models/staging/**",
        model_count=5,
        model_names=model_names,
        confidence=0.8,
    )
    return repo_id, layer


def test_common_columns_above_threshold():
    """Detect common columns appearing in >70% of models."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)

        # Get node IDs for inserting columns
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        # updated_at in 5/5 models (100%), created_at in 4/5 (80%)
        for name in layer.model_names:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, 'updated_at', 'TIMESTAMP', 1, 'definition')",
                [nid_map[name]],
            )
        for name in layer.model_names[:4]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, 'created_at', 'TIMESTAMP', 2, 'definition')",
                [nid_map[name]],
            )
        # rare_col in 2/5 (40%) — below threshold
        for name in layer.model_names[:2]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, 'rare_col', 'TEXT', 3, 'definition')",
                [nid_map[name]],
            )

        engine = ConventionEngine(db, repo_id)
        result = engine.infer_common_columns(layer)

        col_map = {r.column_name: r for r in result}
        assert "updated_at" in col_map
        assert col_map["updated_at"].frequency == 1.0
        assert col_map["updated_at"].source == "definition"
        assert "created_at" in col_map
        assert col_map["created_at"].frequency == 0.8
        assert col_map["created_at"].source == "definition"
        assert "rare_col" not in col_map
    finally:
        db.close()


def test_common_columns_merge_sources():
    """Merge column sources from definitions and usage."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        fids = db._execute_read(
            "SELECT file_id, path FROM files WHERE repo_id = ?",
            [repo_id],
        ).fetchall()
        fid_map = {p.split("/")[-1].replace(".sql", ""): fid for fid, p in fids}

        # _loaded_at in definitions for 4/5 models
        for name in layer.model_names[:4]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, '_loaded_at', 'TIMESTAMP', 1, 'definition')",
                [nid_map[name]],
            )
        # _loaded_at in usage for 4/5 models (overlapping)
        for name in layer.model_names[:4]:
            db.insert_column_usage(
                nid_map[name], name, "_loaded_at", "select", fid_map[name]
            )

        engine = ConventionEngine(db, repo_id)
        result = engine.infer_common_columns(layer)

        col = next((r for r in result if r.column_name == "_loaded_at"), None)
        assert col is not None
        assert col.source == "both"
        assert col.frequency == 0.8
        # stg_events is the 5th model, not in the first 4 → should be missing
        assert "stg_events" in col.missing_in
    finally:
        db.close()


def test_common_columns_below_threshold():
    """Columns below 70% threshold are excluded."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        # Only 2/5 models have this column (40%)
        for name in layer.model_names[:2]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, 'niche_col', 'TEXT', 1, 'definition')",
                [nid_map[name]],
            )

        engine = ConventionEngine(db, repo_id)
        result = engine.infer_common_columns(layer)

        assert all(r.column_name != "niche_col" for r in result)
    finally:
        db.close()


# ── Column style ──


def test_column_style_snake_case():
    """Detect dominant snake_case column style."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        # Distribute snake_case columns across multiple models
        cols_by_model = {
            "stg_orders": ["order_id", "customer_name"],
            "stg_payments": ["payment_amount", "created_at"],
            "stg_customers": ["first_name", "last_name"],
        }
        for model, cols in cols_by_model.items():
            for col in cols:
                db.conn.execute(
                    "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                    "VALUES (?, ?, 'TEXT', 1, 'definition')",
                    [nid_map[model], col],
                )

        engine = ConventionEngine(db, repo_id)
        result = engine.detect_column_style(layer)

        assert result.style == "snake_case"
        assert result.confidence == 1.0
    finally:
        db.close()


def test_column_style_camel_case():
    """Detect camelCase column style."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        cols_by_model = {
            "stg_orders": ["orderId", "customerName"],
            "stg_payments": ["totalAmount", "createdAt"],
        }
        for model, cols in cols_by_model.items():
            for col in cols:
                db.conn.execute(
                    "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                    "VALUES (?, ?, 'TEXT', 1, 'definition')",
                    [nid_map[model], col],
                )

        engine = ConventionEngine(db, repo_id)
        result = engine.detect_column_style(layer)

        assert result.style == "camelCase"
        assert result.confidence == 1.0
    finally:
        db.close()


def test_column_style_mixed():
    """Mixed column styles yield low confidence."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name IN (?,?,?,?,?)",
            layer.model_names,
        ).fetchall()
        nid_map = {name: nid for nid, name in nids}

        # Mix of snake_case and camelCase across models
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, 'order_id', 'TEXT', 1, 'definition')",
            [nid_map["stg_orders"]],
        )
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, 'customer_name', 'TEXT', 2, 'definition')",
            [nid_map["stg_orders"]],
        )
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, 'totalAmount', 'TEXT', 1, 'definition')",
            [nid_map["stg_payments"]],
        )
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, 'createdAt', 'TEXT', 2, 'definition')",
            [nid_map["stg_payments"]],
        )

        engine = ConventionEngine(db, repo_id)
        result = engine.detect_column_style(layer)

        # 4 distinct column names: 2 snake_case + 2 camelCase → confidence = 2/4 = 0.5
        assert result.style in ("snake_case", "camelCase")
        assert result.confidence == 0.5
    finally:
        db.close()


def test_column_style_no_columns():
    """No columns at all returns zero confidence."""
    db = GraphDB()
    try:
        repo_id, layer = _setup_repo_with_columns(db)
        # Don't insert any columns — both columns and column_usage empty

        engine = ConventionEngine(db, repo_id)
        result = engine.detect_column_style(layer)

        assert result.style == "snake_case"
        assert result.confidence == 0.0
    finally:
        db.close()


def test_reference_rules_no_edges():
    """Layers with no edges return empty rules."""
    db = GraphDB()
    try:
        repo_id, _nodes = _setup_layered_repo_with_edges(db)
        # Don't insert any edges

        engine = ConventionEngine(db, repo_id)
        layers = engine.detect_layers()
        rules = engine.infer_reference_rules(layers)

        assert rules == []
    finally:
        db.close()
