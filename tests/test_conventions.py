"""Tests for the convention inference engine."""

import json
from pathlib import Path

from sqlprism.core.conventions import ConventionEngine, Layer
from sqlprism.core.graph import GraphDB
from sqlprism.core.indexer import Indexer


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


# ── Convention storage (run_inference) ──


def test_conventions_computed_after_reindex():
    """run_inference stores conventions in the conventions table."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)

        # Add edges so reference rules are generated
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
        db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")

        # Add columns so column style and common columns work
        nids = db._execute_read(
            "SELECT node_id, name FROM nodes WHERE name LIKE 'stg_%'",
        ).fetchall()
        for nid, _name in nids:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, 'updated_at', 'TIMESTAMP', 1, 'definition')",
                [nid],
            )

        engine = ConventionEngine(db, repo_id)
        result = engine.run_inference()

        assert result["layers_detected"] == 3  # raw, staging, marts
        assert result["conventions_stored"] >= 4  # at least naming per layer + refs + cols

        # Verify conventions table has entries
        rows = db.conn.execute(
            "SELECT layer, convention_type, confidence, source "
            "FROM conventions WHERE repo_id = ? ORDER BY layer, convention_type",
            [repo_id],
        ).fetchall()
        assert len(rows) >= 4

        # Check naming convention stored for staging
        staging_naming = next(
            (r for r in rows if r[0] == "staging" and r[1] == "naming"),
            None,
        )
        assert staging_naming is not None
        assert staging_naming[3] == "inferred"  # source
        assert staging_naming[2] > 0  # confidence > 0
    finally:
        db.close()


def test_conventions_upsert_on_rerun():
    """Running inference twice upserts (not duplicates) conventions."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)

        # First run
        engine.run_inference()
        count1 = db.conn.execute(
            "SELECT COUNT(*) FROM conventions WHERE repo_id = ?",
            [repo_id],
        ).fetchone()[0]

        # Second run — should upsert, not duplicate
        engine.run_inference()
        count2 = db.conn.execute(
            "SELECT COUNT(*) FROM conventions WHERE repo_id = ?",
            [repo_id],
        ).fetchone()[0]

        assert count2 == count1
    finally:
        db.close()


def test_conventions_payload_is_valid_json():
    """Convention payloads are stored as valid JSON."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        rows = db.conn.execute(
            "SELECT convention_type, payload FROM conventions WHERE repo_id = ?",
            [repo_id],
        ).fetchall()

        expected_keys = {
            "naming": "pattern",
            "references": "allowed_targets",
            "required_columns": "columns",
            "column_style": "style",
        }
        for conv_type, payload in rows:
            parsed = json.loads(payload)
            assert isinstance(parsed, dict)
            assert len(parsed) > 0
            # Verify required key per convention type
            if conv_type in expected_keys:
                assert expected_keys[conv_type] in parsed
    finally:
        db.close()


def test_run_inference_empty_repo():
    """run_inference on repo with no models returns zero counts."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("empty", "/tmp/empty")

        engine = ConventionEngine(db, repo_id)
        result = engine.run_inference()

        assert result["layers_detected"] == 0
        assert result["conventions_stored"] == 0
    finally:
        db.close()


# ── get_conventions (query_conventions) ──


def _setup_repo_with_conventions(db: GraphDB) -> int:
    """Set up a repo, run inference, return repo_id."""
    repo_id, nodes = _setup_layered_repo_with_edges(db)
    db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
    db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")

    # Add columns for column style detection
    nids = db._execute_read(
        "SELECT node_id, name FROM nodes WHERE name LIKE 'stg_%'",
    ).fetchall()
    for nid, _name in nids:
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, 'updated_at', 'TIMESTAMP', 1, 'definition')",
            [nid],
        )

    engine = ConventionEngine(db, repo_id)
    engine.run_inference()
    return repo_id


def test_get_conventions_single_layer():
    """Get conventions for a specific layer."""
    db = GraphDB()
    try:
        repo_id = _setup_repo_with_conventions(db)
        repo_name = db._execute_read(
            "SELECT name FROM repos WHERE repo_id = ?", [repo_id]
        ).fetchone()[0]

        result = db.query_conventions(layer="staging", repo=repo_name)

        assert "error" not in result
        assert result["layer"] == "staging"
        assert "naming" in result
        assert result["naming"]["confidence"] > 0
        assert result["naming"]["source"] == "inferred"
    finally:
        db.close()


def test_get_conventions_all_layers():
    """Get conventions for all layers."""
    db = GraphDB()
    try:
        repo_id = _setup_repo_with_conventions(db)
        repo_name = db._execute_read(
            "SELECT name FROM repos WHERE repo_id = ?", [repo_id]
        ).fetchone()[0]

        result = db.query_conventions(repo=repo_name)

        assert "layers" in result
        layer_names = {ly["layer"] for ly in result["layers"]}
        assert "staging" in layer_names
        assert "raw" in layer_names
    finally:
        db.close()


def test_get_conventions_unknown_layer():
    """Layer not found returns error with available layers."""
    db = GraphDB()
    try:
        repo_id = _setup_repo_with_conventions(db)
        repo_name = db._execute_read(
            "SELECT name FROM repos WHERE repo_id = ?", [repo_id]
        ).fetchone()[0]

        result = db.query_conventions(layer="nonexistent", repo=repo_name)

        assert "error" in result
        assert "available_layers" in result
        assert "staging" in result["available_layers"]
    finally:
        db.close()


def test_get_conventions_empty():
    """No conventions returns helpful error message."""
    db = GraphDB()
    try:
        db.upsert_repo("empty", "/tmp/empty")

        result = db.query_conventions(repo="empty")

        assert "error" in result
        assert "reindex" in result["error"].lower() or "refresh" in result["error"].lower()
    finally:
        db.close()


def test_get_conventions_small_project():
    """Small project includes advisory note."""
    db = GraphDB()
    try:
        repo_id = _setup_repo_with_conventions(db)
        repo_name = db._execute_read(
            "SELECT name FROM repos WHERE repo_id = ?", [repo_id]
        ).fetchone()[0]

        # All layers in the fixture have < 10 models (threshold is 10)
        result = db.query_conventions(layer="staging", repo=repo_name)

        assert result.get("model_count", 0) < 10
        assert "note" in result
        assert "small project" in result["note"].lower()
    finally:
        db.close()


def test_get_conventions_unknown_layer_no_repo():
    """Unknown layer without repo filter returns available layers."""
    db = GraphDB()
    try:
        _setup_repo_with_conventions(db)

        result = db.query_conventions(layer="nonexistent")

        assert "error" in result
        assert "available_layers" in result
        assert len(result["available_layers"]) > 0
    finally:
        db.close()


def test_get_conventions_exceptions():
    """Response includes exception details from naming patterns."""
    db = GraphDB()
    try:
        repo_id = _setup_repo_with_conventions(db)
        repo_name = db._execute_read(
            "SELECT name FROM repos WHERE repo_id = ?", [repo_id]
        ).fetchone()[0]

        result = db.query_conventions(layer="staging", repo=repo_name)

        # Naming section should have pattern and exceptions
        assert "naming" in result
        assert "pattern" in result["naming"]
        # exceptions may be empty but key should exist
        assert "exceptions" in result["naming"]
    finally:
        db.close()


# ── YAML override loading ──


def test_load_conventions_yaml(tmp_path):
    """Load conventions overrides from YAML file."""
    yaml_content = """
conventions:
  staging:
    naming: "stg_{source}_{entity}"
    allowed_refs:
      - "raw.*"
    required_columns:
      - _loaded_at
    column_style: snake_case
"""
    (tmp_path / "sqlprism.conventions.yml").write_text(yaml_content)

    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", str(tmp_path))
        engine = ConventionEngine(db, repo_id)

        overrides = engine.load_overrides(tmp_path)
        assert overrides is not None
        assert "conventions" in overrides
        assert "staging" in overrides["conventions"]
        staging = overrides["conventions"]["staging"]
        assert staging["naming"] == "stg_{source}_{entity}"
        assert staging["allowed_refs"] == ["raw.*"]
        assert staging["required_columns"] == ["_loaded_at"]
        assert staging["column_style"] == "snake_case"
    finally:
        db.close()


def test_override_replaces_inferred():
    """Override replaces inferred value entirely with confidence 1.0."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        # Now apply override
        overrides = {
            "conventions": {
                "staging": {
                    "naming": "stg_{source}_{entity}",
                }
            }
        }
        engine.apply_overrides(overrides)

        # Check the convention was overridden
        row = db.conn.execute(
            "SELECT confidence, source, payload FROM conventions "
            "WHERE repo_id = ? AND layer = 'staging' AND convention_type = 'naming'",
            [repo_id],
        ).fetchone()
        assert row is not None
        assert row[0] == 1.0  # confidence
        assert row[1] == "override"  # source
        parsed = json.loads(row[2])
        assert parsed["pattern"] == "stg_{source}_{entity}"
    finally:
        db.close()


def test_override_preserves_other_layers():
    """Layers not in overrides keep inferred values."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        # Override only staging
        overrides = {
            "conventions": {
                "staging": {"naming": "stg_{source}_{entity}"}
            }
        }
        engine.apply_overrides(overrides)

        # Raw layer should still be inferred
        raw_row = db.conn.execute(
            "SELECT source FROM conventions "
            "WHERE repo_id = ? AND layer = 'raw' AND convention_type = 'naming'",
            [repo_id],
        ).fetchone()
        assert raw_row is not None
        assert raw_row[0] == "inferred"
    finally:
        db.close()


def test_override_creates_new_layer():
    """Override creates a layer not in inference."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")

        engine = ConventionEngine(db, repo_id)
        overrides = {
            "conventions": {
                "snapshots": {
                    "naming": "snap_{entity}",
                    "column_style": "snake_case",
                }
            }
        }
        stored = engine.apply_overrides(overrides)
        assert stored == 2  # naming + column_style

        rows = db.conn.execute(
            "SELECT convention_type, source FROM conventions "
            "WHERE repo_id = ? AND layer = 'snapshots' ORDER BY convention_type",
            [repo_id],
        ).fetchall()
        assert len(rows) == 2
        assert all(r[1] == "override" for r in rows)
    finally:
        db.close()


def test_config_discovery_paths(tmp_path):
    """Discover config in .sqlprism/ directory."""
    dotdir = tmp_path / ".sqlprism"
    dotdir.mkdir()
    yaml_content = """
conventions:
  staging:
    naming: "stg_{entity}"
"""
    (dotdir / "sqlprism.conventions.yml").write_text(yaml_content)

    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", str(tmp_path))
        engine = ConventionEngine(db, repo_id)

        overrides = engine.load_overrides(tmp_path)
        assert overrides is not None
        assert overrides["conventions"]["staging"]["naming"] == "stg_{entity}"
    finally:
        db.close()


def test_no_override_file_graceful():
    """No override file present — inference only, no errors."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        result = engine.run_inference(project_path="/nonexistent/path")

        assert result["overrides_applied"] == 0
        assert result["conventions_stored"] > 0
    finally:
        db.close()


def test_inference_preserves_overrides():
    """Re-running inference does not clobber override rows."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        # Apply override
        engine.apply_overrides({
            "conventions": {
                "staging": {"naming": "stg_{source}_{entity}"}
            }
        })

        # Re-run inference — should NOT overwrite the override
        engine.run_inference()

        row = db.conn.execute(
            "SELECT source, confidence FROM conventions "
            "WHERE repo_id = ? AND layer = 'staging' AND convention_type = 'naming'",
            [repo_id],
        ).fetchone()
        assert row[0] == "override"
        assert row[1] == 1.0
    finally:
        db.close()


def test_run_inference_with_overrides(tmp_path):
    """run_inference with project_path applies overrides automatically."""
    yaml_content = """
conventions:
  staging:
    naming: "stg_{source}_{entity}"
    column_style: snake_case
"""
    (tmp_path / "sqlprism.conventions.yml").write_text(yaml_content)

    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        result = engine.run_inference(project_path=tmp_path)

        assert result["overrides_applied"] == 2  # naming + column_style

        # Verify override was stored
        row = db.conn.execute(
            "SELECT source FROM conventions "
            "WHERE repo_id = ? AND layer = 'staging' AND convention_type = 'naming'",
            [repo_id],
        ).fetchone()
        assert row[0] == "override"
    finally:
        db.close()


# ── Bootstrap CLI (generate_yaml, get_diff) ──


def _create_test_db(tmp_path) -> Path:
    """Create a temp DuckDB with a layered repo and conventions."""
    db_path = tmp_path / "test.duckdb"
    db = GraphDB(str(db_path))
    try:
        repo_id = db.upsert_repo("test", str(tmp_path))
        models = [
            ("models/raw/raw_orders.sql", "raw_orders"),
            ("models/raw/raw_payments.sql", "raw_payments"),
            ("models/staging/stg_orders.sql", "stg_orders"),
            ("models/staging/stg_payments.sql", "stg_payments"),
            ("models/staging/stg_customers.sql", "stg_customers"),
            ("models/marts/revenue.sql", "revenue"),
            ("models/marts/customers.sql", "customers"),
        ]
        nodes = {}
        for i, (path, name) in enumerate(models):
            fid = db.insert_file(repo_id, path, "sql", f"ck_{i}")
            nid = db.insert_node(fid, "table", name, "sql", 1, 10)
            nodes[name] = nid
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
        db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()
    finally:
        db.close()
    return db_path


def test_cli_conventions_init(tmp_path):
    """conventions --init generates valid YAML with confidence comments."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        yaml_content = engine.generate_yaml()

        assert "conventions:" in yaml_content
        assert "staging:" in yaml_content
        assert "confidence:" in yaml_content
        assert "naming:" in yaml_content
        # Should be valid YAML
        import yaml

        parsed = yaml.safe_load(yaml_content)
        assert parsed is not None
        assert "conventions" in parsed
    finally:
        db.close()


def test_cli_conventions_init_no_overwrite(tmp_path):
    """conventions --init does not overwrite existing file without --force."""
    from click.testing import CliRunner

    from sqlprism.cli import cli

    db_path = _create_test_db(tmp_path)

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("sqlprism.conventions.yml").write_text("# existing\n")
        result = runner.invoke(
            cli, ["conventions", "init", "--db", str(db_path)]
        )
        assert result.exit_code == 1
        assert "already exists" in result.output


def test_cli_conventions_init_with_force(tmp_path):
    """conventions --init --force overwrites existing file."""
    from click.testing import CliRunner

    from sqlprism.cli import cli

    db_path = _create_test_db(tmp_path)

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("sqlprism.conventions.yml").write_text("# old\n")
        result = runner.invoke(
            cli, ["conventions", "init", "--db", str(db_path), "--force"]
        )
        assert result.exit_code == 0
        assert "Wrote" in result.output
        content = Path("sqlprism.conventions.yml").read_text()
        assert "conventions:" in content
        assert "confidence:" in content


def test_cli_conventions_refresh(tmp_path):
    """conventions --refresh updates tables and prints stats."""
    from click.testing import CliRunner

    from sqlprism.cli import cli

    db_path = _create_test_db(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["conventions", "refresh", "--db", str(db_path)]
    )
    assert result.exit_code == 0
    assert "layers" in result.output
    assert "conventions stored" in result.output
    assert "Done." in result.output


def test_cli_conventions_diff(tmp_path):
    """conventions --diff reports no changes after fresh init."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        # Write YAML and immediately diff — should be "No changes"
        yaml_content = engine.generate_yaml()
        yaml_path = tmp_path / "sqlprism.conventions.yml"
        yaml_path.write_text(yaml_content)

        diff = engine.get_diff(yaml_path)
        assert diff == "No changes detected."
    finally:
        db.close()


def test_cli_conventions_diff_detects_changes(tmp_path):
    """conventions --diff detects when naming pattern changes."""
    db = GraphDB()
    try:
        repo_id, nodes = _setup_layered_repo_with_edges(db)
        db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")

        engine = ConventionEngine(db, repo_id)
        engine.run_inference()

        # Write YAML with a different naming pattern
        yaml_path = tmp_path / "sqlprism.conventions.yml"
        yaml_path.write_text(
            'conventions:\n  staging:\n    naming: "old_pattern"\n'
        )

        diff = engine.get_diff(yaml_path)
        assert "staging.naming" in diff
        assert "old_pattern" in diff
    finally:
        db.close()


def test_cli_conventions_init_empty(tmp_path):
    """conventions --init on empty database shows helpful message."""
    from click.testing import CliRunner

    from sqlprism.cli import cli

    # Create DB with a repo but no models
    db_path = tmp_path / "empty.duckdb"
    db = GraphDB(str(db_path))
    db.upsert_repo("empty", str(tmp_path))
    db.close()

    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(
            cli, ["conventions", "init", "--db", str(db_path)]
        )
        assert result.exit_code == 0
        content = Path("sqlprism.conventions.yml").read_text()
        assert "No conventions found" in content


# ── Semantic tag inference ──


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

        repo_id, name_to_id = _setup_models_with_edges(db, models, edges)
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
        assert confidences == sorted(confidences, reverse=True)

        for m in result["models"]:
            assert "name" in m
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

        assert "avg_confidence" in tag_map["customer"]
        assert "avg_confidence" in tag_map["order"]
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
        assert all(m["name"].startswith("customer_") for m in result_search["models"])

        # search_by_tag for tag that only exists in project_b, filtered to project_a
        result_empty = db.query_search_by_tag(tag="order", repo="project_a")
        assert result_empty["total"] == 0
    finally:
        db.close()
