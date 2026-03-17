"""Tests for the convention inference engine."""

from sqlprism.core.conventions import ConventionEngine
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
