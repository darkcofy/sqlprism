"""Tests for convention inference: reference rules, columns, storage, get_conventions."""

import json

from sqlprism.core.conventions import ConventionEngine, Layer
from sqlprism.core.graph import GraphDB


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


# ── Reference rules ──


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
