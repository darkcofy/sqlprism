"""Tests for find_similar_models and suggest_placement queries."""

import json

from sqlprism.core.graph import GraphDB


def _setup_similar_models_repo(db: GraphDB) -> tuple[int, dict[str, int]]:
    """Set up a repo with models, edges, and columns for similarity tests."""
    repo_id = db.upsert_repo("test", "/tmp/test")
    nodes = {}

    models = [
        ("models/staging/stg_orders.sql", "stg_orders"),
        ("models/staging/stg_payments.sql", "stg_payments"),
        ("models/staging/stg_customers.sql", "stg_customers"),
        ("models/intermediate/int_order_payments.sql", "int_order_payments"),
        ("models/intermediate/int_customer_orders.sql", "int_customer_orders"),
        ("models/marts/revenue.sql", "revenue"),
        ("models/marts/customer_ltv.sql", "customer_ltv"),
    ]
    for i, (path, name) in enumerate(models):
        fid = db.insert_file(repo_id, path, "sql", f"ck_{i}")
        nid = db.insert_node(fid, "table", name, "sql", 1, 10)
        nodes[name] = nid

    # Edges: int_order_payments references stg_orders and stg_payments
    db.insert_edge(nodes["int_order_payments"], nodes["stg_orders"], "references")
    db.insert_edge(nodes["int_order_payments"], nodes["stg_payments"], "references")

    # Edges: int_customer_orders references stg_orders and stg_customers
    db.insert_edge(nodes["int_customer_orders"], nodes["stg_orders"], "references")
    db.insert_edge(nodes["int_customer_orders"], nodes["stg_customers"], "references")

    # Edges: revenue references int_order_payments
    db.insert_edge(nodes["revenue"], nodes["int_order_payments"], "references")

    # Edges: customer_ltv references int_customer_orders
    db.insert_edge(nodes["customer_ltv"], nodes["int_customer_orders"], "references")

    # Columns for int_order_payments: order_id, payment_amount, payment_method
    for col in ["order_id", "payment_amount", "payment_method"]:
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, ?, 'TEXT', 1, 'definition')",
            [nodes["int_order_payments"], col],
        )

    # Columns for int_customer_orders: customer_id, order_id, order_date
    for col in ["customer_id", "order_id", "order_date"]:
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, ?, 'TEXT', 1, 'definition')",
            [nodes["int_customer_orders"], col],
        )

    # Columns for customer_ltv: customer_id, lifetime_value
    for col in ["customer_id", "lifetime_value"]:
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, ?, 'TEXT', 1, 'definition')",
            [nodes["customer_ltv"], col],
        )

    # Columns for revenue: total_revenue, order_count
    for col in ["total_revenue", "order_count"]:
        db.conn.execute(
            "INSERT INTO columns (node_id, column_name, data_type, position, source) "
            "VALUES (?, ?, 'TEXT', 1, 'definition')",
            [nodes["revenue"], col],
        )

    return repo_id, nodes


def _setup_placement_repo(db: GraphDB) -> tuple[int, dict[str, int]]:
    """Set up a repo with layers, edges, and conventions for placement tests.

    Creates: raw (2), staging (3), intermediate (2), marts (2).
    Inserts naming and references conventions for each layer.
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
        ("models/intermediate/int_order_payments.sql", "int_order_payments"),
        ("models/intermediate/int_customer_orders.sql", "int_customer_orders"),
        ("models/marts/revenue.sql", "revenue"),
        ("models/marts/customer_ltv.sql", "customer_ltv"),
    ]
    for i, (path, name) in enumerate(models):
        fid = db.insert_file(repo_id, path, "sql", f"ck_{i}")
        nid = db.insert_node(fid, "table", name, "sql", 1, 10)
        nodes[name] = nid

    # Edges: staging -> raw
    db.insert_edge(nodes["stg_orders"], nodes["raw_orders"], "references")
    db.insert_edge(nodes["stg_payments"], nodes["raw_payments"], "references")
    db.insert_edge(nodes["stg_customers"], nodes["raw_orders"], "references")

    # Edges: intermediate -> staging
    db.insert_edge(nodes["int_order_payments"], nodes["stg_orders"], "references")
    db.insert_edge(nodes["int_order_payments"], nodes["stg_payments"], "references")
    db.insert_edge(nodes["int_customer_orders"], nodes["stg_orders"], "references")
    db.insert_edge(nodes["int_customer_orders"], nodes["stg_customers"], "references")

    # Edges: marts -> intermediate
    db.insert_edge(nodes["revenue"], nodes["int_order_payments"], "references")
    db.insert_edge(nodes["customer_ltv"], nodes["int_customer_orders"], "references")

    # Insert conventions
    def _insert_conv(layer, conv_type, payload, confidence, model_count):
        db._execute_write(
            "INSERT INTO conventions "
            "(repo_id, layer, convention_type, payload, confidence, source, model_count) "
            "VALUES (?, ?, ?, ?, ?, 'inferred', ?)",
            [repo_id, layer, conv_type, json.dumps(payload), confidence, model_count],
        )

    # Naming conventions
    _insert_conv("staging", "naming", {"pattern": "stg_{source}_{entity}"}, 0.9, 3)
    _insert_conv("intermediate", "naming", {"pattern": "int_{domain}_{description}"}, 0.85, 2)
    _insert_conv("marts", "naming", {"pattern": "{entity}"}, 0.8, 2)

    # Reference rules
    _insert_conv("staging", "references", {
        "allowed_targets": ["raw"],
        "target_distribution": {"raw": 1.0},
    }, 1.0, 3)
    _insert_conv("intermediate", "references", {
        "allowed_targets": ["staging"],
        "target_distribution": {"staging": 1.0},
    }, 0.9, 2)
    _insert_conv("marts", "references", {
        "allowed_targets": ["intermediate"],
        "target_distribution": {"intermediate": 1.0},
    }, 0.85, 2)

    return repo_id, nodes


# ── Find similar models ──


def test_find_similar_by_refs():
    """Find models similar by shared references."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        result = db.query_find_similar_models(
            references=["stg_orders", "stg_payments"],
            output_columns=["order_id", "payment_amount"],
        )

        assert "similar" in result
        names = [m["name"] for m in result["similar"]]
        assert "int_order_payments" in names

        iop = next(m for m in result["similar"] if m["name"] == "int_order_payments")
        assert iop["similarity"] == 0.8
        assert "stg_orders" in iop["shared_refs"]
        assert "stg_payments" in iop["shared_refs"]
    finally:
        db.close()


def test_find_similar_by_columns():
    """Find models similar by shared output columns."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        result = db.query_find_similar_models(
            output_columns=["customer_id", "total_revenue"]
        )

        assert "similar" in result
        names = [m["name"] for m in result["similar"]]
        # customer_ltv has customer_id
        assert "customer_ltv" in names
        # int_customer_orders has customer_id
        assert "int_customer_orders" in names

        # Check shared_columns for customer_ltv
        cltv = next(m for m in result["similar"] if m["name"] == "customer_ltv")
        assert "customer_id" in cltv["shared_columns"]
        assert cltv["similarity"] == 0.1
    finally:
        db.close()


def test_find_similar_to_model():
    """Find models similar to a named model using its refs and columns."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        result = db.query_find_similar_models(model="int_order_payments")

        assert "similar" in result
        assert len(result["similar"]) > 0
        names = [m["name"] for m in result["similar"]]
        # int_customer_orders shares ref stg_orders and column order_id
        assert "int_customer_orders" in names
        assert "int_order_payments" not in names
    finally:
        db.close()


def test_find_similar_layer_bonus():
    """Models in the same layer get a 0.1 layer bonus in similarity score."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        result = db.query_find_similar_models(model="int_order_payments")

        assert "similar" in result
        ico = next(
            (m for m in result["similar"] if m["name"] == "int_customer_orders"),
            None,
        )
        assert ico is not None, "int_customer_orders should appear in results"

        # int_customer_orders shares 1/3 refs with int_order_payments (stg_orders)
        # jaccard(refs) = 1/3, ref_sim = 0.6 * 1/3 = 0.2
        # shares "order_id" column: 1/5 union → col_sim = 0.3 * 1/5 = 0.06
        # same layer (intermediate) → layer_bonus = 0.1
        # total ~0.36
        # Without bonus it would be ~0.26
        assert ico["similarity"] > 0.30, (
            f"Expected similarity > 0.30 (with layer bonus), got {ico['similarity']}"
        )
    finally:
        db.close()


def test_find_similar_none_found():
    """No similar models returns empty list with suggestion."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        result = db.query_find_similar_models(references=["brand_new_source"])

        assert result["similar"] == []
        assert result["count"] == 0
        assert "suggestion" in result
    finally:
        db.close()


def test_find_similar_suggestion():
    """Models with similarity >= 0.8 get a suggestion to extend instead."""
    db = GraphDB()
    try:
        repo_id = db.upsert_repo("test", "/tmp/test")

        # Create two intermediate models with identical refs and overlapping columns
        # so similarity = 0.6*1.0 + 0.3*(2/3) + 0.1 = 0.9
        stg_a_fid = db.insert_file(repo_id, "models/staging/stg_a.sql", "sql", "ck_a")
        stg_a_nid = db.insert_node(stg_a_fid, "table", "stg_a", "sql", 1, 10)

        stg_b_fid = db.insert_file(repo_id, "models/staging/stg_b.sql", "sql", "ck_b")
        stg_b_nid = db.insert_node(stg_b_fid, "table", "stg_b", "sql", 1, 10)

        ma_fid = db.insert_file(
            repo_id, "models/intermediate/model_a.sql", "sql", "ck_ma"
        )
        ma_nid = db.insert_node(ma_fid, "table", "model_a", "sql", 1, 10)

        mb_fid = db.insert_file(
            repo_id, "models/intermediate/model_b.sql", "sql", "ck_mb"
        )
        mb_nid = db.insert_node(mb_fid, "table", "model_b", "sql", 1, 10)

        # Both models reference stg_a and stg_b
        db.insert_edge(ma_nid, stg_a_nid, "references")
        db.insert_edge(ma_nid, stg_b_nid, "references")
        db.insert_edge(mb_nid, stg_a_nid, "references")
        db.insert_edge(mb_nid, stg_b_nid, "references")

        # model_a columns: col_x, col_y, col_z
        for col in ["col_x", "col_y", "col_z"]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, ?, 'TEXT', 1, 'definition')",
                [ma_nid, col],
            )

        # model_b columns: col_x, col_y, col_w (shares 2/3 with model_a → union=4)
        for col in ["col_x", "col_y", "col_w"]:
            db.conn.execute(
                "INSERT INTO columns (node_id, column_name, data_type, position, source) "
                "VALUES (?, ?, 'TEXT', 1, 'definition')",
                [mb_nid, col],
            )

        result = db.query_find_similar_models(model="model_a")

        assert "similar" in result
        mb = next(
            (m for m in result["similar"] if m["name"] == "model_b"), None
        )
        assert mb is not None, "model_b should appear in results"
        # similarity = 0.6*1.0 + 0.3*(2/4) + 0.1 = 0.6 + 0.15 + 0.1 = 0.85
        assert mb["similarity"] == 0.85
        assert "suggestion" in mb
        assert "Consider extending" in mb["suggestion"]
    finally:
        db.close()


def test_find_similar_limit():
    """Limit parameter restricts the number of returned results."""
    db = GraphDB()
    try:
        _repo_id, _nodes = _setup_similar_models_repo(db)

        # First verify more results exist without tight limit
        unlimited = db.query_find_similar_models(
            references=["stg_orders"], limit=50
        )
        assert len(unlimited["similar"]) > 1, (
            "Need > 1 matches to test limit"
        )

        # Now verify limit truncates
        result = db.query_find_similar_models(
            references=["stg_orders"], limit=1
        )
        assert len(result["similar"]) == 1
        assert result["total_matches"] > result["count"]
    finally:
        db.close()


def test_find_similar_empty_lists():
    """Empty list inputs produce empty results (distinct from None)."""
    db = GraphDB()
    try:
        _setup_similar_models_repo(db)

        # Empty lists are not None — they bypass model-lookup fallback
        result = db.query_find_similar_models(
            references=[], output_columns=[]
        )

        # Both target sets empty → early return with suggestion
        assert result["similar"] == []
        assert result["count"] == 0
    finally:
        db.close()


def test_find_similar_no_inputs():
    """Error when no inputs provided."""
    db = GraphDB()
    try:
        _setup_similar_models_repo(db)
        result = db.query_find_similar_models()
        assert "error" in result
        assert "at least one" in result["error"].lower()
    finally:
        db.close()


def test_find_similar_model_not_found():
    """Error when model name doesn't exist."""
    db = GraphDB()
    try:
        _setup_similar_models_repo(db)
        result = db.query_find_similar_models(model="nonexistent_model")
        assert "error" in result
        assert "not found" in result["error"]
    finally:
        db.close()


def test_find_similar_repo_not_found():
    """Error when repo name doesn't exist."""
    db = GraphDB()
    try:
        _setup_similar_models_repo(db)
        result = db.query_find_similar_models(
            references=["stg_orders"], repo="nonexistent"
        )
        assert "error" in result
        assert "not found" in result["error"]
    finally:
        db.close()


# ── Suggest placement ──


def test_suggest_placement_intermediate():
    """Suggest intermediate layer for staging references."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["stg_orders", "stg_payments"]
        )

        assert "error" not in result
        assert result["recommended_layer"] == "intermediate"
        assert "intermediate" in result["recommended_path"]
        assert result["naming_pattern"] == "int_{domain}_{description}"
        assert "staging" in result["reason"]
        assert "confidence" in result["reason"]
    finally:
        db.close()


def test_suggest_placement_marts():
    """Suggest marts layer for intermediate references."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["int_order_payments", "int_customer_orders"]
        )

        assert "error" not in result
        assert result["recommended_layer"] == "marts"
        assert "marts" in result["recommended_path"]
    finally:
        db.close()


def test_suggest_placement_name_validation():
    """Validate proposed name against layer naming convention."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["stg_orders", "stg_payments"],
            name="orders_joined",
        )

        assert "error" not in result
        assert result["recommended_layer"] == "intermediate"
        feedback = result["name_feedback"]
        assert feedback["matches_convention"] is False
        assert feedback["suggested_name"] == "int_orders_joined"
    finally:
        db.close()


def test_suggest_placement_name_matches():
    """Name already matches the layer naming convention."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["stg_orders", "stg_payments"],
            name="int_orders_joined",
        )

        assert "error" not in result
        assert result["recommended_layer"] == "intermediate"
        feedback = result["name_feedback"]
        assert feedback["matches_convention"] is True
    finally:
        db.close()


def test_suggest_placement_ambiguous():
    """Mixed-layer references produce an ambiguous recommendation."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        # References from both staging and intermediate — no single rule matches all
        result = db.query_suggest_placement(
            references=["stg_orders", "int_customer_orders"]
        )

        assert "error" not in result
        assert result.get("ambiguous") is True
        assert result["recommended_layer"] in ("intermediate", "marts")
        assert "Mixed" in result["reason"] or "most likely" in result["reason"]
        assert "coverage" in result
        assert 0.0 < result["coverage"] <= 1.0
    finally:
        db.close()


def test_suggest_placement_similar():
    """Similar models included in response."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["stg_orders", "stg_payments"]
        )

        assert "error" not in result
        assert "similar_models" in result
        assert isinstance(result["similar_models"], list)
        # int_order_payments references the same stg_orders + stg_payments
        assert "int_order_payments" in result["similar_models"]
        # Unrelated models should not appear
        assert "revenue" not in result["similar_models"]
    finally:
        db.close()


def test_suggest_placement_no_conventions():
    """Helpful message when conventions have not been inferred yet."""
    db = GraphDB()
    try:
        # Set up models but no conventions
        repo_id = db.upsert_repo("test", "/tmp/test")
        fid = db.insert_file(repo_id, "models/staging/stg_orders.sql", "sql", "ck_0")
        db.insert_node(fid, "table", "stg_orders", "sql", 1, 10)

        result = db.query_suggest_placement(references=["stg_orders"])

        assert "error" in result
        assert "conventions" in result["error"].lower() or "refresh" in result["error"].lower()
    finally:
        db.close()


def test_suggest_placement_unknown_reference():
    """Error when none of the referenced models exist in the index."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(
            references=["nonexistent_model", "also_missing"]
        )

        assert "error" in result
        assert "not found" in result["error"].lower() or "None" in result["error"]
    finally:
        db.close()


def test_suggest_placement_empty_references():
    """Error when references list is empty."""
    db = GraphDB()
    try:
        _setup_placement_repo(db)

        result = db.query_suggest_placement(references=[])

        assert "error" in result
    finally:
        db.close()


def test_suggest_placement_repo_filter():
    """Placement is scoped to the specified repo."""
    db = GraphDB()
    try:
        # Set up first repo with conventions
        _setup_placement_repo(db)

        # Set up second repo with different conventions
        repo2_id = db.upsert_repo("other", "/tmp/other")
        fid = db.insert_file(repo2_id, "models/staging/stg_events.sql", "sql", "ck_r2_0")
        db.insert_node(fid, "table", "stg_events", "sql", 1, 10)

        # Query scoped to "test" repo — stg_events is in "other" repo
        result = db.query_suggest_placement(
            references=["stg_orders"], repo="test"
        )
        assert "error" not in result
        assert result["recommended_layer"] == "intermediate"

        # Query scoped to nonexistent repo
        result2 = db.query_suggest_placement(
            references=["stg_orders"], repo="nonexistent"
        )
        assert "error" in result2
        assert "not found" in result2["error"]
    finally:
        db.close()
