"""Tests for convention YAML overrides and bootstrap CLI."""

import json
from pathlib import Path

from sqlprism.core.conventions import ConventionEngine
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
