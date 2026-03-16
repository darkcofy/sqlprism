"""Tests for load_config discovery and format handling."""

import json

import pytest
import yaml

from sqlprism.cli import load_config


def test_load_config_yaml_precedence(tmp_path, monkeypatch):
    """YAML config takes precedence over JSON when both exist."""
    yaml_cfg = {"db_path": "/from/yaml"}
    json_cfg = {"db_path": "/from/json"}

    (tmp_path / "sqlprism.yml").write_text(yaml.dump(yaml_cfg))
    (tmp_path / "sqlprism.json").write_text(json.dumps(json_cfg))

    monkeypatch.chdir(tmp_path)

    result = load_config()
    assert result["db_path"] == "/from/yaml"


def test_load_config_json_fallback(tmp_path, monkeypatch):
    """JSON config is discovered when no YAML variants exist."""
    json_cfg = {"db_path": "/from/json"}
    (tmp_path / "sqlprism.json").write_text(json.dumps(json_cfg))

    monkeypatch.chdir(tmp_path)

    result = load_config()
    assert result["db_path"] == "/from/json"


def test_load_config_legacy_location(tmp_path, monkeypatch):
    """Legacy ~/.sqlprism/config.json is used when cwd has no config."""
    legacy_path = tmp_path / "legacy" / "config.json"
    legacy_path.parent.mkdir(parents=True)
    legacy_cfg = {"db_path": "/from/legacy"}
    legacy_path.write_text(json.dumps(legacy_cfg))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sqlprism.cli.LEGACY_CONFIG_PATH", legacy_path)

    result = load_config()
    assert result["db_path"] == "/from/legacy"


def test_load_config_explicit_path(tmp_path):
    """Explicit path bypasses discovery; missing path raises FileNotFoundError."""
    custom_dir = tmp_path / "custom"
    custom_dir.mkdir()
    custom_file = custom_dir / "my.yml"
    custom_file.write_text(yaml.dump({"db_path": "/from/custom"}))

    result = load_config(path=str(custom_file))
    assert result["db_path"] == "/from/custom"

    with pytest.raises(FileNotFoundError):
        load_config(path="nonexistent.yml")


def test_load_config_not_found(tmp_path, monkeypatch):
    """FileNotFoundError raised with descriptive message when nothing found."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "sqlprism.cli.LEGACY_CONFIG_PATH", tmp_path / "nope" / "config.json"
    )

    with pytest.raises(FileNotFoundError, match="No config file found"):
        load_config()


def test_yaml_json_schema_parity(tmp_path):
    """Same config structure produces identical dicts from YAML and JSON."""
    config = {
        "db_path": "/shared/graph.duckdb",
        "repos": {"warehouse": {"path": "/data/warehouse"}},
        "sql_dialect": "postgres",
    }

    yaml_file = tmp_path / "config.yml"
    json_file = tmp_path / "config.json"
    yaml_file.write_text(yaml.dump(config))
    json_file.write_text(json.dumps(config))

    from_yaml = load_config(path=str(yaml_file))
    from_json = load_config(path=str(json_file))

    assert from_yaml == from_json
