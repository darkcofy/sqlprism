"""Tests for load_config discovery and format handling."""

import json

import click
import pytest
import yaml

from sqlprism.cli import load_config


def test_load_config_yaml_precedence(tmp_path, monkeypatch):
    """YAML .yml takes precedence over .yaml and .json."""
    (tmp_path / "sqlprism.yml").write_text(yaml.dump({"db_path": "/from/yml"}))
    (tmp_path / "sqlprism.yaml").write_text(yaml.dump({"db_path": "/from/yaml"}))
    (tmp_path / "sqlprism.json").write_text(json.dumps({"db_path": "/from/json"}))

    monkeypatch.chdir(tmp_path)

    result = load_config()
    assert result["db_path"] == "/from/yml"


def test_load_config_yaml_extension_variant(tmp_path, monkeypatch):
    """sqlprism.yaml is discovered when .yml is absent."""
    (tmp_path / "sqlprism.yaml").write_text(yaml.dump({"db_path": "/from/yaml"}))
    (tmp_path / "sqlprism.json").write_text(json.dumps({"db_path": "/from/json"}))

    monkeypatch.chdir(tmp_path)

    result = load_config()
    assert result["db_path"] == "/from/yaml"


def test_load_config_json_fallback(tmp_path, monkeypatch):
    """JSON config is discovered when no YAML variants exist."""
    (tmp_path / "sqlprism.json").write_text(json.dumps({"db_path": "/from/json"}))

    monkeypatch.chdir(tmp_path)

    result = load_config()
    assert result["db_path"] == "/from/json"


def test_load_config_legacy_location(tmp_path, monkeypatch):
    """Legacy ~/.sqlprism/config.json is used when cwd has no config."""
    legacy_path = tmp_path / "legacy" / "config.json"
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(json.dumps({"db_path": "/from/legacy"}))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sqlprism.cli.LEGACY_CONFIG_PATH", legacy_path)

    # Verify cwd is clean
    assert not (tmp_path / "sqlprism.yml").exists()

    result = load_config()
    assert result["db_path"] == "/from/legacy"


def test_load_config_explicit_path(tmp_path):
    """Explicit path bypasses discovery."""
    # YAML explicit path
    yml_file = tmp_path / "custom.yml"
    yml_file.write_text(yaml.dump({"db_path": "/from/yml"}))
    assert load_config(path=str(yml_file))["db_path"] == "/from/yml"

    # JSON explicit path
    json_file = tmp_path / "custom.json"
    json_file.write_text(json.dumps({"db_path": "/from/json"}))
    assert load_config(path=str(json_file))["db_path"] == "/from/json"


def test_load_config_explicit_path_missing():
    """Missing explicit path raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
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
        "sql_dialect": "postgres",
        "repos": {"warehouse": {"path": "/data/warehouse"}},
        "dbt_repos": {
            "my-dbt": {
                "project_path": "/path/to/dbt",
                "target": "dev",
                "dialect": "starrocks",
            }
        },
        "sqlmesh_repos": {
            "my-sm": {
                "project_path": "/path/to/sqlmesh",
                "variables": {"GRACE_PERIOD": 7},
            }
        },
    }

    yaml_file = tmp_path / "config.yml"
    json_file = tmp_path / "config.json"
    yaml_file.write_text(yaml.dump(config))
    json_file.write_text(json.dumps(config))

    from_yaml = load_config(path=str(yaml_file))
    from_json = load_config(path=str(json_file))

    assert from_yaml == from_json


def test_load_config_malformed_yaml(tmp_path):
    """Malformed YAML raises ClickException."""
    bad = tmp_path / "bad.yml"
    bad.write_text("repos: {invalid: [}")
    with pytest.raises(click.ClickException, match="Failed to parse"):
        load_config(path=str(bad))


def test_load_config_malformed_json(tmp_path):
    """Malformed JSON raises ClickException."""
    bad = tmp_path / "bad.json"
    bad.write_text("{invalid json")
    with pytest.raises(click.ClickException, match="Failed to parse"):
        load_config(path=str(bad))


def test_load_config_empty_yaml(tmp_path):
    """Empty YAML file returns empty dict."""
    empty = tmp_path / "empty.yml"
    empty.write_text("")
    assert load_config(path=str(empty)) == {}


def test_load_config_empty_json(tmp_path):
    """Empty JSON file returns empty dict."""
    empty = tmp_path / "empty.json"
    empty.write_text("")
    assert load_config(path=str(empty)) == {}


def test_load_config_non_dict_yaml(tmp_path):
    """YAML file with non-dict content raises ClickException."""
    scalar = tmp_path / "scalar.yml"
    scalar.write_text("just a string")
    with pytest.raises(click.ClickException, match="Config must be a mapping"):
        load_config(path=str(scalar))


def test_load_config_unsupported_extension(tmp_path):
    """Unsupported extension raises ClickException."""
    toml = tmp_path / "config.toml"
    toml.write_text("[repos]")
    with pytest.raises(click.ClickException, match="Unsupported config format"):
        load_config(path=str(toml))
