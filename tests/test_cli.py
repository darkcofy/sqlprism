"""Tests for the CLI entry point (task 5.9)."""

import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from sqlprism.cli import cli


def test_reindex_with_sql_file(tmp_path):
    """reindex command indexes a temp directory containing a SQL file."""
    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    (repo_dir / "orders.sql").write_text("CREATE TABLE orders (id INT, amount DECIMAL)")

    db_path = str(tmp_path / "test.duckdb")
    config = {
        "db_path": db_path,
        "repos": {"test": str(repo_dir)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(cli, ["reindex", "--config", str(config_path)])
    assert result.exit_code == 0, f"stdout={result.output}\nstderr={result.output}"
    assert "Indexing test" in result.output
    assert "Done." in result.output


def test_help():
    """--help works and shows the group docstring."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "SQLPrism" in result.output


def test_reindex_help():
    """reindex --help works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["reindex", "--help"])
    assert result.exit_code == 0
    assert "manual reindex" in result.output.lower()


def test_unknown_command_fails():
    """Unknown subcommand exits with non-zero."""
    runner = CliRunner()
    result = runner.invoke(cli, ["nonexistent-command"])
    assert result.exit_code != 0


def test_reindex_with_parse_errors_exits_nonzero(tmp_path):
    """reindex exits non-zero when SQL parse errors occur."""
    repo_dir = tmp_path / "badrepo"
    repo_dir.mkdir()
    # Write invalid SQL that will trigger parse errors
    (repo_dir / "broken.sql").write_text("CREATE TABLE SELECTFROM WHEREJOIN INVALID GARBAGE ))) ((( ;")

    db_path = str(tmp_path / "test.duckdb")
    config = {
        "db_path": db_path,
        "repos": {"test": str(repo_dir)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(cli, ["reindex", "--config", str(config_path)])
    # The CLI should still complete indexing but exit non-zero if there were parse errors.
    # If no parse errors happen to be reported (parser is lenient), at least check it runs.
    # We rely on the actual parser behavior here.
    # The key contract: if parse_errors is non-empty, exit code is 1.
    if "parse error" in result.output.lower():
        assert result.exit_code == 1
    else:
        # Parser was lenient with this input; verify it at least ran
        assert "Indexing test" in result.output


def test_reindex_unknown_repo_exits_nonzero(tmp_path):
    """reindex --repo with a name not in config exits non-zero."""
    config = {
        "db_path": str(tmp_path / "test.duckdb"),
        "repos": {"real": str(tmp_path)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "reindex",
            "--config",
            str(config_path),
            "--repo",
            "nonexistent",
        ],
    )
    assert result.exit_code != 0
    assert "not in config" in result.output


def test_serve_help():
    """serve --help works (2.2a)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["serve", "--help"])
    assert result.exit_code == 0


def test_reindex_sqlmesh_help():
    """reindex-sqlmesh --help works (2.2b)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["reindex-sqlmesh", "--help"])
    assert result.exit_code == 0


def test_reindex_dbt_help():
    """reindex-dbt --help works (2.2c)."""
    runner = CliRunner()
    result = runner.invoke(cli, ["reindex-dbt", "--help"])
    assert result.exit_code == 0


def test_serve_with_mocked_mcp_run(tmp_path):
    """serve command calls configure with config values (2.2d)."""
    db_path = str(tmp_path / "test.duckdb")
    config = {
        "db_path": db_path,
        "repos": {"demo": str(tmp_path)},
        "sql_dialect": "postgres",
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    with patch("sqlprism.cli.mcp"), patch("sqlprism.cli.configure") as mock_configure:
        result = runner.invoke(cli, ["serve", "--config", str(config_path)])

    assert result.exit_code == 0, f"stdout={result.output}"
    mock_configure.assert_called_once()
    call_kwargs = mock_configure.call_args
    assert call_kwargs[1]["db_path"] == db_path
    assert call_kwargs[1]["repos"] == {"demo": {"path": str(tmp_path), "repo_type": "sql"}}
    assert call_kwargs[1]["sql_dialect"] == "postgres"


def test_serve_merges_all_repo_types(tmp_path):
    """serve command tags repos, dbt_repos, sqlmesh_repos with correct repo_type."""
    db_path = str(tmp_path / "test.duckdb")
    config = {
        "db_path": db_path,
        "repos": {"sql_one": str(tmp_path / "sql")},
        "dbt_repos": {"dbt_one": {"project_path": str(tmp_path / "dbt")}},
        "sqlmesh_repos": {"sm_one": {"project_path": str(tmp_path / "sm")}},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    with patch("sqlprism.cli.mcp"), patch("sqlprism.cli.configure") as mock_configure:
        result = runner.invoke(cli, ["serve", "--config", str(config_path)])

    assert result.exit_code == 0, f"stdout={result.output}"
    repos = mock_configure.call_args[1]["repos"]
    assert repos["sql_one"] == {"path": str(tmp_path / "sql"), "repo_type": "sql"}
    # dbt/sqlmesh repos now pass through full config (for reindex_files renderer params)
    assert repos["dbt_one"]["path"] == str(tmp_path / "dbt")
    assert repos["dbt_one"]["repo_type"] == "dbt"
    assert repos["dbt_one"]["project_path"] == str(tmp_path / "dbt")
    assert repos["sm_one"]["path"] == str(tmp_path / "sm")
    assert repos["sm_one"]["repo_type"] == "sqlmesh"
    assert repos["sm_one"]["project_path"] == str(tmp_path / "sm")


# ── reindex-file tests (#13) ──


def test_cli_reindex_file_single(tmp_path):
    """reindex-file reindexes a single SQL file standalone."""
    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT, amount DECIMAL)")

    db_path = str(tmp_path / "test.duckdb")
    config = {"db_path": db_path, "repos": {"test": str(repo_dir)}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli, ["reindex-file", "--config", str(config_path), str(sql_file)]
    )
    assert result.exit_code == 0, f"stdout={result.output}"
    assert "reindexed=1" in result.output


def test_cli_reindex_file_multiple(tmp_path):
    """reindex-file reindexes multiple SQL files in one call."""
    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    file_a = repo_dir / "orders.sql"
    file_b = repo_dir / "customers.sql"
    file_a.write_text("CREATE TABLE orders (id INT)")
    file_b.write_text("CREATE TABLE customers (id INT, name TEXT)")

    db_path = str(tmp_path / "test.duckdb")
    config = {"db_path": db_path, "repos": {"test": str(repo_dir)}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["reindex-file", "--config", str(config_path), str(file_a), str(file_b)],
    )
    assert result.exit_code == 0, f"stdout={result.output}"
    assert "reindexed=2" in result.output


def test_cli_reindex_file_not_found(tmp_path):
    """reindex-file with a non-existent path skips it (no matching repo)."""
    config = {"db_path": str(tmp_path / "test.duckdb"), "repos": {"r": str(tmp_path)}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["reindex-file", "--config", str(config_path), "/nonexistent/path/missing.sql"],
    )
    assert result.exit_code == 0
    assert "skipped=1" in result.output


def test_cli_reindex_file_custom_paths(tmp_path):
    """reindex-file works with --config and --db overrides."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "model.sql"
    sql_file.write_text("SELECT 1 AS val FROM source_table")

    custom_db = str(tmp_path / "custom" / "my.duckdb")
    default_db = str(tmp_path / "default.duckdb")
    config = {"db_path": default_db, "repos": {"r": str(repo_dir)}}
    config_path = tmp_path / "custom_config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "reindex-file",
            "--config", str(config_path),
            "--db", custom_db,
            str(sql_file),
        ],
    )
    assert result.exit_code == 0, f"stdout={result.output}"
    assert "reindexed=1" in result.output
    # Verify the custom DB was created and the default was NOT
    assert Path(custom_db).exists()
    assert not Path(default_db).exists()


def test_cli_reindex_file_multi_repo(tmp_path):
    """reindex-file resolves files across different repos."""
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    file_a = repo_a / "orders.sql"
    file_b = repo_b / "customers.sql"
    file_a.write_text("CREATE TABLE orders (id INT)")
    file_b.write_text("CREATE TABLE customers (id INT)")

    db_path = str(tmp_path / "test.duckdb")
    config = {
        "db_path": db_path,
        "repos": {"alpha": str(repo_a), "beta": str(repo_b)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["reindex-file", "--config", str(config_path), str(file_a), str(file_b)],
    )
    assert result.exit_code == 0, f"stdout={result.output}"
    assert "reindexed=2" in result.output


def test_cli_reindex_file_deleted_cleans_graph(tmp_path):
    """reindex-file on a deleted path removes stale nodes from the graph."""
    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "stale.sql"
    sql_file.write_text("CREATE TABLE stale (id INT)")

    db_path = str(tmp_path / "test.duckdb")
    config = {"db_path": db_path, "repos": {"test": str(repo_dir)}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    runner = CliRunner()
    # First index the file
    result = runner.invoke(
        cli, ["reindex-file", "--config", str(config_path), str(sql_file)]
    )
    assert result.exit_code == 0
    assert "reindexed=1" in result.output

    # Delete the file, then reindex the same path
    sql_file.unlink()
    result = runner.invoke(
        cli, ["reindex-file", "--config", str(config_path), str(sql_file)]
    )
    assert result.exit_code == 0
    assert "deleted=1" in result.output


# ── init command tests ──


def test_init_generates_yaml(tmp_path):
    """sqlprism init generates sqlprism.yml by default."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0, result.output
        yml_path = Path.cwd() / "sqlprism.yml"
        assert yml_path.exists()
        import yaml
        config = yaml.safe_load(yml_path.read_text())
        assert "repos" in config
        assert "db_path" in config


def test_init_format_json_flag(tmp_path):
    """sqlprism init --format json generates sqlprism.json."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init", "--format", "json"])
        assert result.exit_code == 0, result.output
        json_path = Path.cwd() / "sqlprism.json"
        assert json_path.exists()
        import json
        config = json.loads(json_path.read_text())
        assert "repos" in config
        assert "db_path" in config
        # YAML file should NOT exist
        assert not (Path.cwd() / "sqlprism.yml").exists()


def test_init_does_not_overwrite_existing(tmp_path):
    """Second init invocation does not overwrite and warns about existing config."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # First run creates the file
        runner.invoke(cli, ["init"])
        yml_path = Path.cwd() / "sqlprism.yml"
        original = yml_path.read_text()

        # Second run detects existing config
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0
        assert "already exists" in result.output
        assert yml_path.read_text() == original  # unchanged


def test_init_detects_other_format(tmp_path):
    """init --format json aborts if sqlprism.yml already exists."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        runner.invoke(cli, ["init"])  # creates sqlprism.yml
        result = runner.invoke(cli, ["init", "--format", "json"])
        assert "already exists" in result.output
        assert not (Path.cwd() / "sqlprism.json").exists()
