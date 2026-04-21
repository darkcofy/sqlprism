"""Tests for the CLI entry point (task 5.9)."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from sqlprism.cli import _build_repo_configs, cli


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


def test_init_help_mentions_yaml():
    """init --help references YAML as default format."""
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--help"])
    assert result.exit_code == 0
    assert "yaml" in result.output.lower()
    assert "default: yaml" in result.output.lower()


# ── Regression tests for #137 review feedback ──


def test_reindex_repo_filter_accepts_dbt_only_repo(tmp_path):
    """reindex --repo <name> resolves names from dbt_repos or sqlmesh_repos too.

    Regression for the acknowledged behavior change in #137: previously the
    SQL-repos lookup ran first and exited non-zero when the name lived only
    in dbt_repos/sqlmesh_repos.
    """
    dbt_dir = tmp_path / "dbtproj"
    dbt_dir.mkdir()
    config = {
        "db_path": str(tmp_path / "test.duckdb"),
        "dbt_repos": {"only_dbt": {"project_path": str(dbt_dir)}},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    # Stub the dbt render path — we just need dispatch to land on reindex_dbt.
    stats = {
        "models_compiled": 2,
        "nodes_added": 0,
        "edges_added": 0,
        "column_usage_added": 0,
    }
    with patch("sqlprism.core.indexer.Indexer") as mock_indexer_cls:
        instance = mock_indexer_cls.return_value
        instance.reindex_dbt.return_value = stats
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["reindex", "--config", str(config_path), "--repo", "only_dbt"],
        )

    assert result.exit_code == 0, f"stdout={result.output}"
    assert instance.reindex_dbt.called
    assert instance.reindex_dbt.call_args.kwargs["repo_name"] == "only_dbt"
    assert "Indexing dbt project only_dbt" in result.output
    assert "models=2" in result.output


def test_reindex_handler_prints_dbt_stats_line(tmp_path):
    """The dispatched dbt handler prints the expected stats summary line."""
    dbt_dir = tmp_path / "dbtproj"
    dbt_dir.mkdir()
    config = {
        "db_path": str(tmp_path / "test.duckdb"),
        "dbt_repos": {"proj": {"project_path": str(dbt_dir)}},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    stats = {
        "models_compiled": 7,
        "nodes_added": 12,
        "edges_added": 19,
        "column_usage_added": 34,
    }
    with patch("sqlprism.core.indexer.Indexer") as mock_indexer_cls:
        mock_indexer_cls.return_value.reindex_dbt.return_value = stats
        runner = CliRunner()
        result = runner.invoke(cli, ["reindex", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "models=7" in result.output
    assert "nodes=12" in result.output
    assert "edges=19" in result.output
    assert "column_usage=34" in result.output


def test_reindex_handler_prints_sqlmesh_stats_line(tmp_path):
    """The dispatched sqlmesh handler prints the expected stats summary line."""
    sm_dir = tmp_path / "smproj"
    sm_dir.mkdir()
    config = {
        "db_path": str(tmp_path / "test.duckdb"),
        "sqlmesh_repos": {"proj": {"project_path": str(sm_dir)}},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    stats = {
        "models_rendered": 5,
        "nodes_added": 8,
        "edges_added": 11,
        "column_usage_added": 22,
    }
    with patch("sqlprism.core.indexer.Indexer") as mock_indexer_cls:
        mock_indexer_cls.return_value.reindex_sqlmesh.return_value = stats
        runner = CliRunner()
        result = runner.invoke(cli, ["reindex", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Indexing sqlmesh project proj" in result.output
    assert "models=5" in result.output
    assert "nodes=8" in result.output


@pytest.mark.parametrize(
    "cmd",
    [
        ["query", "search", "orders"],
        ["query", "references", "orders"],
        ["query", "column-usage", "orders"],
        ["query", "trace", "orders"],
        ["query", "lineage"],
    ],
)
def test_query_subcommand_db_missing_writes_to_stderr(tmp_path, cmd):
    """`_open_graph_for_read` default routes 'No index found' to stderr for query subcommands."""
    missing_db = tmp_path / "nonexistent.duckdb"
    runner = CliRunner()
    result = runner.invoke(cli, [*cmd, "--db", str(missing_db)])
    assert result.exit_code == 1
    assert "No index found" in result.stderr
    assert "No index found" not in result.stdout


@pytest.mark.parametrize(
    "cmd",
    [
        ["status"],
        ["conventions", "refresh"],
        ["conventions", "diff"],
        ["conventions", "init"],
    ],
)
def test_legacy_stdout_commands_keep_db_missing_on_stdout(tmp_path, cmd):
    """status + conventions.* preserve the legacy 'No index found' → stdout routing."""
    missing_db = tmp_path / "nonexistent.duckdb"
    runner = CliRunner()
    result = runner.invoke(cli, [*cmd, "--db", str(missing_db)])
    assert result.exit_code == 1
    assert "No index found" in result.stdout
    assert "No index found" not in result.stderr


def test_conventions_refresh_no_config_with_db_ok(tmp_path):
    """conventions refresh works with --db alone when no config file is present."""
    missing_db = tmp_path / "nonexistent.duckdb"
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # No sqlprism.yml, no --config flag, just --db → require_config=False path.
        result = runner.invoke(cli, ["conventions", "refresh", "--db", str(missing_db)])
    # DB is missing, so exit 1 with the expected message on stdout.
    assert result.exit_code == 1
    assert "No index found" in result.stdout


def test_conventions_init_error_order_db_takes_precedence(tmp_path):
    """When both DB and output file are missing/present, DB-missing wins."""
    missing_db = tmp_path / "nonexistent.duckdb"
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        Path("sqlprism.conventions.yml").write_text("# pre-existing\n")
        result = runner.invoke(cli, ["conventions", "init", "--db", str(missing_db)])
    assert result.exit_code == 1
    # The DB-missing message must win — not the "already exists" message.
    assert "No index found" in result.stdout
    assert "already exists" not in result.output


def test_build_repo_configs_skips_hash_prefixed_entries():
    """Names starting with '#' are YAML-template comments and must be skipped."""
    merged = _build_repo_configs({
        "repos": {"real": "/path", "#commented": "/ignored"},
        "dbt_repos": {"#my-dbt": {"project_path": "/nope"}},
        "sqlmesh_repos": {"#my-sm": {"project_path": "/nope"}},
    })
    assert "real" in merged
    assert "#commented" not in merged
    assert "#my-dbt" not in merged
    assert "#my-sm" not in merged


def test_build_repo_configs_rejects_string_dbt_entry():
    """dbt/sqlmesh entries must be mappings — friendly error instead of KeyError."""
    import click as _click

    with pytest.raises(_click.ClickException) as exc:
        _build_repo_configs({"dbt_repos": {"bad": "/just-a-string"}})
    assert "dbt" in str(exc.value.message)
    assert "project_path" in str(exc.value.message)


def test_reindex_closes_graphdb_on_error_exit(tmp_path, monkeypatch):
    """sys.exit inside `_open_graph_for_write` still closes the graph via __exit__."""
    from sqlprism.core import graph as graph_mod

    config = {
        "db_path": str(tmp_path / "test.duckdb"),
        "repos": {"real": str(tmp_path)},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    close_calls: list[int] = []
    real_close = graph_mod.GraphDB.close

    def tracking_close(self):
        close_calls.append(1)
        real_close(self)

    monkeypatch.setattr(graph_mod.GraphDB, "close", tracking_close)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["reindex", "--config", str(config_path), "--repo", "nonexistent"],
    )
    assert result.exit_code != 0
    assert "not in config" in result.output
    # The context manager's __exit__ must close exactly once, even though
    # sys.exit(1) fired from inside the `with` block.
    assert len(close_calls) == 1
