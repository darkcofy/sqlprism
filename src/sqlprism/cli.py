"""CLI entry point for the SQLPrism MCP server.

Reads config from a JSON file or command-line arguments,
initialises the server, and runs it.
"""

import json
import logging
import sys
from pathlib import Path

import click

from sqlprism.core.mcp_tools import configure, mcp
from sqlprism.types import parse_repo_config

DEFAULT_DB_PATH = Path.home() / ".sqlprism" / "graph.duckdb"
DEFAULT_CONFIG_PATH = Path.home() / ".sqlprism" / "config.json"


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="WARNING",
    help="Set logging verbosity",
)
@click.pass_context
def cli(ctx, log_level):
    """SQLPrism — SQL knowledge graph for your codebase."""
    ctx.ensure_object(dict)
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


@cli.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=str(DEFAULT_CONFIG_PATH),
    help="Path to config file",
)
@click.option(
    "--db",
    "db_path",
    type=click.Path(),
    default=None,
    help="Path to DuckDB file (overrides config)",
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "streamable_http"]),
    default="stdio",
    help="MCP transport mode",
)
@click.option("--port", type=int, default=8000, help="Port for HTTP transport")
def serve(config_path: str, db_path: str | None, transport: str, port: int):
    """Start the MCP server."""
    # Servers need more visibility — override to INFO unless already more verbose
    root_logger = logging.getLogger()
    if root_logger.level > logging.INFO:
        logging.basicConfig(
            level=logging.INFO,
            force=True,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    config = _load_config(config_path)

    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))

    # Ensure parent directory exists
    Path(effective_db_path).parent.mkdir(parents=True, exist_ok=True)

    # Merge all repo types with repo_type tag for the graph layer
    all_repos = {}
    for name, cfg in config.get("repos", {}).items():
        if isinstance(cfg, dict):
            all_repos[name] = {**cfg, "repo_type": "sql"}
        else:
            all_repos[name] = {"path": cfg, "repo_type": "sql"}
    for name, cfg in config.get("dbt_repos", {}).items():
        path = cfg["project_path"] if isinstance(cfg, dict) else cfg
        all_repos[name] = {"path": path, "repo_type": "dbt"}
    for name, cfg in config.get("sqlmesh_repos", {}).items():
        path = cfg["project_path"] if isinstance(cfg, dict) else cfg
        all_repos[name] = {"path": path, "repo_type": "sqlmesh"}

    configure(
        db_path=effective_db_path,
        repos=all_repos,
        sql_dialect=config.get("sql_dialect"),
    )

    if transport == "stdio":
        mcp.run()
    else:
        mcp.run(transport="streamable_http", port=port)


@cli.command()
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--repo", "repo_name", type=str, default=None, help="Reindex a specific repo only")
def reindex(config_path: str, db_path: str | None, repo_name: str | None):
    """Run a manual reindex from the command line."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    config = _load_config(config_path)
    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))

    Path(effective_db_path).parent.mkdir(parents=True, exist_ok=True)

    graph = GraphDB(effective_db_path)
    indexer = Indexer(graph)

    # Index SQL repos
    repos = config.get("repos", {})
    if repo_name:
        if repo_name not in repos:
            click.echo(f"Error: repo '{repo_name}' not in config", err=True)
            sys.exit(1)
        repos = {repo_name: repos[repo_name]}

    all_parse_errors: list[str] = []

    for name, cfg in repos.items():
        path, dialect, dialect_overrides = parse_repo_config(cfg, config.get("sql_dialect"))
        click.echo(f"Indexing {name} ({path}){f' [{dialect}]' if dialect else ''}...")
        stats = indexer.reindex_repo(
            name,
            path,
            dialect=dialect,
            dialect_overrides=dialect_overrides,
        )
        click.echo(
            f"  scanned={stats['files_scanned']}, "
            f"added={stats['files_added']}, "
            f"changed={stats['files_changed']}, "
            f"removed={stats['files_removed']}, "
            f"nodes={stats['nodes_added']}, "
            f"edges={stats['edges_added']}, "
            f"column_usage={stats['column_usage_added']}"
        )
        if stats.get("parse_errors"):
            all_parse_errors.extend(stats["parse_errors"])

    # Also index sqlmesh repos from config
    sqlmesh_repos = config.get("sqlmesh_repos", {})
    if repo_name:
        if repo_name in sqlmesh_repos:
            sqlmesh_repos = {repo_name: sqlmesh_repos[repo_name]}
        else:
            sqlmesh_repos = {}

    for name, sm_config in sqlmesh_repos.items():
        if name.startswith("#"):
            continue
        click.echo(f"Indexing sqlmesh project {name} ({sm_config['project_path']})...")
        variables: dict[str, str | int] = sm_config.get("variables", {})
        stats = indexer.reindex_sqlmesh(
            repo_name=name,
            project_path=sm_config["project_path"],
            env_file=sm_config.get("env_file"),
            variables=variables,
            dialect=sm_config.get("dialect", "athena"),
            sqlmesh_command=sm_config.get("sqlmesh_command", "uv run python"),
        )
        click.echo(
            f"  models={stats['models_rendered']}, "
            f"nodes={stats['nodes_added']}, "
            f"edges={stats['edges_added']}, "
            f"column_usage={stats['column_usage_added']}"
        )

    # Also index dbt repos from config
    dbt_repos = config.get("dbt_repos", {})
    if repo_name:
        if repo_name in dbt_repos:
            dbt_repos = {repo_name: dbt_repos[repo_name]}
        else:
            dbt_repos = {}

    for name, dbt_config in dbt_repos.items():
        if name.startswith("#"):
            continue
        click.echo(f"Indexing dbt project {name} ({dbt_config['project_path']})...")
        stats = indexer.reindex_dbt(
            repo_name=name,
            project_path=dbt_config["project_path"],
            profiles_dir=dbt_config.get("profiles_dir"),
            env_file=dbt_config.get("env_file"),
            target=dbt_config.get("target"),
            dbt_command=dbt_config.get("dbt_command", "uv run dbt"),
            dialect=dbt_config.get("dialect"),
        )
        click.echo(
            f"  models={stats['models_compiled']}, "
            f"nodes={stats['nodes_added']}, "
            f"edges={stats['edges_added']}, "
            f"column_usage={stats['column_usage_added']}"
        )

    graph.close()

    if all_parse_errors:
        click.echo(f"\n{len(all_parse_errors)} parse error(s):", err=True)
        for err in all_parse_errors:
            click.echo(f"  {err}", err=True)
        sys.exit(1)

    click.echo("Done.")


@cli.command("reindex-sqlmesh")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--name", "repo_name", type=str, required=True, help="Repo name for the index")
@click.option(
    "--project",
    "project_path",
    type=click.Path(exists=True),
    required=True,
    help="Path to sqlmesh project dir (containing config.yaml)",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True),
    default=None,
    help="Path to .env file for sqlmesh config",
)
@click.option("--dialect", type=str, default="athena", help="SQL dialect (default: athena)")
@click.option(
    "--var",
    "variables",
    type=(str, str),
    multiple=True,
    help="SQLMesh variables as key value pairs, e.g. --var GRACE_PERIOD 7",
)
@click.option(
    "--sqlmesh-command",
    type=str,
    default="uv run python",
    help="Command to run python in sqlmesh venv (default: 'uv run python')",
)
def reindex_sqlmesh(
    config_path: str,
    db_path: str | None,
    repo_name: str,
    project_path: str,
    env_file: str | None,
    dialect: str,
    variables: tuple[tuple[str, str], ...],
    sqlmesh_command: str,
):
    """Index a sqlmesh project by rendering all models."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    config = _load_config(config_path)
    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))
    Path(effective_db_path).parent.mkdir(parents=True, exist_ok=True)

    graph = GraphDB(effective_db_path)
    indexer = Indexer(graph)

    # Convert --var pairs to dict, auto-cast numeric values
    var_dict: dict[str, str | int] = {}
    for k, v in variables:
        try:
            var_dict[k] = int(v)
        except ValueError:
            var_dict[k] = v

    click.echo(f"Rendering sqlmesh models from {project_path}...")
    stats = indexer.reindex_sqlmesh(
        repo_name=repo_name,
        project_path=project_path,
        env_file=env_file,
        variables=var_dict,
        dialect=dialect,
        sqlmesh_command=sqlmesh_command,
    )
    click.echo(
        f"  models={stats['models_rendered']}, "
        f"nodes={stats['nodes_added']}, "
        f"edges={stats['edges_added']}, "
        f"column_usage={stats['column_usage_added']}"
    )

    graph.close()
    click.echo("Done.")


@cli.command("reindex-dbt")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--name", "repo_name", type=str, required=True, help="Repo name for the index")
@click.option(
    "--project",
    "project_path",
    type=click.Path(exists=True),
    required=True,
    help="Path to dbt project dir (containing dbt_project.yml)",
)
@click.option(
    "--profiles-dir",
    type=click.Path(exists=True),
    default=None,
    help="Path to directory containing profiles.yml (defaults to project dir)",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True),
    default=None,
    help="Path to .env file for dbt connection variables",
)
@click.option("--target", type=str, default=None, help="dbt target name")
@click.option(
    "--dbt-command",
    type=str,
    default="uv run dbt",
    help="Command to invoke dbt (default: 'uv run dbt')",
)
@click.option(
    "--dialect",
    type=str,
    default=None,
    help="SQL dialect for parsing (e.g. starrocks, mysql, postgres)",
)
def reindex_dbt_cmd(
    config_path: str,
    db_path: str | None,
    repo_name: str,
    project_path: str,
    profiles_dir: str | None,
    env_file: str | None,
    target: str | None,
    dbt_command: str,
    dialect: str | None,
):
    """Index a dbt project by compiling all models."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    config = _load_config(config_path)
    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))
    Path(effective_db_path).parent.mkdir(parents=True, exist_ok=True)

    graph = GraphDB(effective_db_path)
    indexer = Indexer(graph)

    click.echo(f"Compiling dbt models from {project_path}...")
    stats = indexer.reindex_dbt(
        repo_name=repo_name,
        project_path=project_path,
        profiles_dir=profiles_dir,
        env_file=env_file,
        target=target,
        dbt_command=dbt_command,
        dialect=dialect,
    )
    click.echo(
        f"  models={stats['models_compiled']}, "
        f"nodes={stats['nodes_added']}, "
        f"edges={stats['edges_added']}, "
        f"column_usage={stats['column_usage_added']}"
    )

    graph.close()
    click.echo("Done.")


@cli.group()
def query():
    """Query the knowledge graph."""
    pass


def _open_graph(config_path: str, db_path: str | None):
    """Load config, resolve db_path, and return a GraphDB instance."""
    from sqlprism.core.graph import GraphDB

    config = _load_config(config_path)
    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))

    if not Path(effective_db_path).exists():
        click.echo("No index found. Run 'sqlprism reindex' first.", err=True)
        sys.exit(1)

    return GraphDB(effective_db_path)


@query.command("search")
@click.argument("pattern")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--kind", type=str, default=None, help="Filter by node kind")
@click.option("--schema", type=str, default=None, help="Filter by schema")
@click.option("--repo", type=str, default=None, help="Filter by repo name")
@click.option("--limit", type=int, default=20, help="Max results (default 20)")
def query_search(
    config_path: str,
    db_path: str | None,
    pattern: str,
    kind: str | None,
    schema: str | None,
    repo: str | None,
    limit: int,
):
    """Search nodes by name pattern."""
    graph = _open_graph(config_path, db_path)
    result = graph.query_search(
        pattern=pattern,
        kind=kind,
        schema=schema,
        repo=repo,
        limit=limit,
        include_snippets=False,
    )
    graph.close()
    click.echo(json.dumps(result, indent=2, default=str))


@query.command("references")
@click.argument("name")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--kind", type=str, default=None, help="Filter by node kind")
@click.option("--schema", type=str, default=None, help="Filter by schema")
@click.option("--repo", type=str, default=None, help="Filter by repo name")
@click.option(
    "--direction",
    type=click.Choice(["both", "inbound", "outbound"]),
    default="both",
    help="Edge direction (default both)",
)
def query_references(
    config_path: str,
    db_path: str | None,
    name: str,
    kind: str | None,
    schema: str | None,
    repo: str | None,
    direction: str,
):
    """Find all references to/from a named entity."""
    graph = _open_graph(config_path, db_path)
    result = graph.query_references(
        name=name,
        kind=kind,
        schema=schema,
        repo=repo,
        direction=direction,
        include_snippets=False,
    )
    graph.close()
    click.echo(json.dumps(result, indent=2, default=str))


@query.command("column-usage")
@click.argument("table")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--column", type=str, default=None, help="Filter by column name")
@click.option("--usage-type", type=str, default=None, help="Filter by usage type")
@click.option("--repo", type=str, default=None, help="Filter by repo name")
def query_column_usage(
    config_path: str,
    db_path: str | None,
    table: str,
    column: str | None,
    usage_type: str | None,
    repo: str | None,
):
    """Find column usage for a table."""
    graph = _open_graph(config_path, db_path)
    result = graph.query_column_usage(
        table=table,
        column=column,
        usage_type=usage_type,
        repo=repo,
    )
    graph.close()
    click.echo(json.dumps(result, indent=2, default=str))


@query.command("trace")
@click.argument("name")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--kind", type=str, default=None, help="Filter by node kind")
@click.option(
    "--direction",
    type=click.Choice(["downstream", "upstream", "both"]),
    default="downstream",
    help="Trace direction (default downstream)",
)
@click.option("--max-depth", type=int, default=3, help="Max traversal depth (default 3)")
@click.option("--repo", type=str, default=None, help="Filter by repo name")
def query_trace(
    config_path: str,
    db_path: str | None,
    name: str,
    kind: str | None,
    direction: str,
    max_depth: int,
    repo: str | None,
):
    """Trace multi-hop dependency chains from a named entity."""
    graph = _open_graph(config_path, db_path)
    result = graph.query_trace(
        name=name,
        kind=kind,
        direction=direction,
        max_depth=max_depth,
        repo=repo,
        include_snippets=False,
    )
    graph.close()
    click.echo(json.dumps(result, indent=2, default=str))


@query.command("lineage")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
@click.option("--table", type=str, default=None, help="Filter by hop table name")
@click.option("--column", type=str, default=None, help="Filter by column name")
@click.option("--output-node", type=str, default=None, help="Filter by output node name")
@click.option("--repo", type=str, default=None, help="Filter by repo name")
def query_lineage(
    config_path: str,
    db_path: str | None,
    table: str | None,
    column: str | None,
    output_node: str | None,
    repo: str | None,
):
    """Query column lineage chains."""
    graph = _open_graph(config_path, db_path)
    result = graph.query_column_lineage(
        table=table,
        column=column,
        output_node=output_node,
        repo=repo,
    )
    graph.close()
    click.echo(json.dumps(result, indent=2, default=str))


@cli.command()
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
@click.option("--db", "db_path", type=click.Path(), default=None)
def status(config_path: str, db_path: str | None):
    """Show current index status."""
    from sqlprism.core.graph import GraphDB

    config = _load_config(config_path)
    effective_db_path = db_path or config.get("db_path", str(DEFAULT_DB_PATH))

    if not Path(effective_db_path).exists():
        click.echo("No index found. Run 'sqlprism reindex' first.")
        sys.exit(1)

    graph = GraphDB(effective_db_path)
    info = graph.get_index_status()
    graph.close()

    click.echo(json.dumps(info, indent=2, default=str))


@cli.command("init")
@click.option("--config", "config_path", type=click.Path(), default=str(DEFAULT_CONFIG_PATH))
def init_config(config_path: str):
    """Create a default config file."""
    config_file = Path(config_path)
    if config_file.exists():
        click.echo(f"Config already exists at {config_file}")
        return

    config_file.parent.mkdir(parents=True, exist_ok=True)

    default_config = {
        "db_path": str(DEFAULT_DB_PATH),
        "sql_dialect": None,
        "repos": {
            "my-project": {
                "path": str(Path.cwd()),
                "dialect": None,
                "dialect_overrides": {
                    "# athena/": "athena",
                    "# starrocks/": "starrocks",
                },
            },
        },
        "sqlmesh_repos": {
            "# my-sqlmesh-project": {
                "project_path": "/path/to/sqlmesh/folder",
                "env_file": "/path/to/.env",
                "dialect": "athena",
                "variables": {"GRACE_PERIOD": 7},
            },
        },
        "dbt_repos": {
            "# my-dbt-project": {
                "project_path": "/path/to/dbt/project",
                "env_file": "/path/to/.env",
                "target": "dev",
                "dialect": "starrocks",
                "dbt_command": "uv run dbt",
            },
        },
    }

    config_file.write_text(json.dumps(default_config, indent=2))
    click.echo(f"Created config at {config_file}")
    click.echo("Edit it to add your repos, then run: sqlprism reindex")


def _load_config(config_path: str) -> dict:
    """Load config from JSON file, or return defaults."""
    path = Path(config_path)
    if path.exists():
        return json.loads(path.read_text())
    logging.warning("Config file not found: %s — using defaults", path)
    return {"repos": {}, "db_path": str(DEFAULT_DB_PATH)}


def main():
    cli()


if __name__ == "__main__":
    main()
