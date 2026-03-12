# SQLPrism

[![CI](https://github.com/darkcofy/sqlprism/actions/workflows/ci.yml/badge.svg)](https://github.com/darkcofy/sqlprism/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/darkcofy/sqlprism/branch/main/graph/badge.svg)](https://codecov.io/gh/darkcofy/sqlprism)
[![PyPI](https://img.shields.io/pypi/v/sqlprism)](https://pypi.org/project/sqlprism/)
[![Python](https://img.shields.io/pypi/pyversions/sqlprism)](https://pypi.org/project/sqlprism/)
[![License](https://img.shields.io/github/license/darkcofy/sqlprism)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://darkcofy.github.io/sqlprism/)

An MCP server that indexes SQL codebases into a queryable knowledge graph backed by DuckDB. Instead of grepping through files, ask structural questions: *what touches this table, where is this column transformed, what's the blast radius of this PR.*

Built for SQL-heavy data projects — works with raw SQL, [SQLMesh](https://sqlmesh.com/), and [dbt](https://www.getdbt.com/).

## Why Not Just Grep?

Grep finds strings. This tool understands SQL structure.

| Capability | Grep | SQLPrism |
|---|---|---|
| Find table references | Yes | Yes |
| CTE-to-CTE data flow | No — manual file reading | Yes — edges tracked in graph |
| Column lineage with transforms (CAST, COALESCE, SUM) | No | Yes — parsed from AST |
| Usage type (WHERE vs SELECT vs JOIN vs GROUP BY) | Fragile regex | Precise — parsed from AST |
| Multi-hop impact analysis | Manual tracing | Automatic graph traversal |
| PR blast radius | DIY with git diff | One call |
| Cross-CTE column tracing | Basically impossible | Built-in |

On a 200-model SQLMesh project, a column impact query returns **75 structured results in ~5,000 tokens**. The grep equivalent would need **40-60 files opened, ~100,000+ tokens**, and still wouldn't tell you whether a column appears in a WHERE filter or a SELECT.

## What's New in v1.0

- **Non-blocking reindex** — reindex runs in the background; queries remain available during indexing via DuckDB MVCC. Call `index_status` to check progress.
- **PR Impact delta mode** — `pr_impact` now shows *net-new* blast radius vs the base branch (default), not just total downstream. New fields: `newly_affected`, `no_longer_affected`, `delta`. Note: delta mode captures net-new downstream impact; reduced blast radius from removed edges is not detected.
- **Thread-safe concurrency** — `asyncio.Lock` on reindex guard, atomic status updates, `threading.local` for transaction flags. Safe under both stdio and HTTP transports.
- **SELECT \* lineage for dbt/sqlmesh** — schema catalog is passed to dbt and sqlmesh renderers, enabling column expansion through `SELECT *` statements.
- **Input validation** — `compare_mode`, `direction`, and other string parameters use `Literal` types with Pydantic, rejecting invalid values at the API boundary.
- **Logging** — `--log-level` global CLI option. `serve` defaults to INFO for production visibility.
- **File error resilience** — unreadable files are skipped instead of aborting the entire reindex.
- **Code coverage** — 82%+ line coverage with 235 tests enforced via pytest-cov.

## Quick Start

### From source (local development)

```bash
git clone https://github.com/darkcofy/sqlprism.git && cd sqlprism

# Install dependencies and the package
uv sync

# Create config
uv run sqlprism init

# Edit ~/.sqlprism/config.json (see Configuration below)

# Index your repos
uv run sqlprism reindex

# Start the MCP server
uv run sqlprism serve
```

All commands use `uv run sqlprism` to run within the project's virtualenv. If you activate the venv (`source .venv/bin/activate`), you can drop the `uv run` prefix.

## Configuration

`sqlprism init` creates a default config at `~/.sqlprism/config.json`. You can override the path with `--config PATH` on any command.

```json
{
  "db_path": "~/.sqlprism/graph.duckdb",
  "sql_dialect": null,
  "repos": {
    "my-queries": "/path/to/sql/repo",
    "multi-dialect-repo": {
      "path": "/path/to/repo",
      "dialect": "starrocks",
      "dialect_overrides": {
        "athena/": "athena",
        "postgres/": "postgres"
      }
    }
  },
  "sqlmesh_repos": {
    "my-sqlmesh-project": {
      "project_path": "/path/to/sqlmesh/folder",
      "env_file": "/path/to/.env",
      "dialect": "athena",
      "variables": {
        "GRACE_PERIOD": 7
      }
    }
  },
  "dbt_repos": {
    "my-dbt-project": {
      "project_path": "/path/to/dbt/project",
      "env_file": "/path/to/.env",
      "target": "dev",
      "dialect": "starrocks",
      "dbt_command": "uv run dbt"
    }
  }
}
```

| Field | Description |
|---|---|
| `db_path` | Path to the DuckDB database file. Defaults to `~/.sqlprism/graph.duckdb`. |
| `sql_dialect` | Global default SQL dialect. `null` for auto-detect. |
| `repos` | Plain SQL repos. Value is a path string or an object with `path`, `dialect`, `dialect_overrides`. |
| `dialect` | Per-repo dialect override (e.g. `"starrocks"`, `"athena"`, `"bigquery"`). |
| `dialect_overrides` | Per-directory overrides using prefix matching or glob patterns. |
| `sqlmesh_repos` | SQLMesh projects. Renders models before parsing. See [SQLMesh and dbt Support](#sqlmesh-and-dbt-support). |
| `dbt_repos` | dbt projects. Compiles models before parsing. See [SQLMesh and dbt Support](#sqlmesh-and-dbt-support). |

## SQL Dialect Support

Powered by [sqlglot](https://github.com/tobymao/sqlglot), the indexer supports **33 SQL dialects** out of the box:

| Dialect | Dialect | Dialect | Dialect |
|---|---|---|---|
| Athena | Doris | Materialize | Snowflake |
| BigQuery | Dremio | MySQL | Spark |
| ClickHouse | Drill | Oracle | Spark2 |
| Databricks | Druid | Postgres | SQLite |
| DuckDB | Dune | Presto | StarRocks |
| Exasol | Fabric | PRQL | Tableau |
| Hive | | Redshift | Teradata |
| RisingWave | | SingleStore | Trino / TSQL |

Pass the dialect name as a lowercase string (e.g., `"starrocks"`, `"bigquery"`, `"athena"`). Dialect-specific quoting (backticks for MySQL/StarRocks, double-quotes for Postgres) and identifier case normalization (lowercase for Postgres/DuckDB, uppercase for Snowflake/Oracle) are handled automatically.

## SQLMesh and dbt Support

Both SQLMesh and dbt use macros/Jinja that can't be parsed as raw SQL. The indexer solves this by rendering models first, then parsing the clean SQL output.

Both renderers use subprocess execution — no SQLMesh or dbt Python dependencies are required in the indexer's environment. They use whatever version the project already has installed.

### SQLMesh

```bash
sqlprism reindex-sqlmesh \
  --name my-project \
  --project /path/to/sqlmesh/project \
  --dialect athena \
  --env-file /path/to/.env \
  --var GRACE_PERIOD 7
```

| Parameter | Required | Description |
|---|---|---|
| `--name` | Yes | Repo name used in the index. Used to filter queries later. |
| `--project` | Yes | Path to the sqlmesh project directory (containing `config.yaml`). |
| `--dialect` | No | SQL dialect for rendering output. Default: `athena`. |
| `--env-file` | No | Path to `.env` file. Variables are loaded into the subprocess environment before rendering. Useful for connection strings, S3 paths, etc. |
| `--var` | No | SQLMesh macro variables as key-value pairs. Repeatable. e.g. `--var GRACE_PERIOD 7 --var ENV prod`. These override variables defined in `config.yaml`. |
| `--sqlmesh-command` | No | Command to run python in the sqlmesh project's venv. The indexer runs an inline script that imports sqlmesh's Python API. Default: `uv run python`. |
| `--config` | No | Path to sqlprism config file. Default: `~/.sqlprism/config.json`. |
| `--db` | No | Path to DuckDB file. Overrides the value in config. |

### dbt

```bash
sqlprism reindex-dbt \
  --name my-project \
  --project /path/to/dbt/project \
  --dialect starrocks \
  --env-file /path/to/.env \
  --target dev
```

| Parameter | Required | Description |
|---|---|---|
| `--name` | Yes | Repo name used in the index. |
| `--project` | Yes | Path to dbt project directory (containing `dbt_project.yml`). |
| `--dialect` | No | SQL dialect for parsing the compiled output (e.g. `starrocks`, `postgres`, `bigquery`). |
| `--env-file` | No | Path to `.env` file for dbt connection variables. |
| `--target` | No | dbt target name (e.g. `dev`, `prod`). Maps to the target in `profiles.yml`. |
| `--profiles-dir` | No | Path to directory containing `profiles.yml`. Defaults to the project directory. |
| `--dbt-command` | No | Base command to invoke dbt. `compile` is appended automatically. Default: `uv run dbt`. Use `dbt` if globally installed, or `uvx --with dbt-starrocks dbt` for ephemeral install. |
| `--config` | No | Path to sqlprism config file. |
| `--db` | No | Path to DuckDB file. Overrides config. |

## CLI Commands

> **Global option:** `--log-level` sets logging verbosity (default: `WARNING`). The `serve` command defaults to `INFO`.

### `sqlprism init`

Creates a default config file at `~/.sqlprism/config.json` with example entries for repos, sqlmesh_repos, and dbt_repos.

```bash
sqlprism init [--config PATH]
```

### `sqlprism reindex`

Incremental reindex of all configured plain SQL repos (from `repos` in config). Checksums files and only re-parses what changed.

```bash
sqlprism reindex [--config PATH] [--db PATH] [--repo NAME]
```

| Parameter | Description |
|---|---|
| `--repo` | Reindex a single repo only. Omit to reindex all. |

### `sqlprism serve`

Starts the MCP server. Exposes all 10 tools to any MCP client.

```bash
sqlprism [--log-level DEBUG|INFO|WARNING|ERROR] serve [--config PATH] [--db PATH] [--transport stdio|streamable_http] [--port 8000]
```

| Parameter | Default | Description |
|---|---|---|
| `--transport` | `stdio` | Transport mode. Use `stdio` for Claude Code / Claude Desktop. Use `streamable_http` for web-based clients. |
| `--port` | `8000` | Port for HTTP transport. Only used when `--transport streamable_http`. |

### `sqlprism status`

Shows index status: repos, file counts, node counts, last indexed time, git commit/branch.

```bash
sqlprism status [--config PATH] [--db PATH]
```

## CLI Query Commands

All query commands output JSON to stdout. They share common parameters:

| Parameter | Description |
|---|---|
| `--config PATH` | Path to config file. Default: `~/.sqlprism/config.json`. |
| `--db PATH` | Path to DuckDB file. Overrides config. |
| `--repo TEXT` | Filter results by repo name. Omit to query across all repos. |

### `sqlprism query search`

Find tables, views, CTEs, and queries by name pattern (case-insensitive partial match).

```bash
sqlprism query search PATTERN [--kind KIND] [--schema SCHEMA] [--repo REPO] [--limit 20]
```

| Parameter | Description |
|---|---|
| `PATTERN` | Search string. Matches against node names (partial, case-insensitive). |
| `--kind` | Filter by node kind: `table`, `view`, `cte`, `query`. |
| `--schema` | Filter by SQL schema name (e.g. `bronze`, `silver`, `public`). |
| `--limit` | Max results to return. Default: `20`. |

### `sqlprism query references`

Find what depends on an entity (inbound) and what it depends on (outbound).

```bash
sqlprism query references NAME [--direction both|inbound|outbound] [--kind KIND] [--schema SCHEMA] [--repo REPO]
```

| Parameter | Description |
|---|---|
| `NAME` | Entity name (table, view, CTE, etc.). |
| `--direction` | `inbound` (what depends on this), `outbound` (what this depends on), or `both`. Default: `both`. |
| `--kind` | Filter by node kind to disambiguate if the name exists as multiple kinds. |
| `--schema` | Filter by SQL schema name. |

### `sqlprism query column-usage`

Find where and how a table's columns are used across the codebase.

```bash
sqlprism query column-usage TABLE [--column COL] [--usage-type TYPE] [--repo REPO]
```

| Parameter | Description |
|---|---|
| `TABLE` | Table name to search column usage for. |
| `--column` | Filter by specific column name. Omit for all columns. |
| `--usage-type` | Filter by usage type: `select`, `where`, `join_on`, `group_by`, `order_by`, `having`, `partition_by`, `window_order`, `insert`, `update`. |

Each result includes: table, column, usage type, alias, transform expression, the node/CTE it appears in, file path, and repo.

### `sqlprism query trace`

Multi-hop dependency tracing for impact analysis.

```bash
sqlprism query trace NAME [--direction downstream|upstream|both] [--max-depth 3] [--kind KIND] [--repo REPO]
```

| Parameter | Description |
|---|---|
| `NAME` | Starting entity name. |
| `--direction` | `downstream` (what breaks if I change this), `upstream` (what does this depend on), or `both`. Default: `downstream`. |
| `--max-depth` | Maximum hops to traverse. Default: `3`, max: `6`. Higher values find more transitive dependencies but produce larger results. |
| `--kind` | Filter by node kind to disambiguate. |

### `sqlprism query lineage`

Query end-to-end column lineage chains through CTEs and subqueries.

```bash
sqlprism query lineage [--table TABLE] [--column COL] [--output-node NODE] [--repo REPO]
```

| Parameter | Description |
|---|---|
| `--table` | Filter by source or intermediate table name in the lineage chain. |
| `--column` | Filter by column name at any hop in the chain. |
| `--output-node` | Filter by the output entity name (the final table/view/query the lineage flows into). |

At least one of `--table`, `--column`, or `--output-node` should be provided to avoid returning the entire lineage graph.

## MCP Tools

When running as an MCP server (`sqlprism serve`), the following 10 tools are exposed. These are the same queries as the CLI but with additional parameters like `offset` for pagination and `include_snippets` for source code context.

| Tool | Description |
|---|---|
| `search` | Find tables, views, CTEs, queries by name pattern. Filter by kind, schema, repo. Supports `limit` and `offset` for pagination. |
| `find_references` | What depends on X / what does X depend on. Supports `direction` (inbound/outbound/both), `limit`, `offset`. |
| `find_column_usage` | Where and how columns are used — usage type, transforms, aliases. Supports `limit`, `offset`. |
| `trace_dependencies` | Multi-hop upstream/downstream dependency chains. `max_depth` (1-6), `include_snippets`. |
| `trace_column_lineage` | End-to-end column lineage through CTEs and subqueries. Filter by `table`, `column`, `output_node`. |
| `pr_impact` | Structural diff + blast radius since a `base_commit`. **Delta mode** (default) shows net-new impact vs base branch. `absolute` mode returns total blast radius (v1 behavior). Traces `max_blast_radius_depth` hops. |
| `reindex` | Trigger incremental reindex of plain SQL repos. **Runs in background** — returns immediately, queries remain available during reindex. Call `index_status` to check progress. |
| `reindex_sqlmesh` | Render and index a SQLMesh project. **Runs in background.** Pass `variables` as a dict for macro variables. |
| `reindex_dbt` | Compile and index a dbt project. **Runs in background.** Pass `target`, `dialect`, `profiles_dir`. |
| `index_status` | Repos, file counts, node counts, last commit, staleness. **Includes reindex progress** when a background reindex is running. |

### MCP Tool Parameters

**Pagination** — `search`, `find_references`, `find_column_usage`, and `trace_column_lineage` all support:
- `limit`: Max results to return (default varies by tool, max 100-500).
- `offset`: Number of results to skip. Use for paginating through large result sets.

**Snippets** — `search`, `find_references`, and `trace_dependencies` support:
- `include_snippets`: Include source code snippets in results. Default `true` for search/references, `false` for trace (which can produce large output).

## MCP Client Configuration

### Claude Code
```bash
claude mcp add sqlprism -- uv run --directory /path/to/sqlprism sqlprism serve
```

### Claude Desktop / Cursor / Continue.dev (`.mcp.json`)
```json
{
  "mcpServers": {
    "sqlprism": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/sqlprism", "sqlprism", "serve"]
    }
  }
}
```

Replace `/path/to/sqlprism` with the absolute path to your clone of this repo.

## Architecture

```
src/sqlprism/
  types.py              <- ParseResult, NodeResult, EdgeResult, ColumnUsageResult, parse_repo_config
  languages/
    __init__.py         <- SQL_EXTENSIONS, is_sql_file()
    sql.py              <- sqlglot: tables, views, CTEs, column lineage, transforms
    sqlmesh.py          <- SQLMesh model renderer (subprocess)
    dbt.py              <- dbt model renderer (subprocess)
    utils.py            <- Shared helpers (find_venv_dir, parse_dotenv, build_env, enrich_nodes)
  core/
    graph.py            <- DuckDB schema (v8), read/write separation (MVCC), queries, snippets
    indexer.py          <- Orchestrator: scan -> checksum -> parse -> store (per-file error resilience)
    mcp_tools.py        <- FastMCP tool definitions (10 tools, non-blocking reindex)
  cli.py                <- Click CLI with --log-level: serve, reindex, reindex-sqlmesh, reindex-dbt, status, init
```

The SQL parser extracts:
- **Nodes**: tables, views, CTEs, queries (with schema metadata and dialect-aware case normalization)
- **Edges**: table references, CTE references, JOINs (with context like "FROM clause", "JOIN clause")
- **Column usage**: per-column tracking with usage type (select, where, join_on, group_by, order_by, having, partition_by, window_order), transforms (CAST, COALESCE, SUM, etc.), output aliases, and WHERE filter expressions
- **Column lineage**: end-to-end tracing through CTEs and subqueries back to source tables, with SELECT * expansion when schema catalog is available

## Development

```bash
uv sync
uv run pytest                          # run tests (235 tests)
uv run pytest --cov=sqlprism        # run with coverage report
uv run pytest --cov=sqlprism --cov-report=html:coverage_html  # HTML report
```

### Code Coverage

![Coverage Grid](https://codecov.io/github/darkcofy/sqlprism/graphs/tree.svg?token=8H5XNZEFOW)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
