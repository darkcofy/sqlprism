# CLI Reference

All commands use `uv run sqlprism` (or just `sqlprism` if the venv is activated).

## Global Options

| Parameter | Default | Description |
|---|---|---|
| `--log-level` | `WARNING` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`. The `serve` command automatically uses `INFO` if the configured level is less verbose. |

## Setup Commands

### `sqlprism init`

Creates a default config file with example entries.

```bash
sqlprism init [--format yaml|json]
```

| Parameter | Default | Description |
|---|---|---|
| `--format` | `yaml` | Config file format: `yaml` or `json`. |

### `sqlprism status`

Shows index status: repos, file counts, node counts, last indexed time, git commit/branch.

```bash
sqlprism status [--config PATH] [--db PATH]
```

## Server

### `sqlprism serve`

Starts the MCP server, exposing all 24 tools to any MCP client.

```bash
sqlprism serve [--config PATH] [--db PATH] [--transport stdio|streamable-http] [--port 8000]
```

| Parameter | Default | Description |
|---|---|---|
| `--transport` | `stdio` | Transport mode. Use `stdio` for Claude Code / Claude Desktop. Use `streamable-http` for web-based clients. |
| `--port` | `8000` | Port for HTTP transport. Only used with `streamable-http`. |
| `--config` | Auto-discovered (see Configuration) | Path to config file. |
| `--db` | From config | Path to DuckDB file. Overrides `db_path` in config. |

## Indexing Commands

### `sqlprism reindex-file`

Reindex specific files (fast on-save path). Works standalone without a running MCP server. Resolves each file to its repo, determines the repo type (plain SQL, dbt, sqlmesh), and reindexes accordingly.

```bash
sqlprism reindex-file /path/to/model.sql [/path/to/another.sql ...]
```

| Parameter | Required | Description |
|---|---|---|
| `PATHS` | Yes | One or more file paths to reindex (positional). |
| `--config` | No | Path to config file. Default: auto-discovered. |
| `--db` | No | Path to DuckDB file. Overrides config. |

**Editor integration (non-MCP):**
```bash
# Vim/Neovim
autocmd BufWritePost *.sql silent !sqlprism reindex-file %:p

# Emacs — add to after-save-hook
```

**Output:** `reindexed=N, skipped=M, deleted=K`

### `sqlprism reindex`

Incremental reindex of plain SQL repos (from `repos` in config). Checksums files and only re-parses what changed.

```bash
sqlprism reindex [--config PATH] [--db PATH] [--repo NAME]
```

| Parameter | Description |
|---|---|
| `--repo` | Reindex a single repo only. Omit to reindex all. |

### `sqlprism reindex-sqlmesh`

Renders all SQLMesh models via subprocess, then parses the clean SQL output.

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
| `--name` | Yes | Repo name for the index. Used to filter queries later. |
| `--project` | Yes | Path to the sqlmesh project directory (containing `config.yaml`). |
| `--dialect` | No | SQL dialect for rendering output. Default: `athena`. |
| `--env-file` | No | Path to `.env` file. Variables are loaded into the subprocess environment before rendering. |
| `--var` | No | SQLMesh macro variables as key-value pairs. Repeatable. e.g. `--var GRACE_PERIOD 7 --var ENV prod`. Overrides variables in `config.yaml`. |
| `--sqlmesh-command` | No | Command to run python in the sqlmesh project's venv. Default: `uv run python`. The indexer runs an inline script that imports sqlmesh's Python API. |

### `sqlprism reindex-dbt`

Runs `dbt compile` via subprocess, then parses the compiled SQL from `target/compiled/`.

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
| `--name` | Yes | Repo name for the index. |
| `--project` | Yes | Path to dbt project directory (containing `dbt_project.yml`). |
| `--dialect` | No | SQL dialect for parsing compiled output (e.g. `starrocks`, `postgres`, `bigquery`). |
| `--env-file` | No | Path to `.env` file for dbt connection variables. |
| `--target` | No | dbt target name (e.g. `dev`, `prod`). Maps to the target in `profiles.yml`. |
| `--profiles-dir` | No | Path to directory containing `profiles.yml`. Defaults to the project directory. |
| `--dbt-command` | No | Base command to invoke dbt. `compile` is appended automatically. Default: `uv run dbt`. Use `dbt` if globally installed, or `uvx --with dbt-starrocks dbt` for ephemeral install. |

## Convention Commands

### `sqlprism conventions init`

Generate a `sqlprism.conventions.yml` file from inferred conventions. Includes confidence scores as comments.

```bash
sqlprism conventions init [--config PATH] [--db PATH] [--force]
```

| Parameter | Description |
|---|---|
| `--force` | Overwrite existing conventions file. Without this flag, init refuses to overwrite. |

### `sqlprism conventions refresh`

Re-run convention inference after reindex. Preserves explicit overrides (source: 'override').

```bash
sqlprism conventions refresh [--config PATH] [--db PATH]
```

### `sqlprism conventions diff`

Show what conventions changed since the last `init`. Compares the YAML file against current inferred conventions.

```bash
sqlprism conventions diff [--config PATH] [--db PATH]
```

## Query Commands

All query commands output JSON to stdout. They share common parameters:

| Parameter | Description |
|---|---|
| `--config PATH` | Path to config file. Default: auto-discovered. |
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

**Example:**
```bash
sqlprism query search "animal" --schema bronze --repo sqlmesh
```

### `sqlprism query references`

Find what depends on an entity (inbound) and what it depends on (outbound).

```bash
sqlprism query references NAME [--direction both|inbound|outbound] [--kind KIND] [--schema SCHEMA] [--repo REPO]
```

| Parameter | Description |
|---|---|
| `NAME` | Entity name (table, view, CTE, etc.). |
| `--direction` | `inbound` (what depends on this), `outbound` (what this depends on), or `both`. Default: `both`. |
| `--kind` | Filter by node kind to disambiguate. |
| `--schema` | Filter by SQL schema name. |

**Example:**
```bash
sqlprism query references "entity_event" --direction outbound --repo sqlmesh
```

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

**Example:**
```bash
sqlprism query column-usage "entity_event" --column db_name --usage-type join_on --repo sqlmesh
```

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

**Example:**
```bash
sqlprism query trace "view_animal_metadata" --direction downstream --max-depth 2 --repo sqlmesh
```

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

At least one filter should be provided to avoid returning the entire lineage graph.

**Example:**
```bash
sqlprism query lineage --table entity_event --column db_name --repo sqlmesh
```
