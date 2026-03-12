# MCP Tools

When running as an MCP server (`sqlprism serve`), 10 tools are exposed. Any MCP client (Claude Code, Claude Desktop, Cursor, Continue.dev) can call these.

## Query Tools

### `search`

Find tables, views, CTEs, and queries by name pattern.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `pattern` | string | Yes | | Partial name match, case-insensitive. |
| `kind` | string | No | | Filter: `table`, `view`, `cte`, `query`. |
| `schema` | string | No | | Filter by SQL schema (e.g. `bronze`, `silver`). |
| `repo` | string | No | | Filter by repo name. Omit to search all. |
| `limit` | int | No | 20 | Max results (1-100). |
| `offset` | int | No | 0 | Skip N results for pagination. |
| `include_snippets` | bool | No | true | Include source code snippets in results. |

### `find_references`

Find everything connected to a named SQL entity — both inbound (what depends on this) and outbound (what this depends on).

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | Yes | | Entity name (table, view, CTE). |
| `kind` | string | No | | Filter by node kind to disambiguate. |
| `schema` | string | No | | Filter by SQL schema. |
| `repo` | string | No | | Filter by repo name. |
| `direction` | string | No | `both` | `inbound`, `outbound`, or `both`. |
| `include_snippets` | bool | No | true | Include source code snippets. |
| `limit` | int | No | 100 | Max results per direction (1-500). |
| `offset` | int | No | 0 | Skip N results for pagination. |

### `find_column_usage`

Find where and how columns are used across SQL models. Shows usage type, transforms (CAST, COALESCE, etc.), output aliases, and WHERE conditions.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `table` | string | Yes | | Table name to search column usage for. |
| `column` | string | No | | Specific column name. Omit for all columns. |
| `usage_type` | string | No | | Filter: `select`, `where`, `join_on`, `group_by`, `order_by`, `having`, `insert`, `update`. |
| `repo` | string | No | | Filter by repo name. |
| `limit` | int | No | 100 | Max results (1-500). |
| `offset` | int | No | 0 | Skip N results for pagination. |

### `trace_dependencies`

Trace multi-hop dependency chains through the SQL graph. Use for impact analysis: "if I change this table, what models break?"

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | Yes | | Starting entity name. |
| `kind` | string | No | | Filter by node kind to disambiguate. |
| `direction` | string | No | `downstream` | `upstream`, `downstream`, or `both`. |
| `max_depth` | int | No | 3 | Maximum hops (1-6). |
| `repo` | string | No | | Filter by repo name. |
| `include_snippets` | bool | No | false | Include source code snippets (can be large for traces). |
| `limit` | int | No | 100 | Max results (1-500). |

### `trace_column_lineage`

Trace end-to-end column lineage through CTEs and subqueries. Shows how an output column traces back to source table columns.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `table` | string | No | | Source or intermediate table name. |
| `column` | string | No | | Column name to trace. |
| `output_node` | string | No | | Output entity name to trace lineage from. |
| `repo` | string | No | | Filter by repo name. |
| `limit` | int | No | 100 | Max lineage chains (1-500). |
| `offset` | int | No | 0 | Skip N chains for pagination. |

### `pr_impact`

Analyse the structural impact of SQL changes since a base commit. Computes structural diff (added/removed/modified tables, views, CTEs, column usage) then traces the blast radius.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `base_commit` | string | Yes | | Git commit hash or ref to compare against (e.g. `main`, `abc123f`). |
| `repo` | string | No | | Repo to analyse. Required if multiple repos configured. |
| `max_blast_radius_depth` | int | No | 3 | Hops to trace from changed nodes (1-6). |
| `compare_mode` | string | No | `delta` | `delta` = net-new impact vs base (default). `absolute` = total blast radius (v1 behavior). |

#### Delta Mode (default)

In delta mode, `pr_impact` compares the blast radius at HEAD against an approximation of the base branch's blast radius. The response includes:

- `head_total` / `base_total` — blast radius count on each branch
- `delta` — change in blast radius size
- `newly_affected` — models that are *newly* in the blast radius due to this PR
- `no_longer_affected` — models that were in the blast radius on base but aren't anymore
- `unchanged_affected` — models in the blast radius on both branches

This tells reviewers what *net-new risk* the PR introduces, filtering out pre-existing downstream dependencies.

!!! note
    Delta mode captures **net-new downstream impact** only. It does not detect reduced blast radius from removed edges — `no_longer_affected` may be undercounted when a PR removes dependencies.

#### Absolute Mode

Set `compare_mode: "absolute"` for v1 behavior — total downstream blast radius without comparison to base.

## Index Management Tools

### `reindex`

Trigger an incremental reindex of plain SQL repos. Checksums files and only re-parses what changed.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `repo` | string | No | | Specific repo to reindex. Omit for all. |

> **Non-blocking:** Reindex runs in the background. The tool returns immediately with `{"status": "started"}`. Queries remain available during reindex (reads use DuckDB MVCC snapshots). Call `index_status` to check progress. If a reindex is already running, returns the current status instead of starting a new one.

### `reindex_sqlmesh`

Render and index a SQLMesh project. Uses sqlmesh's rendering engine to expand macros and resolve variables.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | Yes | | Repo name for the index. |
| `project_path` | string | Yes | | Path to sqlmesh project dir (containing `config.yaml`). |
| `env_file` | string | No | | Path to `.env` file for config variables. |
| `dialect` | string | No | `athena` | SQL dialect for rendering. |
| `variables` | object | No | | SQLMesh variables as key-value pairs (e.g. `{"GRACE_PERIOD": "7"}`). |
| `sqlmesh_command` | string | No | `uv run python` | Command to run python in sqlmesh venv. |

> **Non-blocking:** Reindex runs in the background. The tool returns immediately with `{"status": "started"}`. Queries remain available during reindex (reads use DuckDB MVCC snapshots). Call `index_status` to check progress. If a reindex is already running, returns the current status instead of starting a new one.

### `reindex_dbt`

Compile and index a dbt project. Runs `dbt compile`, then parses the compiled SQL.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | Yes | | Repo name for the index. |
| `project_path` | string | Yes | | Path to dbt project dir (containing `dbt_project.yml`). |
| `profiles_dir` | string | No | | Path to directory containing `profiles.yml`. |
| `env_file` | string | No | | Path to `.env` file for dbt connection variables. |
| `target` | string | No | | dbt target name. |
| `dbt_command` | string | No | `uv run dbt` | Command to invoke dbt. `compile` is appended automatically. |
| `dialect` | string | No | | SQL dialect for parsing (e.g. `starrocks`, `mysql`). |

> **Non-blocking:** Reindex runs in the background. The tool returns immediately with `{"status": "started"}`. Queries remain available during reindex (reads use DuckDB MVCC snapshots). Call `index_status` to check progress. If a reindex is already running, returns the current status instead of starting a new one.

### `index_status`

Returns the current state of the index — repos, file counts, node counts, last commit, staleness. When a background reindex is in progress, includes `reindex_in_progress: true` and `reindex_status` with the current state. After completion, includes `last_reindex` with the result. No parameters.

## Pagination

`search`, `find_references`, `find_column_usage`, and `trace_column_lineage` all support `limit` and `offset` for paginating through large result sets:

- **`limit`**: Max results to return per call.
- **`offset`**: Number of results to skip. To get page 2 of 100-result pages, set `offset: 100`.

## Snippets

`search`, `find_references`, and `trace_dependencies` support `include_snippets`:

- When `true`, results include source code context around each match.
- Default is `true` for `search` and `find_references`, `false` for `trace_dependencies` (which can produce very large output).
