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

On a 200-model [SQLMesh](https://sqlmesh.com/) project, a column impact query returns **75 structured results in ~5,000 tokens**. The grep equivalent would need **40-60 files opened, ~100,000+ tokens**, and still wouldn't tell you whether a column appears in a WHERE filter or a SELECT.

## Setup

### 1. Install

```bash
git clone https://github.com/darkcofy/sqlprism.git && cd sqlprism
uv sync
```

### 2. Configure

```bash
uv run sqlprism init                    # creates sqlprism.yml in the current directory
# edit sqlprism.yml to add your repos (see Configuration below)
uv run sqlprism reindex                 # index plain SQL repos
```

For [dbt](https://www.getdbt.com/) and [SQLMesh](https://sqlmesh.com/) projects, use `reindex-dbt` and `reindex-sqlmesh` respectively. See the [CLI guide](https://darkcofy.github.io/sqlprism/guide/cli/) for full options.

> **Prerequisite:** dbt and SQLMesh are **not** dependencies of sqlprism. The renderers shell out to `dbt compile` / `sqlmesh` inside the target project's own virtualenv (via `uv run` by default). Install the renderer in that project — for example `uv add dbt-core dbt-<adapter>` or `uv add sqlmesh` — before running `reindex-dbt` / `reindex-sqlmesh`. If the renderer is missing, sqlprism will raise a clear error pointing at the project directory.

### 3. Connect your MCP client

**Claude Code:**
```bash
claude mcp add sqlprism -- uv run --directory /path/to/sqlprism sqlprism serve
```

**Claude Desktop / Cursor / Continue.dev** (`.mcp.json`):
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

Replace `/path/to/sqlprism` with the absolute path to your clone.

### 4. Reindex on Save

The graph stays fresh automatically when you set up on-save hooks. There are two modes depending on your editor.

#### Claude Code

Add a [PostToolUse hook](https://docs.anthropic.com/en/docs/claude-code/hooks) so the index updates whenever Claude writes or edits a file. Save this as `.claude/settings.json` in your project root:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "FILE=$(cat | jq -r '.tool_input.file_path // empty'); [ -n \"$FILE\" ] && [[ \"$FILE\" =~ \\.sql$ ]] && sqlprism reindex-file \"$FILE\" || true"
          }
        ]
      }
    ]
  }
}
```

This extracts the file path from the hook's stdin JSON, checks it's a `.sql` file, and calls the CLI to reindex it. Run `/hooks` in Claude Code to verify the hook is active.

#### Other MCP clients (Cursor, Continue.dev)

The `reindex_files` MCP tool accepts absolute file paths and reindexes only the affected models. Plain SQL reindexes in ~50ms; [dbt](https://www.getdbt.com/)/[SQLMesh](https://sqlmesh.com/) models compile + reindex in ~2-5s. Calls are debounced per repo (500ms for SQL, 2s for rendered models) so rapid saves batch into a single operation.

Configure your client to call `reindex_files` with the saved file's path on save.

#### Editors without MCP (Vim, Neovim, Emacs, VS Code tasks)

The `reindex-file` CLI command works standalone — no running server needed:

```bash
sqlprism reindex-file /path/to/model.sql
```

**Vim / Neovim:**
```vim
autocmd BufWritePost *.sql silent !sqlprism reindex-file %:p
```

**Emacs:**
```elisp
(add-hook 'after-save-hook
  (lambda ()
    (when (string-match-p "\\.sql\\'" buffer-file-name)
      (start-process "sqlprism" nil "sqlprism" "reindex-file" buffer-file-name))))
```

**VS Code** (using the [Run on Save](https://marketplace.visualstudio.com/items?itemName=emeraldwalk.RunOnSave) extension, `.vscode/settings.json`):
```json
{
  "emeraldwalk.runonsave": {
    "commands": [
      {
        "match": "\\.sql$",
        "cmd": "sqlprism reindex-file ${file}"
      }
    ]
  }
}
```

## Configuration

`sqlprism init` creates a default config at `sqlprism.yml` in the working directory. YAML is the default format; JSON is also supported (`--format json`). Existing `sqlprism.json` files are auto-discovered for backwards compatibility. Override the config path with `--config PATH` on any command.

```yaml
db_path: ~/.sqlprism/graph.duckdb
sql_dialect: null
repos:
  my-queries: /path/to/sql/repo
  multi-dialect-repo:
    path: /path/to/repo
    dialect: starrocks
    dialect_overrides:
      athena/: athena
      postgres/: postgres
sqlmesh_repos:
  my-sqlmesh-project:
    project_path: /path/to/sqlmesh/folder
    env_file: /path/to/.env
    dialect: athena
    variables:
      GRACE_PERIOD: 7
dbt_repos:
  my-dbt-project:
    project_path: /path/to/dbt/project
    env_file: /path/to/.env
    target: dev
    dialect: starrocks
    dbt_command: uv run dbt
```

| Field | Description |
|---|---|
| `db_path` | Path to the DuckDB database file. Defaults to `~/.sqlprism/graph.duckdb`. |
| `sql_dialect` | Global default SQL dialect. `null` for auto-detect. |
| `repos` | Plain SQL repos. Value is a path string or an object with `path`, `dialect`, `dialect_overrides`. |
| `dialect` | Per-repo dialect override (e.g. `"starrocks"`, `"athena"`, `"bigquery"`). |
| `dialect_overrides` | Per-directory overrides using prefix matching or glob patterns. |
| `sqlmesh_repos` | [SQLMesh](https://sqlmesh.com/) projects. Renders models before parsing. |
| `dbt_repos` | [dbt](https://www.getdbt.com/) projects. Compiles models before parsing. |

## SQL Dialect Support

Powered by [sqlglot](https://github.com/tobymao/sqlglot), the indexer supports **33 SQL dialects** out of the box:

Athena, BigQuery, ClickHouse, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL.

Pass the dialect name as a lowercase string (e.g. `"starrocks"`, `"bigquery"`, `"athena"`). Dialect-specific quoting and identifier case normalization are handled automatically.

## CLI Commands

Full reference: [CLI guide](https://darkcofy.github.io/sqlprism/guide/cli/)

| Command | Description |
|---|---|
| `sqlprism init` | Create default config file. |
| `sqlprism reindex` | Incremental reindex of plain SQL repos. |
| `sqlprism reindex-file` | Fast on-save reindex of specific files. |
| `sqlprism reindex-dbt` | Compile and index a [dbt](https://www.getdbt.com/) project. |
| `sqlprism reindex-sqlmesh` | Render and index a [SQLMesh](https://sqlmesh.com/) project. |
| `sqlprism serve` | Start the MCP server (stdio or HTTP). |
| `sqlprism conventions init` | Generate `sqlprism.conventions.yml` from inferred conventions. |
| `sqlprism conventions refresh` | Re-run convention inference after reindex. |
| `sqlprism conventions diff` | Show what changed since last `--init`. |
| `sqlprism status` | Show index status. |
| `sqlprism query search` | Find entities by name pattern. |
| `sqlprism query references` | Find inbound/outbound dependencies. |
| `sqlprism query column-usage` | Find column usage across models. |
| `sqlprism query trace` | Multi-hop dependency tracing. |
| `sqlprism query lineage` | End-to-end column lineage chains. |

## MCP Tools

Full reference: [MCP tools guide](https://darkcofy.github.io/sqlprism/guide/mcp-tools/)

When running as an MCP server (`sqlprism serve`), 24 tools are exposed:

| Tool | Description |
|---|---|
| `search` | Find entities by name pattern with pagination. |
| `find_references` | Inbound/outbound dependencies with snippets. |
| `find_column_usage` | Column usage — type, transforms, aliases. |
| `trace_dependencies` | Multi-hop upstream/downstream chains. |
| `trace_column_lineage` | End-to-end column lineage through CTEs. |
| `get_schema` | Table/view schema with columns, types, and dependencies. |
| `get_context` | One-call comprehensive context dump for a model. |
| `find_path` | Shortest path between two models (DuckPGQ). |
| `find_critical_models` | Rank models by PageRank importance (DuckPGQ). |
| `detect_cycles` | Find circular dependencies in the graph. |
| `find_subgraphs` | Identify disconnected clusters and orphaned models (DuckPGQ). |
| `find_bottlenecks` | High fan-out models with risk classification. |
| `check_impact` | Column-level impact analysis before making changes. |
| `pr_impact` | Structural diff + blast radius since a base commit. |
| `reindex` | Background incremental reindex of SQL repos. |
| `reindex_files` | Fast on-save reindex with per-repo debounce. |
| `reindex_dbt` | Background dbt compile + index. |
| `reindex_sqlmesh` | Background SQLMesh render + index. |
| `get_conventions` | Inferred project conventions — naming, references, columns. |
| `find_similar_models` | Find existing models similar to what you're building. |
| `suggest_placement` | Recommend where to place a new model based on references. |
| `search_by_tag` | Find models by semantic tag (business domain concept). |
| `list_tags` | List all semantic tags with model counts and confidence. |
| `index_status` | Index stats, cross-repo edges, and name collisions. |

## Architecture

```
src/sqlprism/
  types.py              <- ParseResult, NodeResult, EdgeResult, ColumnUsageResult, parse_repo_config
  languages/
    __init__.py         <- SQL_EXTENSIONS, is_sql_file()
    sql.py              <- sqlglot: tables, views, CTEs, column lineage, transforms
    sqlmesh.py          <- SQLMesh renderer (full project + selective render_models)
    dbt.py              <- dbt renderer (full project + selective render_models via --select)
    utils.py            <- Shared helpers (find_venv_dir, parse_dotenv, build_env, enrich_nodes)
  core/
    graph.py            <- DuckDB storage layer (MVCC), queries, snippets, repo_type tracking
    indexer.py          <- Orchestrator: scan -> checksum -> parse -> store; file-level reindex with repo-type dispatch
    mcp_tools.py        <- FastMCP tool definitions (24 tools, non-blocking reindex, per-repo debounce)
    conventions.py      <- Convention inference engine: layers, naming, references, tags, overrides
  cli.py                <- Click CLI: serve, reindex, reindex-file, reindex-sqlmesh, reindex-dbt, conventions, status, init
```

The SQL parser extracts:
- **Nodes**: tables, views, CTEs, queries (with schema metadata and dialect-aware case normalization)
- **Edges**: table references, CTE references, JOINs (with context like "FROM clause", "JOIN clause")
- **Column usage**: per-column tracking with usage type (select, where, join_on, group_by, order_by, having, partition_by, window_order), transforms (CAST, COALESCE, SUM, etc.), output aliases, and WHERE filter expressions
- **Column lineage**: end-to-end tracing through CTEs and subqueries back to source tables, with SELECT * expansion when schema catalog is available

Full architecture docs: [Architecture overview](https://darkcofy.github.io/sqlprism/architecture/overview/) | [DuckDB schema](https://darkcofy.github.io/sqlprism/architecture/schema/)

### DuckPGQ Graph Analytics

SQLPrism optionally integrates with [DuckPGQ](https://github.com/cwida/duckpgq) for advanced graph analytics. When installed, these tools become available: `find_path`, `find_critical_models`, `find_subgraphs`, `find_bottlenecks` (clustering enrichment). DuckPGQ is installed automatically on first use — no manual setup needed.

## Development

```bash
uv sync
uv run pytest                          # run tests (510+ tests)
uv run pytest --cov=sqlprism           # run with coverage report
uv run pytest --cov=sqlprism --cov-report=html:coverage_html  # HTML report
```

### Code Coverage

![Coverage Grid](https://codecov.io/github/darkcofy/sqlprism/graphs/tree.svg?token=8H5XNZEFOW)

## License

Apache License 2.0 — see [LICENSE](LICENSE).