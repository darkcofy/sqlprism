# Configuration

`sqlprism init` creates a config file at `sqlprism.yml` in the working directory. You can override the path with `--config PATH` on any command. JSON is also supported (`--format json`).

## Full Config Example

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

## Top-Level Fields

| Field | Type | Description |
|---|---|---|
| `db_path` | string | Path to the DuckDB database file. Default: `~/.sqlprism/graph.duckdb`. |
| `sql_dialect` | string or null | Global default SQL dialect. Set to `null` for auto-detection. |
| `repos` | object | Plain SQL repositories to index. See [Plain SQL Repos](#plain-sql-repos). |
| `sqlmesh_repos` | object | SQLMesh projects. See [SQLMesh Integration](../guide/sqlmesh.md). |
| `dbt_repos` | object | dbt projects. See [dbt Integration](../guide/dbt.md). |

## Plain SQL Repos

Each entry in `repos` maps a repo name to either a path string or a config object.

**Simple form** — just a path:
```yaml
my-queries: /path/to/sql/repo
```

**Full form** — with dialect and overrides:
```yaml
my-repo:
  path: /path/to/repo
  dialect: starrocks
  dialect_overrides:
    athena/: athena
    postgres/: postgres
```

| Field | Type | Description |
|---|---|---|
| `path` | string | Absolute path to the repo directory. |
| `dialect` | string or null | SQL dialect for this repo. Overrides the global `sql_dialect`. |
| `dialect_overrides` | object | Per-directory dialect overrides. Keys are path prefixes or glob patterns, values are dialect names. |

## SQLMesh Repos

| Field | Required | Description |
|---|---|---|
| `project_path` | Yes | Path to the sqlmesh project directory (containing `config.yaml`). |
| `env_file` | No | Path to `.env` file. Loaded into the subprocess environment before rendering. |
| `dialect` | No | SQL dialect for rendering. Default: `athena`. |
| `variables` | No | SQLMesh macro variables as key-value pairs (e.g. `GRACE_PERIOD: 7`). |
| `sqlmesh_command` | No | Command to run Python in the sqlmesh project's venv. Default: `uv run python`. |

## dbt Repos

| Field | Required | Description |
|---|---|---|
| `project_path` | Yes | Path to dbt project directory (containing `dbt_project.yml`). |
| `env_file` | No | Path to `.env` file for dbt connection variables. |
| `target` | No | dbt target name (e.g. `dev`, `prod`). |
| `dialect` | No | SQL dialect for parsing compiled output (e.g. `starrocks`, `postgres`). |
| `dbt_command` | No | Base command to invoke dbt. Default: `uv run dbt`. `compile` is appended automatically. |
| `profiles_dir` | No | Path to directory containing `profiles.yml`. Defaults to the project directory. |

## SQL Dialect Support

Powered by [sqlglot](https://github.com/tobymao/sqlglot), the indexer supports **33 SQL dialects**:

Athena, BigQuery, ClickHouse, Databricks, DuckDB, Doris, Dremio, Drill, Druid, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL.

Pass the dialect name as a lowercase string (e.g. `"starrocks"`, `"bigquery"`, `"athena"`). Dialect-specific quoting and case normalization are handled automatically.
