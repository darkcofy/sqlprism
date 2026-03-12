# SQLMesh Integration

The indexer renders SQLMesh models into clean SQL before parsing, so macros like `@start_dt`, `@end_dt`, and custom variables are fully expanded.

## How It Works

1. The indexer runs an inline Python script inside the **sqlmesh project's own virtualenv** (via `uv run python`).
2. The script creates a local DuckDB gateway (no remote connections needed), loads the project context, and renders each model.
3. Rendered SQL is returned as JSON to the indexer process.
4. The indexer parses each rendered model with sqlglot, extracting nodes, edges, column usage, and column lineage.

No sqlmesh Python dependency is needed in the indexer's environment — it uses whatever version the project already has installed.

## CLI Usage

```bash
sqlprism reindex-sqlmesh \
  --name my-project \
  --project /path/to/sqlmesh/project \
  --dialect athena \
  --env-file /path/to/.env \
  --var GRACE_PERIOD 7
```

## Config Usage

Add to `sqlmesh_repos` in `~/.sqlprism/config.json`:

```json
{
  "sqlmesh_repos": {
    "my-project": {
      "project_path": "/path/to/sqlmesh/project",
      "env_file": "/path/to/.env",
      "dialect": "athena",
      "variables": {
        "GRACE_PERIOD": 7
      }
    }
  }
}
```

## Variables

SQLMesh projects often define macro variables in `config.yaml` under the `variables:` key. However, the indexer creates a separate lightweight context (with a local DuckDB gateway) that doesn't inherit these variables.

You must pass variables explicitly:

- **CLI**: `--var KEY VALUE` (repeatable)
- **Config**: `"variables": {"KEY": value}`

If a variable is missing, the render will fail with `Macro variable 'X' is undefined.`

## Environment Variables

Use `--env-file` to load environment variables needed by the sqlmesh `config.yaml` (e.g. `S3_WAREHOUSE_LOCATION`, database credentials). These are loaded into the subprocess environment before rendering.

## Dialect

The `--dialect` parameter controls the SQL dialect used for rendering output. This should match the dialect in your sqlmesh `config.yaml` `model_defaults`. Common values: `athena`, `bigquery`, `postgres`, `starrocks`.

## SELECT * Lineage

The indexer expands `SELECT *` into individual column lineage chains when a schema catalog is available. The catalog is built from column usage records in previous indexes. On a fresh database, the first index will not expand `SELECT *` — run a second full reindex to populate the catalog.

## Venv Detection

The indexer auto-detects where the `.venv` lives by searching from the project path upward. If your venv is in a non-standard location, the render script will use whatever `uv run` resolves to from the project directory.
