# dbt Integration

The indexer compiles dbt models via `dbt compile`, then parses the resulting SQL from the `target/compiled/` directory.

## How It Works

1. The indexer runs `dbt compile --project-dir ... --profiles-dir ...` via subprocess.
2. dbt expands all Jinja macros, refs, and sources into clean SQL files under `target/compiled/<project_name>/models/`.
3. The indexer reads each compiled `.sql` file, wraps it as a `CREATE TABLE` statement (dbt compiled SQL is bare SELECT), and parses it with sqlglot.
4. Nodes, edges, column usage, and column lineage are extracted and stored in the graph.

No dbt Python dependency is needed in the indexer's environment — it shells out to whatever `dbt` command you specify.

## CLI Usage

```bash
sqlprism reindex-dbt \
  --name my-project \
  --project /path/to/dbt/project \
  --dialect starrocks \
  --env-file /path/to/.env \
  --target dev
```

## Config Usage

Add to `dbt_repos` in `~/.sqlprism/config.json`:

```json
{
  "dbt_repos": {
    "my-project": {
      "project_path": "/path/to/dbt/project",
      "env_file": "/path/to/.env",
      "target": "dev",
      "dialect": "starrocks",
      "dbt_command": "uv run dbt"
    }
  }
}
```

## dbt Command

The `--dbt-command` parameter is the **base command** to invoke dbt. The indexer appends `compile --project-dir ... --profiles-dir ...` automatically.

Examples:

| Command | When to use |
|---|---|
| `uv run dbt` (default) | dbt installed in the project's `.venv` via uv |
| `dbt` | dbt installed globally or in active venv |
| `uvx --with dbt-starrocks dbt` | Ephemeral install with a specific adapter |

## Profiles

By default, the indexer looks for `profiles.yml` in the project directory. Use `--profiles-dir` to point elsewhere (e.g. `~/.dbt`).

## Environment Variables

Use `--env-file` to load variables needed by `profiles.yml` (database host, credentials, etc.). These are loaded into the subprocess environment before running `dbt compile`.

## Dialect

The `--dialect` parameter tells the sqlglot parser how to interpret the compiled SQL. This should match your target database:

- StarRocks uses backtick quoting
- Postgres uses double-quote quoting
- Snowflake uses uppercase identifiers

If dialect is wrong, you may get parse errors or incorrect identifier resolution.

## SELECT * Lineage

The indexer expands `SELECT *` into individual column lineage chains when a schema catalog is available. The catalog is built from column usage records in previous indexes. On a fresh database, the first index will not expand `SELECT *` — run a second full reindex to populate the catalog.

## Venv Detection

The indexer auto-detects where the `.venv` lives. If your dbt project is nested (e.g. `dbt/dp_starrocks/` but `.venv` is at `dbt/`), the indexer searches parent directories for the venv.
