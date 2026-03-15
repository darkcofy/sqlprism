# Architecture Overview

## Data Flow

```
                              ┌─────────────────────────────────────┐
                              │           Entry Points              │
                              │                                     │
                              │  CLI (cli.py)    MCP Server          │
                              │  sqlprism     (mcp_tools.py)     │
                              │  reindex/query   search/trace/...   │
                              └──────────┬──────────┬───────────────┘
                                         │          │
                              ┌──────────▼──────────▼───────────────┐
                              │         Indexer (indexer.py)         │
                              │                                     │
                              │  Orchestrator — never parses SQL    │
                              │  directly, never writes to DB       │
                              │  directly.                          │
                              │                                     │
                              │  1. Scan repo for .sql files        │
                              │  2. Checksum files (skip unchanged) │
                              │  3. Dispatch to correct parser      │
                              │  4. Store ParseResult in GraphDB    │
                              └──┬──────────┬──────────┬────────────┘
                                 │          │          │
              ┌──────────────────▼┐  ┌──────▼───────┐ ┌▼──────────────────┐
              │  SqlParser         │  │ SqlMesh      │ │ DbtRenderer       │
              │  (sql.py)          │  │ Renderer     │ │ (dbt.py)          │
              │                    │  │ (sqlmesh.py) │ │                   │
              │  sqlglot AST →     │  │              │ │ dbt compile →     │
              │  nodes, edges,     │  │ subprocess → │ │ subprocess →      │
              │  column_usage,     │  │ render all/  │ │ read compiled/    │
              │  column_lineage    │  │ selected     │ │ all or --select   │
              │                    │  │ models →     │ │ models →          │
              │                    │  │ SqlParser    │ │ SqlParser         │
              └────────┬───────────┘  └──────┬───────┘ └┬──────────────────┘
                       │                     │          │
                       │    ParseResult      │          │
                       │  ┌────────────────┐ │          │
                       └──►  nodes[]       ◄─┘          │
                          │  edges[]       ◄────────────┘
                          │  column_usage[]│
                          │  column_lineage[] │
                          └───────┬────────┘
                                  │
                       ┌──────────▼──────────────────────┐
                       │       GraphDB (graph.py)         │
                       │                                  │
                       │  DuckDB storage layer            │
                       │                                  │
                       │  ┌─────────┐  ┌────────┐        │
                       │  │  repos  │  │ files  │        │
                       │  └─────────┘  └────────┘        │
                       │  ┌─────────┐  ┌────────┐        │
                       │  │  nodes  │  │ edges  │        │
                       │  └─────────┘  └────────┘        │
                       │  ┌──────────────┐ ┌───────────┐ │
                       │  │column_usage  │ │col_lineage│ │
                       │  └──────────────┘ └───────────┘ │
                       │                                  │
                       │  Query methods:                  │
                       │  query_search, query_references, │
                       │  query_trace, query_column_usage,│
                       │  query_column_lineage            │
                       └──────────────────────────────────┘
```

## Component Responsibilities

### Separation of Concerns

The architecture enforces a strict separation:

- **Parsers** (`sql.py`, `sqlmesh.py`, `dbt.py`) produce `ParseResult` objects. They never touch the database.
- **GraphDB** (`graph.py`) stores and queries data. It never parses SQL.
- **Indexer** (`indexer.py`) orchestrates the pipeline. It calls parsers, then stores results via GraphDB.
- **MCP Tools / CLI** are thin wrappers that call Indexer (for writes) or GraphDB (for reads).

### SqlParser

The core SQL parser uses [sqlglot](https://github.com/tobymao/sqlglot) to build an AST and extract:

- **Nodes**: Tables, views, CTEs, and top-level queries. Each has a name, kind, optional schema, and line numbers.
- **Edges**: Relationships between nodes — table references from FROM/JOIN clauses, CTE references, subquery dependencies.
- **Column usage**: Per-column tracking with usage type (select, where, join_on, group_by, etc.), transforms (CAST, COALESCE, SUM), aliases, and filter expressions.
- **Column lineage**: End-to-end tracing from output columns back through CTEs and subqueries to source table columns.

### SQLMesh Renderer

Runs an inline Python script inside the sqlmesh project's virtualenv. The script:

1. Creates a lightweight `Context` with a local DuckDB gateway (no remote connections)
2. Passes user-supplied variables
3. Calls `context.render(model_name)` for each model
4. Returns rendered SQL as JSON

The rendered SQL is then parsed by `SqlParser`.

### dbt Renderer

Runs `dbt compile` via subprocess. Reads the compiled SQL from `target/compiled/<project_name>/models/`, wraps each as `CREATE TABLE ... AS <sql>`, and feeds it to `SqlParser`.

### GraphDB

DuckDB-backed storage with 6 tables (see [Schema](schema.md)). Key design decisions:

- **Read/write separation**: Read queries use fresh cursors for MVCC snapshot isolation. Write operations use a dedicated write lock. This allows queries to execute concurrently during reindex — no more blocking.
- **Incremental indexing**: Files are checksummed. Only changed files are re-parsed.
- **Phantom nodes**: When a file is deleted/re-indexed, nodes that are referenced from other files become "phantoms" (file_id=NULL) rather than being deleted. This preserves cross-file edges.
- **Batch inserts**: Nodes, edges, column_usage, and column_lineage are inserted in batches for performance.
- **Schema-aware**: Nodes track their SQL schema (e.g. `bronze`, `silver`) separately from their name.

### Indexer

The orchestrator for all indexing operations:

1. **`reindex_repo`**: Scan directory → checksum → parse changed files → store results
2. **`reindex_sqlmesh`**: Render via subprocess → parse rendered SQL → store results
3. **`reindex_dbt`**: Compile via subprocess → parse compiled SQL → store results
4. **`reindex_files`**: Resolve files to repos → dispatch by repo type (SQL/dbt/sqlmesh) → selective reindex

The indexer also handles:

- File-to-repo resolution (matching file paths to configured repos, picking deepest match for nested repos)
- File-level deletion (cleans up old data before inserting new)
- Edge resolution (matching edge target names to actual node IDs)
- Git metadata (storing current commit and branch per repo)
- Per-file error resilience (unreadable files are skipped, not fatal)

## Control Flow: Indexing

```
reindex_repo("my-repo", "/path/to/repo", dialect="starrocks")
  │
  ├─ scan directory for .sql files
  ├─ for each file:
  │    ├─ compute checksum
  │    ├─ skip if unchanged (checksum matches DB)
  │    ├─ parse(file_path, content, dialect) → ParseResult
  │    │    ├─ sqlglot.parse(content, dialect=dialect)
  │    │    ├─ walk AST → extract nodes, edges
  │    │    ├─ extract column_usage per node
  │    │    └─ extract column_lineage chains
  │    ├─ delete old file data from DB
  │    ├─ insert_file(repo_id, path, checksum)
  │    ├─ insert_nodes_batch(nodes)
  │    ├─ insert_edges_batch(edges)  ← resolves target node IDs
  │    ├─ insert_column_usage_batch(column_usage)
  │    └─ insert_column_lineage_batch(column_lineage)
  │
  ├─ remove files no longer on disk
  └─ return stats {scanned, added, changed, removed, nodes, edges}
```

## Control Flow: Querying

```
query_search("animal", kind="table", schema="bronze", limit=20)
  │
  └─ GraphDB.query_search()
       ├─ SQL: SELECT from nodes JOIN files JOIN repos
       │       WHERE name ILIKE '%animal%'
       │       AND kind = 'table' AND schema = 'bronze'
       │       LIMIT 20 OFFSET 0
       ├─ optionally attach source code snippets
       └─ return {matches: [...], total_count: N}

query_trace("entity_event", direction="downstream", max_depth=3)
  │
  └─ GraphDB.query_trace()
       ├─ find starting node(s)
       ├─ BFS traversal through edges table
       │   depth 1: entity_event → bovine_event_weight, ovine_event_mating, ...
       │   depth 2: bovine_event_weight → computed_weight, ...
       │   depth 3: computed_weight → dashboard_weights, ...
       └─ return {root, paths: [...], depth_summary: {1: N, 2: N, 3: N}}

reindex (MCP tool, non-blocking)
  │
  ├─ If reindex already running → return current status
  ├─ Launch asyncio background task
  │    └─ asyncio.to_thread(_blocking_reindex)
  │         ├─ for each repo: indexer.reindex_repo(...)
  │         └─ update _reindex_status on completion/failure
  ├─ Return immediately: {"status": "started"}
  └─ Queries continue serving via MVCC snapshots

reindex_files (MCP tool, on-save fast path)
  │
  ├─ Filter to SQL files only
  ├─ Resolve each file → (repo_name, repo_type) via _resolve_file_repo
  ├─ Enqueue per repo with debounce:
  │    ├─ SQL repos: 500ms debounce window
  │    └─ dbt/sqlmesh repos: 2s debounce window
  ├─ Return immediately: {"accepted": N}
  └─ On debounce timer fire (_flush_reindex):
       ├─ Deduplicate accumulated paths
       ├─ Acquire _reindex_lock (waits if full reindex running)
       └─ indexer.reindex_files(paths)
            ├─ SQL: read → parse → atomic insert (~50ms)
            ├─ dbt: dbt compile --select model → parse → insert (~2-5s)
            └─ sqlmesh: render with model filter → parse → insert (~2-5s)
```
