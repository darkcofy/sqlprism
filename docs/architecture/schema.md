# DuckDB Schema

The index is stored in a single DuckDB file (default: `~/.sqlprism/graph.duckdb`). Current schema version: **1.0**.

## Tables (9)

### `repos`

Registered repositories.

| Column | Type | Description |
|---|---|---|
| `repo_id` | INTEGER PK | Auto-increment ID. |
| `name` | TEXT UNIQUE | Repo name (used in queries to filter). |
| `path` | TEXT | Absolute path to the repo directory. |
| `repo_type` | TEXT | Repo type: `sql`, `dbt`, or `sqlmesh`. Default: `sql`. Used by `reindex_files` to select the correct renderer. |
| `last_commit` | TEXT | Git commit hash at last index time. |
| `last_branch` | TEXT | Git branch at last index time. |
| `indexed_at` | TIMESTAMP | When the repo was last indexed. |

### `files`

Indexed SQL files. Each file belongs to one repo.

| Column | Type | Description |
|---|---|---|
| `file_id` | INTEGER PK | Auto-increment ID. |
| `repo_id` | INTEGER | FK to `repos`. |
| `path` | TEXT | Relative path within the repo. |
| `language` | TEXT | Always `sql`. |
| `checksum` | TEXT | SHA-256 of file content. Used for incremental indexing. |
| `indexed_at` | TIMESTAMP | When this file was last parsed. |

Unique constraint: `(repo_id, path)`.

### `nodes`

SQL entities: tables, views, CTEs, queries.

| Column | Type | Description |
|---|---|---|
| `node_id` | INTEGER PK | Auto-increment ID. |
| `file_id` | INTEGER or NULL | FK to `files`. NULL for phantom nodes. |
| `kind` | TEXT | `table`, `view`, `cte`, or `query`. |
| `name` | TEXT | Entity name (dialect-normalised). |
| `schema` | TEXT or NULL | SQL schema (e.g. `bronze`, `silver`, `public`). |
| `language` | TEXT | Always `sql`. |
| `line_start` | INTEGER or NULL | First line in source file. |
| `line_end` | INTEGER or NULL | Last line in source file. |
| `metadata` | JSON or NULL | Extra metadata (e.g. `sqlmesh_model`, `dbt_model`). |

Unique constraint: `(file_id, kind, name, schema)`.

**Phantom nodes**: When a file is deleted or re-indexed, nodes that are referenced by edges from *other* files are kept with `file_id = NULL` rather than deleted. This preserves cross-file relationships. When the node reappears (e.g. the file is re-parsed), the phantom is merged with the new node.

### `edges`

Relationships between nodes.

| Column | Type | Description |
|---|---|---|
| `edge_id` | INTEGER PK | Auto-increment ID. |
| `source_id` | INTEGER | FK to `nodes` — the node that contains the reference. |
| `target_id` | INTEGER | FK to `nodes` — the node being referenced. |
| `relationship` | TEXT | e.g. `references`, `cte_reference`. |
| `context` | TEXT or NULL | e.g. `FROM clause`, `JOIN clause`. |
| `metadata` | JSON or NULL | Extra edge metadata. |

### `column_usage`

Per-column usage tracking across models.

| Column | Type | Description |
|---|---|---|
| `usage_id` | INTEGER PK | Auto-increment ID. |
| `node_id` | INTEGER | FK to `nodes` — the CTE/query where the column is used. |
| `table_name` | TEXT | Referenced table name (short name, not fully qualified). |
| `column_name` | TEXT | Column name. |
| `usage_type` | TEXT | `select`, `where`, `join_on`, `group_by`, `order_by`, `having`, `partition_by`, `window_order`, `insert`, `update`. |
| `alias` | TEXT or NULL | Output alias (for SELECT columns). |
| `transform` | TEXT or NULL | Transform expression (e.g. `CAST(x AS DATE)`, `SUM(amount)`). |
| `file_id` | INTEGER | FK to `files`. |

### `column_lineage`

End-to-end column lineage chains.

| Column | Type | Description |
|---|---|---|
| `lineage_id` | INTEGER PK | Auto-increment ID. |
| `file_id` | INTEGER | FK to `files`. |
| `output_node` | TEXT | Final output entity name (table/view/query). |
| `output_column` | TEXT | Output column name. |
| `chain_index` | INTEGER | Disambiguates multiple lineage paths for the same output column. |
| `hop_index` | INTEGER | Position in the chain (0 = source, increasing toward output). |
| `hop_column` | TEXT | Column name at this hop. |
| `hop_table` | TEXT | Table/CTE name at this hop. |
| `hop_expression` | TEXT or NULL | Transform expression at this hop (e.g. `CAST(x AS VARCHAR)`). |

A lineage chain traces one output column back to its source. Multiple chains (different `chain_index` values) exist when a column has multiple source paths (e.g. COALESCE of two columns).

### `columns`

Column definitions extracted from DDL or schema files.

| Column | Type | Description |
|---|---|---|
| `column_id` | INTEGER PK | Auto-increment ID. |
| `node_id` | INTEGER | FK to `nodes`. |
| `column_name` | TEXT | Column name. |
| `data_type` | TEXT or NULL | Column data type (e.g. `INTEGER`, `TEXT`). |
| `position` | INTEGER | Column ordinal position in the table. |
| `source` | TEXT | How the column was discovered: `definition`, `usage`, `lineage`. |

### `conventions`

Inferred or overridden project conventions per layer.

| Column | Type | Description |
|---|---|---|
| `convention_id` | INTEGER PK | Auto-increment ID. |
| `repo_id` | INTEGER | FK to `repos`. |
| `layer` | TEXT | Layer name (e.g. `staging`, `marts`). |
| `convention_type` | TEXT | `naming`, `references`, `required_columns`, or `column_style`. |
| `payload` | JSON | Convention data (pattern, allowed_targets, etc.). |
| `confidence` | FLOAT | Confidence score (0.0-1.0). |
| `source` | TEXT | `inferred` or `override`. |
| `model_count` | INTEGER | Number of models in this layer when inferred. |

Unique constraint: `(repo_id, layer, convention_type)`.

### `semantic_tags`

Semantic tags assigned to models by clustering or explicit override.

| Column | Type | Description |
|---|---|---|
| `tag_id` | INTEGER PK | Auto-increment ID. |
| `repo_id` | INTEGER | FK to `repos`. |
| `tag_name` | TEXT | Tag name (e.g. `customer`, `order`, `revenue`). |
| `node_id` | INTEGER | FK to `nodes`. |
| `confidence` | FLOAT | Confidence score (0.0-1.0). |
| `source` | TEXT | `inferred`, `anchor`, or `explicit`. |

Unique constraint: `(repo_id, tag_name, node_id)`.

## Indexes

```sql
CREATE INDEX idx_nodes_name ON nodes(name);
CREATE INDEX idx_nodes_kind ON nodes(kind);
CREATE INDEX idx_nodes_file ON nodes(file_id);
CREATE INDEX idx_nodes_kind_name ON nodes(kind, name);
CREATE INDEX idx_nodes_schema ON nodes(schema);
CREATE INDEX idx_edges_source ON edges(source_id);
CREATE INDEX idx_edges_target ON edges(target_id);
CREATE INDEX idx_edges_relationship ON edges(relationship);
CREATE INDEX idx_col_table ON column_usage(table_name);
CREATE INDEX idx_col_column ON column_usage(column_name);
CREATE INDEX idx_col_table_column ON column_usage(table_name, column_name);
CREATE INDEX idx_col_usage_type ON column_usage(usage_type);
CREATE INDEX idx_lineage_output ON column_lineage(output_node, output_column);
CREATE INDEX idx_lineage_hop ON column_lineage(hop_table, hop_column);
CREATE INDEX idx_lineage_file ON column_lineage(file_id);
CREATE INDEX idx_conventions_repo ON conventions(repo_id);
CREATE INDEX idx_conventions_type ON conventions(convention_type);
CREATE INDEX idx_tags_name ON semantic_tags(tag_name);
CREATE INDEX idx_tags_node ON semantic_tags(node_id);
CREATE INDEX idx_tags_repo ON semantic_tags(repo_id);
```

## Entity Relationship

```
repos 1──* files 1──* nodes
                       │
                  source_id / target_id
                       │
                      edges

files 1──* column_usage *──1 nodes
files 1──* column_lineage
nodes 1──* columns
repos 1──* conventions
repos 1──* semantic_tags *──1 nodes
```
