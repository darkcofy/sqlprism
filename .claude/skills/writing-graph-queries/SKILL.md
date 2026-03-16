---
name: writing-graph-queries
description: Reference for writing DuckPGQ graph queries against the sqlprism_graph property graph. Use when implementing graph tools (#31-36), writing MATCH/COLUMNS queries, or debugging graph query syntax errors.
---

# Writing DuckPGQ Graph Queries

## Property Graph Schema

```sql
CREATE PROPERTY GRAPH sqlprism_graph
  VERTEX TABLES (nodes)
  EDGE TABLES (edges
    SOURCE KEY (source_id) REFERENCES nodes (node_id)
    DESTINATION KEY (target_id) REFERENCES nodes (node_id))
```

**Edge direction**: `source_id → target_id` means the source model *references* the target.
- `stg_orders -[references]-> raw_orders` = stg_orders depends on raw_orders
- To find **upstream** (what a model depends on): follow edges forward (`->`)
- To find **downstream** (what depends on a model): follow edges backward (`<-`)

## Query Syntax

DuckPGQ uses SQL/PGQ syntax. The core pattern:

```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH <pattern>
  COLUMNS (<expressions>)
)
```

**Critical**: Use `COLUMNS (...)` not `RETURN`. The `RETURN` keyword is Cypher syntax and will cause a parser error.

## Verified Query Patterns

### 1. All vertices

```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (n:nodes)
  COLUMNS (n.node_id, n.name, n.kind)
)
```

### 2. Single-hop edges

```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes)-[e:edges]->(b:nodes)
  COLUMNS (a.name AS source, b.name AS target, e.relationship)
)
```

### 3. Filtered vertex query

```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->(b:nodes)
  COLUMNS (a.name AS model, b.name AS depends_on)
)
```

### 4. Bounded multi-hop paths (1–N hops)

```sql
-- Find all models within 3 hops upstream of stg_orders
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->{1,3}(b:nodes)
  COLUMNS (a.name AS model, b.name AS upstream)
)
```

**Important**: Always specify bounds `{min,max}`. Unbounded `+` causes an error:
> "ALL unbounded with path mode WALK is not possible as this could lead to infinite results"

### 5. Shortest path

```sql
-- Shortest path between two models (forward direction only)
FROM GRAPH_TABLE (sqlprism_graph
  MATCH p = ANY SHORTEST
    (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->{1,10}(b:nodes WHERE b.name = 'raw_orders')
  COLUMNS (a.name, b.name, path_length(p))
)
```

**Limitation**: Shortest path only works with forward edge direction (`->`). Using `<-` causes:
> "Not implemented Error: Cannot do shortest path for edge type MATCH_EDGE_LEFT"

### 6. PageRank

```sql
-- Returns (node_id, pagerank_score) for all vertices
SELECT * FROM pagerank(sqlprism_graph, nodes, edges)
```

Note: PageRank is a **scalar function**, not a GRAPH_TABLE query. It returns `(rowid, score)` where `rowid` maps to the node's position in the table.

### 7. Weakly Connected Components

```sql
-- Returns (node_id, component_id) — nodes in the same component share an ID
SELECT * FROM weakly_connected_component(sqlprism_graph, nodes, edges)
```

### 8. Local Clustering Coefficient

```sql
SELECT * FROM local_clustering_coefficient(sqlprism_graph, nodes, edges)
```

## Performance Tips

1. **Always bound multi-hop traversals** — use `{1,N}` not `+` or `*`
2. **Filter early** — put `WHERE` inside the `MATCH` pattern, not outside
3. **Use forward direction** (`->`) for shortest path — backward (`<-`) is not implemented
4. **Phantom nodes**: The graph includes phantom nodes (`file_id IS NULL`). Filter them in your COLUMNS or post-processing if needed: `WHERE n.file_id IS NOT NULL`
5. **PageRank/WCC/LCC** are table functions, not GRAPH_TABLE queries — call them directly with `SELECT * FROM func(graph, vertex_table, edge_table)`

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| `RETURN n.name` | Use `COLUMNS (n.name)` |
| `MATCH (a)-[e]->+(b)` | Use `MATCH (a)-[e]->{1,N}(b)` with bounds |
| `MATCH p = ANY SHORTEST (a)<-[e]->(b)` | Use forward direction `->` only |
| `FROM pagerank(g, n, e)` | Use `SELECT * FROM pagerank(g, n, e)` |
| Views as vertex tables | Not supported — use base tables only |