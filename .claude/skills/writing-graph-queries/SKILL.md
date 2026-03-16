---
name: writing-graph-queries
description: Provides verified DuckPGQ query patterns for the sqlprism_graph property graph. Triggers when writing SQL/PGQ MATCH queries, implementing graph tools (#31-36), calling pagerank/wcc/lcc functions, or debugging DuckPGQ syntax errors like "RETURN" or unbounded path errors.
---

# DuckPGQ Query Reference

## Graph schema

```sql
-- Vertex: nodes table. Edge: edges table (source_id -> target_id).
-- Edge direction: source REFERENCES target (stg_orders -> raw_orders = stg depends on raw).
-- Upstream = follow -> . Downstream = follow <- .
CREATE PROPERTY GRAPH sqlprism_graph
  VERTEX TABLES (nodes)
  EDGE TABLES (edges SOURCE KEY (source_id) REFERENCES nodes (node_id)
    DESTINATION KEY (target_id) REFERENCES nodes (node_id))
```

## Core syntax

Use `COLUMNS (...)` — never `RETURN`. Use `{min,max}` bounds — never unbounded `+`.

```sql
FROM GRAPH_TABLE (sqlprism_graph MATCH <pattern> COLUMNS (<expressions>))
```

## Query patterns

**Vertices:**
```sql
FROM GRAPH_TABLE (sqlprism_graph MATCH (n:nodes) COLUMNS (n.node_id, n.name, n.kind))
```

**Single-hop edges:**
```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes)-[e:edges]->(b:nodes)
  COLUMNS (a.name AS source, b.name AS target, e.relationship))
```

**Filtered (upstream of a model):**
```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->(b:nodes)
  COLUMNS (a.name AS model, b.name AS depends_on))
```

**Multi-hop (bounded, 1-3 hops):**
```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->{1,3}(b:nodes)
  COLUMNS (a.name AS model, b.name AS upstream))
```

**Shortest path (forward direction only):**
```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH p = ANY SHORTEST
    (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->{1,10}(b:nodes WHERE b.name = 'raw_orders')
  COLUMNS (a.name, b.name, path_length(p)))
```

## Analytics functions

These are table functions, not GRAPH_TABLE queries:

```sql
SELECT * FROM pagerank(sqlprism_graph, nodes, edges)
-- Returns (rowid, score)

SELECT * FROM weakly_connected_component(sqlprism_graph, nodes, edges)
-- Returns (node_id, component_id)

SELECT * FROM local_clustering_coefficient(sqlprism_graph, nodes, edges)
-- Returns (node_id, lcc)
```

**More examples**: See [EXAMPLES.md](EXAMPLES.md) for real-world queries (downstream consumers, PageRank with JOINs, fan-in/fan-out, root sources, leaf models).

## Constraints

- `COLUMNS` not `RETURN` (parser error)
- Bounds required: `{1,N}` not `+` or `*` (infinite results error)
- Shortest path: forward `->` only (`<-` not implemented)
- Vertex tables must be base tables, not views
- Phantom nodes (`file_id IS NULL`) are included — filter in COLUMNS or post-processing
- Filter with `WHERE` inside MATCH, not outside