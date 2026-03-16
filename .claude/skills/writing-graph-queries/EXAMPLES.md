# DuckPGQ Query Examples

Tested against `sqlprism_graph` with DuckDB + DuckPGQ.

## Find all direct dependencies of a model

```sql
-- What does stg_orders depend on?
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->(b:nodes)
  COLUMNS (b.name AS dependency, b.kind, e.relationship))
```

## Find all downstream consumers (1-3 hops)

```sql
-- What breaks if raw_orders changes?
FROM GRAPH_TABLE (sqlprism_graph
  MATCH (a:nodes WHERE a.name = 'raw_orders')<-[e:edges]-{1,3}(b:nodes)
  COLUMNS (b.name AS downstream, b.kind))
```

## Shortest path between two models

```sql
FROM GRAPH_TABLE (sqlprism_graph
  MATCH p = ANY SHORTEST
    (a:nodes WHERE a.name = 'stg_orders')-[e:edges]->{1,10}(b:nodes WHERE b.name = 'raw_orders')
  COLUMNS (a.name AS from_model, b.name AS to_model, path_length(p) AS hops))
```

## PageRank — find most important models

```sql
-- Join pagerank scores back to node names
SELECT n.name, n.kind, pr.pagerank
FROM pagerank(sqlprism_graph, nodes, edges) pr
JOIN nodes n ON n.rowid = pr.rowid
ORDER BY pr.pagerank DESC
LIMIT 10
```

## Weakly connected components — find isolated subgraphs

```sql
SELECT n.name, wcc.componentid
FROM weakly_connected_component(sqlprism_graph, nodes, edges) wcc
JOIN nodes n ON n.rowid = wcc.rowid
ORDER BY wcc.componentid, n.name
```

## Find models with no upstream dependencies (root sources)

```sql
-- Models that don't reference anything
SELECT n.name, n.kind
FROM nodes n
WHERE n.file_id IS NOT NULL
  AND n.node_id NOT IN (SELECT source_id FROM edges)
```

## Find leaf models (no downstream consumers)

```sql
-- Models nothing depends on
SELECT n.name, n.kind
FROM nodes n
WHERE n.file_id IS NOT NULL
  AND n.node_id NOT IN (SELECT target_id FROM edges)
```

## Count edges per model (fan-in / fan-out)

```sql
SELECT n.name,
  (SELECT COUNT(*) FROM edges e WHERE e.source_id = n.node_id) AS fan_out,
  (SELECT COUNT(*) FROM edges e WHERE e.target_id = n.node_id) AS fan_in
FROM nodes n
WHERE n.file_id IS NOT NULL
ORDER BY fan_in + fan_out DESC
LIMIT 20
```