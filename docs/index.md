# SQLPrism

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

On a 200-model SQLMesh project, a column impact query returns **75 structured results in ~5,000 tokens**. The grep equivalent would need **40-60 files opened, ~100,000+ tokens**, and still wouldn't tell you whether a column appears in a WHERE filter or a SELECT.

## Quick Start

```bash
git clone <repo-url> && cd sqlprism
uv sync
uv run sqlprism init       # creates sqlprism.yml
# edit config to add your repos
uv run sqlprism reindex     # index plain SQL repos
uv run sqlprism serve       # start MCP server
```

See [Installation](getting-started/installation.md) and [Configuration](getting-started/configuration.md) for details.
