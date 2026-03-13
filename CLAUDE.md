# SQLPrism

SQL knowledge graph MCP server — indexes SQL, dbt, and sqlmesh repos into a DuckDB-backed graph for lineage, impact analysis, and column tracing.

## Quick Reference

- **Language**: Python 3.12+
- **Package manager**: uv
- **Lint**: `uv run ruff check .`
- **Tests**: `uv run pytest tests/ -v`
- **Single test**: `uv run pytest tests/test_indexer.py::test_name -v`

## Project Structure

```
src/sqlprism/
  core/
    graph.py       — DuckDB storage layer (only module that touches DB)
    indexer.py      — Orchestrates parsing + indexing across repos
    mcp_tools.py   — MCP server tools (query, reindex, pr_impact)
  languages/
    sql.py         — sqlglot-based SQL parser
    dbt.py         — dbt renderer (compile → parse)
    sqlmesh.py     — sqlmesh renderer (render → parse)
    utils.py       — Shared venv/env utilities
  types.py         — Data classes (ParseResult, NodeResult, etc.)
  cli.py           — Click CLI entry point
tests/
  test_indexer.py  — Indexer + integration tests
  test_renderers.py — dbt/sqlmesh renderer tests
```

## Conventions

- **Branch naming**: `<type>-<issue>-<short-description>` (e.g. `feat-11-indexer-reindex-files`)
- **Types**: `feat` (enhancement), `fix` (bug), `chore` (maintenance)
- **Issues**: BDD format with Given/When/Then scenarios
- **PRs**: Link with `Closes #<number>`, draft first, mark ready after lint+tests pass
- **Lint before PR**: Always run `uv run ruff check .` before creating or marking a PR ready

## Skills

Custom skills are in `.claude/skills/`:

- **implementing-issues** — End-to-end issue implementation with task planning and sub-agents
- **creating-branches-and-prs** — Branch/PR naming conventions and creation workflow
- **reviewing-prs** — Three-persona parallel PR review (SWE, data eng, QA)
- **managing-project-releases** — Decompose releases into BDD issues with milestones
