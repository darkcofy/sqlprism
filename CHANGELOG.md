# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.1] — 2026-04-21

### Fixed
- `find_path` / `find_bottlenecks` / `check_impact` no longer follow `defines`
  (non-dataflow) edges or `inserts_into` self-loops when tracing dependencies
  (#127).
- Cross-repo trace now walks the name-quotient graph so dependency traversal
  crosses shadow `ref()` nodes between federated repos (#131).
- dbt `schema.yml` column definitions are persisted into the graph; column
  inference from `CREATE TABLE AS SELECT` is also picked up (#125).
- sqlmesh column-definition resolution aligned with the dbt path (#124).

## [1.2.0] — 2026-03-26

### Added
- **Conventions engine**: layer detection, naming-pattern inference, reference
  rules, common columns, and column-style inference. Exposed via
  `get_conventions` MCP tool and the `sqlprism conventions --init/--refresh/--diff`
  CLI. YAML overrides can be loaded and merged on top of inferred conventions.
- **Semantic tags**: clustering and auto-labeling of models, with
  `search_by_tag` and `list_tags` MCP tools.
- **Similarity & placement**: `find_similar_models` and `suggest_placement`
  MCP tools to support new-model authoring workflows.

### Changed
- Upgraded `sqlglot` to v30 with the `[c]` (native) extension for faster
  parsing.

## [1.1.0] — 2026-03-16

### Added
- **Cross-repo federation**: `cross_repo_edges` and `name_collisions` surfaced
  via `get_index_status`; synthetic shadow nodes for referenced-but-unindexed
  models.
- **YAML config support** with discovery order; `sqlprism.yml` recognised
  alongside legacy JSON.
- **Graph-analytics tools**: `find_critical_models` (PageRank),
  `detect_cycles`, `find_subgraphs` (weakly connected components), and
  `find_bottlenecks` (fan-in/out analysis), backed by DuckPGQ.
- **Column & context tools**: `get_schema`, `get_context`, `check_impact`,
  `find_path`, and DuckPGQ-backed `trace_dependencies`.
- `ty` type checker added to CI.

## [1.0.1] — 2026-03-15

### Fixed
- Patch release following v1.0.0; see git history for details.

## [1.0.0] — 2026-03-12

### Added
- Initial release: DuckDB-backed knowledge graph for SQL, dbt, and sqlmesh
  repos. MCP server with parsing, indexing, lineage, impact analysis, and
  column tracing.

[Unreleased]: https://github.com/darkcofy/sqlprism/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/darkcofy/sqlprism/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/darkcofy/sqlprism/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/darkcofy/sqlprism/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/darkcofy/sqlprism/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/darkcofy/sqlprism/releases/tag/v1.0.0
