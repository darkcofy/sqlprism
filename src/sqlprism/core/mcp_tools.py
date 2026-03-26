"""MCP server exposing SQL indexer tools.

This is the interface LLMs interact with. Tools are provider-agnostic —
any MCP client (Claude, Cursor, Continue.dev, etc.) can connect via
stdio or streamable HTTP.

Focused entirely on SQL: tables, views, CTEs, column lineage, transforms,
WHERE filters, and dependency tracing across dialects.
"""

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field
from pydantic import model_validator as pydantic_model_validator

from sqlprism.core.graph import GraphDB
from sqlprism.core.indexer import Indexer
from sqlprism.languages import is_sql_file
from sqlprism.types import NodeResult, ParseResult, parse_repo_config

logger = logging.getLogger(__name__)

# ── Server initialisation ──

mcp = FastMCP("sqlprism")


@dataclass(frozen=True)
class _ServerState:
    """Immutable bundle of server state, swapped atomically on configure()."""

    graph: GraphDB
    indexer: Indexer
    config: dict


# Single atomic reference — readers snapshot this once; no lock needed.
_state: _ServerState | None = None

# Background reindex state — shared across reindex, reindex_dbt, reindex_sqlmesh.
# Only one reindex may run at a time to avoid write-lock conflicts.
_reindex_lock = asyncio.Lock()
_reindex_task: asyncio.Task | None = None
_reindex_status: dict = {"state": "idle"}
_last_parse_errors: list[str] = []

# Per-repo debounce state for reindex_files
_reindex_pending: dict[str, list[str]] = defaultdict(list)  # repo_name → [paths]
_reindex_timers: dict[str, asyncio.TimerHandle] = {}         # repo_name → timer

_DEBOUNCE_SQL = 0.5       # 500ms for plain SQL (fast parse)
_DEBOUNCE_RENDERED = 2.0  # 2s for dbt/sqlmesh (subprocess)


def configure(db_path: str | Path, repos: dict, sql_dialect: str | None = None):
    """Initialise the graph and indexer with repo configuration.

    Args:
        db_path: Path to DuckDB file
        repos: {repo_name: path_or_config} — value is either a string path
               or a dict with "path", "dialect", "dialect_overrides" keys
        sql_dialect: Global fallback SQL dialect (overridden by per-repo config)

    Thread-safety: builds a new immutable ``_ServerState`` and swaps it in
    with a single assignment, so concurrent readers never see partial updates.
    """
    global _state
    graph = GraphDB(db_path)
    indexer = Indexer(graph)
    config = {
        "db_path": str(db_path),
        "repos": repos,
        "sql_dialect": sql_dialect,
    }

    # Register repos before publishing new state
    for name, cfg in repos.items():
        path = cfg["path"] if isinstance(cfg, dict) else cfg
        repo_type = cfg.get("repo_type", "sql") if isinstance(cfg, dict) else "sql"
        graph.upsert_repo(name, path, repo_type=repo_type)

    # Atomic swap — readers always get a consistent triple
    _state = _ServerState(graph=graph, indexer=indexer, config=config)


def _get_state() -> _ServerState:
    """Snapshot current server state or raise if not yet configured."""
    state = _state
    if state is None:
        raise RuntimeError("Server not configured. Call configure() first.")
    return state


def _get_graph() -> GraphDB:
    return _get_state().graph


def _get_indexer() -> Indexer:
    return _get_state().indexer


def _resolve_repo_config(repo_name: str) -> tuple[str, str | None, dict[str, str] | None]:
    """Extract (path, dialect, dialect_overrides) from repo config."""
    config = _get_state().config
    cfg = config["repos"].get(repo_name)
    if cfg is None:
        raise ValueError(f"Repo '{repo_name}' not found in config")
    return parse_repo_config(cfg, config.get("sql_dialect"))


# ── Query tools ──


class SearchInput(BaseModel):
    model_config = {"populate_by_name": True}
    pattern: str = Field(
        ...,
        description="Search pattern (partial name match, case-insensitive)",
    )
    kind: str | None = Field(
        None,
        description="Filter by node kind: 'table', 'view', 'cte', 'query'",
    )
    sql_schema: str | None = Field(
        None,
        alias="schema",
        description="Filter by SQL schema name (e.g. 'staging', 'public')",
    )
    repo: str | None = Field(None, description="Filter by repo name. Omit to search all repos.")
    limit: int = Field(20, description="Max results (default 20)", ge=1, le=100)
    offset: int = Field(0, description="Number of results to skip for pagination (default 0)", ge=0)
    include_snippets: bool = Field(True, description="Include source code snippets in results")


@mcp.tool(
    name="search",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def search(params: SearchInput) -> dict:
    """Search for SQL entities by name across the codebase graph.

    Finds tables, views, CTEs, and queries by partial name match.
    Returns matches with name, kind, file path, repo, and line numbers.
    """
    return await asyncio.to_thread(
        _get_graph().query_search,
        pattern=params.pattern,
        kind=params.kind,
        schema=params.sql_schema,
        repo=params.repo,
        limit=params.limit,
        offset=params.offset,
        include_snippets=params.include_snippets,
    )


class FindReferencesInput(BaseModel):
    model_config = {"populate_by_name": True}
    name: str = Field(..., description="Entity name (table, view, CTE, etc.)")
    kind: str | None = Field(None, description="Filter by node kind to disambiguate")
    sql_schema: str | None = Field(
        None,
        alias="schema",
        description="Filter by SQL schema name (e.g. 'staging', 'public')",
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )
    direction: Literal["both", "inbound", "outbound"] = Field(
        "both",
        description="'inbound', 'outbound', or 'both'",
    )
    include_snippets: bool = Field(True, description="Include source code snippets in results")
    limit: int = Field(100, description="Max results per direction (default 100)", ge=1, le=500)
    offset: int = Field(0, description="Number of results to skip for pagination (default 0)", ge=0)


@mcp.tool(
    name="find_references",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_references(params: FindReferencesInput) -> dict:
    """Find everything connected to a named SQL entity.

    Returns both inbound (what depends on this) and outbound (what this depends on)
    relationships. Each result includes: name, kind, relationship type, file path, repo.
    """
    return await asyncio.to_thread(
        _get_graph().query_references,
        name=params.name,
        kind=params.kind,
        schema=params.sql_schema,
        repo=params.repo,
        direction=params.direction,
        include_snippets=params.include_snippets,
        limit=params.limit,
        offset=params.offset,
    )


class FindColumnUsageInput(BaseModel):
    table: str = Field(..., description="Table name to search column usage for")
    column: str | None = Field(None, description="Specific column name. Omit for all columns.")
    usage_type: str | None = Field(
        None,
        description=("Filter: 'select', 'where', 'join_on', 'group_by', 'order_by', 'having', 'insert', 'update'"),
    )
    repo: str | None = Field(None, description="Filter by repo name. Omit to search all repos.")
    limit: int = Field(100, description="Max results (default 100)", ge=1, le=500)
    offset: int = Field(0, description="Number of results to skip for pagination (default 0)", ge=0)


@mcp.tool(
    name="find_column_usage",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_column_usage(params: FindColumnUsageInput) -> dict:
    """Find where and how columns are used across SQL models.

    Powered by sqlglot's column lineage analysis. Shows usage type,
    transforms (CAST, COALESCE, etc.), output aliases, and WHERE conditions.

    Answers: "where is customer_id used in WHERE clauses?",
    "how is animal.breed_id transformed?", "show all column usage on orders."
    """
    return await asyncio.to_thread(
        _get_graph().query_column_usage,
        table=params.table,
        column=params.column,
        usage_type=params.usage_type,
        repo=params.repo,
        limit=params.limit,
        offset=params.offset,
    )


class TraceDependenciesInput(BaseModel):
    name: str = Field(..., description="Starting entity name")
    kind: str | None = Field(None, description="Filter by node kind to disambiguate")
    direction: Literal["upstream", "downstream", "both"] = Field(
        "downstream",
        description="'upstream', 'downstream', or 'both'",
    )
    max_depth: int = Field(
        3,
        description="Maximum hops to traverse (default 3, max 6)",
        ge=1,
        le=6,
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to trace across all repos.",
    )
    include_snippets: bool = Field(
        False,
        description="Include source code snippets (default false for trace, can be large)",
    )
    limit: int = Field(100, description="Max results (default 100)", ge=1, le=500)


@mcp.tool(
    name="trace_dependencies",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def trace_dependencies(params: TraceDependenciesInput) -> dict:
    """Trace multi-hop dependency chains through the SQL graph.

    Follows table → view → CTE → query chains. Use for impact analysis:
    "if I change this table, what models break?"
    """
    return await asyncio.to_thread(
        _get_graph().query_trace,
        name=params.name,
        kind=params.kind,
        direction=params.direction,
        max_depth=params.max_depth,
        repo=params.repo,
        include_snippets=params.include_snippets,
        limit=params.limit,
    )


class TraceColumnLineageInput(BaseModel):
    table: str | None = Field(
        None,
        description="Source or intermediate table name to trace lineage for",
    )
    column: str | None = Field(
        None,
        description="Column name to trace",
    )
    output_node: str | None = Field(
        None,
        description="Output entity name (table/view/query) to trace lineage from",
    )
    repo: str | None = Field(None, description="Filter by repo name. Omit to search all repos.")
    limit: int = Field(100, description="Max lineage chains to return (default 100)", ge=1, le=500)
    offset: int = Field(0, description="Number of chains to skip for pagination (default 0)", ge=0)


@mcp.tool(
    name="trace_column_lineage",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def trace_column_lineage(params: TraceColumnLineageInput) -> dict:
    """Trace end-to-end column lineage through CTEs and subqueries.

    Shows how an output column traces back to source table columns, with
    each intermediate hop (CTE, subquery) and any transforms (CAST, etc.).

    Answers: "where does dim_users.created_date come from?",
    "which output columns depend on orders.amount?"

    Note: SELECT * lineage requires a schema catalog built from prior column
    usage data. On a fresh index, SELECT * columns may not be expanded.
    Run a second full reindex to populate the catalog and resolve them.
    """
    return await asyncio.to_thread(
        _get_graph().query_column_lineage,
        table=params.table,
        column=params.column,
        output_node=params.output_node,
        repo=params.repo,
        limit=params.limit,
        offset=params.offset,
    )


class GetSchemaInput(BaseModel):
    model_config = {"populate_by_name": True}
    name: str = Field(..., description="Table or model name (e.g. 'staging.orders', 'stg_orders')")
    repo: str | None = Field(
        None,
        description="Filter by repo name. Required if same model name exists in multiple repos.",
    )


@mcp.tool(
    name="get_schema",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def get_schema(params: GetSchemaInput) -> dict:
    """Get the schema of a table or model — columns, types, descriptions, and dependencies.

    Returns column definitions (name, type, position, source, description),
    upstream dependencies (what this model reads from), and downstream
    dependencies (what reads from this model). The primary tool for
    understanding table structure.
    """
    return await asyncio.to_thread(
        _get_graph().query_schema,
        name=params.name,
        repo=params.repo,
    )


class GetContextInput(BaseModel):
    model_config = {"populate_by_name": True}
    name: str = Field(..., description="Table or model name (e.g. 'staging.orders', 'stg_orders')")
    repo: str | None = Field(None, description="Filter by repo name. Omit to search all repos.")


@mcp.tool(
    name="get_context",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def get_context(params: GetContextInput) -> dict:
    """Get comprehensive context for a model — the first tool to call when working with a model.

    Returns a complete context dump including:
    - Model metadata (name, kind, file, repo)
    - Column definitions with types and descriptions
    - Upstream and downstream dependencies
    - Column usage summary (most used columns, join keys, aggregations)
    - Source code snippet (first 30 lines)
    - Graph metrics (PageRank importance) when DuckPGQ is available
    """
    return await asyncio.to_thread(
        _get_graph().query_context,
        name=params.name,
        repo=params.repo,
    )


class FindPathInput(BaseModel):
    model_config = {"populate_by_name": True}
    from_model: str = Field(..., description="Starting model name (e.g. 'raw.orders')")
    to_model: str = Field(..., description="Target model name (e.g. 'marts.revenue')")
    max_hops: int = Field(10, description="Maximum path length (default 10, max 10)", ge=1, le=10)


@mcp.tool(
    name="find_path",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_path(params: FindPathInput) -> dict:
    """Find the shortest dependency path between two models.

    Uses DuckPGQ graph traversal to find the shortest chain of
    dependencies connecting two models. Returns the full path
    with intermediate models and path length.

    Requires DuckPGQ extension. Returns an error if not installed.
    """
    return await asyncio.to_thread(
        _get_graph().query_find_path,
        from_model=params.from_model,
        to_model=params.to_model,
        max_hops=params.max_hops,
    )


class FindCriticalModelsInput(BaseModel):
    model_config = {"populate_by_name": True}
    top_n: int = Field(20, description="Number of top models to return (default 20, max 100)", ge=1, le=100)
    repo: str | None = Field(None, description="Filter by repo name. Omit for all repos.")


@mcp.tool(
    name="find_critical_models",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_critical_models(params: FindCriticalModelsInput) -> dict:
    """Find the most critical models by importance (PageRank) and downstream impact.

    Ranks models by their graph centrality — models with high PageRank are
    referenced by many important models. Use to identify high-impact models
    that need extra care when modifying.

    Requires DuckPGQ extension.
    """
    return await asyncio.to_thread(
        _get_graph().query_find_critical_models,
        top_n=params.top_n,
        repo=params.repo,
    )


class DetectCyclesInput(BaseModel):
    model_config = {"populate_by_name": True}
    repo: str | None = Field(None, description="Filter by repo name. Omit for all repos.")
    max_cycle_length: int = Field(
        10,
        description="Maximum cycle length to detect (default 10, max 15)",
        ge=2,
        le=15,
    )


@mcp.tool(
    name="detect_cycles",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def detect_cycles(params: DetectCyclesInput) -> dict:
    """Detect circular dependencies in the SQL dependency graph.

    Finds cycles where models form dependency loops (A -> B -> C -> A).
    Uses recursive CTE traversal — no DuckPGQ extension required.
    """
    return await asyncio.to_thread(
        _get_graph().query_detect_cycles,
        repo=params.repo,
        max_cycle_length=params.max_cycle_length,
    )


class FindSubgraphsInput(BaseModel):
    model_config = {"populate_by_name": True}
    repo: str | None = Field(None, description="Filter by repo name. Omit for all repos.")


@mcp.tool(
    name="find_subgraphs",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_subgraphs(params: FindSubgraphsInput) -> dict:
    """Identify weakly connected components (subgraphs) in the dependency graph.

    Reveals isolated model clusters, orphaned models, and overall graph topology.
    Requires DuckPGQ extension.
    """
    return await asyncio.to_thread(
        _get_graph().query_find_subgraphs,
        repo=params.repo,
    )


class FindBottlenecksInput(BaseModel):
    model_config = {"populate_by_name": True}
    min_downstream: int = Field(
        5,
        description="Minimum downstream dependents to qualify as bottleneck (default 5, max 100)",
        ge=1,
        le=100,
    )
    repo: str | None = Field(None, description="Filter by repo name. Omit for all repos.")


@mcp.tool(
    name="find_bottlenecks",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_bottlenecks(params: FindBottlenecksInput) -> dict:
    """Find bottleneck models with high fan-in/out that are single points of failure.

    Combines edge counting (plain SQL) with optional DuckPGQ clustering coefficient.
    Models with high downstream count and low clustering are flagged as high risk.
    """
    return await asyncio.to_thread(
        _get_graph().query_find_bottlenecks,
        min_downstream=params.min_downstream,
        repo=params.repo,
    )


class ColumnChange(BaseModel):
    action: Literal["remove_column", "rename_column", "add_column"] = Field(
        ...,
        description="Type of column change: 'remove_column', 'rename_column', or 'add_column'",
    )
    column: str | None = Field(None, description="Column name (for remove_column and add_column)")
    old: str | None = Field(None, description="Old column name (for rename_column)")
    new: str | None = Field(None, description="New column name (for rename_column)")

    @pydantic_model_validator(mode="after")
    def _validate_fields_per_action(self) -> "ColumnChange":
        if self.action in ("remove_column", "add_column") and not self.column:
            raise ValueError(f"'{self.action}' requires 'column' to be set")
        if self.action == "rename_column":
            if not self.old or not self.new:
                raise ValueError("'rename_column' requires both 'old' and 'new' to be set")
        return self


class CheckImpactInput(BaseModel):
    model_config = {"populate_by_name": True}
    model: str = Field(..., description="Model or table name to check impact for (e.g. 'staging.orders')")
    changes: list[ColumnChange] = Field(
        ...,
        description="List of proposed column changes to analyze",
        min_length=1,
    )
    repo: str | None = Field(None, description="Filter by repo name. Omit to search all repos.")


@mcp.tool(
    name="check_impact",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def check_impact(params: CheckImpactInput) -> dict:
    """Check the downstream impact of proposed column changes BEFORE modifying code.

    Analyzes column usage across downstream models to classify each change as:
    - **breaking**: SELECT/JOIN usage — downstream model will error
    - **warning**: WHERE/GROUP BY usage — filter breaks but model may not error
    - **safe**: column not referenced downstream

    Call this BEFORE removing, renaming, or adding columns to understand the blast radius.

    Note: ``add_column`` does not detect ``SELECT *`` usage — downstream models
    using wildcard selects may still be affected by new columns.
    """
    return await asyncio.to_thread(
        _get_graph().query_check_impact,
        model=params.model,
        changes=[c.model_dump() for c in params.changes],
        repo=params.repo,
    )


class PrImpactInput(BaseModel):
    base_commit: str = Field(
        ...,
        description="Git commit hash or ref to compare against (e.g., 'main', 'abc123f')",
    )
    repo: str | None = Field(
        None,
        description="Repo to analyse. Required if multiple repos configured.",
    )
    max_blast_radius_depth: int = Field(
        3,
        description="Hops to trace from changed nodes (default 3)",
        ge=1,
        le=6,
    )
    compare_mode: Literal["delta", "absolute"] = Field(
        "delta",
        description=("'delta' = net-new impact vs base (default), 'absolute' = total blast radius (v1 behavior)"),
    )


@mcp.tool(
    name="pr_impact",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    ),
)
async def pr_impact(params: PrImpactInput) -> dict:
    """Analyse the structural impact of SQL changes since a base commit.

    Computes structural diff (added/removed/modified tables, views, CTEs,
    column usage) then traces the blast radius through the full index.

    **Delta mode caveat:** ``compare_mode="delta"`` shows **net-new downstream
    impact** by approximating the base blast radius via edge exclusion on the
    HEAD graph.  It does **not** detect reduced blast radius from removed edges
    — ``no_longer_affected`` will be empty when a PR only removes dependencies.
    Use ``compare_mode="absolute"`` for a full picture when edge removals are
    the primary change.
    """
    state = _get_state()
    indexer = state.indexer
    graph = state.graph
    config = state.config

    # Determine which repo
    if params.repo:
        path, dialect, dialect_overrides = _resolve_repo_config(params.repo)
        repo_path = Path(path)
    elif len(config["repos"]) == 1:
        repo_name = list(config["repos"].keys())[0]
        path, dialect, dialect_overrides = _resolve_repo_config(repo_name)
        repo_path = Path(path)
    else:
        return {"error": "Multiple repos configured — specify which repo to analyse."}

    def _blocking_pr_impact() -> dict:
        changed_files = indexer.get_changed_files(repo_path, params.base_commit)
        if not changed_files:
            return {"files_changed": [], "structural_diff": {}, "blast_radius": {}}

        old_results: dict[str, ParseResult] = {}
        new_results: dict[str, ParseResult] = {}

        for file_path in changed_files:
            full_path = repo_path / file_path
            if full_path.exists() and is_sql_file(file_path):
                content = full_path.read_text(errors="replace")
                new_results[file_path] = indexer.parse_file(file_path, content, dialect)

            old = indexer.parse_file_at_commit(repo_path, file_path, params.base_commit, dialect)
            if old:
                old_results[file_path] = old

        diff = _compute_structural_diff(old_results, new_results)

        affected_node_names = (
            [n["name"] for n in diff["nodes_added"]]
            + [n["name"] for n in diff["nodes_removed"]]
            + [n["name"] for n in diff["nodes_modified"]]
        )

        # Names of truly new nodes (no base trace needed for these)
        added_names = {n["name"] for n in diff["nodes_added"]}

        # Build exclude set: edges added in HEAD that did not exist at base
        edges_added_set: set[tuple[str, str]] = {(e["source"], e["target"]) for e in diff.get("edges_added", [])}

        is_delta = params.compare_mode == "delta"

        blast_radius: dict = {}
        if affected_node_names:
            head_affected: set[tuple[str, str]] = set()
            base_affected: set[tuple[str, str]] = set()
            all_head_paths: list[dict] = []  # flat list for repo counting
            repos_hit: set[str] = set()
            truncated = len(affected_node_names) > 20

            affected_node_names.sort()
            for node_name in affected_node_names[:20]:
                # HEAD blast radius (current graph)
                head_trace = graph.query_trace(
                    name=node_name,
                    direction="downstream",
                    max_depth=params.max_blast_radius_depth,
                )
                head_paths = head_trace.get("paths", [])
                head_affected.update((p["name"], p["kind"]) for p in head_paths)
                all_head_paths.extend(head_paths)
                repos_hit.update(head_trace.get("repos_affected", []))

                # Base blast radius approximation (exclude new edges)
                if is_delta and node_name not in added_names:
                    base_trace = graph.query_trace(
                        name=node_name,
                        direction="downstream",
                        max_depth=params.max_blast_radius_depth,
                        exclude_edges=edges_added_set,
                    )
                    base_affected.update((p["name"], p["kind"]) for p in base_trace.get("paths", []))

            if is_delta:
                newly_affected = head_affected - base_affected
                no_longer_affected = base_affected - head_affected

                blast_radius = {
                    "compare_mode": "delta",
                    "head_total": len(head_affected),
                    "base_total": len(base_affected),
                    "delta": len(head_affected) - len(base_affected),
                    "newly_affected": [{"name": n, "kind": k} for n, k in sorted(newly_affected)],
                    "no_longer_affected": [{"name": n, "kind": k} for n, k in sorted(no_longer_affected)],
                    "unchanged_affected": len(head_affected & base_affected),
                    "note": (
                        "Delta mode approximates the base blast radius by "
                        "excluding newly-added edges from the HEAD graph. "
                        "It shows net-new downstream impact but does not "
                        "detect reduced blast radius from removed edges."
                    ),
                    # Backward-compat fields
                    "transitively_affected": len(head_affected),
                    "repos_affected": sorted(repos_hit),
                    "truncated": truncated,
                    "total_affected_nodes": len(affected_node_names),
                }
            else:
                # Absolute mode (v1 behavior)
                blast_radius = {
                    "compare_mode": "absolute",
                    "transitively_affected": len(all_head_paths),
                    "affected_by_repo": {r: sum(1 for a in all_head_paths if a.get("repo") == r) for r in repos_hit},
                    "repos_affected": sorted(repos_hit),
                    "truncated": truncated,
                    "total_affected_nodes": len(affected_node_names),
                }

            if truncated:
                blast_radius["truncation_message"] = (
                    f"Blast radius incomplete — {len(affected_node_names)} affected nodes, "
                    "only first 20 traced. Use trace_dependencies "
                    "on specific nodes for full picture."
                )

        return {
            "files_changed": changed_files,
            "structural_diff": diff,
            "blast_radius": blast_radius,
        }

    return await asyncio.to_thread(_blocking_pr_impact)


# ── Index management tools ──


class GetConventionsInput(BaseModel):
    model_config = {"populate_by_name": True}
    layer: str | None = Field(
        None,
        description="Layer name (e.g. 'staging', 'marts'). Omit to get all layers.",
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )


@mcp.tool(
    name="get_conventions",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def get_conventions(params: GetConventionsInput) -> dict:
    """Get naming conventions, reference rules, and required columns for a layer.

    Returns inferred conventions with confidence scores. Agents should follow
    high-confidence conventions (>0.9) and ask about low-confidence ones (<0.7).

    Use this before writing new models to understand project patterns:
    naming conventions, allowed layer references, required columns, and
    column naming style.
    """
    return await asyncio.to_thread(
        _get_graph().query_conventions,
        layer=params.layer,
        repo=params.repo,
    )


class SearchByTagInput(BaseModel):
    model_config = {"populate_by_name": True}
    tag: str = Field(
        ...,
        description="Tag name to search for (e.g. 'customer', 'order').",
    )
    min_confidence: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold (0.0-1.0). Only return models above this confidence.",
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )


@mcp.tool(
    name="search_by_tag",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def search_by_tag(params: SearchByTagInput) -> dict:
    """Find models tagged with a business domain concept, ranked by confidence.

    Returns models whose semantic tags match the given tag name, ordered by
    confidence score (highest first). Use list_tags first to discover the
    available tags in the project's business domain vocabulary.
    """
    return await asyncio.to_thread(
        _get_graph().query_search_by_tag,
        tag=params.tag,
        repo=params.repo,
        min_confidence=params.min_confidence,
    )


class ListTagsInput(BaseModel):
    model_config = {"populate_by_name": True}
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )


@mcp.tool(
    name="list_tags",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def list_tags(params: ListTagsInput) -> dict:
    """Return all semantic tags with model counts and average confidence.

    Provides the project's business domain vocabulary — the set of conceptual
    tags that have been assigned to models. Use this to discover available tags
    before calling search_by_tag.
    """
    return await asyncio.to_thread(
        _get_graph().query_list_tags,
        repo=params.repo,
    )


class FindSimilarModelsInput(BaseModel):
    references: list[str] | None = Field(
        None,
        description="Tables this model will reference (e.g. ['stg_orders', 'stg_payments']).",
    )
    output_columns: list[str] | None = Field(
        None,
        description="Columns this model will output (e.g. ['customer_id', 'total_revenue']).",
    )
    model: str | None = Field(
        None,
        min_length=1,
        description="Existing model name to find similar models to.",
    )
    limit: int = Field(
        5,
        ge=1,
        le=50,
        description="Maximum number of similar models to return (default 5).",
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )


@mcp.tool(
    name="find_similar_models",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def find_similar_models(params: FindSimilarModelsInput) -> dict:
    """Find existing models similar to what you're building.

    Compares reference overlap, column overlap, and layer placement to find
    models that already do something similar. Helps avoid duplicate work and
    suggests models to extend rather than recreate.
    """
    return await asyncio.to_thread(
        _get_graph().query_find_similar_models,
        references=params.references,
        output_columns=params.output_columns,
        model=params.model,
        limit=params.limit,
        repo=params.repo,
    )


class SuggestPlacementInput(BaseModel):
    references: list[str] = Field(
        ...,
        min_length=1,
        description="Tables this new model will reference (e.g. ['stg_orders', 'stg_payments']).",
    )
    name: str | None = Field(
        None,
        description="Proposed model name — will be validated against layer naming conventions.",
    )
    repo: str | None = Field(
        None,
        description="Filter by repo name. Omit to search all repos.",
    )


@mcp.tool(
    name="suggest_placement",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def suggest_placement(params: SuggestPlacementInput) -> dict:
    """Suggest where to place a new model based on its references.

    Uses inferred layer flow rules and naming conventions to recommend the
    right layer, directory, and model name. Returns similar existing models
    to help avoid duplicate work.
    """
    return await asyncio.to_thread(
        _get_graph().query_suggest_placement,
        references=params.references,
        name=params.name,
        repo=params.repo,
    )


class ReindexInput(BaseModel):
    repo: str | None = Field(None, description="Specific repo to reindex. Omit for all repos.")


@mcp.tool(
    name="reindex",
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def reindex(params: ReindexInput) -> dict:
    """Trigger a reindex of SQL files. Checksums and re-parses only what changed.

    Runs in the background so queries remain available during reindex.
    Supports per-repo SQL dialects and path-based dialect overrides.
    """
    global _reindex_task, _reindex_status

    async with _reindex_lock:
        # If already running, return status
        if _reindex_task and not _reindex_task.done():
            return {"status": "in_progress", **_reindex_status}

        state = _get_state()
        indexer = state.indexer

        repos = state.config["repos"]
        if params.repo:
            if params.repo not in repos:
                return {"error": f"Repo '{params.repo}' not found in config"}
            repos = {params.repo: repos[params.repo]}

        repo_names = list(repos.keys())
        _reindex_status = {
            "state": "started",
            "started_at": datetime.now().isoformat(),
            "repos": repo_names,
        }

        async def _background_reindex():
            global _reindex_status
            try:

                def _blocking():
                    global _reindex_status
                    results = {}
                    for name, cfg in repos.items():
                        _reindex_status = {**_reindex_status, "current_repo": name}
                        path, dialect, dialect_overrides = _resolve_repo_config(name)
                        results[name] = indexer.reindex_repo(
                            name,
                            path,
                            dialect=dialect,
                            dialect_overrides=dialect_overrides,
                        )
                    return results

                result = await asyncio.to_thread(_blocking)
                global _last_parse_errors
                all_errors = []
                for repo_result in result.values():
                    all_errors.extend(repo_result.get("parse_errors", []))
                _last_parse_errors = all_errors
                _reindex_status = {
                    **_reindex_status,
                    "state": "completed",
                    "completed_at": datetime.now().isoformat(),
                    "result": result,
                }
                return result
            except Exception as e:
                _reindex_status = {
                    **_reindex_status,
                    "state": "failed",
                    "error": str(e),
                    "failed_at": datetime.now().isoformat(),
                }

        _reindex_task = asyncio.create_task(_background_reindex())

    return {
        "status": "started",
        "message": ("Reindex running in background. Queries remain available. Call index_status to check progress."),
        "repos": repo_names,
    }


class ReindexSqlmeshInput(BaseModel):
    name: str = Field(..., description="Repo name for the index")
    project_path: str = Field(
        ...,
        description="Path to sqlmesh project dir (containing config.yaml)",
    )
    env_file: str | None = Field(
        None,
        description="Path to .env file for sqlmesh config variables",
    )
    dialect: str = Field(
        "athena",
        description="SQL dialect for rendering (default: athena)",
    )
    variables: dict[str, str] | None = Field(
        None,
        description='SQLMesh variables, e.g. {"GRACE_PERIOD": "7"}',
    )
    sqlmesh_command: str = Field(
        "uv run python",
        description="Command to run python in sqlmesh venv",
    )


@mcp.tool(
    name="reindex_sqlmesh",
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def reindex_sqlmesh(params: ReindexSqlmeshInput) -> dict:
    """Index a sqlmesh project by rendering all models into clean SQL.

    Runs in the background so queries remain available during reindex.
    Uses sqlmesh's rendering engine to expand macros and resolve variables,
    then parses with sqlglot to extract tables, CTEs, edges, column lineage.
    """
    global _reindex_task, _reindex_status

    async with _reindex_lock:
        # If already running, return status
        if _reindex_task and not _reindex_task.done():
            return {"status": "in_progress", **_reindex_status}

        indexer = _get_indexer()

        var_dict: dict[str, str | int] = {}
        if params.variables:
            for k, v in params.variables.items():
                try:
                    var_dict[k] = int(v)
                except ValueError:
                    var_dict[k] = v

        _reindex_status = {
            "state": "started",
            "started_at": datetime.now().isoformat(),
            "repos": [params.name],
            "tool": "reindex_sqlmesh",
        }

        async def _background_reindex():
            global _reindex_status
            try:
                result = await asyncio.to_thread(
                    indexer.reindex_sqlmesh,
                    repo_name=params.name,
                    project_path=params.project_path,
                    env_file=params.env_file,
                    variables=var_dict,
                    dialect=params.dialect,
                    sqlmesh_command=params.sqlmesh_command,
                )
                global _last_parse_errors
                if isinstance(result, dict):
                    _last_parse_errors = result.get("parse_errors", [])
                _reindex_status = {
                    **_reindex_status,
                    "state": "completed",
                    "completed_at": datetime.now().isoformat(),
                    "result": result,
                }
                return result
            except Exception as e:
                _reindex_status = {
                    **_reindex_status,
                    "state": "failed",
                    "error": str(e),
                    "failed_at": datetime.now().isoformat(),
                }

        _reindex_task = asyncio.create_task(_background_reindex())

    return {
        "status": "started",
        "message": (
            "SQLMesh reindex running in background. Queries remain available. Call index_status to check progress."
        ),
        "repos": [params.name],
    }


class ReindexDbtInput(BaseModel):
    name: str = Field(..., description="Repo name for the index")
    project_path: str = Field(
        ...,
        description="Path to dbt project dir (containing dbt_project.yml)",
    )
    profiles_dir: str | None = Field(
        None,
        description="Path to directory containing profiles.yml",
    )
    env_file: str | None = Field(
        None,
        description="Path to .env file for dbt connection variables",
    )
    target: str | None = Field(None, description="dbt target name")
    dbt_command: str = Field(
        "uv run dbt",
        description="Command to invoke dbt",
    )
    dialect: str | None = Field(
        None,
        description="SQL dialect for parsing (e.g. 'starrocks', 'mysql', 'postgres')",
    )


@mcp.tool(
    name="reindex_dbt",
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def reindex_dbt(params: ReindexDbtInput) -> dict:
    """Index a dbt project by compiling all models into clean SQL.

    Runs in the background so queries remain available during reindex.
    Runs `dbt compile`, then parses with sqlglot to extract tables, CTEs,
    edges, column lineage with transforms.
    """
    global _reindex_task, _reindex_status

    async with _reindex_lock:
        # If already running, return status
        if _reindex_task and not _reindex_task.done():
            return {"status": "in_progress", **_reindex_status}

        indexer = _get_indexer()

        _reindex_status = {
            "state": "started",
            "started_at": datetime.now().isoformat(),
            "repos": [params.name],
            "tool": "reindex_dbt",
        }

        async def _background_reindex():
            global _reindex_status
            try:
                result = await asyncio.to_thread(
                    indexer.reindex_dbt,
                    repo_name=params.name,
                    project_path=params.project_path,
                    profiles_dir=params.profiles_dir,
                    env_file=params.env_file,
                    target=params.target,
                    dbt_command=params.dbt_command,
                    dialect=params.dialect,
                )
                global _last_parse_errors
                if isinstance(result, dict):
                    _last_parse_errors = result.get("parse_errors", [])
                _reindex_status = {
                    **_reindex_status,
                    "state": "completed",
                    "completed_at": datetime.now().isoformat(),
                    "result": result,
                }
                return result
            except Exception as e:
                _reindex_status = {
                    **_reindex_status,
                    "state": "failed",
                    "error": str(e),
                    "failed_at": datetime.now().isoformat(),
                }

        _reindex_task = asyncio.create_task(_background_reindex())

    return {
        "status": "started",
        "message": (
            "dbt reindex running in background. Queries remain available. Call index_status to check progress."
        ),
        "repos": [params.name],
    }


# ── Per-file reindex with debounce ──


def _log_flush_exception(task: asyncio.Task):
    """Done-callback: log exceptions from debounced flush tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error("reindex_files flush failed: %s", exc, exc_info=exc)


async def _enqueue_reindex(repo_name: str, repo_type: str, paths: list[str]):
    """Add paths to pending reindex, reset debounce timer."""
    _reindex_pending[repo_name].extend(paths)

    # Cancel existing timer
    if repo_name in _reindex_timers:
        _reindex_timers[repo_name].cancel()

    delay = _DEBOUNCE_SQL if repo_type == "sql" else _DEBOUNCE_RENDERED
    loop = asyncio.get_running_loop()

    def _schedule_flush(rn=repo_name):
        task = asyncio.ensure_future(_flush_reindex(rn))
        task.add_done_callback(_log_flush_exception)

    _reindex_timers[repo_name] = loop.call_later(delay, _schedule_flush)


async def _flush_reindex(repo_name: str):
    """Execute pending reindex for a repo."""
    paths = _reindex_pending.pop(repo_name, [])
    _reindex_timers.pop(repo_name, None)

    if not paths:
        return

    # Deduplicate (same file saved twice rapidly)
    unique_paths: list[str | Path] = list(dict.fromkeys(paths))

    state = _state
    if not state:
        return

    # Respects existing _reindex_lock — won't conflict with full reindex
    async with _reindex_lock:
        try:
            await asyncio.to_thread(
                state.indexer.reindex_files,
                paths=unique_paths,
                repo_configs=state.config.get("repos", {}),
            )
        except Exception:
            logger.error(
                "reindex_files failed for %d paths", len(unique_paths), exc_info=True,
            )


class ReindexFilesInput(BaseModel):
    paths: list[str] = Field(
        min_length=1,
        description="Absolute paths to files that changed. "
        "Non-SQL files are silently ignored.",
    )


@mcp.tool(
    name="reindex_files",
    annotations=ToolAnnotations(
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    ),
)
async def reindex_files(params: ReindexFilesInput) -> dict:
    """Reindex specific files after save. Non-blocking.

    Fast path for on-save reindex. Accepts absolute file paths,
    resolves to repos, and reindexes only the affected models.

    - Plain SQL files: reindexed in ~50ms
    - dbt/sqlmesh models: compiled + reindexed in ~2-5s

    Multiple rapid calls are debounced per repo. Returns immediately;
    reindex runs in background.
    """
    state = _state
    if not state:
        return {"error": "Server not configured. Call configure() first."}

    sql_files = [p for p in params.paths if is_sql_file(p)]
    if not sql_files:
        return {"accepted": 0, "skipped": len(params.paths), "reason": "No SQL files in paths"}

    # Resolve files to repos and group by (repo_name, repo_type)
    all_repos = state.indexer.graph.get_all_repos()
    enqueued = 0
    skipped = 0
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in sql_files:
        resolved = state.indexer._resolve_file_repo(Path(path).resolve(), all_repos)
        if resolved:
            repo_id, repo_name, repo_path, repo_type = resolved
            grouped[(repo_name, repo_type)].append(path)
            enqueued += 1
        else:
            skipped += 1

    for (repo_name, repo_type), paths in grouped.items():
        await _enqueue_reindex(repo_name, repo_type, paths)

    non_sql_skipped = len(params.paths) - len(sql_files)
    result: dict = {
        "accepted": enqueued,
        "skipped": skipped + non_sql_skipped,
        "queued_at": datetime.now().isoformat(),
    }
    if enqueued > 0:
        result["note"] = "Reindex queued. Check index_status for progress."
    else:
        result["reason"] = "No SQL files matched a configured repo"
    return result


@mcp.tool(
    name="index_status",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
async def index_status() -> dict:
    """Current state of the index — repos, file counts, last commit, staleness."""
    status = await asyncio.to_thread(_get_graph().get_index_status)
    if _reindex_task and not _reindex_task.done():
        status["reindex_in_progress"] = True
        status["reindex_status"] = _reindex_status
    elif _reindex_status.get("state") in ("completed", "failed"):
        status["last_reindex"] = _reindex_status
    status["parse_error_count"] = len(_last_parse_errors)
    if _last_parse_errors:
        status["last_parse_errors"] = _last_parse_errors[:50]  # cap at 50
    return status


# ── Internal helpers ──


def _node_fingerprint(node: NodeResult) -> str:
    """Create a comparable fingerprint for a node including metadata."""
    return json.dumps(node.metadata, sort_keys=True) if node.metadata else ""


def _compute_structural_diff(
    old_results: dict[str, ParseResult],
    new_results: dict[str, ParseResult],
) -> dict:
    """Compare old and new parse results to find structural changes."""
    old_nodes = set()
    new_nodes = set()
    old_edges = set()
    new_edges = set()
    old_columns = set()
    new_columns = set()

    # Track edges, columns, and metadata per node to detect actual modifications
    old_node_edges: dict[tuple[str, str, str | None], set] = {}
    new_node_edges: dict[tuple[str, str, str | None], set] = {}
    old_node_columns: dict[tuple[str, str, str | None], set] = {}
    new_node_columns: dict[tuple[str, str, str | None], set] = {}
    old_node_meta: dict[tuple[str, str, str | None], str] = {}
    new_node_meta: dict[tuple[str, str, str | None], str] = {}

    # Build (name, kind) -> schema lookups so edges/columns can resolve schema
    old_schema_lookup: dict[tuple[str, str], str | None] = {}
    new_schema_lookup: dict[tuple[str, str], str | None] = {}

    for result in old_results.values():
        for n in result.nodes:
            schema = (n.metadata or {}).get("schema")
            key = (n.name, n.kind, schema)
            old_nodes.add(key)
            old_node_edges.setdefault(key, set())
            old_node_columns.setdefault(key, set())
            old_node_meta[key] = _node_fingerprint(n)
            old_schema_lookup[(n.name, n.kind)] = schema
        for e in result.edges:
            edge_tuple = (
                e.source_name,
                e.source_kind,
                e.target_name,
                e.target_kind,
                e.relationship,
            )
            old_edges.add(edge_tuple)
            src_schema = old_schema_lookup.get((e.source_name, e.source_kind))
            src_key = (e.source_name, e.source_kind, src_schema)
            old_node_edges.setdefault(src_key, set()).add(edge_tuple)
        for c in result.column_usage:
            col_tuple = (c.node_name, c.table_name, c.column_name, c.usage_type)
            old_columns.add(col_tuple)
            col_schema = old_schema_lookup.get((c.node_name, c.node_kind))
            col_key = (c.node_name, c.node_kind, col_schema)
            old_node_columns.setdefault(col_key, set()).add(col_tuple)

    for result in new_results.values():
        for n in result.nodes:
            schema = (n.metadata or {}).get("schema")
            key = (n.name, n.kind, schema)
            new_nodes.add(key)
            new_node_edges.setdefault(key, set())
            new_node_columns.setdefault(key, set())
            new_node_meta[key] = _node_fingerprint(n)
            new_schema_lookup[(n.name, n.kind)] = schema
        for e in result.edges:
            edge_tuple = (
                e.source_name,
                e.source_kind,
                e.target_name,
                e.target_kind,
                e.relationship,
            )
            new_edges.add(edge_tuple)
            src_schema = new_schema_lookup.get((e.source_name, e.source_kind))
            src_key = (e.source_name, e.source_kind, src_schema)
            new_node_edges.setdefault(src_key, set()).add(edge_tuple)
        for c in result.column_usage:
            col_tuple = (c.node_name, c.table_name, c.column_name, c.usage_type)
            new_columns.add(col_tuple)
            col_schema = new_schema_lookup.get((c.node_name, c.node_kind))
            col_key = (c.node_name, c.node_kind, col_schema)
            new_node_columns.setdefault(col_key, set()).add(col_tuple)

    # A node present in both is "modified" if its edges, columns, or metadata changed
    nodes_modified = []
    for key in old_nodes & new_nodes:
        if (
            old_node_edges.get(key, set()) != new_node_edges.get(key, set())
            or old_node_columns.get(key, set()) != new_node_columns.get(key, set())
            or old_node_meta.get(key, "") != new_node_meta.get(key, "")
        ):
            entry = {"name": key[0], "kind": key[1]}
            if key[2] is not None:
                entry["schema"] = key[2]
            nodes_modified.append(entry)

    def _node_dict(n):
        d = {"name": n[0], "kind": n[1]}
        if n[2] is not None:
            d["schema"] = n[2]
        return d

    return {
        "nodes_added": [_node_dict(n) for n in new_nodes - old_nodes],
        "nodes_removed": [_node_dict(n) for n in old_nodes - new_nodes],
        "nodes_modified": nodes_modified,
        "edges_added": [
            {
                "source": e[0],
                "source_kind": e[1],
                "target": e[2],
                "target_kind": e[3],
                "relationship": e[4],
            }
            for e in new_edges - old_edges
        ],
        "edges_removed": [
            {
                "source": e[0],
                "source_kind": e[1],
                "target": e[2],
                "target_kind": e[3],
                "relationship": e[4],
            }
            for e in old_edges - new_edges
        ],
        "columns_added": [
            {"node": c[0], "table": c[1], "column": c[2], "usage_type": c[3]} for c in new_columns - old_columns
        ],
        "columns_removed": [
            {"node": c[0], "table": c[1], "column": c[2], "usage_type": c[3]} for c in old_columns - new_columns
        ],
    }
