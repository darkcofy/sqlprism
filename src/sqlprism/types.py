"""Shared data types for the SQL indexer.

These dataclasses define the contract between parsers and the indexer orchestrator.
Every language parser returns a ParseResult. The orchestrator consumes ParseResults
and writes to DuckDB. Parsers never touch the database. The orchestrator never
does language-specific parsing.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class NodeResult:
    """A nameable entity found in a file.

    Nodes are the universal unit of the knowledge graph. A node is anything
    a parser identifies as structurally meaningful: a table, view, CTE,
    function, class, module, API endpoint, Terraform resource, etc.

    The ``kind`` field is parser-defined and unconstrained -- each language
    emits whatever kinds are meaningful for it.

    Attributes:
        kind: Entity type (e.g. ``"table"``, ``"view"``, ``"cte"``).
        name: Unqualified entity name (e.g. ``"orders"``).
        line_start: First line in the source file, or ``None`` if unknown.
        line_end: Last line in the source file, or ``None`` if unknown.
        metadata: Arbitrary parser-supplied metadata (schema, dialect, filters, etc.).
    """

    kind: str
    name: str
    line_start: int | None = None
    line_end: int | None = None
    metadata: dict | None = None


@dataclass(frozen=True)
class EdgeResult:
    """A relationship between two entities.

    Edges reference nodes by ``(name, kind)`` pairs, not database IDs. The
    indexer orchestrator resolves these to node IDs during insertion. This
    means parsers don't need to know about the database and parse order
    doesn't matter.

    The target may be in another file or even another repo. If unresolved at
    insert time, the orchestrator creates a phantom node.

    Attributes:
        source_name: Name of the source node.
        source_kind: Kind of the source node (e.g. ``"query"``).
        target_name: Name of the target node.
        target_kind: Kind of the target node (e.g. ``"table"``).
        relationship: Edge label (e.g. ``"references"``, ``"defines"``,
            ``"inserts_into"``, ``"cte_references"``).
        context: Human-readable context (e.g. ``"FROM clause"``, ``"JOIN clause"``).
        metadata: Arbitrary edge metadata (source_schema, target_schema, etc.).
    """

    source_name: str
    source_kind: str
    target_name: str
    target_kind: str
    relationship: str
    context: str | None = None
    metadata: dict | None = None


@dataclass(frozen=True)
class ColumnUsageResult:
    """SQL-specific: column-level lineage from sqlglot.

    Records which columns are used where and how. Only the SQL parser
    populates these -- all other parsers return an empty list.

    This data is stored in a separate table from edges because column
    usage is high-volume with its own query patterns (flat scans, not
    graph traversals).

    Attributes:
        node_name: Name of the query/CTE/view that uses this column.
        node_kind: Kind of the owning node (e.g. ``"query"``, ``"cte"``).
        table_name: Source table the column belongs to.
        column_name: Column name (``"*"`` for ``SELECT *``).
        usage_type: How the column is used. One of ``"select"``,
            ``"where"``, ``"join_on"``, ``"group_by"``, ``"order_by"``,
            ``"having"``, ``"insert"``, ``"update"``, ``"partition_by"``,
            ``"window_order"``, ``"qualify"``.
        alias: Output alias if the column is aliased (``AS name``).
        transform: Wrapping expression, e.g. ``"CAST(a.updated AS DATETIME)"``.
    """

    node_name: str
    node_kind: str
    table_name: str
    column_name: str
    # 'select', 'where', 'join_on', 'group_by', 'order_by', 'having', 'insert', 'update'
    usage_type: str
    alias: str | None = None
    transform: str | None = None  # wrapping expression e.g. "CAST(a.updated AS DATETIME)"


@dataclass(frozen=True)
class LineageHop:
    """One hop in a column lineage chain.

    Attributes:
        column: Column name at this hop.
        table: Table, CTE, or subquery name at this hop.
        expression: Transform applied at this hop (e.g. ``"CAST(amount AS DECIMAL)"``),
            or ``None`` if the column passes through unchanged.
    """

    column: str
    table: str  # table, CTE, or subquery name
    expression: str | None = None  # transform at this hop, e.g. "CAST(amount AS DECIMAL)"


@dataclass(frozen=True)
class ColumnLineageResult:
    """End-to-end column lineage through CTEs and subqueries.

    Traces an output column back to its source table column(s),
    recording each intermediate hop (CTE, subquery, transform).

    Attributes:
        output_column: Column name in the final output.
        output_node: The query, table, or view that produces this column.
        chain: Ordered hops from output back to source.
    """

    output_column: str  # column name in the final output
    output_node: str  # the query/table/view that produces this column
    chain: list[LineageHop] = field(default_factory=list)  # ordered hops from output → source


@dataclass(frozen=True)
class ColumnDefResult:
    """Column definition metadata extracted from SQL or schema files.

    Records column-level metadata for tables and views, including the
    column's data type, ordinal position, provenance, and optional
    description. Parsers emit these alongside nodes and edges so the
    indexer can build a column-level catalogue.

    Attributes:
        node_name: The table or view this column belongs to.
        column_name: Column name as declared.
        data_type: SQL data type (e.g. ``"VARCHAR"``, ``"INT"``), or ``None``
            if unknown.
        position: Ordinal position in the column list (0-based), or ``None``
            if unavailable.
        source: How this column was discovered. One of ``"definition"``
            (from CREATE/ALTER DDL), ``"inferred"`` (from SELECT output),
            ``"schema_yml"`` (from dbt schema.yml), ``"sqlmesh_schema"``
            (from sqlmesh model schema).
        description: Human-readable column description, or ``None``.
    """

    node_name: str
    column_name: str
    data_type: str | None = None
    position: int | None = None
    source: str = "definition"
    description: str | None = None


def parse_repo_config(
    cfg: str | dict,
    global_dialect: str | None = None,
) -> tuple[str, str | None, dict[str, str] | None]:
    """Parse a repo config value into (path, dialect, dialect_overrides).

    Supports both simple string paths and full config dicts::

        "my-repo": "/path/to/repo"
        "my-repo": {"path": "/path", "dialect": "starrocks",
                    "dialect_overrides": {"athena/": "athena"}}
    """
    if isinstance(cfg, str):
        return cfg, global_dialect, None
    return (
        cfg["path"],
        cfg.get("dialect", global_dialect),
        cfg.get("dialect_overrides"),
    )


@dataclass
class ParseResult:
    """Everything a parser returns for one file.

    This is the complete interface contract. A parser receives a file path
    and its content, and returns one of these. The orchestrator handles
    everything from here -- ID assignment, edge resolution, database writes.

    Mutation contract:
        ParseResult is intentionally **mutable** (not ``frozen=True``).
        Renderers and post-processing steps mutate ``nodes``, ``edges``, and
        other lists **in-place** -- e.g. appending synthetic nodes, deduplicating
        edges, or rewriting names during normalisation. This is by design:
        allocating a new ParseResult for every transform would add complexity
        with no practical benefit, since a ParseResult is owned by a single
        file-processing pipeline and is never shared across threads.

    Attributes:
        language: Parser language identifier (e.g. ``"sql"``).
        nodes: Entities discovered in the file.
        edges: Relationships between entities.
        column_usage: Column-level usage records (SQL only).
        column_lineage: End-to-end column lineage chains (SQL only).
        columns: Column definitions extracted from DDL or schema files.
        errors: Non-fatal parse errors encountered during processing.
    """

    language: str
    nodes: list[NodeResult] = field(default_factory=list)
    edges: list[EdgeResult] = field(default_factory=list)
    column_usage: list[ColumnUsageResult] = field(default_factory=list)
    column_lineage: list[ColumnLineageResult] = field(default_factory=list)
    columns: list[ColumnDefResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
