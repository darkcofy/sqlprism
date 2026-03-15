"""SQL parser using sqlglot.

This is the richest parser in the system. sqlglot provides semantic analysis
beyond what tree-sitter can offer for SQL: CTE scope tracking, column-level
lineage via the Scope module, multi-dialect awareness, and proper resolution
of aliased references.

CTEs are tracked as first-class nodes, not flattened into the parent query.
"""

from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.scope import build_scope

from sqlprism.types import (
    ColumnDefResult,
    ColumnLineageResult,
    ColumnUsageResult,
    EdgeResult,
    LineageHop,
    NodeResult,
    ParseResult,
)


class SqlParser:
    """Parses SQL files into nodes, edges, column usage, and column lineage using sqlglot.

    Handles multi-statement files, CTE extraction, column-level scope analysis,
    transform detection, and end-to-end column lineage tracing. Dialect-aware
    identifier normalisation ensures consistent casing across Postgres, Snowflake,
    DuckDB, and other engines.
    """

    # Dialects that fold unquoted identifiers to lowercase
    _LOWERCASE_DIALECTS = frozenset({"postgres", "postgresql", "redshift", "duckdb"})
    # Dialects that fold unquoted identifiers to uppercase
    _UPPERCASE_DIALECTS = frozenset({"snowflake", "oracle", "db2"})

    def __init__(self, dialect: str | None = None):
        """Initialise with an optional SQL dialect.

        Args:
            dialect: sqlglot dialect string (e.g., 'postgres', 'mysql', 'duckdb').
                     None for auto-detection.
        """
        self.dialect = dialect

    def parse(self, file_path: str, file_content: str, schema: dict | None = None) -> ParseResult:
        """Parse a SQL file into nodes, edges, column usage, and column lineage.

        Handles multiple statements per file. Each statement is parsed
        independently. Errors in one statement don't prevent parsing others.

        Args:
            file_path: Path to the SQL file (used for naming nodes).
            file_content: Raw SQL content.
            schema: Optional schema catalog ``{table: {col: type}}`` for
                expanding ``SELECT *`` in lineage tracing via
                ``qualify_columns``.

        Returns:
            A ``ParseResult`` containing all extracted nodes, edges,
            column usage records, column lineage chains, and any
            non-fatal parse errors.
        """
        nodes: list[NodeResult] = []
        edges: list[EdgeResult] = []
        column_usage: list[ColumnUsageResult] = []
        column_lineage: list[ColumnLineageResult] = []
        columns: list[ColumnDefResult] = []
        errors: list[str] = []

        file_stem = Path(file_path).stem

        try:
            statements = sqlglot.parse(file_content, dialect=self.dialect)
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as e:
            return ParseResult(language="sql", errors=[f"Parse error: {e}"])

        # Persistent dedup sets across all statements in this file
        seen_nodes: set[tuple[str, str, str | None]] = set()
        seen_ctes: set[str] = set()

        for stmt_idx, stmt in enumerate(statements):
            if stmt is None:
                continue

            try:
                self._process_statement(
                    stmt,
                    file_stem,
                    file_path,
                    nodes,
                    edges,
                    column_usage,
                    columns,
                    seen_nodes=seen_nodes,
                    seen_ctes=seen_ctes,
                )
            except Exception as e:
                errors.append(f"Statement {stmt_idx}: {type(e).__name__}: {e}")
                continue

            # Column lineage via sqlglot.lineage — separate pass
            try:
                self._extract_column_lineage(stmt, file_stem, file_content, column_lineage, schema=schema)
            except Exception as e:
                errors.append(f"Lineage stmt {stmt_idx}: {type(e).__name__}: {e}")
                continue

        return ParseResult(
            language="sql",
            nodes=nodes,
            edges=edges,
            column_usage=column_usage,
            column_lineage=column_lineage,
            columns=columns,
            errors=errors,
        )

    def _process_statement(
        self,
        stmt: exp.Expression,
        file_stem: str,
        file_path: str,
        nodes: list[NodeResult],
        edges: list[EdgeResult],
        column_usage: list[ColumnUsageResult],
        columns: list[ColumnDefResult] | None = None,
        seen_nodes: set[tuple[str, str, str | None]] | None = None,
        seen_ctes: set[str] | None = None,
    ) -> None:
        """Process a single SQL statement."""
        if columns is None:
            columns = []
        # Use persistent dedup sets across statements, or create fresh ones
        if seen_nodes is None:
            seen_nodes = {(n.name, n.kind, (n.metadata or {}).get("schema")) for n in nodes}
        seen_edges: set[tuple[str, str, str]] = set()

        # CREATE TABLE / CREATE VIEW
        if isinstance(stmt, exp.Create):
            self._process_create(stmt, file_stem, nodes, edges, columns)

        # Extract table references from any statement type
        self._extract_table_references(stmt, file_stem, nodes, edges, seen_nodes, seen_edges)

        # Extract CTEs as first-class nodes
        self._extract_ctes(stmt, file_stem, nodes, edges, seen_ctes=seen_ctes)

        # Column-level lineage via sqlglot's scope analysis
        self._extract_column_usage(stmt, file_stem, nodes, column_usage)

        # INSERT...SELECT column mapping
        if isinstance(stmt, exp.Insert):
            self._extract_insert_select_mapping(stmt, file_stem, column_usage)

    def _process_create(
        self,
        stmt: exp.Create,
        file_stem: str,
        nodes: list[NodeResult],
        edges: list[EdgeResult],
        columns: list[ColumnDefResult] | None = None,
    ) -> None:
        """Handle CREATE TABLE / CREATE VIEW statements."""
        kind_expr = stmt.args.get("kind")
        if not kind_expr:
            return

        kind_str = kind_expr.upper() if isinstance(kind_expr, str) else str(kind_expr).upper()

        # Unwrap Schema -> Table if needed
        table_expr = stmt.this
        schema_expr = None
        if isinstance(table_expr, exp.Schema):
            schema_expr = table_expr
            table_expr = table_expr.this
        if not isinstance(table_expr, exp.Table):
            return

        name = self._normalize_identifier(table_expr.name, self._is_quoted_identifier(table_expr))
        if not name:
            return

        node_kind = "view" if "VIEW" in kind_str else "table"
        metadata = self._build_table_metadata(table_expr)
        metadata["dialect"] = self.dialect
        metadata["create_type"] = kind_str

        nodes.append(
            NodeResult(
                kind=node_kind,
                name=name,
                line_start=None,  # sqlglot doesn't track line numbers reliably
                metadata=metadata,
            )
        )
        edges.append(
            EdgeResult(
                source_name=file_stem,
                source_kind="query",
                target_name=name,
                target_kind=node_kind,
                relationship="defines",
                context="CREATE statement",
            )
        )

        if columns is None:
            return

        # Extract column definitions from CREATE TABLE (ColumnDef nodes)
        if schema_expr is not None and schema_expr.expressions:
            for i, col_expr in enumerate(schema_expr.expressions):
                if isinstance(col_expr, exp.ColumnDef):
                    col_name = col_expr.this.name if col_expr.this else None
                    if not col_name:
                        continue
                    col_type = col_expr.kind.sql(dialect=self.dialect) if col_expr.kind else None
                    columns.append(
                        ColumnDefResult(
                            node_name=name,
                            column_name=col_name,
                            data_type=col_type,
                            position=i,
                            source="definition",
                        )
                    )

        # Infer output columns from CREATE VIEW AS SELECT
        if node_kind == "view" and stmt.expression:
            select_expr = stmt.expression
            if isinstance(select_expr, exp.Select) or isinstance(select_expr, exp.Query):
                self._extract_inferred_columns(select_expr, name, columns)

    def _extract_inferred_columns(
        self,
        select_expr: exp.Expression,
        node_name: str,
        columns: list[ColumnDefResult],
    ) -> None:
        """Infer output column names from a SELECT expression."""
        # Find the outermost SELECT
        select = select_expr.find(exp.Select) if not isinstance(select_expr, exp.Select) else select_expr
        if not select or not select.expressions:
            return

        for i, sel_col in enumerate(select.expressions):
            # Use alias if present, otherwise try to get column name
            if isinstance(sel_col, exp.Alias):
                col_name = sel_col.alias
            elif isinstance(sel_col, exp.Column):
                col_name = sel_col.name
            elif isinstance(sel_col, exp.Star):
                continue  # can't infer from *
            else:
                # Try alias_or_name for other expression types
                col_name = sel_col.alias_or_name if hasattr(sel_col, "alias_or_name") else None

            if col_name and col_name != "*":
                columns.append(
                    ColumnDefResult(
                        node_name=node_name,
                        column_name=col_name,
                        position=i,
                        source="inferred",
                    )
                )

    def _extract_table_references(
        self,
        stmt: exp.Expression,
        file_stem: str,
        nodes: list[NodeResult],
        edges: list[EdgeResult],
        seen_nodes: set[tuple[str, str, str | None]] | None = None,
        seen_edges: set[tuple[str, str, str]] | None = None,
    ) -> None:
        """Extract all table references from a statement."""
        if seen_nodes is None:
            seen_nodes = {(n.name, n.kind, (n.metadata or {}).get("schema")) for n in nodes}
        if seen_edges is None:
            seen_edges = set()

        # Identify the CREATE target so we don't double-count it as a reference
        create_target: str | None = None
        if isinstance(stmt, exp.Create):
            target_expr = stmt.this
            if isinstance(target_expr, exp.Schema):
                target_expr = target_expr.this
            if isinstance(target_expr, exp.Table) and target_expr.name:
                create_target = self._normalize_identifier(
                    target_expr.name,
                    self._is_quoted_identifier(target_expr),
                )

        for table in stmt.find_all(exp.Table):
            name = self._normalize_identifier(table.name, self._is_quoted_identifier(table))
            if not name:
                continue

            # Skip the CREATE target — it's already handled by _process_create
            if name == create_target:
                # Check if this is the actual CREATE target (direct child of Create/Schema)
                parent = table.parent
                if isinstance(parent, (exp.Create, exp.Schema)):
                    continue

            # Avoid duplicating nodes for the same table+schema within one file (O(1) check)
            metadata = self._build_table_metadata(table)
            table_schema = metadata.get("schema")
            node_key = (name, "table", table_schema)
            if node_key not in seen_nodes:
                seen_nodes.add(node_key)
                nodes.append(NodeResult(kind="table", name=name, metadata=metadata or None))

            # Determine context from parent expression
            context = self._get_table_context(table)

            relationship = "inserts_into" if isinstance(stmt, exp.Insert) else "references"

            # Skip duplicate edges with the same (source, target, context)
            edge_key = (file_stem, name, context)
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)

            edges.append(
                EdgeResult(
                    source_name=file_stem,
                    source_kind="query",
                    target_name=name,
                    target_kind="table",
                    relationship=relationship,
                    context=context,
                )
            )

    def _extract_ctes(
        self,
        stmt: exp.Expression,
        file_stem: str,
        nodes: list[NodeResult],
        edges: list[EdgeResult],
        seen_ctes: set[str] | None = None,
    ) -> None:
        """Extract CTEs as first-class nodes with their own edges.

        When a CTE references another CTE from the same statement, the edge
        uses target_kind='cte' so trace queries follow CTE chains correctly.

        Args:
            seen_ctes: Set of CTE names already added across statements in this file.
                       Used to deduplicate CTEs with the same name across statements.
        """
        if seen_ctes is None:
            seen_ctes = set()

        # Collect all CTE names in this statement first
        cte_names: set[str] = set()
        for cte in stmt.find_all(exp.CTE):
            if cte.alias:
                alias_node = cte.args.get("alias")
                quoted = self._is_quoted_identifier(alias_node) if alias_node else False
                cte_names.add(self._normalize_identifier(cte.alias, quoted))

        for cte in stmt.find_all(exp.CTE):
            alias_node = cte.args.get("alias")
            cte_quoted = self._is_quoted_identifier(alias_node) if alias_node else False
            cte_name = self._normalize_identifier(cte.alias, cte_quoted) if cte.alias else None
            if not cte_name:
                continue

            # Deduplicate CTEs across statements in the same file
            if cte_name in seen_ctes:
                continue
            seen_ctes.add(cte_name)

            nodes.append(
                NodeResult(
                    kind="cte",
                    name=cte_name,
                    metadata={"parent_query": file_stem},
                )
            )

            # Find tables referenced within this CTE
            for table in cte.find_all(exp.Table):
                table_name = self._normalize_identifier(
                    table.name,
                    self._is_quoted_identifier(table),
                )
                if not table_name or table_name == cte_name:
                    continue

                # If the reference is to another CTE, use target_kind='cte'
                target_kind = "cte" if table_name in cte_names else "table"

                edges.append(
                    EdgeResult(
                        source_name=cte_name,
                        source_kind="cte",
                        target_name=table_name,
                        target_kind=target_kind,
                        relationship="cte_references",
                        context=self._get_table_context(table),
                    )
                )

    def _extract_column_usage(
        self,
        stmt: exp.Expression,
        file_stem: str,
        nodes: list[NodeResult],
        column_usage: list[ColumnUsageResult],
    ) -> None:
        """Extract column-level usage via sqlglot's scope analysis.

        This is where sqlglot's investment pays off — scope-aware column
        resolution that understands aliases, CTEs, and subqueries.
        Also captures wrapping transforms (CAST, COALESCE, etc.) and
        extracts WHERE clause filters as node metadata.
        """
        # Only works on SELECT-like statements
        select = stmt
        if not isinstance(stmt, (exp.Select, exp.Union)):
            select = stmt.find(exp.Select)
            if select is None:
                return

        try:
            root_scope = build_scope(select)
        except Exception:
            return

        if root_scope is None:
            return

        seen_scopes = set()
        for scope in [root_scope] + list(root_scope.traverse()):
            scope_id = id(scope)
            if scope_id in seen_scopes:
                continue
            seen_scopes.add(scope_id)

            # Determine scope name
            scope_name = file_stem
            scope_kind = "query"
            parent_expr = scope.expression.parent
            if scope.is_cte:
                # Extract CTE name from the expression's parent
                if isinstance(parent_expr, exp.CTE) and parent_expr.alias:
                    alias_node = parent_expr.args.get("alias")
                    quoted = self._is_quoted_identifier(alias_node) if alias_node else False
                    scope_name = self._normalize_identifier(parent_expr.alias, quoted)
                    scope_kind = "cte"
            elif isinstance(parent_expr, exp.Subquery) and parent_expr.alias:
                # Derived table (subquery in FROM/JOIN)
                alias_node = parent_expr.args.get("alias")
                quoted = self._is_quoted_identifier(alias_node) if alias_node else False
                scope_name = self._normalize_identifier(parent_expr.alias, quoted)
                scope_kind = "subquery"
                # Create a node for the subquery alias so column_usage can resolve
                nodes.append(
                    NodeResult(
                        kind="subquery",
                        name=scope_name,
                        metadata={"parent_query": file_stem},
                    )
                )
            elif isinstance(parent_expr, exp.Create):
                # Root scope inside CREATE TABLE/VIEW — use the table name
                table_expr = parent_expr.this
                if isinstance(table_expr, exp.Schema):
                    table_expr = table_expr.this
                if isinstance(table_expr, exp.Table) and table_expr.name:
                    scope_name = self._normalize_identifier(
                        table_expr.name,
                        self._is_quoted_identifier(table_expr),
                    )
            elif scope_kind == "query" and (scope_name, "query", None) not in {
                (n.name, n.kind, (n.metadata or {}).get("schema")) for n in nodes
            }:
                # Bare SELECT root scope — create a query node so column_usage resolves
                nodes.append(NodeResult(kind="query", name=scope_name, metadata={"bare_query": True}))

            # Build alias → real table name mapping
            alias_map: dict[str, str] = {}
            for source_name, source in scope.sources.items():
                if isinstance(source, exp.Table):
                    alias_map[source_name] = self._normalize_identifier(
                        source.name,
                        self._is_quoted_identifier(source),
                    )

            # When there's exactly one source and no table qualifier, infer the table
            single_table = ""
            if len(alias_map) == 1:
                single_table = next(iter(alias_map.values()))

            for col in scope.columns:
                if not isinstance(col, exp.Column):
                    continue
                col_name = self._normalize_identifier(col.name, self._is_quoted_identifier(col))
                if not col_name:
                    continue

                # Resolve alias to real table name
                table_alias = col.table or ""
                table_name = alias_map.get(table_alias, table_alias)
                if not table_name and single_table:
                    table_name = single_table

                usage_type = self._classify_column_context(col)
                transform = self._extract_transform(col)
                alias = self._extract_alias(col)

                column_usage.append(
                    ColumnUsageResult(
                        node_name=scope_name,
                        node_kind=scope_kind,
                        table_name=table_name,
                        column_name=col_name,
                        usage_type=usage_type,
                        alias=alias,
                        transform=transform,
                    )
                )

            # Handle SELECT * — emit usage for each source table
            select_expr = scope.expression
            if isinstance(select_expr, exp.Select):
                for expr in select_expr.expressions:
                    if isinstance(expr, exp.Star):
                        # Unqualified * — emit for each source table
                        for source_name, source in scope.sources.items():
                            table_name = source.name if isinstance(source, exp.Table) else source_name
                            column_usage.append(
                                ColumnUsageResult(
                                    node_name=scope_name,
                                    node_kind=scope_kind,
                                    table_name=table_name,
                                    column_name="*",
                                    usage_type="select",
                                )
                            )
                    elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                        # Qualified table.* — emit for that specific table
                        table_alias = expr.table or ""
                        table_name = alias_map.get(table_alias, table_alias)
                        column_usage.append(
                            ColumnUsageResult(
                                node_name=scope_name,
                                node_kind=scope_kind,
                                table_name=table_name,
                                column_name="*",
                                usage_type="select",
                            )
                        )

            # Extract WHERE filters as metadata on the scope's node
            self._extract_where_filters(scope, scope_name, scope_kind, nodes)

    def _classify_column_context(self, col: exp.Column) -> str:
        """Determine how a column is used based on its AST position.

        Distinguishes window function sub-clauses (PARTITION BY, window ORDER BY)
        from regular usage types.
        """
        parent = col.parent

        while parent:
            # Window function sub-clauses — check before general Order
            if isinstance(parent, exp.Window):
                # Determine if column is in PARTITION BY or ORDER BY within window
                return self._classify_window_position(col, parent)
            if isinstance(parent, exp.Where):
                return "where"
            if isinstance(parent, exp.Join):
                return "join_on"
            if isinstance(parent, exp.Group):
                return "group_by"
            if isinstance(parent, exp.Order):
                # Check if this Order is inside a Window (window ORDER BY)
                order_parent = parent.parent
                if isinstance(order_parent, exp.Window):
                    return "window_order"
                return "order_by"
            if isinstance(parent, exp.Having):
                return "having"
            if isinstance(parent, exp.Qualify):
                return "qualify"
            if isinstance(parent, exp.Select):
                return "select"
            parent = parent.parent

        return "unknown"

    def _classify_window_position(self, col: exp.Column, window: exp.Window) -> str:
        """Classify a column's position within a window function."""
        # Walk from column up to the window, checking if we pass through
        # partition_by or order clause
        parent = col.parent
        while parent and parent is not window:
            if isinstance(parent, exp.Order):
                return "window_order"
            parent = parent.parent

        # Check if column is in the partition_by list
        partition_by = window.args.get("partition_by")
        if partition_by:
            for partition_col in partition_by:
                if col in partition_col.walk():
                    return "partition_by"

        return "select"  # fallback — column is in the aggregate part of the window

    def _extract_transform(self, col: exp.Column) -> str | None:
        """Extract the wrapping transform expression around a column.

        Walks up from the Column node to find wrapping functions like
        CAST, COALESCE, IF, CASE, arithmetic, etc. Returns the SQL string
        of the outermost meaningful wrapper, or None if the column is bare.
        """
        # Wrapping expression types that constitute a "transform"
        transform_types = (
            exp.Cast,
            exp.TryCast,
            exp.Coalesce,
            exp.If,
            exp.Case,
            exp.Anonymous,  # function calls like NVL, IFNULL, etc.
            exp.Func,  # base class for all functions (UPPER, LOWER, etc.)
            exp.Add,
            exp.Sub,
            exp.Mul,
            exp.Div,
            exp.Mod,
            exp.Concat,
            exp.DPipe,  # || concat operator
            exp.Substring,
            exp.Trim,
            exp.Extract,  # EXTRACT(YEAR FROM ...)
            exp.DateAdd,
            exp.DateSub,
            exp.DateDiff,
            exp.Between,
            exp.In,
            exp.Like,
            exp.Neg,  # unary minus
        )

        # Comparison types — include as transforms but don't traverse past
        comparison_types = (
            exp.EQ,
            exp.NEQ,
            exp.GT,
            exp.GTE,
            exp.LT,
            exp.LTE,
            exp.Is,
            exp.Not,
        )

        parent = col.parent
        outermost = None

        while parent:
            if isinstance(parent, transform_types):
                outermost = parent
            elif isinstance(parent, comparison_types):
                outermost = parent
                break  # comparisons are the natural boundary for WHERE/JOIN
            elif isinstance(parent, (exp.And, exp.Or)):
                break  # don't capture the full AND/OR chain
            elif isinstance(
                parent,
                (
                    exp.Select,
                    exp.Where,
                    exp.Group,
                    exp.Order,
                    exp.Having,
                    exp.Join,
                    exp.From,
                    exp.Subquery,
                    exp.CTE,
                ),
            ):
                # Stop at clause boundaries
                break
            parent = parent.parent

        if outermost is None:
            return None

        try:
            sql = outermost.sql(dialect=self.dialect)
            # Skip if the transform is just the column itself
            col_sql = col.sql(dialect=self.dialect)
            if sql == col_sql:
                return None
            return sql
        except Exception:
            return None

    def _extract_alias(self, col: exp.Column) -> str | None:
        """Extract the output alias for a column (AS name)."""
        parent = col.parent
        while parent:
            if isinstance(parent, exp.Alias):
                return parent.alias
            if isinstance(parent, (exp.Select, exp.Where, exp.Group, exp.Order, exp.Having)):
                break
            parent = parent.parent
        return None

    def _extract_where_filters(
        self,
        scope,
        scope_name: str,
        scope_kind: str,
        nodes: list[NodeResult],
    ) -> None:
        """Extract WHERE clause conditions and attach as metadata to the scope's node.

        Finds the WHERE clause in the scope expression and extracts each
        top-level condition as a string. These are stored as node metadata
        so they're searchable in the graph.
        """
        try:
            # Use .args["where"] to get only the direct WHERE, not from subqueries
            where = scope.expression.args.get("where")
        except Exception:
            return

        if not where:
            return

        filters = []
        # Split AND conditions into individual filters
        conditions = self._split_conditions(where.this)
        for cond in conditions:
            try:
                sql = cond.sql(dialect=self.dialect)
                if sql and len(sql) < 500:  # skip absurdly long conditions
                    filters.append(sql)
            except Exception:
                continue

        if not filters:
            return

        # Find the matching node and update its metadata
        # Try exact match first, then match by name only (handles query→table/view mapping)
        # Use enumerate to avoid O(N) nodes.index() and wrong-match-on-duplicates bug
        for idx, node in enumerate(nodes):
            if node.name == scope_name and (node.kind == scope_kind or node.kind in ("table", "view", "cte")):
                existing_meta = dict(node.metadata) if node.metadata else {}
                existing_meta["filters"] = filters
                # NodeResult is frozen, so we need to replace it
                nodes[idx] = NodeResult(
                    kind=node.kind,
                    name=node.name,
                    line_start=node.line_start,
                    line_end=node.line_end,
                    metadata=existing_meta,
                )
                return

    def _split_conditions(self, expr: exp.Expression) -> list[exp.Expression]:
        """Split an AND chain into individual conditions."""
        if isinstance(expr, exp.And):
            return self._split_conditions(expr.left) + self._split_conditions(expr.right)
        return [expr]

    def _extract_column_lineage(
        self,
        stmt: exp.Expression,
        file_stem: str,
        file_content: str,
        column_lineage: list[ColumnLineageResult],
        schema: dict | None = None,
    ) -> None:
        """Extract end-to-end column lineage using sqlglot.lineage.lineage().

        Traces each output column through CTEs and subqueries back to source tables.
        If a schema catalog is provided, it's passed to sqlglot_lineage to help
        resolve SELECT * and improve lineage accuracy.
        """
        # Find the output SELECT to get column names
        select = stmt
        output_name = file_stem

        if isinstance(stmt, exp.Create):
            # Get the CREATE target name
            table_expr = stmt.this
            if isinstance(table_expr, exp.Schema):
                table_expr = table_expr.this
            if isinstance(table_expr, exp.Table) and table_expr.name:
                output_name = table_expr.name
            select = stmt.find(exp.Select)
        elif not isinstance(stmt, (exp.Select, exp.Union)):
            select = stmt.find(exp.Select)

        if select is None:
            return

        # If schema available, try qualify_columns to expand SELECT *
        qualified_stmt = stmt
        if schema:
            try:
                qualified_stmt = qualify_columns(stmt.copy(), schema=schema, dialect=self.dialect)
                # Re-find the select from the qualified version
                if isinstance(qualified_stmt, exp.Create):
                    select = qualified_stmt.find(exp.Select)
                elif isinstance(qualified_stmt, (exp.Select, exp.Union)):
                    select = qualified_stmt
                else:
                    select = qualified_stmt.find(exp.Select)
                if select is None:
                    return
            except Exception:
                pass  # fall back to unqualified

        # Get output column names from the SELECT
        # For UNION, enumerate output columns from ALL branches
        if isinstance(select, exp.Union):
            output_cols = []
            seen_cols: set[str] = set()
            for branch_select in select.find_all(exp.Select):
                for expr in branch_select.expressions:
                    col_name = None
                    if isinstance(expr, exp.Alias):
                        col_name = expr.alias
                    elif isinstance(expr, exp.Column):
                        col_name = expr.name
                    elif isinstance(expr, exp.Star):
                        col_name = "*"
                    if col_name and col_name not in seen_cols:
                        seen_cols.add(col_name)
                        output_cols.append(col_name)
        elif isinstance(select, exp.Select):
            output_cols = []
            for expr in select.expressions:
                if isinstance(expr, exp.Alias):
                    output_cols.append(expr.alias)
                elif isinstance(expr, exp.Column):
                    output_cols.append(expr.name)
                elif isinstance(expr, exp.Star):
                    # SELECT * — can't trace individual columns without schema
                    output_cols.append("*")
                else:
                    # Complex expression without alias — skip
                    continue
        else:
            return

        # Trace each output column — pass AST directly to avoid re-serializing
        for col_name in output_cols:
            if col_name == "*":
                # Can't trace SELECT * without schema catalog
                continue
            try:
                root = sqlglot_lineage(
                    col_name,
                    qualified_stmt,
                    dialect=self.dialect,
                    schema=schema,
                )
            except Exception:
                continue

            # Walk the lineage tree to build hop chains
            chains = self._walk_lineage_tree(root, [])
            for chain in chains:
                if chain:  # skip empty chains
                    column_lineage.append(
                        ColumnLineageResult(
                            output_column=col_name,
                            output_node=output_name,
                            chain=chain,
                        )
                    )

    def _walk_lineage_tree(
        self,
        node,
        current_chain: list[LineageHop],
        max_depth: int = 50,
        max_chains: int = 1000,
        _chain_count: list | None = None,
    ) -> list[list[LineageHop]]:
        """Recursively walk a sqlglot lineage node tree into flat chains.

        Each leaf produces one complete chain from output to source.

        Args:
            node: Current lineage node.
            current_chain: Chain built so far.
            max_depth: Maximum recursion depth before treating node as leaf.
            max_chains: Maximum total chains to collect before stopping early.
            _chain_count: Mutable counter shared across recursion to track total chains.
        """
        if _chain_count is None:
            _chain_count = [0]

        # Stop if depth or chain limit exceeded — treat current node as leaf
        if len(current_chain) >= max_depth or _chain_count[0] >= max_chains:
            return [current_chain] if current_chain else []

        # Extract info from this node
        name = node.name if hasattr(node, "name") else ""
        source = node.source.sql() if hasattr(node, "source") and node.source else ""
        expr_str = (
            node.expression.sql(dialect=self.dialect) if hasattr(node, "expression") and node.expression else None
        )

        # Parse column and table from the node name (format: "table.column" or just "column")
        parts = name.split(".") if name else []
        hop_column = parts[-1] if parts else name
        hop_table = parts[-2] if len(parts) >= 2 else ""

        # If no table from name, try to extract from source
        if not hop_table and source:
            # Source often looks like "table AS alias" or just "table"
            source_parts = source.strip().split()
            if source_parts:
                hop_table = source_parts[0].strip('"').strip("'")

        hop = LineageHop(
            column=hop_column,
            table=hop_table,
            expression=expr_str if expr_str and expr_str != hop_column else None,
        )

        new_chain = current_chain + [hop]

        downstream = node.downstream if hasattr(node, "downstream") else []
        if not downstream:
            # Leaf node — return the completed chain
            _chain_count[0] += 1
            return [new_chain]

        # Recurse into downstream nodes
        all_chains = []
        for child in downstream:
            if _chain_count[0] >= max_chains:
                break
            all_chains.extend(self._walk_lineage_tree(child, new_chain, max_depth, max_chains, _chain_count))
        return all_chains

    def _extract_insert_select_mapping(
        self,
        stmt: exp.Insert,
        file_stem: str,
        column_usage: list[ColumnUsageResult],
    ) -> None:
        """Extract positional column mapping from INSERT...SELECT.

        When INSERT INTO target (a, b) SELECT x, y FROM source,
        maps source column x -> target column a, y -> b by position.
        """
        # Get the target table name
        target_table = stmt.this
        if isinstance(target_table, exp.Schema):
            # INSERT INTO table (col1, col2) — columns are Identifier nodes
            target_cols = [col.name for col in target_table.expressions if hasattr(col, "name")]
            target_table = target_table.this
        else:
            target_cols = []

        if not isinstance(target_table, exp.Table) or not target_table.name:
            return

        target_name = target_table.name

        # Get the SELECT statement
        select = stmt.expression
        if not isinstance(select, exp.Select):
            return

        # Get SELECT expressions (output columns)
        select_exprs = select.expressions
        if not select_exprs:
            return

        # Build alias → real table name mapping from the SELECT's FROM/JOIN sources
        alias_map: dict[str, str] = {}
        for table_ref in select.find_all(exp.Table):
            tbl_name = self._normalize_identifier(
                table_ref.name,
                self._is_quoted_identifier(table_ref),
            )
            if tbl_name:
                alias_map[tbl_name] = tbl_name
                if table_ref.alias:
                    alias_map[table_ref.alias] = tbl_name

        # Map each SELECT expression to its target column by position
        for i, select_expr in enumerate(select_exprs):
            target_col = target_cols[i] if i < len(target_cols) else None

            # Find the source column in this expression
            source_cols = list(select_expr.find_all(exp.Column))
            for src_col in source_cols:
                if not src_col.name:
                    continue

                # Resolve table alias to real table name
                table_alias = src_col.table or ""
                source_table = alias_map.get(table_alias, table_alias)

                column_usage.append(
                    ColumnUsageResult(
                        node_name=file_stem,
                        node_kind="query",
                        table_name=source_table or target_name,
                        column_name=src_col.name,
                        usage_type="insert",
                        transform=self._extract_transform(src_col),
                        alias=target_col,
                    )
                )

    def _normalize_identifier(self, name: str, quoted: bool = False) -> str:
        """Normalize an identifier based on the SQL dialect's case folding rules.

        Unquoted identifiers are folded: lowercase for Postgres/Redshift/DuckDB,
        uppercase for Snowflake/Oracle. Other dialects preserve case.

        Quoted identifiers are never folded — they preserve the exact case
        the user wrote.
        """
        if not name or not self.dialect or quoted:
            return name
        d = self.dialect.lower()
        if d in self._LOWERCASE_DIALECTS:
            return name.lower()
        if d in self._UPPERCASE_DIALECTS:
            return name.upper()
        return name

    @staticmethod
    def _is_quoted_identifier(node: exp.Expression) -> bool:
        """Check whether a sqlglot expression's name identifier is quoted.

        Works for Table (node.this is Identifier), Column (node.this is Identifier),
        CTE/Subquery aliases (via TableAlias wrapping an Identifier), etc.
        """
        ident = node.this if hasattr(node, "this") else None
        if isinstance(ident, exp.Identifier):
            return bool(ident.quoted)
        return False

    def _build_table_metadata(self, table: exp.Table) -> dict:
        """Build metadata dict with catalog/schema from a qualified table reference.

        Catalog and schema values are normalized using the same dialect-aware
        case folding as table/column names.  Quoted identifiers keep their
        original case.
        """
        meta: dict = {}
        if table.catalog:
            catalog_node = table.args.get("catalog")
            quoted = isinstance(catalog_node, exp.Identifier) and bool(catalog_node.quoted)
            meta["catalog"] = self._normalize_identifier(table.catalog, quoted)
        if table.db:
            db_node = table.args.get("db")
            quoted = isinstance(db_node, exp.Identifier) and bool(db_node.quoted)
            meta["schema"] = self._normalize_identifier(table.db, quoted)
        return meta

    def _get_table_context(self, table: exp.Table) -> str:
        """Determine context of a table reference from its AST position."""
        parent = table.parent

        while parent:
            if isinstance(parent, exp.Join):
                return "JOIN clause"
            if isinstance(parent, exp.From):
                return "FROM clause"
            if isinstance(parent, exp.Subquery):
                return "subquery"
            if isinstance(parent, exp.Insert):
                return "INSERT INTO"
            if isinstance(parent, exp.Merge):
                return "MERGE target"
            if isinstance(parent, exp.Update):
                return "UPDATE target"
            if isinstance(parent, exp.Lateral):
                return "LATERAL subquery"
            parent = parent.parent

        return "FROM clause"
