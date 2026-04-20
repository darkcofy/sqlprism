"""Tests for the SQL parser and core types."""

import pytest
import sqlglot

from sqlprism.languages.sql import SqlParser
from sqlprism.types import ParseResult


def test_sqlglot_version():
    """Verify sqlglot >= 30 is installed."""
    major = int(sqlglot.__version__.split(".")[0])
    assert major >= 30, f"Expected sqlglot >= 30, got {sqlglot.__version__}"


def test_parse_result_is_returned():
    parser = SqlParser()
    result = parser.parse("test.sql", "SELECT 1")
    assert isinstance(result, ParseResult)
    assert result.language == "sql"


def test_create_table_emits_node_and_edge():
    parser = SqlParser()
    result = parser.parse("schema.sql", "CREATE TABLE orders (id INT, total DECIMAL)")

    table_nodes = [n for n in result.nodes if n.kind == "table" and n.name == "orders"]
    assert len(table_nodes) >= 1

    define_edges = [e for e in result.edges if e.relationship == "defines" and e.target_name == "orders"]
    assert len(define_edges) >= 1


def test_select_extracts_table_reference():
    parser = SqlParser()
    result = parser.parse("query.sql", "SELECT id, name FROM customers WHERE active = 1")

    table_nodes = [n for n in result.nodes if n.kind == "table" and n.name == "customers"]
    assert len(table_nodes) >= 1

    ref_edges = [e for e in result.edges if e.target_name == "customers" and e.relationship == "references"]
    assert len(ref_edges) >= 1


def test_cte_is_first_class_node():
    sql = """
    WITH recent_orders AS (
        SELECT id, customer_id FROM orders WHERE created_at > '2024-01-01'
    )
    SELECT * FROM recent_orders
    """
    parser = SqlParser()
    result = parser.parse("cte_test.sql", sql)

    cte_nodes = [n for n in result.nodes if n.kind == "cte" and n.name == "recent_orders"]
    assert len(cte_nodes) == 1

    # CTE should reference the orders table
    cte_edges = [e for e in result.edges if e.source_name == "recent_orders" and e.target_name == "orders"]
    assert len(cte_edges) >= 1


def test_column_usage_extracted():
    sql = """
    SELECT o.id, o.customer_id, c.name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'active'
    GROUP BY c.name
    """
    parser = SqlParser()
    result = parser.parse("col_test.sql", sql)

    # Should have column usage entries
    assert len(result.column_usage) > 0

    # Check for WHERE usage
    where_usage = [cu for cu in result.column_usage if cu.usage_type == "where"]
    assert len(where_usage) >= 1


def test_multiple_statements():
    sql = """
    CREATE TABLE users (id INT, name TEXT);
    SELECT * FROM users;
    INSERT INTO orders SELECT * FROM temp_orders;
    """
    parser = SqlParser()
    result = parser.parse("multi.sql", sql)

    # Should find both tables
    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "users" in table_names


def test_invalid_sql_returns_empty():
    parser = SqlParser()
    result = parser.parse("bad.sql", "THIS IS NOT SQL AT ALL ???")
    assert isinstance(result, ParseResult)
    assert result.language == "sql"


def test_join_context():
    sql = "SELECT a.id FROM orders a JOIN customers b ON a.customer_id = b.id"
    parser = SqlParser()
    result = parser.parse("join.sql", sql)

    join_refs = [e for e in result.edges if e.target_name == "customers" and "JOIN" in (e.context or "")]
    assert len(join_refs) >= 1


def test_column_transform_captured():
    """Column transforms (CAST, COALESCE, functions) are captured."""
    sql = """
    CREATE TABLE dim_user AS
    SELECT
        CAST(u.created_at AS DATE) AS created_date,
        COALESCE(u.name, 'Unknown') AS user_name,
        UPPER(u.country) AS country,
        u.id
    FROM users u
    """
    parser = SqlParser()
    result = parser.parse("transform_test.sql", sql)

    # CAST should be captured
    cast_usage = [cu for cu in result.column_usage if cu.column_name == "created_at"]
    assert len(cast_usage) >= 1
    assert cast_usage[0].transform is not None
    assert "CAST" in cast_usage[0].transform

    # COALESCE should be captured
    coalesce_usage = [cu for cu in result.column_usage if cu.column_name == "name"]
    assert len(coalesce_usage) >= 1
    assert coalesce_usage[0].transform is not None
    assert "COALESCE" in coalesce_usage[0].transform

    # Bare column should have no transform
    bare_usage = [cu for cu in result.column_usage if cu.column_name == "id" and cu.usage_type == "select"]
    assert len(bare_usage) >= 1
    assert bare_usage[0].transform is None

    # Aliases should be captured
    assert cast_usage[0].alias == "created_date"
    assert coalesce_usage[0].alias == "user_name"


def test_where_filters_extracted():
    """WHERE clause conditions are extracted as node metadata."""
    sql = """
    CREATE TABLE active_orders AS
    SELECT o.id, o.total
    FROM orders o
    WHERE o.status = 'active'
      AND o.deleted = 0
      AND o.created_at >= '2024-01-01'
    """
    parser = SqlParser()
    result = parser.parse("filter_test.sql", sql)

    # Find the table node
    table_nodes = [n for n in result.nodes if n.name == "active_orders"]
    assert len(table_nodes) >= 1

    node = table_nodes[0]
    assert node.metadata is not None
    assert "filters" in node.metadata
    filters = node.metadata["filters"]

    assert len(filters) == 3
    # Each filter should be an individual condition, not the full AND chain
    filter_strs = " ".join(filters)
    assert "status" in filter_strs
    assert "deleted" in filter_strs
    assert "created_at" in filter_strs


def test_insert_select_column_mapping():
    """INSERT...SELECT maps source columns to target columns by position."""
    sql = """
    INSERT INTO dim_orders (order_id, customer_name, order_total)
    SELECT o.id, c.name, SUM(o.amount)
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY o.id, c.name
    """
    parser = SqlParser()
    result = parser.parse("insert_select.sql", sql)

    # Should have insert usage records
    insert_usage = [cu for cu in result.column_usage if cu.usage_type == "insert"]
    assert len(insert_usage) >= 2

    # o.id -> order_id
    id_insert = [cu for cu in insert_usage if cu.column_name == "id"]
    assert len(id_insert) >= 1
    assert id_insert[0].alias == "order_id"

    # c.name -> customer_name
    name_insert = [cu for cu in insert_usage if cu.column_name == "name"]
    assert len(name_insert) >= 1
    assert name_insert[0].alias == "customer_name"

    # SUM(o.amount) -> order_total should have transform
    amount_insert = [cu for cu in insert_usage if cu.column_name == "amount"]
    assert len(amount_insert) >= 1
    assert amount_insert[0].transform is not None
    assert "SUM" in amount_insert[0].transform.upper()


def test_insert_select_no_column_list():
    """INSERT...SELECT without explicit column list still captures usage."""
    sql = """
    INSERT INTO archive_orders
    SELECT id, customer_id, total FROM orders WHERE status = 'closed'
    """
    parser = SqlParser()
    result = parser.parse("insert_no_cols.sql", sql)

    insert_usage = [cu for cu in result.column_usage if cu.usage_type == "insert"]
    assert len(insert_usage) >= 3
    col_names = {cu.column_name for cu in insert_usage}
    assert "id" in col_names
    assert "customer_id" in col_names
    assert "total" in col_names


def test_window_function_classification():
    """Window function columns are classified as partition_by or window_order."""
    sql = """
    SELECT
        o.customer_id,
        o.amount,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_at DESC) AS rn,
        SUM(o.amount) OVER (PARTITION BY o.region) AS region_total
    FROM orders o
    """
    parser = SqlParser()
    result = parser.parse("window_test.sql", sql)

    # customer_id in PARTITION BY should be 'partition_by'
    partition_usage = [
        cu for cu in result.column_usage if cu.column_name == "customer_id" and cu.usage_type == "partition_by"
    ]
    assert len(partition_usage) >= 1

    # created_at in ORDER BY within window should be 'window_order'
    window_order_usage = [
        cu for cu in result.column_usage if cu.column_name == "created_at" and cu.usage_type == "window_order"
    ]
    assert len(window_order_usage) >= 1

    # region in PARTITION BY
    region_partition = [
        cu for cu in result.column_usage if cu.column_name == "region" and cu.usage_type == "partition_by"
    ]
    assert len(region_partition) >= 1

    # amount in SUM() window should have a transform containing SUM
    amount_window = [
        cu
        for cu in result.column_usage
        if cu.column_name == "amount" and cu.transform and "SUM" in cu.transform.upper()
    ]
    assert len(amount_window) >= 1


def test_aggregate_transform():
    """Aggregate functions (SUM, COUNT, AVG) are captured as transforms."""
    sql = """
    SELECT
        customer_id,
        COUNT(order_id) AS order_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount
    FROM orders
    GROUP BY customer_id
    """
    parser = SqlParser()
    result = parser.parse("agg_test.sql", sql)

    # order_id should have COUNT transform
    count_usage = [
        cu
        for cu in result.column_usage
        if cu.column_name == "order_id" and cu.transform and "COUNT" in cu.transform.upper()
    ]
    assert len(count_usage) >= 1

    # amount with SUM
    sum_usage = [
        cu
        for cu in result.column_usage
        if cu.column_name == "amount" and cu.transform and "SUM" in cu.transform.upper()
    ]
    assert len(sum_usage) >= 1

    # amount with AVG
    avg_usage = [
        cu
        for cu in result.column_usage
        if cu.column_name == "amount" and cu.transform and "AVG" in cu.transform.upper()
    ]
    assert len(avg_usage) >= 1


def test_cte_chain_edges():
    """CTE B referencing CTE A creates a cte->cte edge, not cte->table."""
    sql = """
    WITH base AS (
        SELECT id, amount FROM orders
    ),
    enriched AS (
        SELECT b.id, b.amount, c.name
        FROM base b
        JOIN customers c ON b.id = c.order_id
    ),
    final AS (
        SELECT * FROM enriched WHERE amount > 100
    )
    SELECT * FROM final
    """
    parser = SqlParser()
    result = parser.parse("cte_chain.sql", sql)

    # All three CTEs should be nodes
    cte_names = {n.name for n in result.nodes if n.kind == "cte"}
    assert cte_names == {"base", "enriched", "final"}

    # base -> orders should be cte -> table
    base_to_orders = [e for e in result.edges if e.source_name == "base" and e.target_name == "orders"]
    assert len(base_to_orders) >= 1
    assert base_to_orders[0].target_kind == "table"

    # enriched -> base should be cte -> cte
    enriched_to_base = [e for e in result.edges if e.source_name == "enriched" and e.target_name == "base"]
    assert len(enriched_to_base) >= 1
    assert enriched_to_base[0].target_kind == "cte"

    # final -> enriched should be cte -> cte
    final_to_enriched = [e for e in result.edges if e.source_name == "final" and e.target_name == "enriched"]
    assert len(final_to_enriched) >= 1
    assert final_to_enriched[0].target_kind == "cte"


def test_qualified_table_names():
    """Schema-qualified and catalog-qualified table names store metadata."""
    sql = """
    SELECT a.id, b.name
    FROM public.orders a
    JOIN mydb.staging.customers b ON a.customer_id = b.id
    """
    parser = SqlParser()
    result = parser.parse("qualified.sql", sql)

    # orders should have schema metadata
    orders_node = next((n for n in result.nodes if n.name == "orders"), None)
    assert orders_node is not None
    assert orders_node.metadata is not None
    assert orders_node.metadata.get("schema") == "public"
    assert "catalog" not in orders_node.metadata

    # customers should have both catalog and schema
    customers_node = next((n for n in result.nodes if n.name == "customers"), None)
    assert customers_node is not None
    assert customers_node.metadata is not None
    assert customers_node.metadata.get("catalog") == "mydb"
    assert customers_node.metadata.get("schema") == "staging"


def test_qualified_create_table():
    """CREATE TABLE with qualified name stores schema in metadata."""
    sql = "CREATE TABLE analytics.dim_users AS SELECT id FROM users"
    parser = SqlParser()
    result = parser.parse("qualified_create.sql", sql)

    dim_node = next((n for n in result.nodes if n.name == "dim_users"), None)
    assert dim_node is not None
    assert dim_node.metadata is not None
    assert dim_node.metadata.get("schema") == "analytics"


def test_subquery_scope_separation():
    """Subquery columns and filters don't leak into parent scope."""
    sql = """
    CREATE TABLE result AS
    SELECT c.id, o.total
    FROM customers c
    JOIN (
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        WHERE status != 'cancelled'
        GROUP BY customer_id
    ) o ON c.id = o.customer_id
    WHERE c.active = 1
    """
    parser = SqlParser()
    result = parser.parse("subquery_test.sql", sql)

    # Parent scope columns should reference 'customers', not unnamed table
    # Root scope uses the CREATE TABLE name ("result"), not the file stem
    parent_cols = [cu for cu in result.column_usage if cu.node_name == "result"]
    subquery_cols = [cu for cu in result.column_usage if cu.node_name == "o"]

    assert len(parent_cols) > 0
    assert len(subquery_cols) > 0

    # Subquery WHERE filter shouldn't appear on the parent node's metadata
    table_node = next((n for n in result.nodes if n.name == "result"), None)
    assert table_node is not None
    if table_node.metadata and "filters" in table_node.metadata:
        filter_strs = " ".join(table_node.metadata["filters"])
        assert "cancelled" not in filter_strs
        assert "active" in filter_strs


def test_column_lineage_through_ctes():
    """Column lineage traces output columns back through CTEs to source tables."""
    sql = """
    WITH base AS (
        SELECT id, CAST(amount AS DECIMAL) AS clean_amount
        FROM orders
    ),
    enriched AS (
        SELECT b.id, b.clean_amount, c.name
        FROM base b
        JOIN customers c ON b.id = c.order_id
    )
    SELECT id, clean_amount, name FROM enriched
    """
    parser = SqlParser()
    result = parser.parse("lineage_test.sql", sql)

    assert len(result.column_lineage) > 0

    # id should trace back to orders.id through enriched -> base -> orders
    id_lineage = [cl for cl in result.column_lineage if cl.output_column == "id"]
    assert len(id_lineage) >= 1
    # The chain should have multiple hops
    chain = id_lineage[0].chain
    assert len(chain) >= 2  # at least: enriched.id -> orders.id
    # Last hop should reference orders table
    hop_tables = [h.table for h in chain]
    assert any("orders" in t for t in hop_tables if t)

    # clean_amount should trace back to orders.amount with CAST transform
    amount_lineage = [cl for cl in result.column_lineage if cl.output_column == "clean_amount"]
    assert len(amount_lineage) >= 1
    chain = amount_lineage[0].chain
    # Should have a CAST expression somewhere in the chain
    expressions = [h.expression for h in chain if h.expression]
    assert any("CAST" in e.upper() for e in expressions)


def test_column_lineage_simple_select():
    """Lineage works for simple SELECT without CTEs."""
    sql = "SELECT id, name FROM customers"
    parser = SqlParser()
    result = parser.parse("simple_lineage.sql", sql)

    assert len(result.column_lineage) >= 2
    col_names = {cl.output_column for cl in result.column_lineage}
    assert "id" in col_names
    assert "name" in col_names


# ── P1.1: SELECT * tests ──


def test_select_star_column_usage():
    """SELECT * emits column_usage with column_name='*' for each source table."""
    sql = "SELECT * FROM orders"
    parser = SqlParser()
    result = parser.parse("star.sql", sql)

    star_usage = [cu for cu in result.column_usage if cu.column_name == "*"]
    assert len(star_usage) == 1  # single table, one star
    assert star_usage[0].table_name == "orders"
    assert star_usage[0].usage_type == "select"


def test_select_star_qualified():
    """SELECT t.* emits column_usage for the qualified table."""
    sql = "SELECT o.* FROM orders o JOIN customers c ON o.id = c.order_id"
    parser = SqlParser()
    result = parser.parse("star_qualified.sql", sql)

    star_usage = [cu for cu in result.column_usage if cu.column_name == "*"]
    orders_star = [cu for cu in star_usage if cu.table_name == "orders"]
    assert len(orders_star) == 1  # exactly one star for the qualified o.*
    assert orders_star[0].usage_type == "select"


def test_select_star_multiple_tables():
    """SELECT * with JOINs emits * for each source table."""
    sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
    parser = SqlParser()
    result = parser.parse("star_multi.sql", sql)

    star_usage = [cu for cu in result.column_usage if cu.column_name == "*"]
    tables = {cu.table_name for cu in star_usage}
    assert "orders" in tables
    assert "customers" in tables


def test_select_star_with_explicit_columns():
    """Mix of * and explicit columns extracts both."""
    sql = """
    SELECT *, o.id AS order_id
    FROM orders o
    """
    parser = SqlParser()
    result = parser.parse("star_mixed.sql", sql)

    star_usage = [cu for cu in result.column_usage if cu.column_name == "*"]
    assert len(star_usage) >= 1

    id_usage = [cu for cu in result.column_usage if cu.column_name == "id"]
    assert len(id_usage) >= 1


def test_select_star_lineage_skipped():
    """SELECT * doesn't produce lineage (can't trace without schema)."""
    sql = "SELECT * FROM orders"
    parser = SqlParser()
    result = parser.parse("star_lineage.sql", sql)

    # Should have column_usage but no column_lineage for *
    star_lineage = [cl for cl in result.column_lineage if cl.output_column == "*"]
    assert len(star_lineage) == 0


# ── P1.2: UNION / UNION ALL tests ──


def test_union_column_usage():
    """UNION extracts column usage from both branches."""
    sql = """
    SELECT id, name FROM customers
    UNION ALL
    SELECT id, name FROM suppliers
    """
    parser = SqlParser()
    result = parser.parse("union.sql", sql)

    # Both tables should appear in column usage
    tables = {cu.table_name for cu in result.column_usage}
    assert "customers" in tables
    assert "suppliers" in tables

    # Each branch selects exactly id and name
    cust_cols = {
        cu.column_name for cu in result.column_usage if cu.table_name == "customers" and cu.usage_type == "select"
    }
    supp_cols = {
        cu.column_name for cu in result.column_usage if cu.table_name == "suppliers" and cu.usage_type == "select"
    }
    assert "id" in cust_cols
    assert "name" in cust_cols
    assert "id" in supp_cols
    assert "name" in supp_cols


def test_union_nodes_extracted():
    """UNION extracts table references from both branches."""
    sql = """
    SELECT id FROM orders
    UNION
    SELECT id FROM archive_orders
    """
    parser = SqlParser()
    result = parser.parse("union_nodes.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "orders" in table_names
    assert "archive_orders" in table_names


def test_union_lineage():
    """UNION lineage traces all branches, not just the first (task 1.8)."""
    sql = """
    CREATE VIEW combined AS
    SELECT id, name FROM customers
    UNION ALL
    SELECT id, title AS name FROM suppliers
    """
    parser = SqlParser()
    result = parser.parse("union_lineage.sql", sql)

    # Lineage should trace from first branch columns
    lineage_cols = {cl.output_column for cl in result.column_lineage}
    assert "id" in lineage_cols
    assert "name" in lineage_cols

    # Both branches must be traced — each output column should have chains
    # reaching both source tables
    name_chains = [cl for cl in result.column_lineage if cl.output_column == "name"]
    source_tables = set()
    for chain in name_chains:
        for hop in chain.chain:
            if hop.table and hop.table not in ("SELECT",):
                source_tables.add(hop.table)
    assert "customers" in source_tables, "missing lineage from customers branch"
    assert "suppliers" in source_tables, "missing lineage from suppliers branch"


def test_create_view_union():
    """CREATE VIEW with UNION extracts all referenced tables."""
    sql = """
    CREATE VIEW all_contacts AS
    SELECT id, name, 'customer' AS type FROM customers
    UNION ALL
    SELECT id, name, 'supplier' AS type FROM suppliers
    """
    parser = SqlParser()
    result = parser.parse("union_view.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "customers" in table_names
    assert "suppliers" in table_names

    view_nodes = [n for n in result.nodes if n.kind == "view"]
    assert len(view_nodes) >= 1
    assert view_nodes[0].name == "all_contacts"


# ── P1.3 (partial): Schema-qualified name collision tests ──


def test_schema_qualified_same_name_different_schema():
    """Two tables with same name but different schemas produce separate nodes."""
    sql = """
    SELECT a.id, b.id
    FROM staging.orders a
    JOIN production.orders b ON a.id = b.id
    """
    parser = SqlParser()
    result = parser.parse("schema_collision.sql", sql)

    # Both should have metadata with different schemas
    order_nodes = [n for n in result.nodes if n.name == "orders"]
    # Currently produces one node (known issue for P1.3 full fix),
    # but metadata should at least capture one schema
    assert len(order_nodes) >= 1
    assert any(n.metadata and n.metadata.get("schema") for n in order_nodes)


# ── P1.5: CREATE VIEW duplicate node bug ──


def test_create_view_no_duplicate_node():
    """CREATE VIEW should produce one view node, not an extra table node."""
    sql = "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1"
    parser = SqlParser()
    result = parser.parse("view_test.sql", sql)

    # Should have exactly one node for active_users (view), not also a table
    active_nodes = [n for n in result.nodes if n.name == "active_users"]
    assert len(active_nodes) == 1
    assert active_nodes[0].kind == "view"


def test_create_table_as_no_duplicate():
    """CREATE TABLE AS SELECT should not duplicate the target as a table reference."""
    sql = "CREATE TABLE summary AS SELECT COUNT(*) AS cnt FROM orders"
    parser = SqlParser()
    result = parser.parse("ctas_test.sql", sql)

    summary_nodes = [n for n in result.nodes if n.name == "summary"]
    assert len(summary_nodes) == 1
    assert summary_nodes[0].kind == "table"


# ── P1.3: Schema-qualified name collisions ──


def test_schema_qualified_produces_separate_nodes():
    """Same table name in different schemas produces separate nodes."""
    sql = """
    SELECT a.id, b.id
    FROM staging.orders a
    JOIN production.orders b ON a.id = b.id
    """
    parser = SqlParser()
    result = parser.parse("schema_sep.sql", sql)

    order_nodes = [n for n in result.nodes if n.name == "orders"]
    assert len(order_nodes) == 2
    schemas = {(n.metadata or {}).get("schema") for n in order_nodes}
    assert schemas == {"staging", "production"}


# ── P1.4: Identifier case normalization ──


def test_case_normalization_postgres():
    """Postgres dialect lowercases unquoted identifiers."""
    sql = "SELECT ID, Name FROM Orders WHERE Status = 'active'"
    parser = SqlParser(dialect="postgres")
    result = parser.parse("pg_case.sql", sql)

    table_nodes = [n for n in result.nodes if n.kind == "table"]
    assert any(n.name == "orders" for n in table_nodes)

    col_names = {cu.column_name for cu in result.column_usage}
    assert "id" in col_names
    assert "name" in col_names
    assert "status" in col_names


def test_case_normalization_snowflake():
    """Snowflake dialect uppercases unquoted identifiers."""
    sql = "SELECT id, name FROM orders WHERE status = 'active'"
    parser = SqlParser(dialect="snowflake")
    result = parser.parse("sf_case.sql", sql)

    table_nodes = [n for n in result.nodes if n.kind == "table"]
    assert any(n.name == "ORDERS" for n in table_nodes)

    col_names = {cu.column_name for cu in result.column_usage}
    assert "ID" in col_names
    assert "NAME" in col_names


def test_case_normalization_none_preserves():
    """No dialect preserves original case."""
    sql = "SELECT Id, Name FROM Orders"
    parser = SqlParser()
    result = parser.parse("no_dialect.sql", sql)

    table_nodes = [n for n in result.nodes if n.kind == "table"]
    assert any(n.name == "Orders" for n in table_nodes)


# ── P3.7: Parse errors collected instead of swallowed ──


def test_parse_errors_on_invalid_sql():
    """Completely unparseable SQL should return errors list."""
    parser = SqlParser()
    result = parser.parse("bad.sql", "THIS IS NOT SQL AT ALL @@@ !!!")
    assert len(result.errors) > 0
    assert "Parse error" in result.errors[0]


def test_parse_errors_empty_for_valid_sql():
    """Valid SQL should have empty errors list."""
    parser = SqlParser()
    result = parser.parse("good.sql", "SELECT a, b FROM orders WHERE a > 1")
    assert result.errors == []


def test_parse_errors_partial_success():
    """Multiple statements: valid ones succeed, invalid ones produce errors."""
    parser = SqlParser()
    # First statement valid, second has issues that may or may not error
    sql = "SELECT 1;\nSELECT a FROM orders"
    result = parser.parse("mixed.sql", sql)
    # Should parse without fatal errors
    assert isinstance(result, ParseResult)


# ── P4.0: SELECT * lineage with schema catalog ──


def test_select_star_lineage_with_schema():
    """SELECT * expands to individual column lineage when schema catalog provided."""
    sql = "CREATE VIEW v AS SELECT * FROM orders"
    schema = {"orders": {"id": "TEXT", "total": "TEXT", "status": "TEXT"}}
    parser = SqlParser()
    result = parser.parse("star_lineage.sql", sql, schema=schema)

    # With schema, SELECT * should be expanded and lineage traced for individual columns
    if result.column_lineage:
        traced_cols = {cl.output_column for cl in result.column_lineage}
        # At least some of the columns from the schema should be traced
        assert len(traced_cols) > 0
        assert "*" not in traced_cols  # should not have literal *


def test_select_star_lineage_without_schema():
    """SELECT * without schema skips lineage (no expansion possible)."""
    sql = "CREATE VIEW v AS SELECT * FROM orders"
    parser = SqlParser()
    result = parser.parse("star_no_schema.sql", sql)

    # Without schema, SELECT * can't be expanded — lineage should be empty or skip *
    star_lineage = [cl for cl in result.column_lineage if cl.output_column == "*"]
    assert len(star_lineage) == 0


# ── P4.2: AST passed directly (no re-serialization) ──


def test_lineage_works_without_reserialization():
    """Column lineage still works when passing AST directly to sqlglot_lineage."""
    sql = "CREATE VIEW totals AS SELECT id, SUM(amount) AS total FROM orders GROUP BY id"
    parser = SqlParser()
    result = parser.parse("ast_direct.sql", sql)

    # Should have lineage for 'id' at minimum
    lineage_cols = {cl.output_column for cl in result.column_lineage}
    assert "id" in lineage_cols or len(result.column_lineage) > 0


# ── P6.2: High-priority test gaps ──


def test_recursive_cte():
    """WITH RECURSIVE extracts CTE node and references."""
    sql = """
    WITH RECURSIVE subordinates AS (
        SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.manager_id
        FROM employees e
        JOIN subordinates s ON e.manager_id = s.id
    )
    SELECT * FROM subordinates
    """
    parser = SqlParser()
    result = parser.parse("recursive.sql", sql)

    # CTE should be a first-class node
    cte_nodes = [n for n in result.nodes if n.kind == "cte" and n.name == "subordinates"]
    assert len(cte_nodes) == 1

    # Should reference the employees table
    emp_edges = [e for e in result.edges if e.target_name == "employees"]
    assert len(emp_edges) >= 1

    # Column usage from both branches (base + recursive)
    tables = {cu.table_name for cu in result.column_usage}
    assert "employees" in tables


def test_recursive_cte_column_usage():
    """Recursive CTE captures column usage from both the base and recursive parts."""
    sql = """
    WITH RECURSIVE tree AS (
        SELECT id, parent_id, name, 1 AS depth FROM categories WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.parent_id, c.name, t.depth + 1
        FROM categories c
        JOIN tree t ON c.parent_id = t.id
    )
    SELECT id, name, depth FROM tree
    """
    parser = SqlParser()
    result = parser.parse("tree.sql", sql)

    col_names = {cu.column_name for cu in result.column_usage if cu.table_name == "categories"}
    assert "id" in col_names
    assert "parent_id" in col_names
    assert "name" in col_names


def test_self_join():
    """Self-join extracts both aliases correctly."""
    sql = """
    SELECT e.name AS employee, m.name AS manager
    FROM employees e
    JOIN employees m ON e.manager_id = m.id
    """
    parser = SqlParser()
    result = parser.parse("self_join.sql", sql)

    # Should have at least one employees table node
    emp_nodes = [n for n in result.nodes if n.name == "employees"]
    assert len(emp_nodes) >= 1

    # Should have two reference edges (FROM + JOIN)
    emp_edges = [e for e in result.edges if e.target_name == "employees"]
    assert len(emp_edges) >= 2

    # Column usage should reference employees for both aliases
    emp_cols = [cu for cu in result.column_usage if cu.table_name == "employees"]
    assert len(emp_cols) >= 2

    # Both name and manager_id should appear
    col_names = {cu.column_name for cu in emp_cols}
    assert "name" in col_names
    assert "manager_id" in col_names or "id" in col_names


def test_self_join_column_aliases():
    """Self-join output aliases are captured."""
    sql = """
    SELECT e.name AS employee_name, m.name AS manager_name
    FROM employees e
    JOIN employees m ON e.manager_id = m.id
    """
    parser = SqlParser()
    result = parser.parse("self_join_alias.sql", sql)

    aliases = {cu.alias for cu in result.column_usage if cu.alias}
    assert "employee_name" in aliases
    assert "manager_name" in aliases


def test_case_normalization_bigquery():
    """BigQuery preserves case for unquoted identifiers."""
    sql = "SELECT userId, orderTotal FROM MySchema.Orders"
    parser = SqlParser(dialect="bigquery")
    result = parser.parse("bq.sql", sql)

    table_nodes = [n for n in result.nodes if n.kind == "table"]
    assert any(n.name == "Orders" for n in table_nodes)

    col_names = {cu.column_name for cu in result.column_usage}
    assert "userId" in col_names
    assert "orderTotal" in col_names


def test_case_normalization_duckdb():
    """DuckDB (lowercase dialect) lowercases identifiers."""
    sql = "SELECT ID, Name FROM Orders"
    parser = SqlParser(dialect="duckdb")
    result = parser.parse("duck.sql", sql)

    table_nodes = [n for n in result.nodes if n.kind == "table"]
    assert any(n.name == "orders" for n in table_nodes)


def test_case_normalization_redshift():
    """Redshift lowercases unquoted identifiers (same as postgres)."""
    sql = "SELECT UserId FROM Events"
    parser = SqlParser(dialect="redshift")
    result = parser.parse("rs.sql", sql)

    col_names = {cu.column_name for cu in result.column_usage}
    assert "userid" in col_names


# ── P6.4: Edge case tests ──


def test_empty_file():
    """Empty SQL file returns valid empty ParseResult."""
    parser = SqlParser()
    result = parser.parse("empty.sql", "")
    assert isinstance(result, ParseResult)
    assert result.nodes == []
    assert result.edges == []
    assert result.errors == []


def test_whitespace_only_file():
    """Whitespace-only SQL returns valid empty ParseResult."""
    parser = SqlParser()
    result = parser.parse("blank.sql", "   \n\n  \t  \n")
    assert isinstance(result, ParseResult)
    assert result.errors == []


def test_comment_only_file():
    """SQL file with only comments returns valid ParseResult."""
    parser = SqlParser()
    result = parser.parse("comments.sql", "-- this is a comment\n/* block comment */")
    assert isinstance(result, ParseResult)


def test_malformed_sql_returns_errors():
    """Completely unparseable multi-statement SQL returns errors gracefully."""
    parser = SqlParser()
    sql = "SELECT id, name FROM orders WHERE id > 1;\nSELECT FROM WHERE INVALID @@@ !!!"
    result = parser.parse("partial.sql", sql)

    # sqlglot may fail the entire parse — should return errors, not crash
    assert isinstance(result, ParseResult)
    assert len(result.errors) > 0


def test_very_long_column_list():
    """SQL with many columns doesn't crash."""
    cols = ", ".join(f"col_{i}" for i in range(200))
    sql = f"SELECT {cols} FROM big_table"
    parser = SqlParser()
    result = parser.parse("wide.sql", sql)

    assert len(result.column_usage) >= 200


def test_deeply_nested_subqueries():
    """Deeply nested subqueries don't cause excessive recursion."""
    sql = """
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM (
                SELECT id FROM orders
            ) a
        ) b
    ) c
    """
    parser = SqlParser()
    result = parser.parse("nested.sql", sql)

    # Should at least find the orders table
    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "orders" in table_names


def test_merge_statement():
    """MERGE statement extracts source and target table references."""
    sql = """
    MERGE INTO target_table t
    USING source_table s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.name = s.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
    """
    parser = SqlParser()
    result = parser.parse("merge.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    # At minimum should find the source and/or target
    assert len(table_names) >= 1


def test_grant_ddl():
    """GRANT statement doesn't crash (may not extract useful data)."""
    parser = SqlParser()
    result = parser.parse("grant.sql", "GRANT SELECT ON orders TO analyst")
    assert isinstance(result, ParseResult)


def test_drop_table():
    """DROP TABLE doesn't crash."""
    parser = SqlParser()
    result = parser.parse("drop.sql", "DROP TABLE IF EXISTS old_orders")
    assert isinstance(result, ParseResult)


def test_correlated_subquery():
    """Correlated subquery extracts column usage from both scopes."""
    sql = """
    SELECT o.id, o.amount
    FROM orders o
    WHERE o.amount > (
        SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = o.customer_id
    )
    """
    parser = SqlParser()
    result = parser.parse("correlated.sql", sql)

    # Should find the orders table
    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "orders" in table_names

    # Column usage should include amount from both scopes
    amount_usage = [cu for cu in result.column_usage if cu.column_name == "amount"]
    assert len(amount_usage) >= 1


def test_lateral_join():
    """LATERAL join extracts table references."""
    sql = """
    SELECT o.id, t.total
    FROM orders o,
    LATERAL (SELECT SUM(amount) AS total FROM order_items oi WHERE oi.order_id = o.id) t
    """
    parser = SqlParser()
    result = parser.parse("lateral.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "orders" in table_names


def test_multiple_semicolon_separated():
    """Multiple semicolon-separated statements all get processed."""
    sql = """
    CREATE TABLE a (id INT);
    CREATE TABLE b (id INT);
    CREATE TABLE c (id INT);
    SELECT * FROM a JOIN b ON a.id = b.id;
    """
    parser = SqlParser()
    result = parser.parse("multi.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "a" in table_names
    assert "b" in table_names
    assert "c" in table_names


def test_create_table_with_complex_types():
    """CREATE TABLE with complex type definitions doesn't crash."""
    sql = """
    CREATE TABLE events (
        id BIGINT,
        payload JSON,
        tags ARRAY(VARCHAR),
        created_at TIMESTAMP WITH TIME ZONE
    )
    """
    parser = SqlParser()
    result = parser.parse("complex_types.sql", sql)

    table_nodes = [n for n in result.nodes if n.name == "events"]
    assert len(table_nodes) >= 1


# ── Quoted identifier case preservation ──


def test_duplicate_from_table_no_duplicate_edges():
    """SELECT * FROM a, a should not produce duplicate edges with the same context."""
    sql = "SELECT * FROM a, a"
    parser = SqlParser()
    result = parser.parse("dup.sql", sql)

    # Should only have one FROM edge to 'a', not two
    from_edges = [e for e in result.edges if e.target_name == "a" and e.context == "FROM clause"]
    assert len(from_edges) == 1


def test_self_join_produces_two_edges():
    """Self-join (FROM + JOIN) should produce two edges with different contexts."""
    sql = """
    SELECT e.name, m.name
    FROM employees e
    JOIN employees m ON e.manager_id = m.id
    """
    parser = SqlParser()
    result = parser.parse("self_join_edges.sql", sql)

    emp_edges = [e for e in result.edges if e.target_name == "employees"]
    assert len(emp_edges) == 2

    contexts = {e.context for e in emp_edges}
    assert "FROM clause" in contexts
    assert "JOIN clause" in contexts


def test_quoted_identifier_preserves_case_postgres():
    """Postgres: quoted "Orders" stays "Orders", unquoted Orders becomes "orders"."""
    sql = 'SELECT "Price" FROM "Orders" JOIN customers ON "Orders".id = customers.order_id'
    parser = SqlParser(dialect="postgres")
    result = parser.parse("quoted_pg.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    # Quoted "Orders" must preserve case
    assert "Orders" in table_names
    # Unquoted customers must be lowered
    assert "customers" in table_names
    assert "orders" not in table_names  # should NOT be folded

    # Quoted column "Price" should preserve case
    col_names = {cu.column_name for cu in result.column_usage}
    assert "Price" in col_names
    assert "price" not in col_names


def test_quoted_identifier_preserves_case_snowflake():
    """Snowflake: quoted "orders" stays "orders", unquoted orders becomes "ORDERS"."""
    sql = 'SELECT "price" FROM "orders" JOIN customers ON "orders".id = customers.order_id'
    parser = SqlParser(dialect="snowflake")
    result = parser.parse("quoted_sf.sql", sql)

    table_names = {n.name for n in result.nodes if n.kind == "table"}
    # Quoted "orders" must preserve lowercase
    assert "orders" in table_names
    # Unquoted customers must be uppercased
    assert "CUSTOMERS" in table_names
    assert "ORDERS" not in table_names  # should NOT be folded

    # Quoted column "price" should preserve case
    col_names = {cu.column_name for cu in result.column_usage}
    assert "price" in col_names
    assert "PRICE" not in col_names


# ── 5.3: UPDATE/HAVING usage type tests ──


def test_having_usage_type():
    """HAVING clause is parsed without crashing and produces reasonable output."""
    sql = "SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) > 5"
    parser = SqlParser()
    result = parser.parse("having.sql", sql)
    assert isinstance(result, ParseResult)
    # dept should appear in column usage (at least in select or group_by)
    dept_usage = [cu for cu in result.column_usage if cu.column_name == "dept"]
    assert len(dept_usage) >= 1
    # Parser should not crash on HAVING
    assert result.errors == []


def test_having_with_column_reference():
    """HAVING with a column expression captures usage."""
    sql = """
    SELECT department, SUM(salary) AS total
    FROM employees
    GROUP BY department
    HAVING SUM(salary) > 100000
    """
    parser = SqlParser()
    result = parser.parse("having_col.sql", sql)
    assert isinstance(result, ParseResult)
    assert result.errors == []
    # salary should appear in column usage
    salary_usage = [cu for cu in result.column_usage if cu.column_name == "salary"]
    assert len(salary_usage) >= 1


def test_update_statement():
    """UPDATE statement extracts table reference and column usage."""
    sql = "UPDATE orders SET status = 'shipped', updated_at = NOW() WHERE id = 42"
    parser = SqlParser()
    result = parser.parse("update.sql", sql)
    assert isinstance(result, ParseResult)
    # Should reference the orders table
    table_names = {n.name for n in result.nodes if n.kind == "table"}
    assert "orders" in table_names


# ── 5.8: UNION lineage completeness, QUALIFY, CASE WHEN ──


def test_union_lineage_both_branches_complete():
    """UNION lineage traces id column from both branches, not just the first."""
    sql = """
    CREATE VIEW combined AS
    SELECT id, amount FROM orders
    UNION ALL
    SELECT id, total AS amount FROM returns
    """
    parser = SqlParser()
    result = parser.parse("union_both.sql", sql)

    # The 'id' output column should have lineage chains reaching both tables
    id_chains = [cl for cl in result.column_lineage if cl.output_column == "id"]
    source_tables = set()
    for chain in id_chains:
        for hop in chain.chain:
            if hop.table and hop.table not in ("SELECT",):
                source_tables.add(hop.table)
    assert "orders" in source_tables, "missing lineage from orders branch"
    assert "returns" in source_tables, "missing lineage from returns branch"

    # The 'amount' output column should also have chains from both branches
    amount_chains = [cl for cl in result.column_lineage if cl.output_column == "amount"]
    amount_tables = set()
    for chain in amount_chains:
        for hop in chain.chain:
            if hop.table and hop.table not in ("SELECT",):
                amount_tables.add(hop.table)
    assert "orders" in amount_tables, "missing amount lineage from orders branch"
    assert "returns" in amount_tables, "missing amount lineage from returns branch"


def test_qualify_column_classification():
    """QUALIFY clause window columns are classified correctly (Snowflake dialect)."""
    sql = """
    SELECT id, name, department
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date) = 1
    """
    parser = SqlParser(dialect="snowflake")
    result = parser.parse("qualify_test.sql", sql)

    assert isinstance(result, ParseResult)
    assert result.errors == []

    # hire_date in QUALIFY's window ORDER BY should be 'window_order'
    hire_usage = [cu for cu in result.column_usage if cu.column_name == "HIRE_DATE" and cu.usage_type == "window_order"]
    assert len(hire_usage) >= 1, "ORDER BY hire_date in QUALIFY not classified as window_order"

    # department appears in SELECT (as 'select') — the PARTITION BY reference
    # is a separate column node but may share the same underlying column_usage.
    # At minimum, department should appear in usage.
    dept_usage = [cu for cu in result.column_usage if cu.column_name == "DEPARTMENT"]
    assert len(dept_usage) >= 1, "DEPARTMENT column not captured"


def test_case_when_column_classification():
    """CASE WHEN using columns captures them with correct usage and transform."""
    sql = """
    SELECT
        id,
        CASE WHEN status = 'active' THEN amount ELSE 0 END AS adj_amount
    FROM orders
    """
    parser = SqlParser()
    result = parser.parse("case_test.sql", sql)

    assert isinstance(result, ParseResult)
    assert result.errors == []

    # 'status' is used in the CASE WHEN condition (in SELECT context)
    status_usage = [cu for cu in result.column_usage if cu.column_name == "status"]
    assert len(status_usage) >= 1, "status column not captured in CASE WHEN"

    # 'amount' is used inside CASE THEN (in SELECT context)
    amount_usage = [cu for cu in result.column_usage if cu.column_name == "amount"]
    assert len(amount_usage) >= 1, "amount column not captured in CASE WHEN"

    # At least one of them should have a CASE transform
    case_transforms = [cu for cu in result.column_usage if cu.transform and "CASE" in cu.transform.upper()]
    assert len(case_transforms) >= 1, "CASE transform not captured"


def test_case_when_in_where():
    """CASE WHEN in WHERE clause classifies columns as 'where'."""
    sql = """
    SELECT id FROM orders
    WHERE CASE WHEN region = 'US' THEN amount > 100 ELSE amount > 200 END
    """
    parser = SqlParser()
    result = parser.parse("case_where.sql", sql)

    # region and amount should be classified as 'where'
    where_usage = [cu for cu in result.column_usage if cu.usage_type == "where"]
    where_cols = {cu.column_name for cu in where_usage}
    assert "region" in where_cols or "amount" in where_cols, "CASE WHEN columns in WHERE not classified as 'where'"


def test_union_lineage_different_column_names_all_branches():
    """UNION lineage traces through ALL branches, not just the first.

    SQL UNION output columns are named from the first branch (per SQL standard).
    For SELECT a FROM t1 UNION ALL SELECT b FROM t2, the output column is "a"
    but lineage should trace back to BOTH t1.a and t2.b.
    """
    sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
    parser = SqlParser()
    result = parser.parse("union_diff_cols.sql", sql)

    lineage_cols = {cl.output_column for cl in result.column_lineage}
    assert "a" in lineage_cols, "missing lineage for output column 'a'"

    # Output column "a" should trace back to both source tables
    a_chains = [cl for cl in result.column_lineage if cl.output_column == "a"]
    a_sources = set()
    for chain in a_chains:
        for hop in chain.chain:
            if hop.table and hop.table not in ("SELECT",):
                a_sources.add((hop.table, hop.column))
    assert ("t1", "a") in a_sources, "output 'a' should trace back to t1.a"
    assert ("t2", "b") in a_sources, "output 'a' should also trace back to t2.b (second branch)"


# ── v1.1: Column extraction from DDL / SELECT ──


def test_extract_columns_create_table():
    """CREATE TABLE with typed columns produces ColumnDefResult entries."""
    parser = SqlParser()
    result = parser.parse(
        "orders.sql",
        "CREATE TABLE orders (order_id INT, status TEXT, amount DECIMAL(10,2))",
    )
    assert len(result.columns) == 3
    assert result.columns[0].node_name == "orders"
    assert result.columns[0].column_name == "order_id"
    assert result.columns[0].data_type is not None
    assert "INT" in result.columns[0].data_type.upper()
    assert result.columns[0].position == 0
    assert result.columns[0].source == "definition"

    assert result.columns[1].column_name == "status"
    assert result.columns[2].column_name == "amount"
    assert result.columns[2].position == 2


def test_extract_columns_snowflake_ddl():
    """Snowflake-style DDL preserves column types."""
    parser = SqlParser(dialect="snowflake")
    result = parser.parse(
        "orders.sql",
        "CREATE TABLE IF NOT EXISTS db.schema.orders (id NUMBER, name VARCHAR(255))",
    )
    cols = result.columns
    assert len(cols) == 2
    assert cols[0].column_name == "id"
    assert cols[1].column_name == "name"
    # Types should be preserved
    assert cols[0].data_type is not None
    assert cols[1].data_type is not None


def test_extract_columns_bigquery_ddl():
    """BigQuery-style DDL preserves column types."""
    parser = SqlParser(dialect="bigquery")
    result = parser.parse(
        "orders.sql",
        "CREATE TABLE dataset.orders (id INT64, name STRING, ts TIMESTAMP)",
    )
    cols = result.columns
    assert len(cols) == 3
    assert cols[0].column_name == "id"
    assert cols[1].column_name == "name"
    assert cols[2].column_name == "ts"


def test_extract_columns_create_view_select():
    """CREATE VIEW with SELECT output columns produces inferred ColumnDefResult entries."""
    parser = SqlParser()
    result = parser.parse(
        "v_orders.sql",
        "CREATE VIEW v_orders AS SELECT o.order_id, o.status, SUM(amount) AS total FROM orders o GROUP BY 1, 2",
    )
    inferred = [c for c in result.columns if c.source == "inferred"]
    assert len(inferred) == 3
    col_names = [c.column_name for c in inferred]
    assert "order_id" in col_names
    assert "status" in col_names
    assert "total" in col_names  # alias used


def test_extract_columns_ctas_no_defs():
    """CTAS with no column definitions produces no 'definition' source columns."""
    parser = SqlParser()
    result = parser.parse(
        "new_orders.sql",
        "CREATE TABLE new_orders AS SELECT * FROM orders",
    )
    definition_cols = [c for c in result.columns if c.source == "definition"]
    assert len(definition_cols) == 0
    # SELECT * can't be inferred without schema, so no inferred columns either
    inferred_cols = [c for c in result.columns if c.source == "inferred"]
    assert len(inferred_cols) == 0


def test_extract_columns_cte_output():
    """CTE columns are tracked via column_usage; final SELECT inferred for views."""
    parser = SqlParser()
    result = parser.parse(
        "cte_view.sql",
        "CREATE VIEW cte_view AS WITH cte AS (SELECT a, b FROM t) SELECT a, b FROM cte",
    )
    inferred = [c for c in result.columns if c.source == "inferred"]
    assert len(inferred) == 2
    col_names = [c.column_name for c in inferred]
    assert "a" in col_names
    assert "b" in col_names


def test_extract_columns_create_view_union():
    """CREATE VIEW with UNION infers columns from the first SELECT branch."""
    parser = SqlParser()
    result = parser.parse(
        "v_combined.sql",
        "CREATE VIEW v_combined AS SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2",
    )
    inferred = [c for c in result.columns if c.source == "inferred"]
    assert len(inferred) == 2
    col_names = [c.column_name for c in inferred]
    assert "a" in col_names
    assert "b" in col_names


def test_bare_select_file_gets_query_node():
    """A bare SELECT file should produce a file-level query node."""
    parser = SqlParser()
    result = parser.parse("stg_orders.sql", "SELECT id, name FROM customers")
    query_nodes = [n for n in result.nodes if n.kind == "query" and n.name == "stg_orders"]
    assert len(query_nodes) == 1
    metadata = query_nodes[0].metadata
    assert metadata is not None
    assert metadata.get("file_node") is True


def test_generic_filename_uses_parent_dir():
    """When file stem is 'query', use parent directory as node name."""
    parser = SqlParser()
    result = parser.parse(
        "telemetry_derived/clients_daily_v6/query.sql",
        "WITH base AS (SELECT * FROM raw_data) SELECT * FROM base",
    )
    file_nodes = [n for n in result.nodes if n.metadata and n.metadata.get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "clients_daily_v6"


def test_create_view_no_duplicate_file_node():
    """CREATE VIEW should not produce an extra file-level node when names match."""
    parser = SqlParser()
    result = parser.parse(
        "stg_orders.sql",
        "CREATE VIEW stg_orders AS SELECT id FROM raw_orders",
    )
    # Should have the CREATE-defined view node but NOT a separate file_node
    view_nodes = [n for n in result.nodes if n.name == "stg_orders" and n.kind == "view"]
    file_nodes = [n for n in result.nodes if n.metadata and n.metadata.get("file_node")]
    assert len(view_nodes) == 1
    assert len(file_nodes) == 0


def test_create_view_different_name_gets_file_node():
    """When CREATE VIEW name differs from file, both nodes exist."""
    parser = SqlParser()
    result = parser.parse(
        "my_file.sql",
        "CREATE VIEW other_name AS SELECT id FROM raw_orders",
    )
    view_nodes = [n for n in result.nodes if n.name == "other_name" and n.kind == "view"]
    file_nodes = [n for n in result.nodes if n.metadata and n.metadata.get("file_node")]
    assert len(view_nodes) == 1
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "my_file"


def test_file_node_edges_use_smart_name():
    """Edges should use the smart file name as source, not raw file stem."""
    parser = SqlParser()
    result = parser.parse(
        "telemetry_derived/clients_daily_v6/query.sql",
        "SELECT * FROM some_table",
    )
    ref_edges = [e for e in result.edges if e.target_name == "some_table" and e.relationship == "references"]
    assert len(ref_edges) >= 1
    assert ref_edges[0].source_name == "clients_daily_v6"


def test_unparseable_file_no_file_node():
    """A completely unparseable file should NOT produce a file node."""
    parser = SqlParser()
    result = parser.parse("bad.sql", "THIS IS NOT SQL AT ALL ;;; %%%")
    file_nodes = [n for n in result.nodes if n.name == "bad" and (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 0


def test_empty_file_no_file_node():
    """An empty SQL file should NOT produce a file node."""
    parser = SqlParser()
    result = parser.parse("empty.sql", "")
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 0


def test_comment_only_file_no_file_node():
    """A comment-only SQL file should NOT produce a file node."""
    parser = SqlParser()
    result = parser.parse("comments.sql", "-- just a comment\n/* block comment */")
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 0


def test_bigquery_dialect_file_node():
    """BigQuery backtick-quoted identifiers should still produce correct file nodes."""
    parser = SqlParser(dialect="bigquery")
    result = parser.parse(
        "clients_daily_v6/query.sql",
        "SELECT * FROM `project.dataset.raw_clients`",
    )
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "clients_daily_v6"


def test_cte_does_not_suppress_file_node():
    """A CTE whose name matches the file stem should NOT suppress the file-level node."""
    parser = SqlParser()
    result = parser.parse(
        "orders.sql",
        "WITH orders AS (SELECT id FROM raw_orders) SELECT * FROM orders",
    )
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "orders"


def test_referenced_table_does_not_suppress_file_node():
    """A referenced table matching the file stem should NOT suppress the file-level node."""
    parser = SqlParser()
    result = parser.parse(
        "customers.sql",
        "SELECT id FROM customers",
    )
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "customers"


def test_create_with_cte_no_file_node_for_cte():
    """CREATE VIEW with CTE: CTE should not get file_node, only the view is CREATE-defined."""
    parser = SqlParser()
    result = parser.parse(
        "stg_orders.sql",
        "CREATE VIEW stg_orders AS WITH source AS (SELECT id FROM raw) SELECT * FROM source",
    )
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 0  # CREATE name matches file stem, so no file node
    # CTE 'source' should exist but NOT as file_node
    cte_nodes = [n for n in result.nodes if n.name == "source"]
    assert all(not (n.metadata or {}).get("file_node") for n in cte_nodes)


def test_multi_statement_bare_select_and_create():
    """Multi-statement file with bare SELECT and CREATE: file gets file_node if name differs."""
    parser = SqlParser()
    result = parser.parse(
        "mixed.sql",
        "SELECT 1 FROM a; CREATE TABLE t1 (id INT)",
    )
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "mixed"
    table_nodes = [n for n in result.nodes if n.name == "t1" and n.kind == "table"]
    assert len(table_nodes) == 1


@pytest.mark.parametrize("stem", ["view", "init", "index", "main", "script"])
def test_generic_stems_use_parent_dir(stem):
    """All generic stems should fall back to parent directory name."""
    parser = SqlParser()
    result = parser.parse(f"my_model/{stem}.sql", "SELECT 1 FROM t")
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 1
    assert file_nodes[0].name == "my_model"


def test_snowflake_dialect_normalize_file_stem():
    """Snowflake normalizes to uppercase; file stem should match CREATE-defined name."""
    parser = SqlParser(dialect="snowflake")
    result = parser.parse(
        "stg_orders.sql",
        "CREATE VIEW STG_ORDERS AS SELECT id FROM raw_orders",
    )
    # File stem 'stg_orders' normalized to 'STG_ORDERS' should match the CREATE name
    file_nodes = [n for n in result.nodes if (n.metadata or {}).get("file_node")]
    assert len(file_nodes) == 0  # No file node because normalized names match
