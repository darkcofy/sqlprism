"""Tests for MCP Pydantic input validation and field aliases."""

import pytest
from pydantic import ValidationError

from sqlprism.core.mcp_tools import (
    FindColumnUsageInput,
    FindReferencesInput,
    SearchInput,
    configure,
)


# ── 5.1: Pydantic validation and field aliases ──


def test_search_input_schema_alias():
    """SearchInput accepts 'schema' as field alias for sql_schema."""
    inp = SearchInput(pattern="orders", schema="staging")
    assert inp.sql_schema == "staging"


def test_search_input_sql_schema_direct():
    """SearchInput accepts sql_schema directly too (populate_by_name)."""
    inp = SearchInput(pattern="orders", sql_schema="public")  # type: ignore[unknown-argument]
    assert inp.sql_schema == "public"


def test_find_references_input_schema_alias():
    """FindReferencesInput accepts 'schema' as field alias for sql_schema."""
    inp = FindReferencesInput(name="orders", schema="staging")
    assert inp.sql_schema == "staging"


def test_find_references_input_sql_schema_direct():
    """FindReferencesInput accepts sql_schema directly too."""
    inp = FindReferencesInput(name="orders", sql_schema="production")  # type: ignore[unknown-argument]
    assert inp.sql_schema == "production"


def test_configure_sets_repo_type_from_config(tmp_path):
    """configure() stores repo_type based on config section (repos/dbt_repos/sqlmesh_repos)."""
    from sqlprism.core.graph import GraphDB

    db_path = str(tmp_path / "test.duckdb")
    repos = {
        "sql_repo": {"path": str(tmp_path / "sql"), "repo_type": "sql"},
        "dbt_repo": {"path": str(tmp_path / "dbt"), "repo_type": "dbt"},
        "sm_repo": {"path": str(tmp_path / "sm"), "repo_type": "sqlmesh"},
    }
    configure(db_path=db_path, repos=repos)

    graph = GraphDB(db_path)
    rows = graph.conn.execute(
        "SELECT name, repo_type FROM repos ORDER BY name"
    ).fetchall()
    result = {r[0]: r[1] for r in rows}
    assert result == {"dbt_repo": "dbt", "sm_repo": "sqlmesh", "sql_repo": "sql"}
    graph.close()


def test_search_input_validation_limit_too_low():
    """SearchInput rejects limit < 1."""
    with pytest.raises(ValidationError):
        SearchInput(pattern="x", limit=0)


def test_search_input_validation_limit_too_high():
    """SearchInput rejects limit > 100."""
    with pytest.raises(ValidationError):
        SearchInput(pattern="x", limit=200)


def test_search_input_validation_limit_boundary():
    """SearchInput accepts boundary values for limit."""
    inp_min = SearchInput(pattern="x", limit=1)
    assert inp_min.limit == 1
    inp_max = SearchInput(pattern="x", limit=100)
    assert inp_max.limit == 100


def test_find_references_input_validation_limit():
    """FindReferencesInput rejects limit out of range."""
    with pytest.raises(ValidationError):
        FindReferencesInput(name="x", limit=0)
    with pytest.raises(ValidationError):
        FindReferencesInput(name="x", limit=501)


def test_find_column_usage_input_validation_limit():
    """FindColumnUsageInput rejects limit out of range."""
    with pytest.raises(ValidationError):
        FindColumnUsageInput(table="x", limit=0)
    with pytest.raises(ValidationError):
        FindColumnUsageInput(table="x", limit=501)


def test_search_input_defaults():
    """SearchInput has correct defaults."""
    inp = SearchInput(pattern="orders")
    assert inp.kind is None
    assert inp.sql_schema is None
    assert inp.repo is None
    assert inp.limit == 20
    assert inp.include_snippets is True


def test_find_references_input_defaults():
    """FindReferencesInput has correct defaults."""
    inp = FindReferencesInput(name="orders")
    assert inp.kind is None
    assert inp.sql_schema is None
    assert inp.direction == "both"
    assert inp.limit == 100
