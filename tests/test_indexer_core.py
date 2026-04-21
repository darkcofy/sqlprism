"""Tests for the indexer orchestrator."""

import importlib.metadata

import pytest

from sqlprism.core.indexer import _resolve_dialect
from sqlprism.core.mcp_tools import _compute_structural_diff
from sqlprism.languages.sqlmesh import _validate_command
from sqlprism.types import (
    ColumnUsageResult,
    EdgeResult,
    NodeResult,
    ParseResult,
)


def test_version_string():
    """Verify package version is 1.2.2."""
    version = importlib.metadata.version("sqlprism")
    assert version == "1.2.2"


def test_resolve_dialect_no_overrides():
    assert _resolve_dialect("models/foo.sql", "athena", None) == "athena"
    assert _resolve_dialect("models/foo.sql", None, None) is None


def test_resolve_dialect_prefix_override():
    overrides = {
        "starrocks/": "starrocks",
        "athena/": "athena",
    }
    assert _resolve_dialect("starrocks/models/foo.sql", "postgres", overrides) == "starrocks"
    assert _resolve_dialect("athena/queries/bar.sql", "postgres", overrides) == "athena"
    # No match falls back to default
    assert _resolve_dialect("other/baz.sql", "postgres", overrides) == "postgres"


def test_resolve_dialect_glob_override():
    overrides = {
        "**/*_sr.sql": "starrocks",
        "legacy/*.sql": "mysql",
    }
    assert _resolve_dialect("models/fact_orders_sr.sql", None, overrides) == "starrocks"
    assert _resolve_dialect("legacy/old_query.sql", None, overrides) == "mysql"
    assert _resolve_dialect("models/normal.sql", "athena", overrides) == "athena"


# ── P2.2: Command injection validation ──


def test_validate_command_allowed():
    """Valid commands pass validation."""
    _validate_command("uv run python", allowed_keywords={"python", "sqlmesh", "uv"})
    _validate_command("python", allowed_keywords={"python", "sqlmesh", "uv"})
    _validate_command("/usr/bin/python3", allowed_keywords={"python3"})
    _validate_command("uv run dbt", allowed_keywords={"dbt", "uv", "uvx"})
    _validate_command("uvx --with dbt-starrocks dbt", allowed_keywords={"dbt", "uv", "uvx"})


def test_validate_command_rejects_shell_metachar():
    """Commands with shell metacharacters are rejected."""
    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python; rm -rf /", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python | cat", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("$(whoami)", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("uv run python & bg", allowed_keywords={"python", "uv"})


def test_validate_command_rejects_unknown_base():
    """Commands with unrecognized base command are rejected."""
    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("rm -rf /", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("curl http://evil.com", allowed_keywords={"python", "uv"})


def test_validate_command_rejects_substring_bypass():
    """Commands that contain an allowed keyword as a substring are rejected."""
    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("pythonmalicious", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("mypython", allowed_keywords={"python", "uv"})

    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("/usr/bin/uvxploit", allowed_keywords={"uv", "uvx"})


def test_validate_command_rejects_empty():
    """Empty command is rejected."""
    with pytest.raises(ValueError, match="Empty command"):
        _validate_command("", allowed_keywords={"python"})


# ── P2.4: Checksum rendered models by content not path ──


def test_checksum_parse_result_content_based():
    """Checksum should change when parse result content changes."""
    from sqlprism.core.indexer import _checksum_parse_result
    from sqlprism.types import NodeResult, ParseResult

    r1 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="orders")])
    r2 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="orders")])
    r3 = ParseResult(language="sql", nodes=[NodeResult(kind="table", name="customers")])

    # Same content → same checksum
    assert _checksum_parse_result(r1) == _checksum_parse_result(r2)
    # Different content → different checksum
    assert _checksum_parse_result(r1) != _checksum_parse_result(r3)


# ── P3.3: Fix nodes_modified false positives ──


def test_structural_diff_unchanged_nodes_not_modified():
    """Nodes that exist in both old and new with same edges/columns should NOT be modified."""
    edge = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="orders",
        target_kind="table",
        relationship="references",
    )
    col = ColumnUsageResult(
        node_name="q",
        node_kind="query",
        table_name="orders",
        column_name="id",
        usage_type="select",
    )
    old = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="orders"), NodeResult(kind="query", name="q")],
            edges=[edge],
            column_usage=[col],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="table", name="orders"), NodeResult(kind="query", name="q")],
            edges=[edge],
            column_usage=[col],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert diff["nodes_added"] == []
    assert diff["nodes_removed"] == []
    assert diff["nodes_modified"] == []


def test_structural_diff_detects_actual_modification():
    """Nodes with changed edges/columns should show as modified."""
    old_edge = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="orders",
        target_kind="table",
        relationship="references",
    )
    new_edge = EdgeResult(
        source_name="q",
        source_kind="query",
        target_name="customers",
        target_kind="table",
        relationship="references",
    )
    old = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            edges=[old_edge],
        )
    }
    new = {
        "f.sql": ParseResult(
            language="sql",
            nodes=[NodeResult(kind="query", name="q")],
            edges=[new_edge],
        )
    }

    diff = _compute_structural_diff(old, new)
    assert len(diff["nodes_modified"]) == 1
    assert diff["nodes_modified"][0]["name"] == "q"


# ── P5.1: Shared utils ──


def test_parse_dotenv_matching_quotes():
    """parse_dotenv strips matching quotes, not mismatched ones."""
    import os
    import tempfile
    from pathlib import Path

    from sqlprism.languages.utils import parse_dotenv

    content = (
        "SIMPLE=hello\n"
        'DOUBLE_QUOTED="world"\n'
        "SINGLE_QUOTED='value'\n"
        'PARTIAL_QUOTE="not closed\n'
        "EMPTY=\n"
        "# comment\n"
        'STARTS_WITH_QUOTE="abc\n'
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write(content)
        f.flush()
        path = Path(f.name)

    try:
        result = parse_dotenv(path)
        assert result["SIMPLE"] == "hello"
        assert result["DOUBLE_QUOTED"] == "world"
        assert result["SINGLE_QUOTED"] == "value"
        assert result["EMPTY"] == ""
        # Mismatched quote should NOT be stripped
        assert result["PARTIAL_QUOTE"] == '"not closed'
        assert result["STARTS_WITH_QUOTE"] == '"abc'
    finally:
        os.unlink(path)


def test_find_venv_dir_fallback():
    """find_venv_dir falls back to project_path when no .venv found."""
    import tempfile
    from pathlib import Path

    from sqlprism.languages.utils import find_venv_dir

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "deep" / "project"
        p.mkdir(parents=True)
        assert find_venv_dir(p) == p


# ── P5.4: chain_index ──


def test_chain_index_disambiguates_multi_path():
    """Multiple lineage chains for same output column get distinct chain_index values."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id = db.upsert_repo("test", "/tmp/test")
    file_id = db.insert_file(repo_id, "query.sql", "sql", "abc123")

    # Two chains for the same output column (e.g. COALESCE(a.x, b.x) AS x)
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=0,
        hop_column="x",
        hop_table="v",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=1,
        hop_column="x",
        hop_table="a",
        chain_index=0,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=0,
        hop_column="x",
        hop_table="v",
        chain_index=1,
    )
    db.insert_column_lineage(
        file_id,
        "v",
        "x",
        hop_index=1,
        hop_column="x",
        hop_table="b",
        chain_index=1,
    )

    result = db.query_column_lineage(output_node="v", column="x")
    assert result["total_count"] == 2
    chain_indices = {c["chain_index"] for c in result["chains"]}
    assert chain_indices == {0, 1}
    db.close()
