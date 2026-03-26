"""Tests for shared data types."""

from dataclasses import FrozenInstanceError

import pytest

from sqlprism.types import ColumnDefResult, ParseResult


def test_column_def_result_dataclass():
    """ColumnDefResult holds all column metadata and is frozen."""
    col = ColumnDefResult(
        node_name="orders",
        column_name="order_id",
        data_type="INT",
        position=0,
        source="definition",
        description="Primary key",
    )
    assert col.node_name == "orders"
    assert col.column_name == "order_id"
    assert col.data_type == "INT"
    assert col.position == 0
    assert col.source == "definition"
    assert col.description == "Primary key"

    # Frozen — cannot mutate
    with pytest.raises(FrozenInstanceError):
        col.column_name = "id"  # type: ignore[invalid-assignment]


def test_column_def_result_defaults():
    """ColumnDefResult optional fields default correctly."""
    col = ColumnDefResult(node_name="orders", column_name="status")
    assert col.data_type is None
    assert col.position is None
    assert col.source == "definition"
    assert col.description is None


def test_parse_result_columns_default_empty():
    """ParseResult.columns defaults to empty list (backwards compatible)."""
    result = ParseResult(language="sql")
    assert result.columns == []
    assert isinstance(result.columns, list)


def test_parse_result_columns_populated():
    """ParseResult accepts columns list."""
    cols = [
        ColumnDefResult(node_name="orders", column_name="id", data_type="INT"),
        ColumnDefResult(node_name="orders", column_name="status", data_type="TEXT"),
    ]
    result = ParseResult(language="sql", columns=cols)
    assert len(result.columns) == 2
    assert result.columns[0].column_name == "id"
