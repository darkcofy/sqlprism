"""Tests for sqlprism.languages."""

import pytest

from sqlprism.languages import is_sql_file


@pytest.mark.parametrize(
    "path, expected",
    [
        # Lowercase extensions
        ("schema.sql", True),
        ("tables.ddl", True),
        ("query.hql", True),
        # Uppercase extensions (the bug this test guards against)
        ("SCHEMA.SQL", True),
        ("TABLES.DDL", True),
        ("QUERY.HQL", True),
        # Mixed case
        ("Schema.Sql", True),
        ("Tables.Ddl", True),
        # Non-SQL extensions
        ("readme.md", False),
        ("main.py", False),
        ("data.csv", False),
        # Edge cases
        ("", False),
        ("sql", False),
    ],
)
def test_is_sql_file(path: str, expected: bool) -> None:
    assert is_sql_file(path) is expected
