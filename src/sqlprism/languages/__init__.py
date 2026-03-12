"""SQL parser registry and file-type utilities.

All SQL dialects are handled by a single ``SqlParser`` class backed by sqlglot.
The dialect is specified at indexing time per-repo or per-path, not per-parser.

Attributes:
    SQL_EXTENSIONS: Set of file extensions recognised as SQL
        (``{".sql", ".ddl", ".hql"}``).
"""

# Extensions we recognise as SQL
SQL_EXTENSIONS: set[str] = {".sql", ".ddl", ".hql"}


def is_sql_file(file_path: str) -> bool:
    """Check if a file path has a recognised SQL extension.

    Args:
        file_path: File path or name to check.

    Returns:
        ``True`` if the path ends with any extension in ``SQL_EXTENSIONS``.
    """
    lowered = file_path.lower()
    for ext in SQL_EXTENSIONS:
        if lowered.endswith(ext):
            return True
    return False
