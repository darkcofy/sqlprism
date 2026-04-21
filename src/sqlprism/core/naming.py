"""Qualified-identifier parsing shared across the indexer and language renderers."""

from __future__ import annotations

import sqlglot
from sqlglot import exp


def parse_qualified_name(raw: str) -> tuple[str, str | None]:
    """Split a possibly quoted/qualified identifier into ``(base_name, schema)``.

    Handles ``"catalog"."schema"."name"`` (3-part), ``"schema"."name"``
    (2-part), ``schema.name`` (unquoted), bare names, and single quoted
    identifiers that themselves contain dots (e.g. ``"weird.name"`` →
    base ``weird.name``, not ``name``). Falls back to a string split if
    sqlglot cannot parse the input.
    """
    if not raw:
        return raw, None
    try:
        table = exp.to_table(raw)
        base = table.name
        schema = table.db or None
        if base:
            return base, schema
    except (sqlglot.errors.ParseError, ValueError):
        pass
    stripped = raw.replace('"."', ".").strip('"')
    parts = stripped.split(".")
    base = parts[-1] or raw
    schema = parts[-2] if len(parts) >= 2 and parts[-2] else None
    return base, schema
