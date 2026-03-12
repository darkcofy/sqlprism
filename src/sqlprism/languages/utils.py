"""Shared utilities for renderer modules (sqlmesh, dbt)."""

import os
from pathlib import Path

from sqlprism.types import NodeResult, ParseResult


def find_venv_dir(project_path: Path) -> Path:
    """Find the directory containing .venv for uv run.

    Checks project_path first, then walks up to 3 parent levels.
    Falls back to project_path if no .venv found.
    """
    if (project_path / ".venv").exists():
        return project_path
    current = project_path.parent
    for _ in range(3):
        if (current / ".venv").exists():
            return current
        current = current.parent
    return project_path


def parse_dotenv(env_path: Path) -> dict[str, str]:
    """Parse a .env file into a dict.

    Handles comments, blank lines, and properly strips matching quotes
    (single or double) from values.
    """
    result: dict[str, str] = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip matching quotes only (not mismatched ones)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        result[key] = value
    return result


def build_env(env_file: str | Path | None = None) -> dict[str, str]:
    """Build subprocess environment, optionally loading a .env file."""
    env = os.environ.copy()
    if env_file:
        env_path = Path(env_file).resolve()
        if env_path.exists():
            env.update(parse_dotenv(env_path))
    return env


def enrich_nodes(result: ParseResult, metadata_key: str, metadata_value: str) -> None:
    """Add renderer metadata to all nodes in a ParseResult (mutates in place)."""
    enriched = []
    for node in result.nodes:
        meta = dict(node.metadata) if node.metadata else {}
        meta[metadata_key] = metadata_value
        enriched.append(
            NodeResult(
                kind=node.kind,
                name=node.name,
                line_start=node.line_start,
                line_end=node.line_end,
                metadata=meta,
            )
        )
    result.nodes = enriched
