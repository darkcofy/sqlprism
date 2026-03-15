"""SQLMesh model renderer.

Runs an inline Python script via `uv run python` in the sqlmesh project's
own virtualenv. The script uses sqlmesh's Python API to load the project,
create a local DuckDB gateway (no remote connections needed), render all
models, and output JSON to stdout.

This avoids needing sqlmesh as a dependency of this project — it uses
whatever sqlmesh version the project already has installed.
"""

import json
import logging
import shlex
import subprocess
import textwrap
from pathlib import Path

from sqlprism.languages.sql import SqlParser
from sqlprism.languages.utils import build_env, enrich_nodes, find_venv_dir
from sqlprism.types import ColumnDefResult, ParseResult

logger = logging.getLogger(__name__)

# Inline script that runs inside the sqlmesh project's venv.
# Accepts a model_filter arg (JSON list). Empty list = render all models.
_RENDER_SCRIPT = textwrap.dedent("""\
    import json
    import sys
    import os

    project_path = sys.argv[1]
    dialect = sys.argv[2]
    gateway = sys.argv[3]
    variables = json.loads(sys.argv[4])
    model_filter = json.loads(sys.argv[5])

    from sqlmesh import Context
    from sqlmesh.core.config import (
        Config, DuckDBConnectionConfig, GatewayConfig, ModelDefaultsConfig,
    )

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=dialect),
        gateways={gateway: GatewayConfig(connection=DuckDBConnectionConfig())},
        default_gateway=gateway,
        variables=variables,
    )

    context = Context(paths=[project_path], config=config)

    targets = model_filter if model_filter else list(context.models)

    rendered = {}
    errors = []
    column_schemas = {}
    for model_name in targets:
        try:
            model = context.models.get(model_name)
            query = context.render(model_name)
            sql = query.sql(dialect=dialect)
            if sql:
                rendered[model_name] = sql
            if model and hasattr(model, 'columns_to_types') and model.columns_to_types:
                column_schemas[model_name] = {
                    col: str(typ) for col, typ in model.columns_to_types.items()
                }
        except Exception as e:
            errors.append({"model": model_name, "error": str(e)})

    json.dump({"rendered": rendered, "errors": errors, "column_schemas": column_schemas}, sys.stdout)
""")


class SqlMeshRenderer:
    """Renders sqlmesh models into ``ParseResult`` objects via subprocess.

    Runs an inline Python script inside the sqlmesh project's own virtualenv
    to load the project, render every model to SQL, and output JSON to stdout.
    The rendered SQL is then parsed by ``SqlParser``. This avoids requiring
    sqlmesh as a direct dependency of the indexer.
    """

    def __init__(self, sql_parser: SqlParser | None = None):
        """Initialise the renderer.

        Args:
            sql_parser: ``SqlParser`` instance to use for parsing rendered SQL.
                Creates a default instance if not provided.
        """
        self.sql_parser = sql_parser or SqlParser()

    def render_project(
        self,
        project_path: str | Path,
        env_file: str | Path | None = None,
        variables: dict[str, str | int] | None = None,
        gateway: str = "local",
        dialect: str = "athena",
        sqlmesh_command: str = "uv run python",
        venv_dir: str | Path | None = None,
        schema_catalog: dict | None = None,
    ) -> dict[str, ParseResult]:
        """Render all models in a sqlmesh project.

        Args:
            project_path: Path to the sqlmesh project directory (containing config.yaml)
            env_file: Path to .env file to source before loading context
            variables: Extra sqlmesh variables (e.g. {"GRACE_PERIOD": 7})
            gateway: Gateway name to use (default "local" — uses duckdb, no remote deps)
            dialect: SQL dialect for rendering output
            sqlmesh_command: Command to run python in the sqlmesh venv (default: "uv run python")
            venv_dir: Directory to run from (where .venv lives). Auto-detects if not set.

        Returns:
            Dict mapping model name -> ParseResult
        """
        return self._render_and_parse(
            project_path=project_path,
            env_file=env_file,
            variables=variables,
            gateway=gateway,
            dialect=dialect,
            sqlmesh_command=sqlmesh_command,
            venv_dir=venv_dir,
            schema_catalog=schema_catalog,
        )

    def render_models(
        self,
        project_path: str | Path,
        model_names: list[str],
        env_file: str | Path | None = None,
        variables: dict[str, str | int] | None = None,
        gateway: str = "local",
        dialect: str = "athena",
        sqlmesh_command: str = "uv run python",
        venv_dir: str | Path | None = None,
        schema_catalog: dict | None = None,
    ) -> dict[str, ParseResult]:
        """Render specific models in a sqlmesh project.

        Args:
            project_path: Path to the sqlmesh project directory
            model_names: List of model names to render (passed as filter to render script)
            env_file: Path to .env file to source before loading context
            variables: Extra sqlmesh variables
            gateway: Gateway name to use (default "local")
            dialect: SQL dialect for rendering output
            sqlmesh_command: Command to run python in the sqlmesh venv
            venv_dir: Directory to run from (where .venv lives)
            schema_catalog: Optional schema catalog for column resolution

        Returns:
            Dict mapping model name -> ParseResult
        """
        return self._render_and_parse(
            project_path=project_path,
            env_file=env_file,
            variables=variables,
            gateway=gateway,
            dialect=dialect,
            sqlmesh_command=sqlmesh_command,
            venv_dir=venv_dir,
            schema_catalog=schema_catalog,
            model_filter=model_names,
        )

    def _render_and_parse(
        self,
        project_path: str | Path,
        env_file: str | Path | None,
        variables: dict[str, str | int] | None,
        gateway: str,
        dialect: str,
        sqlmesh_command: str,
        venv_dir: str | Path | None,
        schema_catalog: dict | None,
        model_filter: list[str] | None = None,
    ) -> dict[str, ParseResult]:
        """Shared implementation for render_project and render_models."""
        project_path = Path(project_path).resolve()

        if venv_dir:
            cwd = Path(venv_dir).resolve()
        else:
            cwd = find_venv_dir(project_path)

        env = build_env(env_file)

        models, errors, column_schemas = self._run_render_script(
            project_path=project_path,
            cwd=cwd,
            env=env,
            variables=variables or {},
            gateway=gateway,
            dialect=dialect,
            sqlmesh_command=sqlmesh_command,
            model_filter=model_filter or [],
        )

        for err in errors:
            logger.warning(
                "sqlmesh render error for model %s: %s",
                err.get("model", "<unknown>"),
                err.get("error", "<no message>"),
            )

        results: dict[str, ParseResult] = {}
        for model_name, rendered_sql in models.items():
            clean_name = model_name.strip('"').replace('"."', "/")
            result = self.sql_parser.parse(clean_name + ".sql", rendered_sql, schema=schema_catalog)
            enrich_nodes(result, "sqlmesh_model", model_name)

            # Attach column definitions from sqlmesh model schema
            if model_name in column_schemas:
                col_defs = _build_column_defs(model_name, column_schemas[model_name])
                result.columns.extend(col_defs)

            results[model_name] = result

        return results

    def _run_render_script(
        self,
        project_path: Path,
        cwd: Path,
        env: dict[str, str],
        variables: dict[str, str | int],
        gateway: str,
        dialect: str,
        sqlmesh_command: str,
        model_filter: list[str],
    ) -> tuple[dict[str, str], list[dict], dict[str, dict[str, str]]]:
        """Run the inline render script via subprocess.

        Returns:
            Tuple of (rendered_models, errors, column_schemas) where
            column_schemas maps model_name -> {col_name: data_type}.
        """
        _validate_command(sqlmesh_command, allowed_keywords={"python", "sqlmesh", "uv"})
        cmd = shlex.split(sqlmesh_command) + [
            "-c",
            _RENDER_SCRIPT,
            str(project_path),
            dialect,
            gateway,
            json.dumps(variables),
            json.dumps(model_filter),
        ]

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout for large projects
        )

        if result.returncode != 0:
            raise RuntimeError(f"sqlmesh render failed (exit {result.returncode}):\n{result.stderr}")

        output = json.loads(result.stdout)
        return output.get("rendered", {}), output.get("errors", []), output.get("column_schemas", {})


def _build_column_defs(
    model_name: str,
    columns: dict[str, str],
) -> list[ColumnDefResult]:
    """Convert a {col_name: data_type} dict into ColumnDefResult entries.

    Args:
        model_name: The sqlmesh model name (used as node_name).
        columns: Mapping of column name to SQL data type string.

    Returns:
        List of ColumnDefResult with source='sqlmesh_schema'.
    """
    return [
        ColumnDefResult(
            node_name=model_name,
            column_name=col_name,
            data_type=data_type,
            position=idx,
            source="sqlmesh_schema",
        )
        for idx, (col_name, data_type) in enumerate(columns.items())
    ]


def _validate_command(command: str, allowed_keywords: set[str]) -> None:
    """Validate a subprocess command against an allowlist.

    The first token of the command must contain one of the allowed keywords.
    Rejects shell metacharacters that could enable command injection.
    """
    # Reject shell metacharacters
    dangerous_chars = set(";|&`$(){}!")
    if dangerous_chars & set(command):
        raise ValueError(f"Command contains disallowed shell characters: {command!r}")

    parts = shlex.split(command)
    if not parts:
        raise ValueError("Empty command")

    # The base command (first token) must exactly match an allowed keyword
    base = parts[0].rsplit("/", 1)[-1]  # strip path prefix
    if base not in allowed_keywords:
        raise ValueError(
            f"Command {parts[0]!r} not in allowlist. Base command must be one of: {', '.join(sorted(allowed_keywords))}"
        )
