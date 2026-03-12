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
from sqlprism.types import ParseResult

logger = logging.getLogger(__name__)

# Inline script that runs inside the sqlmesh project's venv
_RENDER_SCRIPT = textwrap.dedent("""\
    import json
    import sys
    import os

    project_path = sys.argv[1]
    dialect = sys.argv[2]
    gateway = sys.argv[3]
    variables = json.loads(sys.argv[4])

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

    rendered = {}
    errors = []
    for model_name in context.models:
        try:
            query = context.render(model_name)
            sql = query.sql(dialect=dialect)
            if sql:
                rendered[model_name] = sql
        except Exception as e:
            errors.append({"model": model_name, "error": str(e)})

    json.dump({"rendered": rendered, "errors": errors}, sys.stdout)
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
        project_path = Path(project_path).resolve()

        # Determine where to run uv from (where .venv lives)
        if venv_dir:
            cwd = Path(venv_dir).resolve()
        else:
            cwd = find_venv_dir(project_path)

        env = build_env(env_file)

        # Run the render script in the project's venv
        models, errors = self._run_render_script(
            project_path=project_path,
            cwd=cwd,
            env=env,
            variables=variables or {},
            gateway=gateway,
            dialect=dialect,
            sqlmesh_command=sqlmesh_command,
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
    ) -> tuple[dict[str, str], list[dict]]:
        """Run the inline render script via subprocess. Returns ({model_name: sql}, errors)."""
        _validate_command(sqlmesh_command, allowed_keywords={"python", "sqlmesh", "uv"})
        cmd = shlex.split(sqlmesh_command) + [
            "-c",
            _RENDER_SCRIPT,
            str(project_path),
            dialect,
            gateway,
            json.dumps(variables),
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
        return output.get("rendered", {}), output.get("errors", [])


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
