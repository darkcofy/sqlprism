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
import os
import shlex
import subprocess
import textwrap
from concurrent.futures import ProcessPoolExecutor
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
            if model is None:
                errors.append({"model": model_name, "error": f"Model {model_name} not found in context"})
                continue
            query = context.render(model_name)
            sql = query.sql(dialect=dialect)
            if sql:
                rendered[model_name] = sql
            if hasattr(model, 'columns_to_types') and model.columns_to_types:
                column_schemas[model_name] = {
                    col: (typ.sql() if hasattr(typ, 'sql') else str(typ))
                    for col, typ in model.columns_to_types.items()
                }
        except Exception as e:
            errors.append({"model": model_name, "error": str(e)})

    json.dump({"rendered": rendered, "errors": errors, "column_schemas": column_schemas}, sys.stdout)
""")


_LIST_MODELS_SCRIPT = textwrap.dedent("""\
    import json
    import sys

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
    json.dump(list(context.models), sys.stdout)
""")


def _split_into_batches(items: list, num_batches: int) -> list[list]:
    """Split a list into num_batches balanced sublists."""
    if num_batches <= 0:
        return [items]
    batches: list[list] = [[] for _ in range(min(num_batches, len(items)))]
    for i, item in enumerate(items):
        batches[i % len(batches)].append(item)
    return batches


def _parse_model_worker(args: tuple) -> tuple[str, ParseResult]:
    """Worker function for parallel parsing. Must be top-level for pickling."""
    model_name, rendered_sql, dialect, schema_catalog = args
    parser = SqlParser(dialect=dialect)
    clean_name = model_name.strip('"').replace('"."', "/")
    result = parser.parse(clean_name + ".sql", rendered_sql, schema=schema_catalog)
    enrich_nodes(result, "sqlmesh_model", model_name)
    return model_name, result


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

    def _list_models(
        self,
        project_path: Path,
        cwd: Path,
        env: dict[str, str],
        variables: dict[str, str | int],
        gateway: str,
        dialect: str,
        sqlmesh_command: str,
    ) -> list[str]:
        """Run a lightweight subprocess to discover all model names."""
        _validate_command(sqlmesh_command, allowed_keywords={"python", "sqlmesh", "uv"})
        cmd = shlex.split(sqlmesh_command) + [
            "-c",
            _LIST_MODELS_SCRIPT,
            str(project_path),
            dialect,
            gateway,
            json.dumps(variables),
        ]

        result = subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=300,
        )

        if result.returncode != 0:
            raise RuntimeError(f"sqlmesh list-models failed (exit {result.returncode}):\n{result.stderr}")

        return json.loads(result.stdout)

    def _render_batches_parallel(
        self,
        project_path: Path,
        cwd: Path,
        env: dict[str, str],
        variables: dict[str, str | int],
        gateway: str,
        dialect: str,
        sqlmesh_command: str,
        all_models: list[str],
    ) -> tuple[dict[str, str], list[dict], dict[str, dict[str, str]]]:
        """Render models in parallel batches via concurrent subprocesses."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        num_workers = min(os.cpu_count() or 1, 4)
        batches = _split_into_batches(all_models, num_workers)

        merged_models: dict[str, str] = {}
        merged_errors: list[dict] = []
        merged_schemas: dict[str, dict[str, str]] = {}

        def render_batch(batch: list[str]):
            return self._run_render_script(
                project_path=project_path,
                cwd=cwd,
                env=env,
                variables=variables,
                gateway=gateway,
                dialect=dialect,
                sqlmesh_command=sqlmesh_command,
                model_filter=batch,
            )

        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = {pool.submit(render_batch, batch): i for i, batch in enumerate(batches)}
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    models, errors, schemas = future.result()
                    merged_models.update(models)
                    merged_errors.extend(errors)
                    merged_schemas.update(schemas)
                except Exception as e:
                    logger.error("Render batch %d failed: %s", batch_idx, e)
                    merged_errors.append({"model": f"batch_{batch_idx}", "error": str(e)})

        return merged_models, merged_errors, merged_schemas

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
        vars_ = variables or {}

        if model_filter:
            # Selective render — single subprocess, no discovery needed
            models, errors, column_schemas = self._run_render_script(
                project_path=project_path, cwd=cwd, env=env,
                variables=vars_, gateway=gateway, dialect=dialect,
                sqlmesh_command=sqlmesh_command, model_filter=model_filter,
            )
        else:
            # Full project render — discover models, then parallel batches
            try:
                all_models = self._list_models(
                    project_path, cwd, env, vars_, gateway, dialect, sqlmesh_command,
                )
            except Exception:
                logger.warning("Model discovery failed, falling back to single subprocess", exc_info=True)
                all_models = []

            if len(all_models) >= 20:
                models, errors, column_schemas = self._render_batches_parallel(
                    project_path, cwd, env, vars_, gateway, dialect,
                    sqlmesh_command, all_models,
                )
            else:
                models, errors, column_schemas = self._run_render_script(
                    project_path=project_path, cwd=cwd, env=env,
                    variables=vars_, gateway=gateway, dialect=dialect,
                    sqlmesh_command=sqlmesh_command, model_filter=all_models if all_models else [],
                )

        for err in errors:
            logger.warning(
                "sqlmesh render error for model %s: %s",
                err.get("model", "<unknown>"),
                err.get("error", "<no message>"),
            )

        # Use parallel parsing for large model sets, sequential for small
        if len(models) >= 20:
            results = self._parse_models_parallel(models, column_schemas, schema_catalog)
        else:
            results = self._parse_models_sequential(models, column_schemas, schema_catalog)

        return results

    def _parse_models_sequential(
        self,
        models: dict[str, str],
        column_schemas: dict[str, dict[str, str]],
        schema_catalog: dict | None,
    ) -> dict[str, ParseResult]:
        """Parse rendered models sequentially (baseline / fallback)."""
        results: dict[str, ParseResult] = {}
        for model_name, rendered_sql in models.items():
            clean_name = model_name.strip('"').replace('"."', "/")
            result = self.sql_parser.parse(clean_name + ".sql", rendered_sql, schema=schema_catalog)
            enrich_nodes(result, "sqlmesh_model", model_name)

            if model_name in column_schemas:
                col_defs = _build_column_defs(model_name, column_schemas[model_name])
                schema_names = {c.column_name for c in col_defs}
                result.columns = [
                    c for c in result.columns
                    if c.node_name != model_name or c.column_name not in schema_names
                ]
                result.columns.extend(col_defs)

            results[model_name] = result
        return results

    def _parse_models_parallel(
        self,
        models: dict[str, str],
        column_schemas: dict[str, dict[str, str]],
        schema_catalog: dict | None,
    ) -> dict[str, ParseResult]:
        """Parse rendered models in parallel using ProcessPoolExecutor."""
        max_workers = min(os.cpu_count() or 1, 8)
        dialect = self.sql_parser.dialect

        work_items = [
            (model_name, rendered_sql, dialect, schema_catalog)
            for model_name, rendered_sql in models.items()
        ]

        results: dict[str, ParseResult] = {}
        try:
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                for model_name, result in pool.map(_parse_model_worker, work_items):
                    if model_name in column_schemas:
                        col_defs = _build_column_defs(model_name, column_schemas[model_name])
                        schema_names = {c.column_name for c in col_defs}
                        result.columns = [
                            c for c in result.columns
                            if c.node_name != model_name or c.column_name not in schema_names
                        ]
                        result.columns.extend(col_defs)
                    results[model_name] = result
        except Exception:
            logger.warning("Parallel parsing failed, falling back to sequential", exc_info=True)
            return self._parse_models_sequential(models, column_schemas, schema_catalog)

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
    # Position relies on columns_to_types insertion order, which sqlmesh preserves
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
