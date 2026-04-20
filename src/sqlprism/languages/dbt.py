"""dbt model renderer.

Runs `dbt compile` via subprocess to expand Jinja macros and resolve refs,
then reads the compiled SQL from target/compiled/ and feeds each model
to the standard SQL parser.

Unlike sqlmesh, dbt is NOT a Python dependency — we shell out to `dbt compile`
via `uv run` (so it uses the dbt project's own virtualenv). The user passes
the path to the dbt project directory and optionally a profiles dir and env file.

The venv may live in a parent directory (e.g. dbt/ has the .venv, but the
actual project is dbt/dp_starrocks/). Use `venv_dir` to control where
`uv run` executes from (defaults to project_path's parent if a .venv
is found there).

The dbt command can be customised (e.g. "uvx --with dbt-starrocks dbt" or
just "dbt" if globally installed).
"""

import json
import logging
import os
import shlex
import subprocess
from pathlib import Path

from sqlprism.languages.sql import SqlParser
from sqlprism.languages.sqlmesh import _validate_command
from sqlprism.languages.utils import build_env, enrich_nodes, find_venv_dir
from sqlprism.types import ColumnDefResult, EdgeResult, ParseResult

logger = logging.getLogger(__name__)


class DbtRenderer:
    """Compiles dbt models via ``dbt compile`` and parses the resulting SQL.

    Shells out to dbt (via ``uv run`` or a custom command) in the project's
    own virtualenv, then reads compiled SQL from ``target/compiled/`` and
    feeds each model through ``SqlParser``. dbt is not a Python dependency
    of the indexer -- it uses whatever version the project has installed.
    """

    def __init__(self, sql_parser: SqlParser | None = None):
        """Initialise the renderer.

        Args:
            sql_parser: ``SqlParser`` instance to use for parsing compiled SQL.
                Creates a default instance if not provided.
        """
        self.sql_parser = sql_parser or SqlParser()

    def render_project(
        self,
        project_path: str | Path,
        profiles_dir: str | Path | None = None,
        env_file: str | Path | None = None,
        target: str | None = None,
        dbt_command: str = "uv run dbt",
        venv_dir: str | Path | None = None,
        dialect: str | None = None,
        schema_catalog: dict | None = None,
    ) -> dict[str, ParseResult]:
        """Compile all dbt models and parse the resulting SQL.

        Args:
            project_path: Path to dbt project dir (containing dbt_project.yml)
            profiles_dir: Path to directory containing profiles.yml (defaults to project_path)
            env_file: Optional .env file to source before running dbt compile
            target: dbt target name (default: whatever profiles.yml specifies)
            dbt_command: Command to invoke dbt (default: "uv run dbt")
            venv_dir: Directory to run `uv run` from (where .venv lives).
                      Defaults to project_path, but auto-detects parent if
                      parent has .venv and project_path doesn't.
            dialect: SQL dialect for parsing (e.g. "starrocks", "mysql", "postgres").
                     Needed for dialect-specific syntax like backtick quoting.

        Returns:
            Dict mapping model relative path -> ParseResult
        """
        return self._compile_and_parse(
            project_path=project_path,
            profiles_dir=profiles_dir,
            env_file=env_file,
            target=target,
            dbt_command=dbt_command,
            venv_dir=venv_dir,
            dialect=dialect,
            schema_catalog=schema_catalog,
        )

    def render_models(
        self,
        project_path: str | Path,
        model_names: list[str],
        profiles_dir: str | Path | None = None,
        env_file: str | Path | None = None,
        target: str | None = None,
        dbt_command: str = "uv run dbt",
        venv_dir: str | Path | None = None,
        dialect: str | None = None,
        schema_catalog: dict | None = None,
    ) -> dict[str, ParseResult]:
        """Compile specific dbt models using ``--select`` and parse the resulting SQL.

        Args:
            project_path: Path to dbt project dir (containing dbt_project.yml)
            model_names: List of model names to compile (passed to ``--select``)
            profiles_dir: Path to directory containing profiles.yml (defaults to project_path)
            env_file: Optional .env file to source before running dbt compile
            target: dbt target name (default: whatever profiles.yml specifies)
            dbt_command: Command to invoke dbt (default: "uv run dbt")
            venv_dir: Directory to run `uv run` from (where .venv lives).
            dialect: SQL dialect for parsing.
            schema_catalog: Optional schema catalog for column resolution.

        Returns:
            Dict mapping model relative path -> ParseResult
        """
        return self._compile_and_parse(
            project_path=project_path,
            profiles_dir=profiles_dir,
            env_file=env_file,
            target=target,
            dbt_command=dbt_command,
            venv_dir=venv_dir,
            dialect=dialect,
            schema_catalog=schema_catalog,
            select=model_names,
        )

    def _compile_and_parse(
        self,
        project_path: str | Path,
        profiles_dir: str | Path | None,
        env_file: str | Path | None,
        target: str | None,
        dbt_command: str,
        venv_dir: str | Path | None,
        dialect: str | None,
        schema_catalog: dict | None,
        select: list[str] | None = None,
    ) -> dict[str, ParseResult]:
        """Shared implementation for render_project and render_models."""
        project_path = Path(project_path).resolve()
        profiles_dir = Path(profiles_dir).resolve() if profiles_dir else project_path

        # Determine where to run uv from (where .venv lives)
        if venv_dir:
            cwd = Path(venv_dir).resolve()
        else:
            cwd = find_venv_dir(project_path)

        # Use dialect-specific parser if needed (e.g. starrocks uses backticks)
        parser = self.sql_parser
        if dialect and dialect != getattr(parser, "dialect", None):
            parser = SqlParser(dialect=dialect)

        env = build_env(env_file)

        # Run dbt compile
        self._run_dbt_compile(
            project_path=project_path,
            profiles_dir=profiles_dir,
            cwd=cwd,
            env=env,
            target=target,
            dbt_command=dbt_command,
            select=select,
        )

        # Read dbt_project.yml to get the project name (for compiled path)
        project_name = self._get_project_name(project_path)

        # Read compiled SQL files from <target-path>/compiled/<project_name>/models/
        target_dir = self._resolve_target_dir(project_path)
        compiled_dir = target_dir / "compiled" / project_name / "models"
        if not compiled_dir.exists():
            return {}

        # Only read files for selected models when filtering
        selected = set(select) if select else None
        results: dict[str, ParseResult] = {}
        for sql_file in compiled_dir.rglob("*.sql"):
            if selected and sql_file.stem not in selected:
                continue

            rel_path = str(sql_file.relative_to(compiled_dir))
            content = sql_file.read_text(errors="replace")
            if not content.strip():
                continue

            # dbt compiled SQL is bare SELECT — wrap as CREATE TABLE
            # so the SQL parser extracts nodes, edges, and column usage.
            # Derive model name from file stem, schema from parent directory.
            path_parts = rel_path.removesuffix(".sql").split("/")
            model_name = path_parts[-1]  # e.g. "orders"
            # e.g. "staging"
            model_schema = "/".join(path_parts[:-1]) if len(path_parts) > 1 else None

            # Quote names to handle dashes and special chars
            safe_name = model_name.replace('"', '""')
            if model_schema:
                safe_schema = model_schema.replace('"', '""')
                wrapped_sql = f'CREATE TABLE "{safe_schema}"."{safe_name}" AS\n{content}'
            else:
                wrapped_sql = f'CREATE TABLE "{safe_name}" AS\n{content}'

            result = parser.parse(rel_path, wrapped_sql, schema=schema_catalog)
            enrich_nodes(result, "dbt_model", rel_path)

            results[rel_path] = result

        # Merge in authoritative ref/source edges from the dbt manifest so
        # model→model relationships survive the loss of `ref()` context
        # during compilation (including cross-project mesh refs).
        manifest_edges = self._extract_manifest_edges(
            project_path, project_name, parser, select=select
        )
        for rel_path, extra_edges in manifest_edges.items():
            pr = results.get(rel_path)
            if pr is None:
                continue
            # Dedup by full tuple including context so ref()/source() tags
            # coexist with parser-extracted "FROM clause" edges rather than
            # being silently dropped on identical name match.
            existing = {
                (e.source_name, e.source_kind, e.target_name, e.target_kind,
                 e.relationship, e.context)
                for e in pr.edges
            }
            for edge in extra_edges:
                key = (
                    edge.source_name,
                    edge.source_kind,
                    edge.target_name,
                    edge.target_kind,
                    edge.relationship,
                    edge.context,
                )
                if key not in existing:
                    pr.edges.append(edge)
                    existing.add(key)

        return results

    def _resolve_target_dir(self, project_path: Path) -> Path:
        """Resolve dbt's target directory path.

        Order of precedence: ``DBT_TARGET_PATH`` env var, then ``target-path``
        in ``dbt_project.yml``, then the default ``target``. Returned path is
        absolute (resolved against ``project_path`` when relative).
        """
        env_path = os.environ.get("DBT_TARGET_PATH")
        if env_path:
            p = Path(env_path)
            return p if p.is_absolute() else project_path / p

        configured: str | None = None
        try:
            import yaml

            data = yaml.safe_load((project_path / "dbt_project.yml").read_text())
            if isinstance(data, dict):
                value = data.get("target-path")
                if isinstance(value, str):
                    configured = value
        except (ImportError, OSError, Exception):
            pass

        if configured:
            p = Path(configured)
            return p if p.is_absolute() else project_path / p

        return project_path / "target"

    def _extract_manifest_edges(
        self,
        project_path: Path,
        project_name: str,
        parser: SqlParser,
        select: list[str] | None = None,
    ) -> dict[str, list[EdgeResult]]:
        """Read ``<target-path>/manifest.json`` and derive ref/source edges per model.

        dbt's manifest preserves the logical dependency graph expressed via
        ``ref()`` and ``source()`` — information that is lost when Jinja is
        compiled to SQL. For each model owned by ``project_name``, emit an
        edge per entry in ``depends_on.nodes`` (unioned with
        ``depends_on.public_nodes`` for mesh-across-dbt-versions safety)
        pointing at the referenced model (or a source's physical
        ``identifier``).

        Names are normalized through ``parser._normalize_identifier`` so the
        emitted edges line up with the nodes the SQL parser creates under
        case-folding dialects (Snowflake uppercase, DuckDB/Postgres lowercase).

        Edges are keyed by the model's ``path`` from the manifest, which
        matches the ``rel_path`` used by ``_compile_and_parse`` results.

        Args:
            project_path: Absolute path to the dbt project directory.
            project_name: ``name`` field from ``dbt_project.yml``.
            parser: The ``SqlParser`` used to parse compiled SQL — provides
                dialect-aware identifier normalization.
            select: If non-empty, only emit edges for models whose name is
                in this list (matches the ``--select`` behaviour of
                ``render_models``).

        Returns an empty dict if the manifest is missing or unreadable.
        """
        manifest_path = self._resolve_target_dir(project_path) / "manifest.json"
        if not manifest_path.exists():
            return {}

        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            logger.error(
                "Cannot read dbt manifest %s: %s — graph lineage will be incomplete "
                "(ref()/source() edges unavailable)",
                manifest_path,
                e,
            )
            return {}

        nodes = manifest.get("nodes") or {}
        sources = manifest.get("sources") or {}
        selected = set(select) if select else None

        def _norm(name: str) -> str:
            return parser._normalize_identifier(name) if name else name

        def _resolve_dep(key: str) -> tuple[str, str] | None:
            """Resolve a manifest dep key to (target_name, context).

            Prefers the node/source entry for accurate naming (sources may
            have an ``identifier`` that differs from ``name``). Falls back
            to the last segment of the key — strictly validated — so that
            disabled models or partial manifests still yield an edge rather
            than a silent drop.
            """
            if key in nodes:
                dep = nodes[key]
                if dep.get("resource_type") in ("model", "seed", "snapshot"):
                    dep_name = dep.get("name")
                    if dep_name:
                        return _norm(dep_name), "ref()"
            if key in sources:
                dep = sources[key]
                dep_name = dep.get("identifier") or dep.get("name")
                if dep_name:
                    return _norm(dep_name), "source()"
            # Strict fallback — key shapes:
            #   model|seed|snapshot.<pkg>.<name>        (3 parts)
            #   source.<pkg>.<source_name>.<table>      (4 parts)
            parts = key.split(".")
            kind = parts[0] if parts else ""
            if kind == "source" and len(parts) == 4:
                logger.debug("manifest dep %s resolved via fallback", key)
                return _norm(parts[-1]), "source()"
            if kind in ("model", "seed", "snapshot") and len(parts) == 3:
                logger.debug("manifest dep %s resolved via fallback", key)
                return _norm(parts[-1]), "ref()"
            return None

        edges_by_path: dict[str, list[EdgeResult]] = {}
        for node in nodes.values():
            if node.get("resource_type") != "model":
                continue
            if node.get("package_name") != project_name:
                continue

            raw_name = node.get("name")
            rel_path = node.get("path")
            if not raw_name or not rel_path:
                continue
            if selected is not None and raw_name not in selected:
                continue

            # Align source_name with the SQL parser's file_stem so edges
            # collapse onto the same node the parser creates for this model.
            source_name = _norm(Path(rel_path).stem)

            deps_obj = node.get("depends_on") or {}
            deps = list(deps_obj.get("nodes") or [])
            # Some dbt versions expose cross-project mesh refs via
            # `public_nodes`; union for defensive forward-compat.
            deps.extend(deps_obj.get("public_nodes") or [])

            edges: list[EdgeResult] = []
            seen: set[tuple[str, str]] = set()
            for dep_key in deps:
                resolved = _resolve_dep(dep_key)
                if resolved is None:
                    continue
                target_name, context = resolved
                # dbt shouldn't list self-deps, but guard against malformed manifests.
                if target_name == source_name:
                    continue
                dedup_key = (target_name, context)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                edges.append(
                    EdgeResult(
                        source_name=source_name,
                        source_kind="query",
                        target_name=target_name,
                        target_kind="table",
                        relationship="references",
                        context=context,
                    )
                )
            if edges:
                edges_by_path[rel_path] = edges

        return edges_by_path

    def _run_dbt_compile(
        self,
        project_path: Path,
        profiles_dir: Path,
        cwd: Path,
        env: dict[str, str],
        target: str | None,
        dbt_command: str,
        select: list[str] | None = None,
    ) -> None:
        """Run dbt compile, pointing at the project directory."""
        _validate_command(dbt_command, allowed_keywords={"dbt", "uv", "uvx"})
        cmd = shlex.split(dbt_command) + [
            "compile",
            "--project-dir",
            str(project_path),
            "--profiles-dir",
            str(profiles_dir),
        ]
        if target:
            cmd.extend(["--target", target])
        if select:
            cmd.extend(["--select", *select])

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min timeout for large projects
        )

        if result.returncode != 0:
            output = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"dbt compile failed (exit {result.returncode}):\n{output}")

    def _get_project_name(self, project_path: Path) -> str:
        """Read project name from dbt_project.yml."""
        dbt_project_file = project_path / "dbt_project.yml"
        if not dbt_project_file.exists():
            raise FileNotFoundError(f"No dbt_project.yml found in {project_path}")

        content = dbt_project_file.read_text()

        # Try proper YAML parsing first (pyyaml may be available via dbt)
        try:
            import yaml

            data = yaml.safe_load(content)
            if isinstance(data, dict) and "name" in data:
                return str(data["name"])
        except (ImportError, Exception):
            pass

        # Fallback: line scanning for top-level name: field
        for line in content.splitlines():
            if line.lstrip().startswith("#"):
                continue
            if line.startswith("name:"):  # only match unindented
                name = line.split(":", 1)[1].strip().strip("'\"")
                return name

        raise ValueError(f"Could not find 'name:' in {dbt_project_file}")

    def extract_schema_yml(
        self, project_path: str | Path
    ) -> dict[str, list[ColumnDefResult]]:
        """Extract column definitions from dbt schema.yml files.

        Scans all ``*.yml`` and ``*.yaml`` files under the project's ``models/``
        directory for model and source entries with ``columns:`` lists. Returns
        a mapping of model name to ``ColumnDefResult`` entries with
        ``source='schema_yml'``.

        Args:
            project_path: Path to dbt project dir (containing ``models/``).

        Returns:
            Dict mapping model name -> list of ``ColumnDefResult``.
        """
        import yaml

        project_path = Path(project_path).resolve()
        models_dir = project_path / "models"
        if not models_dir.exists():
            return {}

        result: dict[str, list[ColumnDefResult]] = {}

        yml_files = [f for f in models_dir.rglob("*") if f.suffix in (".yml", ".yaml")]
        for yml_file in yml_files:
            try:
                data = yaml.safe_load(yml_file.read_text())
            except Exception as e:
                logger.warning("Skipping malformed YAML %s: %s", yml_file, e)
                continue

            if not isinstance(data, dict):
                continue

            # Extract columns from models: entries
            for model in data.get("models") or []:
                if not isinstance(model, dict):
                    continue
                model_name = model.get("name")
                if not model_name:
                    continue
                self._extract_yml_columns(model_name, model.get("columns"), result)

            # Extract columns from sources: entries
            for source_entry in data.get("sources") or []:
                if not isinstance(source_entry, dict):
                    continue
                source_name = source_entry.get("name", "")
                for table_entry in source_entry.get("tables") or []:
                    if not isinstance(table_entry, dict):
                        continue
                    table_name = table_entry.get("name")
                    if not table_name:
                        continue
                    full_name = f"{source_name}.{table_name}" if source_name else table_name
                    self._extract_yml_columns(full_name, table_entry.get("columns"), result)

        return result

    @staticmethod
    def _extract_yml_columns(
        model_name: str,
        columns: list | None,
        result: dict[str, list[ColumnDefResult]],
    ) -> None:
        """Extract ColumnDefResult entries from a YAML columns list."""
        if not isinstance(columns, list):
            return

        # Offset position to avoid collision when same model spans multiple files
        offset = len(result[model_name]) if model_name in result else 0
        col_defs: list[ColumnDefResult] = []
        for i, col in enumerate(columns):
            if not isinstance(col, dict):
                continue
            col_name = col.get("name")
            if not col_name:
                continue
            col_defs.append(
                ColumnDefResult(
                    node_name=model_name,
                    column_name=col_name,
                    data_type=col.get("data_type"),
                    position=offset + i,
                    source="schema_yml",
                    description=col.get("description"),
                )
            )

        if col_defs:
            if model_name in result:
                result[model_name].extend(col_defs)
            else:
                result[model_name] = col_defs
