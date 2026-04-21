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
from dataclasses import dataclass, field
from pathlib import Path

from sqlprism.languages.sql import SqlParser
from sqlprism.languages.sqlmesh import _merge_column_schemas, _validate_command
from sqlprism.languages.utils import build_env, enrich_nodes, find_venv_dir
from sqlprism.types import ColumnDefResult, EdgeResult, ParseResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SchemaYmlSource:
    """One ``sources:`` table parsed from schema.yml.

    Keeps the source *family* and physical *schema/identifier* separate so
    the indexer can target the exact graph node a referencing model created.
    ``schema`` defaults to ``family`` when the YAML doesn't override it,
    matching dbt's own default.
    """

    family: str
    identifier: str
    schema: str | None
    database: str | None
    columns: list[ColumnDefResult] = field(default_factory=list)


def _manifest_columns_dict(columns: dict | None) -> dict[str, str | None]:
    """Flatten a manifest node's ``columns`` field to ``{col: type_or_None}``.

    dbt manifests store columns as ``{col_name: {data_type, description, ...}}``.
    ``data_type`` is preserved as ``None`` when undeclared — the caller decides
    whether to fall back to ``"TEXT"`` or defer to a better-typed source (e.g.
    a ``schema.yml`` entry for the same column). Coercing to ``"TEXT"`` here
    would mask real types from ``schema.yml`` when the manifest lists the same
    column without a declared type.
    """
    if not isinstance(columns, dict):
        return {}
    out: dict[str, str | None] = {}
    for col_name, meta in columns.items():
        if not col_name:
            continue
        data_type = None
        if isinstance(meta, dict):
            data_type = meta.get("data_type")
        out[col_name] = data_type
    return out


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

        # Parse manifest.json once per compile — both the schema catalog
        # build and the edge extraction below consume it, and the file is
        # typically multi-MB on real projects.
        manifest = self._load_manifest(project_path)

        # Walk schema.yml once per compile. The split form (models + sources
        # kept apart) is the critical input for the post-parse merge — a
        # flattened key can't safely distinguish a source from a model whose
        # name happens to contain a dot. The catalog builder still uses the
        # flat form via _schema_yml_columns.
        schema_yml_models, schema_yml_sources = (
            self._extract_schema_yml_by_kind(project_path)
        )
        schema_yml_flat: dict[str, list[ColumnDefResult]] = dict(schema_yml_models)
        for src in schema_yml_sources:
            key = (
                f"{src.family}.{src.identifier}" if src.family else src.identifier
            )
            schema_yml_flat.setdefault(key, []).extend(src.columns)

        # Build an effective schema catalog that layers dbt-sourced columns
        # (manifest + schema.yml) over the graph-derived catalog. Needed so
        # SELECT * through CTEs can expand on a fresh index — before any
        # columns have been inserted into the graph.
        #
        # The catalog is a point-in-time snapshot: models parsed later in
        # this loop do not see columns produced by earlier models in the
        # same run. Downstream models depending on another model in the
        # same reindex pass therefore resolve SELECT * only via manifest/
        # schema.yml metadata, not via mid-run in-graph writes.
        effective_schema = self._build_effective_schema(
            project_path, parser, schema_catalog, manifest=manifest,
            schema_yml_per_model=schema_yml_flat,
        )

        # Only read files for selected models when filtering
        selected = set(select) if select else None
        results: dict[str, ParseResult] = {}
        for sql_file in compiled_dir.rglob("*.sql"):
            if selected and sql_file.stem not in selected:
                continue

            # Use posix-style separators so dict keys match manifest `path`
            # values (always forward-slash) across Windows and Unix.
            rel_path = sql_file.relative_to(compiled_dir).as_posix()
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

            result = parser.parse(rel_path, wrapped_sql, schema=effective_schema)
            enrich_nodes(result, "dbt_model", rel_path)

            results[rel_path] = result

        # Merge in authoritative ref/source edges from the dbt manifest so
        # model→model relationships survive the loss of `ref()` context
        # during compilation (including cross-project mesh refs).
        manifest_edges = self._extract_manifest_edges(
            project_path, project_name, parser, select=select, manifest=manifest,
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

        # Merge schema.yml column defs into matching ParseResults so the
        # `columns` table carries real types (`source='schema_yml'`),
        # winning over the inferred projection entries the parser emitted
        # from the CTAS wrap. Source entries go into a synthetic
        # ParseResult whose columns attach to source table nodes created
        # by referencing models. Models and sources are handed in
        # separately so a partial reindex (``render_models(select=...)``)
        # can't misclassify an unselected model as a source.
        self._merge_schema_yml_into_results(
            results, schema_yml_models, schema_yml_sources, parser,
        )

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

    def _load_manifest(self, project_path: Path) -> dict | None:
        """Read and parse ``<target-path>/manifest.json`` once.

        Returns the parsed dict, or ``None`` when the manifest is missing or
        unreadable. Logs at warning level on parse failure so a silent empty
        ``columns`` table / missing edges isn't the first signal the user sees.
        """
        manifest_path = self._resolve_target_dir(project_path) / "manifest.json"
        if not manifest_path.exists():
            return None
        try:
            return json.loads(manifest_path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(
                "Could not parse dbt manifest at %s: %s — schema catalog and "
                "ref()/source() edges will be incomplete",
                manifest_path,
                e,
            )
            return None

    def _extract_manifest_edges(
        self,
        project_path: Path,
        project_name: str,
        parser: SqlParser,
        select: list[str] | None = None,
        manifest: dict | None = None,
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
            manifest: Pre-parsed manifest dict. When provided the file isn't
                re-read — callers that also need column metadata can share
                one parse across both helpers.

        Returns an empty dict if the manifest is missing or unreadable.
        """
        if manifest is None:
            manifest = self._load_manifest(project_path)
        if not manifest:
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

            # Align source_name/kind/schema with the `CREATE TABLE
            # "<path-schema>"."<stem>"` node the renderer wraps around each
            # compiled model, so manifest edges resolve to the exact table
            # node (not a kind-relaxed fallback that may pick a CTE sharing
            # the stem's name).
            path_parts = rel_path.removesuffix(".sql").split("/")
            source_name = _norm(path_parts[-1])
            source_schema = "/".join(path_parts[:-1]) if len(path_parts) > 1 else None

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
                        source_kind="table",
                        target_name=target_name,
                        target_kind="table",
                        relationship="references",
                        context=context,
                        metadata={"source_schema": source_schema} if source_schema else None,
                    )
                )
            if edges:
                edges_by_path[rel_path] = edges

        return edges_by_path

    def _build_effective_schema(
        self,
        project_path: Path,
        parser: SqlParser,
        schema_catalog: dict | None,
        manifest: dict | None = None,
        schema_yml_per_model: dict[str, list[ColumnDefResult]] | None = None,
    ) -> dict[str, dict[str, str]]:
        """Merge dbt-sourced column schemas on top of the graph schema catalog.

        Reads fresh column metadata from ``manifest.json`` (authoritative when
        it carries a ``data_type``) and ``schema.yml`` (primary type source
        when users have documented their models). Layering rules:

        * ``schema.yml`` entries seed the overlay with their declared types,
          falling back to ``"TEXT"`` for undocumented columns on a documented
          model.
        * Manifest entries with a declared type override ``schema.yml`` only
          when the manifest's type is concrete — a ``None`` manifest type
          never clobbers a typed ``schema.yml`` entry. Manifest-only columns
          are added as ``"TEXT"`` so ``SELECT *`` expansion can see them.

        ``schema_yml_per_model`` may be passed pre-parsed to avoid a second
        disk walk when the caller already ran ``extract_schema_yml``.
        """
        yml_overlay = self._schema_yml_columns(
            project_path, parser, schema_yml_per_model=schema_yml_per_model,
        )
        manifest_overlay = self._extract_manifest_columns(
            project_path, parser, manifest=manifest,
        )

        overlays: dict[str, dict[str, str]] = {
            k: dict(v) for k, v in yml_overlay.items()
        }
        for name, cols in manifest_overlay.items():
            bucket = overlays.setdefault(name, {})
            for col, dtype in cols.items():
                if dtype is not None:
                    bucket[col] = dtype
                else:
                    # Gap-fill only — never overwrite a typed schema.yml entry
                    # with the manifest's ``None`` (the critical bug the
                    # earlier implementation had).
                    bucket.setdefault(col, "TEXT")
        if not overlays and not schema_catalog:
            return {}
        return _merge_column_schemas(schema_catalog, overlays)

    def _extract_manifest_columns(
        self,
        project_path: Path,
        parser: SqlParser,
        manifest: dict | None = None,
    ) -> dict[str, dict[str, str | None]]:
        """Read per-model column schemas from ``manifest.json``.

        Each manifest node carries a ``columns`` dict (populated from
        ``schema.yml`` at compile time) keyed by column name. Returns a flat
        mapping whose values are ``{col: data_type | None}`` — ``None``
        signals "column documented but type not declared" so the caller can
        defer to a better-typed source (``schema.yml``) rather than assuming
        ``"TEXT"`` unconditionally.

        Keys are dialect-normalized and emitted in multiple forms so lookups
        resolve against fully-qualified or bare references in compiled SQL:

        * Models/seeds/snapshots use the ``alias`` (if set) else ``name``.
          dbt models commonly override their physical name via ``alias:`` in
          ``config``, and compiled ``FROM`` clauses resolve to the alias.
        * Sources use ``identifier`` (physical table name) if present, else
          ``name``.
        * Both emit a bare-name key and, when the manifest provides a
          ``schema`` field, a ``schema.name`` qualified key so two entities
          sharing a bare name across different schemas don't collide.

        ``manifest`` may be passed pre-parsed to avoid re-reading the file.
        """
        if manifest is None:
            manifest = self._load_manifest(project_path)
        if not manifest:
            return {}

        out: dict[str, dict[str, str | None]] = {}

        def _layer(key: str, cols: dict[str, str | None]) -> None:
            bucket = out.setdefault(key, {})
            for col, dtype in cols.items():
                if dtype is not None:
                    bucket[col] = dtype
                else:
                    bucket.setdefault(col, None)

        # Models + seeds + snapshots — anything downstream refs via ref().
        for node in (manifest.get("nodes") or {}).values():
            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                continue
            name = node.get("alias") or node.get("name")
            if not name:
                continue
            cols = _manifest_columns_dict(node.get("columns"))
            if not cols:
                continue
            for key in self._catalog_keys(name, node.get("schema"), parser):
                _layer(key, cols)
        # Sources — referenced by physical identifier, not ref() name.
        for src in (manifest.get("sources") or {}).values():
            identifier = src.get("identifier") or src.get("name")
            if not identifier:
                continue
            cols = _manifest_columns_dict(src.get("columns"))
            if not cols:
                continue
            for key in self._catalog_keys(identifier, src.get("schema"), parser):
                _layer(key, cols)
        return out

    @staticmethod
    def _catalog_keys(
        name: str,
        schema: str | None,
        parser: SqlParser,
    ) -> list[str]:
        """Schema-catalog keys for a dbt model/source.

        Returns ``[base]`` when no schema is known, else ``[base, schema.base]``
        so downstream references resolve whether the compiled SQL qualifies
        the reference or not. Two entities sharing a bare ``name`` across
        different schemas each get a unique qualified key, avoiding silent
        column clobbering — while the bare key still covers unqualified refs
        (and collapses same-name collisions by design, matching the existing
        flat-catalog contract elsewhere).
        """
        base = parser._normalize_identifier(name)
        if not schema:
            return [base]
        qualified = f"{parser._normalize_identifier(schema)}.{base}"
        return [base, qualified]

    def _schema_yml_columns(
        self,
        project_path: Path,
        parser: SqlParser,
        schema_yml_per_model: dict[str, list[ColumnDefResult]] | None = None,
    ) -> dict[str, dict[str, str]]:
        """Column schemas from ``schema.yml`` files as a flat catalog.

        Reuses the existing ``extract_schema_yml`` walker and flattens each
        ``ColumnDefResult`` into ``{normalized_name: {col: type_or_TEXT}}``.
        Entries with no columns are dropped so they can't short-circuit the
        ``if not overlays`` guard in ``_build_effective_schema``.
        """
        per_model = (
            schema_yml_per_model
            if schema_yml_per_model is not None
            else self.extract_schema_yml(project_path)
        )
        out: dict[str, dict[str, str]] = {}
        for name, col_defs in per_model.items():
            if not col_defs:
                continue
            key = parser._normalize_identifier(name)
            bucket = out.setdefault(key, {})
            for c in col_defs:
                bucket[c.column_name] = c.data_type or "TEXT"
        return out

    # Synthetic ParseResult path that carries schema.yml `sources:` column
    # defs into the graph. A stable key lets `delete_file_data` clean it up
    # between reindex runs the same way it does for real compiled files.
    _SCHEMA_YML_SOURCES_PATH = "__schema_yml_sources__.sql"

    def _merge_schema_yml_into_results(
        self,
        results: dict[str, ParseResult],
        schema_yml_models: dict[str, list[ColumnDefResult]],
        schema_yml_sources: list[_SchemaYmlSource],
        parser: SqlParser,
    ) -> None:
        """Fold schema.yml ``ColumnDefResult`` entries into the render output.

        For each compiled model, schema.yml entries win per ``column_name``
        over the inferred projection the parser emitted — concrete types and
        descriptions replace name-only ``inferred`` rows. Schema.yml source
        entries land in a synthetic ParseResult so their columns persist
        via the graph fallback in ``Indexer._resolve_column_def_node``
        (which resolves to the source table node any referencing model has
        already created).

        Models are only merged when their compiled ParseResult is present in
        ``results`` — on a partial reindex where the caller passed
        ``select=[...]``, documented models that aren't being re-rendered
        are skipped rather than being misrouted to the synthetic source
        file.
        """
        if not schema_yml_models and not schema_yml_sources:
            return

        # Index compiled results by normalized file stem so schema.yml
        # model keys (the raw model name) line up with the CTAS-wrapped
        # node the parser stored. Log a warning if two compiled files
        # share a stem — schema.yml can only merge into one of them.
        stem_to_result: dict[str, tuple[str, ParseResult]] = {}
        for rel_path, pr in results.items():
            stem = Path(rel_path).stem
            key = parser._normalize_identifier(stem)
            if key in stem_to_result:
                prior_path, _ = stem_to_result[key]
                logger.warning(
                    "Duplicate compiled model stem %s — schema.yml merge will"
                    " only target %s, not %s",
                    key, prior_path, rel_path,
                )
                continue
            stem_to_result[key] = (rel_path, pr)

        for model_name, col_defs in schema_yml_models.items():
            if not col_defs:
                continue
            normalized = parser._normalize_identifier(model_name)
            match = stem_to_result.get(normalized)
            if match is None:
                # No compiled match — on partial reindex this is expected
                # for unselected models. Skip rather than mislabel as a
                # source.
                continue
            _rel, pr = match
            self._apply_schema_yml_to_parse_result(
                pr, node_name=normalized, col_defs=col_defs,
            )
            self._renumber_columns_for_node(pr.columns, normalized)

        source_col_defs: list[ColumnDefResult] = []
        seen_source_targets: dict[str, tuple[str | None, str]] = {}
        for src in schema_yml_sources:
            if not src.columns:
                continue
            identifier = parser._normalize_identifier(src.identifier)
            schema = (
                parser._normalize_identifier(src.schema) if src.schema else None
            )
            # Qualify the node_name with the source's physical schema so
            # two sources sharing a bare identifier across different
            # schemas resolve to their own graph nodes (collisions are
            # otherwise silent — UNIQUE upsert makes the last writer win).
            node_name = f'"{schema}"."{identifier}"' if schema else identifier

            prior = seen_source_targets.get(node_name)
            if prior is not None:
                logger.warning(
                    "Duplicate schema.yml source target %s (families %s vs %s)"
                    " — later columns will clobber earlier on the same node",
                    node_name, prior[1], src.family,
                )
            seen_source_targets[node_name] = (schema, src.family)

            for c in src.columns:
                source_col_defs.append(
                    ColumnDefResult(
                        node_name=node_name,
                        column_name=c.column_name,
                        data_type=c.data_type,
                        position=c.position,
                        source="schema_yml",
                        description=c.description,
                    )
                )

        if source_col_defs:
            # Renumber each target node's columns sequentially so entries
            # merged from distinct source families don't share positions.
            by_node: dict[str, list[ColumnDefResult]] = {}
            for c in source_col_defs:
                by_node.setdefault(c.node_name, []).append(c)
            flattened: list[ColumnDefResult] = []
            for target_name, rows in by_node.items():
                rows.sort(key=lambda r: (r.position if r.position is not None else 0))
                flattened.extend(
                    ColumnDefResult(
                        node_name=target_name,
                        column_name=r.column_name,
                        data_type=r.data_type,
                        position=i,
                        source=r.source,
                        description=r.description,
                    )
                    for i, r in enumerate(rows)
                )
            results[self._SCHEMA_YML_SOURCES_PATH] = ParseResult(
                language="sql",
                columns=flattened,
            )

    @staticmethod
    def _apply_schema_yml_to_parse_result(
        pr: ParseResult,
        node_name: str,
        col_defs: list[ColumnDefResult],
    ) -> None:
        """Replace existing columns on ``node_name`` that collide with schema.yml.

        Schema.yml wins over ``inferred`` rows — a documented type must not
        be shadowed by a name-only entry the parser emitted from the CTAS
        projection. Non-colliding entries from either side are preserved.
        """
        incoming = {c.column_name for c in col_defs}
        pr.columns = [
            c for c in pr.columns
            if not (c.node_name == node_name and c.column_name in incoming)
        ]
        for c in col_defs:
            pr.columns.append(
                ColumnDefResult(
                    node_name=node_name,
                    column_name=c.column_name,
                    data_type=c.data_type,
                    position=c.position,
                    source="schema_yml",
                    description=c.description,
                )
            )

    @staticmethod
    def _renumber_columns_for_node(
        columns: list[ColumnDefResult],
        node_name: str,
    ) -> None:
        """Reassign ``position`` 0..N-1 on every row targeting ``node_name``.

        Merging schema.yml rows (carrying their YAML ordinals) with surviving
        CTAS-inferred rows (carrying SELECT ordinals) yields duplicate
        positions on a single node. Renumbering stably preserves the current
        relative order — inferred rows come first (the parser appends them
        before the merge writes schema.yml rows) — so ``position`` stays
        monotonic for consumers that order by it.
        """
        targets = [(i, c) for i, c in enumerate(columns) if c.node_name == node_name]
        if not targets:
            return
        targets.sort(
            key=lambda pair: (
                pair[1].position if pair[1].position is not None else 0,
                pair[0],
            )
        )
        for new_pos, (orig_idx, c) in enumerate(targets):
            if c.position == new_pos:
                continue
            columns[orig_idx] = ColumnDefResult(
                node_name=c.node_name,
                column_name=c.column_name,
                data_type=c.data_type,
                position=new_pos,
                source=c.source,
                description=c.description,
            )

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
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            combined = f"{stderr}\n{stdout}"
            if "Failed to spawn: dbt" in combined or (
                "No module named" in combined and "dbt" in combined
            ):
                raise RuntimeError(
                    f"dbt is not installed in the project environment at {cwd}. "
                    "Install it in the dbt project's virtualenv "
                    "(e.g. `uv add dbt-core dbt-<adapter>` or "
                    "`pip install dbt-core dbt-<adapter>`), "
                    "or point `dbt_command` at a command that can launch dbt."
                )
            output = stderr or stdout
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
        ``source='schema_yml'``. Sources are keyed as
        ``"{source_family}.{table_name}"`` for backwards compat with the
        pre-existing catalog flatten.

        Args:
            project_path: Path to dbt project dir (containing ``models/``).

        Returns:
            Dict mapping model name -> list of ``ColumnDefResult``.
        """
        models, sources = self._extract_schema_yml_by_kind(project_path)
        result: dict[str, list[ColumnDefResult]] = dict(models)
        for src in sources:
            key = (
                f"{src.family}.{src.identifier}" if src.family else src.identifier
            )
            existing = result.get(key)
            if existing is None:
                result[key] = list(src.columns)
            else:
                existing.extend(src.columns)
        return result

    def _extract_schema_yml_by_kind(
        self,
        project_path: str | Path,
    ) -> tuple[dict[str, list[ColumnDefResult]], list[_SchemaYmlSource]]:
        """Parse schema.yml into separated models and sources.

        Callers that need to distinguish models from sources (the critical
        use case: the indexer merge can't guess from a flattened key whether
        ``raw.users`` is a source table or a model named with a dot) should
        consume this directly. ``extract_schema_yml`` remains the flat-form
        public API for the SELECT * catalog builder and existing tests.

        Source entries preserve the source's family, physical schema
        (``sources[].schema`` defaulting to ``sources[].name``), optional
        database, and the physical ``identifier`` (``tables[].identifier``
        defaulting to ``tables[].name``). Both a schema and identifier
        override at dbt's semantic layer must flow through to the graph —
        two sources sharing a bare name across different schemas must
        resolve to distinct nodes.
        """
        import yaml

        project_path = Path(project_path).resolve()
        models_dir = project_path / "models"
        models: dict[str, list[ColumnDefResult]] = {}
        sources: list[_SchemaYmlSource] = []
        if not models_dir.exists():
            return models, sources

        # Skip the rglob if no YAML files exist at all — avoids a full
        # directory traversal on projects that aren't documented.
        yml_files = [f for f in models_dir.rglob("*") if f.suffix in (".yml", ".yaml")]
        if not yml_files:
            return models, sources
        for yml_file in yml_files:
            try:
                data = yaml.safe_load(yml_file.read_text())
            except Exception as e:
                logger.warning("Skipping malformed YAML %s: %s", yml_file, e)
                continue

            if not isinstance(data, dict):
                continue

            # Models — keyed by plain model name.
            for model in data.get("models") or []:
                if not isinstance(model, dict):
                    continue
                model_name = model.get("name")
                if not model_name:
                    continue
                self._extract_yml_columns(model_name, model.get("columns"), models)

            # Sources — keep family/schema/identifier separate.
            for source_entry in data.get("sources") or []:
                if not isinstance(source_entry, dict):
                    continue
                family = source_entry.get("name") or ""
                source_schema = source_entry.get("schema") or family or None
                source_database = source_entry.get("database") or None
                for table_entry in source_entry.get("tables") or []:
                    if not isinstance(table_entry, dict):
                        continue
                    table_name = table_entry.get("name")
                    if not table_name:
                        continue
                    identifier = table_entry.get("identifier") or table_name
                    per_table: dict[str, list[ColumnDefResult]] = {}
                    self._extract_yml_columns(
                        identifier, table_entry.get("columns"), per_table,
                    )
                    col_defs = per_table.get(identifier, [])
                    if not col_defs:
                        continue
                    sources.append(
                        _SchemaYmlSource(
                            family=family,
                            identifier=identifier,
                            schema=source_schema,
                            database=source_database,
                            columns=col_defs,
                        )
                    )

        return models, sources

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
