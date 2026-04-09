"""Indexer orchestrator.

The only component that connects parsers to storage. It:
1. Scans repos for SQL files
2. Checksums files and compares against stored checksums
3. Determines the dialect per file (repo default or path-based override)
4. Calls SqlParser with the appropriate dialect
5. Resolves edge references (name/kind → node IDs)
6. Inserts results into DuckDB via GraphDB
"""

import fnmatch
import hashlib
import logging
import subprocess
from collections import OrderedDict
from pathlib import Path

from sqlprism.core.conventions import ConventionEngine, TagAssignment
from sqlprism.core.graph import GraphDB
from sqlprism.languages import SQL_EXTENSIONS, is_sql_file
from sqlprism.languages.sql import SqlParser
from sqlprism.types import ParseResult

logger = logging.getLogger(__name__)


class Indexer:
    """Orchestrates parsing and indexing across repos.

    Connects language parsers to the ``GraphDB`` storage layer. Handles
    file scanning, checksum diffing, dialect resolution, and batch
    insertion of parse results. Supports plain SQL repos, sqlmesh
    projects, and dbt projects.
    """

    def __init__(self, graph: GraphDB):
        """Initialise the indexer.

        Args:
            graph: The ``GraphDB`` instance to write parsed data into.
        """
        self.graph = graph
        self._parser_cache: dict[str | None, SqlParser] = {}
        self._sqlmesh_renderer = None
        self._dbt_renderer = None
        # Stat-based pre-filter cache: abs_path -> (mtime, size, checksum)
        # Avoids re-reading file bytes when mtime+size are unchanged.
        self._file_stat_cache: OrderedDict[str, tuple[float, int, str]] = OrderedDict()

    def get_parser(self, dialect: str | None = None) -> SqlParser:
        """Get or create a SqlParser for the given dialect."""
        if dialect not in self._parser_cache:
            self._parser_cache[dialect] = SqlParser(dialect=dialect)
        return self._parser_cache[dialect]

    def get_sqlmesh_renderer(self, dialect: str | None = None):
        """Get or create a SqlMeshRenderer with the correct dialect parser."""
        from sqlprism.languages.sqlmesh import SqlMeshRenderer

        if self._sqlmesh_renderer is None or (dialect and self._sqlmesh_renderer.sql_parser.dialect != dialect):
            self._sqlmesh_renderer = SqlMeshRenderer(sql_parser=self.get_parser(dialect))
        return self._sqlmesh_renderer

    @property
    def dbt_renderer(self):
        if self._dbt_renderer is None:
            from sqlprism.languages.dbt import DbtRenderer

            self._dbt_renderer = DbtRenderer(sql_parser=self.get_parser())
        return self._dbt_renderer

    def reindex_repo(
        self,
        name: str,
        path: str | Path,
        dialect: str | None = None,
        dialect_overrides: dict[str, str] | None = None,
    ) -> dict:
        """Reindex a single repo by scanning for SQL files.

        Compares file checksums against the stored index to determine
        added, changed, and deleted files. Only changed files are re-parsed.

        Args:
            name: Repo name in the index.
            path: Absolute path to the repo root.
            dialect: Default SQL dialect (e.g. ``"starrocks"``, ``"athena"``).
            dialect_overrides: Per-path dialect overrides as
                ``{glob_pattern: dialect}``, e.g.
                ``{"athena/": "athena", "starrocks/**": "starrocks"}``.

        Returns:
            Stats dict with keys ``files_scanned``, ``files_added``,
            ``files_changed``, ``files_removed``, ``nodes_added``,
            ``edges_added``, ``column_usage_added``, ``lineage_chains``,
            ``column_usage_dropped``, ``parse_errors``, and
            ``phantoms_cleaned``.
        """
        path = Path(path).resolve()
        repo_id = self.graph.upsert_repo(name, str(path), repo_type="sql")

        # Get current checksums from DB
        stored_checksums = self.graph.get_file_checksums(repo_id)

        # Scan filesystem
        current_files = self._scan_files(path)

        # Determine what changed
        changed = []
        added = []
        for rel_path, checksum in current_files.items():
            if rel_path not in stored_checksums:
                added.append(rel_path)
            elif stored_checksums[rel_path] != checksum:
                changed.append(rel_path)

        deleted = [p for p in stored_checksums if p not in current_files]

        # Parse and insert changed/added files
        stats = {
            "files_scanned": len(current_files),
            "files_added": len(added),
            "files_changed": len(changed),
            "files_removed": len(deleted),
            "nodes_added": 0,
            "edges_added": 0,
            "column_usage_added": 0,
            "columns_added": 0,
            "lineage_chains": 0,
            "column_usage_dropped": 0,
            "parse_errors": [],
        }

        # Delete truly removed files in a transaction
        if deleted:
            with self.graph.write_transaction():
                for rel_path in deleted:
                    self.graph.delete_file_data(repo_id, rel_path)

        # Build schema catalog from existing index for SELECT * expansion
        schema_catalog = self.graph.get_table_columns(repo_id) or None

        # Changed + added files: delete old + insert new in same transaction
        # so a crash never leaves a file in a "deleted but not yet reinserted" state
        changed_set = set(changed)
        for rel_path in changed + added:
            # Resolve dialect for this file
            file_dialect = _resolve_dialect(rel_path, dialect, dialect_overrides)
            parser = self.get_parser(file_dialect)

            full_path = path / rel_path
            try:
                content = full_path.read_text(errors="replace")
            except (OSError, PermissionError):
                logger.warning("Cannot read file %s — skipping", full_path)
                stats["parse_errors"].append(f"{rel_path}: unreadable (OS/permission error)")
                continue
            checksum = current_files[rel_path]

            # Parse — pass schema catalog for SELECT * lineage expansion
            result = parser.parse(rel_path, content, schema=schema_catalog)
            if result.errors:
                for err in result.errors:
                    stats["parse_errors"].append(f"{rel_path}: {err}")

            # Wrap per-file delete + insert in a transaction for atomicity
            with self.graph.write_transaction():
                if rel_path in changed_set:
                    self.graph.delete_file_data(repo_id, rel_path)
                file_id = self.graph.insert_file(repo_id, rel_path, "sql", checksum)
                self._insert_parse_result(result, file_id, repo_id, stats)

        # Clean up phantom nodes that now have real counterparts
        phantoms_cleaned = self.graph.cleanup_phantoms()
        stats["phantoms_cleaned"] = phantoms_cleaned
        # Merge stub "table" nodes into their defining query/view counterparts
        stubs_merged = self.graph.merge_duplicate_nodes()
        stats["stubs_merged"] = stubs_merged

        # Update repo metadata
        commit, branch = self._get_git_info(path)
        self.graph.update_repo_metadata(repo_id, commit=commit, branch=branch)

        self._run_convention_inference(repo_id, project_path=path)
        self.graph.refresh_property_graph()
        self.graph.clear_snippet_cache()
        return stats

    def reindex_sqlmesh(
        self,
        repo_name: str,
        project_path: str | Path,
        env_file: str | Path | None = None,
        variables: dict[str, str | int] | None = None,
        dialect: str = "athena",
        sqlmesh_command: str = "uv run python",
        venv_dir: str | Path | None = None,
    ) -> dict:
        """Index a sqlmesh project by rendering all models first.

        Uses ``SqlMeshRenderer`` to render every model via subprocess,
        then parses the rendered SQL and inserts results into the graph.

        When source files haven't changed (matching fingerprint), skips the
        expensive rendering subprocess and re-parses cached SQL with the
        current schema catalog. This allows schema enrichment (SELECT *
        expansion) to converge while avoiding redundant rendering.

        Args:
            repo_name: Repo name in the index.
            project_path: Path to the sqlmesh project directory
                (containing ``config.yaml``).
            env_file: Optional ``.env`` file to source before rendering.
            variables: Extra sqlmesh variables (e.g. ``{"GRACE_PERIOD": 7}``).
            dialect: SQL dialect for rendering (default ``"athena"``).
            sqlmesh_command: Command to invoke Python in the sqlmesh venv.
            venv_dir: Directory containing ``.venv``. Auto-detected if not set.

        Returns:
            Stats dict with keys ``models_rendered``, ``nodes_added``,
            ``edges_added``, ``column_usage_added``, and ``lineage_chains``.
        """
        project_path = Path(project_path).resolve()
        repo_id = self.graph.upsert_repo(repo_name, str(project_path), repo_type="sqlmesh")

        # Build schema catalog from existing index for SELECT * expansion
        schema_catalog = self.graph.get_table_columns(repo_id) or None

        # Check if source files changed — skip rendering subprocess if not
        current_fingerprint = _source_fingerprint(project_path)
        stored_fingerprint = self.graph.get_source_fingerprint(repo_id)
        cache_hit = (stored_fingerprint == current_fingerprint) and stored_fingerprint is not None

        renderer = self.get_sqlmesh_renderer(dialect)

        if cache_hit:
            # Source unchanged — load cached rendered SQL
            cached = self.graph.get_rendered_cache(repo_id)
            if cached:
                logger.info("Source unchanged, using cached rendered SQL (%d models)", len(cached))
                raw_models = {name: sql for name, (sql, _) in cached.items()}
                column_schemas = {name: schemas for name, (_, schemas) in cached.items()}
            else:
                cache_hit = False

        if not cache_hit:
            # Render from scratch via subprocess
            raw_models, column_schemas = renderer.render_project_raw(
                project_path=project_path,
                env_file=env_file,
                variables=variables,
                dialect=dialect,
                sqlmesh_command=sqlmesh_command,
                venv_dir=venv_dir,
            )

        # Parse rendered SQL with current schema catalog (always re-parse
        # to allow schema enrichment even when rendering was cached)
        if len(raw_models) >= 20:
            rendered = renderer._parse_models_parallel(raw_models, column_schemas, schema_catalog)
        else:
            rendered = renderer._parse_models_sequential(raw_models, column_schemas, schema_catalog)

        stats = {
            "models_rendered": len(rendered),
            "models_skipped": 0,
            "models_removed": 0,
            "render_cached": cache_hit,
            "nodes_added": 0,
            "edges_added": 0,
            "column_usage_added": 0,
            "columns_added": 0,
            "lineage_chains": 0,
        }

        with self.graph.write_transaction():
            # Load existing checksums inside transaction to avoid TOCTOU
            existing_checksums = self.graph.get_file_checksums(repo_id)

            # Track which file paths are in the current render
            seen_paths: set[str] = set()

            for model_name, result in rendered.items():
                clean_name = model_name.strip('"').replace('"."', "/")
                file_path = clean_name + ".sql"
                seen_paths.add(file_path)

                checksum = _checksum_parse_result(result)

                # Skip if checksum matches — model hasn't changed
                if existing_checksums.get(file_path) == checksum:
                    stats["models_skipped"] += 1
                    continue

                self.graph.delete_file_data(repo_id, file_path)
                file_id = self.graph.insert_file(repo_id, file_path, "sql", checksum)
                self._insert_parse_result(result, file_id, repo_id, stats)

            # Delete stale models that no longer exist in the project
            for stale_path in set(existing_checksums) - seen_paths:
                self.graph.delete_file_data(repo_id, stale_path)
                stats["models_removed"] += 1

            # Cleanup phantoms inside the transaction for atomicity
            phantoms_cleaned = self.graph.cleanup_phantoms()
            self.graph.merge_duplicate_nodes()

        # Update fingerprint and render cache after successful index
        self.graph.update_source_fingerprint(repo_id, current_fingerprint)
        if not cache_hit:
            self.graph.update_rendered_cache(repo_id, raw_models, column_schemas)

        commit, branch = self._get_git_info(project_path)
        self.graph.update_repo_metadata(repo_id, commit=commit, branch=branch)

        self._run_convention_inference(repo_id, project_path=project_path)
        self.graph.refresh_property_graph()
        self.graph.clear_snippet_cache()
        stats["phantoms_cleaned"] = phantoms_cleaned
        return stats

    def reindex_dbt(
        self,
        repo_name: str,
        project_path: str | Path,
        profiles_dir: str | Path | None = None,
        env_file: str | Path | None = None,
        target: str | None = None,
        dbt_command: str = "uv run dbt",
        venv_dir: str | Path | None = None,
        dialect: str | None = None,
    ) -> dict:
        """Index a dbt project by compiling all models first.

        Runs ``dbt compile`` via ``DbtRenderer``, then parses each
        compiled SQL file and inserts results into the graph.

        Args:
            repo_name: Repo name in the index.
            project_path: Path to the dbt project directory
                (containing ``dbt_project.yml``).
            profiles_dir: Path to the directory containing ``profiles.yml``.
            env_file: Optional ``.env`` file to source before compilation.
            target: dbt target name override.
            dbt_command: Command to invoke dbt (e.g. ``"uv run dbt"``).
            venv_dir: Directory to run from (where ``.venv`` lives).
            dialect: SQL dialect for parsing compiled output.

        Returns:
            Stats dict with keys ``models_compiled``, ``nodes_added``,
            ``edges_added``, ``column_usage_added``, and ``lineage_chains``.
        """
        project_path = Path(project_path).resolve()
        repo_id = self.graph.upsert_repo(repo_name, str(project_path), repo_type="dbt")

        # Build schema catalog from existing index for SELECT * expansion
        schema_catalog = self.graph.get_table_columns(repo_id) or None

        rendered = self.dbt_renderer.render_project(
            project_path=project_path,
            profiles_dir=profiles_dir,
            env_file=env_file,
            target=target,
            dbt_command=dbt_command,
            venv_dir=venv_dir,
            dialect=dialect,
            schema_catalog=schema_catalog,
        )

        stats = {
            "models_compiled": len(rendered),
            "nodes_added": 0,
            "edges_added": 0,
            "column_usage_added": 0,
            "columns_added": 0,
            "lineage_chains": 0,
        }

        for model_path, result in rendered.items():
            # Wrap delete + insert per model in a transaction for atomicity
            with self.graph.write_transaction():
                self.graph.delete_file_data(repo_id, model_path)
                checksum = _checksum_parse_result(result)
                file_id = self.graph.insert_file(repo_id, model_path, "sql", checksum)
                self._insert_parse_result(result, file_id, repo_id, stats)

        commit, branch = self._get_git_info(project_path)
        self.graph.update_repo_metadata(repo_id, commit=commit, branch=branch)

        self._run_convention_inference(repo_id, project_path=project_path)
        self.graph.refresh_property_graph()
        self.graph.clear_snippet_cache()
        return stats

    def reindex_files(self, paths: list[str | Path], repo_configs: dict | None = None) -> dict:
        """Reindex specific files. Fast path for on-save.

        Resolves each path to its repo, determines repo type (plain SQL,
        dbt, sqlmesh), and reindexes accordingly.

        For plain SQL: read, parse, insert immediately.
        For dbt/sqlmesh: call ``render_models()`` for the affected models,
        then parse and insert the rendered SQL.

        Args:
            paths: Absolute file paths that changed. Non-SQL files and
                files outside configured repos are silently skipped.
            repo_configs: Optional repo config dict (repo_name → config).
                Required for dbt/sqlmesh repos to pass renderer params.
                If not provided, only plain SQL repos are supported.

        Returns:
            Stats dict with ``reindexed``, ``skipped``, ``deleted``,
            ``errors``, and per-file ``details``.
        """
        repo_configs = repo_configs or {}
        stats = {
            "reindexed": 0,
            "skipped": 0,
            "deleted": 0,
            "errors": [],
            "details": [],
        }

        # Fetch repo list once for all path resolutions (avoid N queries)
        all_repos = self.graph.get_all_repos()

        # Group files by repo
        files_by_repo: dict[int, list[Path]] = {}
        repo_info: dict[int, tuple[str, str, str]] = {}  # repo_id → (name, path, type)

        for raw_path in paths:
            file_path = Path(raw_path).resolve()

            # Skip non-SQL files
            if not is_sql_file(str(file_path)):
                stats["skipped"] += 1
                stats["details"].append({"path": str(file_path), "status": "skipped", "reason": "not a SQL file"})
                continue

            resolved = self._resolve_file_repo(file_path, all_repos)
            if resolved is None:
                stats["skipped"] += 1
                stats["details"].append({"path": str(file_path), "status": "skipped", "reason": "no matching repo"})
                continue

            repo_id, repo_name, repo_path, repo_type = resolved
            files_by_repo.setdefault(repo_id, []).append(file_path)
            repo_info[repo_id] = (repo_name, repo_path, repo_type)

        # Process each repo group
        for repo_id, files in files_by_repo.items():
            repo_name, repo_path, repo_type = repo_info[repo_id]
            cfg = repo_configs.get(repo_name, {})

            if repo_type == "sql":
                self._reindex_sql_files(repo_id, Path(repo_path), files, stats, cfg)
            elif repo_type == "dbt":
                self._reindex_dbt_files(repo_id, Path(repo_path), files, stats, cfg)
            elif repo_type == "sqlmesh":
                self._reindex_sqlmesh_files(repo_id, Path(repo_path), files, stats, cfg)
            else:
                for f in files:
                    stats["skipped"] += 1
                    stats["details"].append({
                        "path": str(f), "status": "skipped", "reason": f"unknown repo_type '{repo_type}'",
                    })

        # Single refresh after all file groups processed (not per-file)
        if files_by_repo:
            self.graph.refresh_property_graph()

        return stats

    @staticmethod
    def _resolve_file_repo(
        file_path: Path,
        repos: list[tuple[int, str, str, str]],
    ) -> tuple[int, str, str, str] | None:
        """Find which configured repo a file belongs to.

        Iterates the pre-fetched repo list and checks if the file is under
        the repo path. If multiple match (nested repos), picks the deepest
        (longest path prefix).

        Args:
            file_path: Absolute, resolved file path.
            repos: List of ``(repo_id, name, path, repo_type)`` tuples
                from ``GraphDB.get_all_repos()``.

        Returns:
            ``(repo_id, repo_name, repo_path, repo_type)`` or ``None``.
        """
        best: tuple[int, str, str, str] | None = None
        best_depth = -1

        file_str = str(file_path)
        for repo_id, name, path, repo_type in repos:
            # Normalise repo path for comparison
            repo_path = str(Path(path).resolve())
            if not repo_path.endswith("/"):
                repo_path_prefix = repo_path + "/"
            else:
                repo_path_prefix = repo_path

            if file_str.startswith(repo_path_prefix):
                depth = repo_path_prefix.count("/")
                if depth > best_depth:
                    best = (repo_id, name, repo_path, repo_type)
                    best_depth = depth

        return best

    def _reindex_sql_files(
        self,
        repo_id: int,
        repo_path: Path,
        files: list[Path],
        stats: dict,
        repo_config: dict | str | None = None,
    ) -> None:
        """Reindex plain SQL files: read, parse, insert."""
        from sqlprism.types import parse_repo_config

        dialect = None
        dialect_overrides = None
        if repo_config:
            _, dialect, dialect_overrides = parse_repo_config(repo_config)

        schema_catalog = self.graph.get_table_columns(repo_id) or None
        did_reindex = False

        for file_path in files:
            rel_path = str(file_path.relative_to(repo_path))

            # Handle deleted files
            if not file_path.exists():
                with self.graph.write_transaction():
                    self.graph.delete_file_data(repo_id, rel_path)
                stats["deleted"] += 1
                stats["details"].append({"path": str(file_path), "status": "deleted"})
                continue

            # Read and checksum
            try:
                content = file_path.read_text(errors="replace")
            except (OSError, PermissionError):
                stats["errors"].append(f"{rel_path}: unreadable")
                stats["details"].append({"path": str(file_path), "status": "error", "reason": "unreadable"})
                continue

            checksum = hashlib.sha256(content.encode()).hexdigest()

            # Skip if unchanged
            stored_checksum = self.graph.get_file_checksum(repo_id, rel_path)
            if stored_checksum == checksum:
                stats["skipped"] += 1
                stats["details"].append({"path": str(file_path), "status": "skipped", "reason": "unchanged"})
                continue

            # Parse
            file_dialect = _resolve_dialect(rel_path, dialect, dialect_overrides)
            result = self.get_parser(file_dialect).parse(rel_path, content, schema=schema_catalog)
            if result.errors:
                for err in result.errors:
                    stats["errors"].append(f"{rel_path}: {err}")

            # Atomic delete + insert
            with self.graph.write_transaction():
                self.graph.delete_file_data(repo_id, rel_path)
                file_id = self.graph.insert_file(repo_id, rel_path, "sql", checksum)
                insert_stats = {
                    "nodes_added": 0, "edges_added": 0,
                    "column_usage_added": 0, "columns_added": 0, "lineage_chains": 0,
                }
                self._insert_parse_result(result, file_id, repo_id, insert_stats)

            stats["reindexed"] += 1
            stats["details"].append({"path": str(file_path), "status": "reindexed"})
            did_reindex = True

        if did_reindex:
            self.graph.cleanup_phantoms()
            self.graph.merge_duplicate_nodes()
        self.graph.clear_snippet_cache()

    def _delete_stored_files_by_stem(self, repo_id: int, stem: str, stats: dict, display_path: str) -> None:
        """Delete stored file data for dbt/sqlmesh models by stem.

        Stored file paths for dbt (e.g. ``staging/orders.sql``) and sqlmesh
        (e.g. ``catalog/schema/orders.sql``) differ from filesystem relative
        paths. This method looks up the stored path by stem and deletes it.
        """
        stored_paths = self.graph.find_file_paths_by_stem(repo_id, stem)
        if stored_paths:
            with self.graph.write_transaction():
                for sp in stored_paths:
                    self.graph.delete_file_data(repo_id, sp)
            stats["deleted"] += len(stored_paths)
        else:
            stats["deleted"] += 1
        stats["details"].append({"path": display_path, "status": "deleted"})

    def _reindex_dbt_files(
        self,
        repo_id: int,
        repo_path: Path,
        files: list[Path],
        stats: dict,
        repo_config: dict,
    ) -> None:
        """Reindex dbt model files via render_models()."""
        # Handle deleted files first — look up stored paths by stem
        remaining = []
        for file_path in files:
            if not file_path.exists():
                self._delete_stored_files_by_stem(repo_id, file_path.stem, stats, str(file_path))
            else:
                remaining.append(file_path)

        if not remaining:
            return

        # Derive model names from file stems
        model_names = [f.stem for f in remaining]

        # Call render_models with config params
        schema_catalog = self.graph.get_table_columns(repo_id) or None
        try:
            rendered = self.dbt_renderer.render_models(
                project_path=repo_config.get("project_path", str(repo_path)),
                model_names=model_names,
                profiles_dir=repo_config.get("profiles_dir"),
                env_file=repo_config.get("env_file"),
                target=repo_config.get("target"),
                dbt_command=repo_config.get("dbt_command", "uv run dbt"),
                venv_dir=repo_config.get("venv_dir"),
                dialect=repo_config.get("dialect"),
                schema_catalog=schema_catalog,
            )
        except Exception as e:
            for f in remaining:
                stats["errors"].append(f"{f.stem}: dbt compile failed: {e}")
                stats["details"].append({"path": str(f), "status": "error", "reason": str(e)})
            return

        # Insert rendered results
        for model_path, result in rendered.items():
            with self.graph.write_transaction():
                self.graph.delete_file_data(repo_id, model_path)
                checksum = _checksum_parse_result(result)
                file_id = self.graph.insert_file(repo_id, model_path, "sql", checksum)
                insert_stats = {
                    "nodes_added": 0, "edges_added": 0,
                    "column_usage_added": 0, "columns_added": 0, "lineage_chains": 0,
                }
                self._insert_parse_result(result, file_id, repo_id, insert_stats)
            stats["reindexed"] += 1
            stats["details"].append({"path": model_path, "status": "reindexed"})

        self.graph.clear_snippet_cache()

    def _reindex_sqlmesh_files(
        self,
        repo_id: int,
        repo_path: Path,
        files: list[Path],
        stats: dict,
        repo_config: dict,
    ) -> None:
        """Reindex sqlmesh model files via render_models()."""
        # Handle deleted files first — look up stored paths by stem
        remaining = []
        for file_path in files:
            if not file_path.exists():
                self._delete_stored_files_by_stem(repo_id, file_path.stem, stats, str(file_path))
            else:
                remaining.append(file_path)

        if not remaining:
            return

        # Resolve model names: look up stored node names by file stem,
        # since stored file paths differ from filesystem paths
        model_names = self._resolve_model_names_by_stem(repo_id, remaining)

        dialect = repo_config.get("dialect", "athena")
        schema_catalog = self.graph.get_table_columns(repo_id) or None
        variables = _coerce_variables(repo_config.get("variables"))

        try:
            rendered = self.get_sqlmesh_renderer(dialect).render_models(
                project_path=repo_config.get("project_path", str(repo_path)),
                model_names=model_names,
                env_file=repo_config.get("env_file"),
                variables=variables or None,
                gateway=repo_config.get("gateway", "local"),
                dialect=dialect,
                sqlmesh_command=repo_config.get("sqlmesh_command", "uv run python"),
                venv_dir=repo_config.get("venv_dir"),
                schema_catalog=schema_catalog,
            )
        except Exception as e:
            for f in remaining:
                stats["errors"].append(f"{f.stem}: sqlmesh render failed: {e}")
                stats["details"].append({"path": str(f), "status": "error", "reason": str(e)})
            return

        # Insert rendered results
        for model_name, result in rendered.items():
            clean_name = model_name.strip('"').replace('"."', "/")
            file_path_key = clean_name + ".sql"

            with self.graph.write_transaction():
                self.graph.delete_file_data(repo_id, file_path_key)
                checksum = _checksum_parse_result(result)
                file_id = self.graph.insert_file(repo_id, file_path_key, "sql", checksum)
                insert_stats = {
                    "nodes_added": 0, "edges_added": 0,
                    "column_usage_added": 0, "columns_added": 0, "lineage_chains": 0,
                }
                self._insert_parse_result(result, file_id, repo_id, insert_stats)
            stats["reindexed"] += 1
            stats["details"].append({"path": file_path_key, "status": "reindexed"})

        self.graph.clear_snippet_cache()

    def _resolve_model_names_by_stem(
        self,
        repo_id: int,
        files: list[Path],
    ) -> list[str]:
        """Resolve file paths to model names via stored node names.

        For dbt/sqlmesh repos, the stored file paths differ from filesystem
        paths. This looks up the stored node name by matching file stems
        against the files table, then extracting the node name. Falls back
        to file stem for new/unindexed files.
        """
        model_names = []
        for file_path in files:
            stem = file_path.stem
            # Find stored file paths that match this stem
            stored_paths = self.graph.find_file_paths_by_stem(repo_id, stem)
            if stored_paths:
                # Look up the node name from the first matching stored path
                node_name = self.graph.find_node_name_by_file(repo_id, stored_paths[0])
                if node_name:
                    model_names.append(node_name)
                    continue
            model_names.append(stem)
        return model_names

    def _run_convention_inference(
        self, repo_id: int, project_path: str | Path | None = None
    ) -> dict:
        """Run convention inference for a repo after reindex.

        Non-fatal: logs errors but does not block reindex completion.

        Args:
            repo_id: The repo to run inference for.
            project_path: Project directory for override file discovery.
        """
        engine = ConventionEngine(self.graph, repo_id)

        try:
            result = engine.run_inference(project_path=project_path)
        except Exception as e:
            logger.warning(
                "Convention inference failed for repo %d: %s",
                repo_id,
                e,
            )
            result = {"layers_detected": 0, "conventions_stored": 0}

        # ── Semantic tag inference ──
        try:
            # Fetch existing tags for stability (anti-flap) logic
            existing_rows = self.graph.get_tags(repo_id)
            existing_tags = [
                TagAssignment(
                    tag_name=r["tag_name"],
                    node_id=r["node_id"],
                    model_name=r["node_name"],
                    confidence=r["confidence"],
                    source=r["source"],
                )
                for r in existing_rows
            ]

            tags = engine.infer_semantic_tags(
                existing_tags=existing_tags or None,
            )
            self.graph.delete_repo_tags(repo_id)
            if tags:
                tag_dicts = [
                    {
                        "tag_name": t.tag_name,
                        "node_id": t.node_id,
                        "confidence": t.confidence,
                        "source": t.source,
                    }
                    for t in tags
                ]
                self.graph.upsert_tags(repo_id, tag_dicts)
            logger.info(
                "Semantic tags: %d assigned for repo %d",
                len(tags),
                repo_id,
            )
            result["tags_assigned"] = len(tags)
        except Exception as e:
            logger.warning(
                "Semantic tag inference failed for repo %d: %s",
                repo_id,
                e,
            )
            result["tags_assigned"] = 0

        return result

    def _insert_parse_result(
        self,
        result: ParseResult,
        file_id: int,
        repo_id: int,
        stats: dict,
    ) -> None:
        """Insert nodes, edges, column usage, and lineage from a ParseResult.

        Shared by reindex_repo, reindex_sqlmesh, and reindex_dbt.
        Uses batch inserts for performance. Updates stats dict in-place.
        """
        import json

        # ── Batch insert nodes ──
        # Key includes schema to avoid collisions between staging.orders and production.orders
        node_id_map: dict[tuple[str, str, str | None], int] = {}
        if result.nodes:
            node_rows = [
                (
                    file_id,
                    node.kind,
                    node.name,
                    result.language,
                    node.line_start,
                    node.line_end,
                    json.dumps(node.metadata) if node.metadata else None,
                    (node.metadata or {}).get("schema") if node.metadata else None,
                )
                for node in result.nodes
            ]
            node_ids = self.graph.insert_nodes_batch(node_rows)
            for node, nid in zip(result.nodes, node_ids):
                schema = (node.metadata or {}).get("schema") if node.metadata else None
                node_id_map[(node.name, node.kind, schema)] = nid
            stats["nodes_added"] += len(result.nodes)

        # ── Batch insert edges ──
        if result.edges:
            edge_rows = []
            for edge in result.edges:
                source_id = self._resolve_edge_endpoint(
                    edge.source_name,
                    edge.source_kind,
                    node_id_map,
                    repo_id,
                    schema=(edge.metadata or {}).get("source_schema") if edge.metadata else None,
                )
                target_id = self._resolve_edge_endpoint(
                    edge.target_name,
                    edge.target_kind,
                    node_id_map,
                    repo_id,
                    schema=(edge.metadata or {}).get("target_schema") if edge.metadata else None,
                )
                edge_rows.append(
                    (
                        source_id,
                        target_id,
                        edge.relationship,
                        edge.context,
                        json.dumps(edge.metadata) if edge.metadata else None,
                    )
                )
            self.graph.insert_edges_batch(edge_rows)
            stats["edges_added"] += len(edge_rows)

        # ── Batch insert column usage ──
        if result.column_usage:
            cu_rows = []
            for cu in result.column_usage:
                # Try schema-aware lookup first, then fall back to schema=None
                cu_node_id = node_id_map.get((cu.node_name, cu.node_kind, None))
                if not cu_node_id:
                    # Try all schemas for this (name, kind)
                    for key, nid in node_id_map.items():
                        if key[0] == cu.node_name and key[1] == cu.node_kind:
                            cu_node_id = nid
                            break
                if not cu_node_id:
                    cu_node_id = self.graph.resolve_node(cu.node_name, cu.node_kind, repo_id)
                if cu_node_id:
                    cu_rows.append(
                        (
                            cu_node_id,
                            cu.table_name,
                            cu.column_name,
                            cu.usage_type,
                            file_id,
                            cu.alias,
                            cu.transform,
                        )
                    )
                else:
                    stats["column_usage_dropped"] = stats.get("column_usage_dropped", 0) + 1
                    logger.warning(
                        "Dropped column_usage: node %s/%s not found (table=%s col=%s)",
                        cu.node_name,
                        cu.node_kind,
                        cu.table_name,
                        cu.column_name,
                    )
            if cu_rows:
                self.graph.insert_column_usage_batch(cu_rows)
            stats["column_usage_added"] += len(cu_rows)

        # ── Batch insert column lineage ──
        if result.column_lineage:
            lineage_rows = []
            # Track chain_index per (output_node, output_column) to disambiguate multi-path lineage
            chain_counters: dict[tuple[str, str], int] = {}
            for cl in result.column_lineage:
                key = (cl.output_node, cl.output_column)
                chain_idx = chain_counters.get(key, 0)
                chain_counters[key] = chain_idx + 1
                for i, hop in enumerate(cl.chain):
                    lineage_rows.append(
                        (
                            file_id,
                            cl.output_node,
                            cl.output_column,
                            chain_idx,
                            i,
                            hop.column,
                            hop.table,
                            hop.expression,
                        )
                    )
                stats["lineage_chains"] += 1
            if lineage_rows:
                self.graph.insert_column_lineage_batch(lineage_rows)

        # ── Batch insert column definitions ──
        if result.columns:
            col_rows = []
            for col_def in result.columns:
                # Try to resolve node_id from local map (table/view only, matching kind)
                node_id = None
                for key, nid in node_id_map.items():
                    if key[0] == col_def.node_name and key[1] in ("table", "view", "source"):
                        node_id = nid
                        break
                # Fall back to graph resolution
                if not node_id:
                    node_id = self.graph.resolve_node(col_def.node_name, "table", repo_id)
                if not node_id:
                    node_id = self.graph.resolve_node(col_def.node_name, "view", repo_id)
                if not node_id:
                    node_id = self.graph.resolve_node(col_def.node_name, "source", repo_id)
                if not node_id:
                    logger.warning(
                        "Column def skipped: cannot resolve node '%s' for column '%s'",
                        col_def.node_name,
                        col_def.column_name,
                    )
                    continue
                col_rows.append(
                    (
                        node_id,
                        col_def.column_name,
                        col_def.data_type,
                        col_def.position,
                        col_def.source,
                        col_def.description,
                    )
                )
            if col_rows:
                self.graph.insert_columns_batch(col_rows)
            stats["columns_added"] += len(col_rows)

    def _resolve_edge_endpoint(
        self,
        name: str,
        kind: str,
        local_map: dict[tuple[str, str, str | None], int],
        repo_id: int,
        schema: str | None = None,
    ) -> int:
        """Resolve an edge endpoint to a node_id."""
        # Try with schema first, then without
        node_id = local_map.get((name, kind, schema))
        if node_id:
            return node_id
        if schema:
            node_id = local_map.get((name, kind, None))
            if node_id:
                return node_id

        node_id = self.graph.resolve_node(name, kind, repo_id, schema=schema)
        if node_id:
            return node_id

        return self.graph.get_or_create_phantom(name, kind, "sql")

    def _scan_files(self, repo_path: Path) -> dict[str, str]:
        """Scan a repo directory for SQL files. Returns {relative_path: sha256}.

        Uses mtime + size as a pre-filter: if both match the cached values
        from a previous scan, the stored checksum is reused without reading
        the file contents.
        """
        result: dict[str, str] = {}

        for file_path in repo_path.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix not in SQL_EXTENSIONS:
                continue
            # Skip common non-source directories
            parts = file_path.relative_to(repo_path).parts
            if any(
                p.startswith(".") or p in ("node_modules", "__pycache__", "venv", ".venv", "target", "build")
                for p in parts
            ):
                continue

            rel_path = str(file_path.relative_to(repo_path))
            abs_key = str(file_path)

            # Stat-based pre-filter: skip checksum if mtime+size unchanged
            try:
                st = file_path.stat()
            except OSError:
                logger.warning("Cannot stat file %s — skipping", file_path)
                continue
            mtime = st.st_mtime
            size = st.st_size

            cached = self._file_stat_cache.get(abs_key)
            if cached is not None and cached[0] == mtime and cached[1] == size:
                checksum = cached[2]
            else:
                try:
                    content = file_path.read_bytes()
                except OSError:
                    logger.warning("Cannot read file %s — skipping", file_path)
                    continue
                checksum = hashlib.sha256(content).hexdigest()
                self._file_stat_cache[abs_key] = (mtime, size, checksum)
                if len(self._file_stat_cache) > 10_000:
                    self._file_stat_cache.popitem(last=False)  # evict oldest

            result[rel_path] = checksum

        return result

    def _get_git_info(self, repo_path: Path) -> tuple[str | None, str | None]:
        """Get current git commit hash and branch name."""
        try:
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=5,
            )
            branch = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=5,
            )
            return (
                commit.stdout.strip() if commit.returncode == 0 else None,
                branch.stdout.strip() if branch.returncode == 0 else None,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None, None

    def parse_file(
        self,
        file_path: str,
        content: str,
        dialect: str | None = None,
        schema: dict | None = None,
    ) -> ParseResult:
        """Parse a single SQL file without writing to the database.

        Args:
            file_path: File path (used for naming nodes, not read from disk).
            content: Raw SQL content.
            dialect: Optional SQL dialect override.
            schema: Optional schema catalog for ``SELECT *`` expansion.

        Returns:
            A ``ParseResult`` with extracted nodes, edges, column usage,
            and lineage. Returns an empty result for non-SQL files.
        """
        if not is_sql_file(file_path):
            return ParseResult(language="sql")
        return self.get_parser(dialect).parse(file_path, content, schema=schema)

    def parse_file_at_commit(
        self,
        repo_path: Path,
        file_path: str,
        commit: str,
        dialect: str | None = None,
    ) -> ParseResult | None:
        """Parse a file at a specific git commit.

        Retrieves file content via ``git show`` and parses it without
        writing to the database. Used by pr_impact analysis.

        Args:
            repo_path: Absolute path to the git repo root.
            file_path: Relative file path within the repo.
            commit: Git commit hash or ref to read from.
            dialect: Optional SQL dialect override.

        Returns:
            A ``ParseResult``, or ``None`` if the file doesn't exist at
            that commit or is not a SQL file.
        """
        if not is_sql_file(file_path):
            return None
        try:
            result = subprocess.run(
                ["git", "show", f"{commit}:{file_path}"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return None
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

        return self.get_parser(dialect).parse(file_path, result.stdout)

    def get_changed_files(self, repo_path: Path, base_commit: str) -> list[str]:
        """Get SQL files changed between a base commit and HEAD.

        Args:
            repo_path: Absolute path to the git repo root.
            base_commit: Git commit hash or ref to diff against HEAD.

        Returns:
            List of relative file paths for changed SQL files. Returns
            an empty list on git errors or timeouts.
        """
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", f"{base_commit}..HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                return []
            return [f.strip() for f in result.stdout.strip().split("\n") if f.strip() and is_sql_file(f.strip())]
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return []


def _resolve_dialect(
    file_path: str,
    default_dialect: str | None,
    overrides: dict[str, str] | None,
) -> str | None:
    """Determine the SQL dialect for a file path.

    Checks overrides first (glob patterns), falls back to default.
    """
    if overrides:
        for pattern, dialect in overrides.items():
            # Support both "dir/" prefix matching and full glob
            if file_path.startswith(pattern) or fnmatch.fnmatch(file_path, pattern):
                return dialect
    return default_dialect


def _coerce_variables(raw: dict | None) -> dict[str, str | int]:
    """Coerce variable values to int where possible.

    JSON config preserves int types, but values may arrive as strings
    from CLI args or env vars.
    """
    if not raw:
        return {}
    result: dict[str, str | int] = {}
    for k, v in raw.items():
        if isinstance(v, int):
            result[k] = v
        else:
            try:
                result[k] = int(v)
            except (ValueError, TypeError):
                result[k] = v
    return result


def _source_fingerprint(project_path: Path) -> str:
    """Compute a fingerprint of all source files in a project directory.

    Uses file paths, mtimes, and sizes — no content reads needed.
    Includes .sql, .py, .yaml, .yml, and .cfg files to capture
    model definitions, macros, and config changes.
    """
    entries = []
    extensions = {".sql", ".py", ".yaml", ".yml", ".cfg"}
    for p in sorted(project_path.rglob("*")):
        if p.is_file() and p.suffix in extensions and ".venv" not in p.parts:
            stat = p.stat()
            entries.append(f"{p.relative_to(project_path)}:{stat.st_mtime_ns}:{stat.st_size}")
    return hashlib.sha256("\n".join(entries).encode()).hexdigest()


def _checksum_parse_result(result: ParseResult) -> str:
    """Hash the structural content of a ParseResult.

    Used for rendered models (sqlmesh/dbt) where we don't have the raw SQL
    content to hash directly. Produces a stable, order-independent checksum
    based on the extracted nodes, edges, and column usage.
    """
    parts = sorted(f"N:{n.kind}:{n.name}" for n in result.nodes)
    parts += sorted(f"E:{e.source_name}:{e.target_name}:{e.relationship}" for e in result.edges)
    parts += sorted(
        f"CU:{cu.node_name}:{cu.table_name}:{cu.column_name}:{cu.usage_type}"
        for cu in result.column_usage
    )
    for cl in sorted(result.column_lineage, key=lambda c: (c.output_node, c.output_column)):
        hops = "|".join(f"{h.table}.{h.column}:{h.expression or ''}" for h in cl.chain)
        parts.append(f"CL:{cl.output_node}:{cl.output_column}:{hops}")
    parts += sorted(
        f"CD:{col.node_name}:{col.column_name}:{col.data_type}:{col.position}:{col.source}"
        for col in result.columns
    )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()
