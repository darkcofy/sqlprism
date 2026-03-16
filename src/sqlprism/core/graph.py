"""DuckDB graph storage layer.

This module owns the database. It initialises the schema, handles inserts,
resolves edges (name/kind pairs → node IDs), manages phantom nodes, and
provides the query methods that MCP tools call.

No other module touches DuckDB directly.

Thread-safety model (read/write separation):
    DuckDB provides MVCC, so concurrent reads are safe without locking.
    Only write operations need serialisation via ``_write_lock``.

    - **Read path** (``_execute_read``): creates a fresh cursor, executes,
      returns results via ``fetchall()``, then closes the cursor.  No lock
      needed -- safe for concurrent access from MCP query handlers while a
      reindex is in progress.

    - **Write path** (``_execute_write``): uses ``self.conn.execute()``
      directly.  Caller must hold ``_write_lock``.

    - **Transactions** (``write_transaction``): acquires ``_write_lock``
      for the full ``BEGIN .. COMMIT`` scope.
"""

import json
import logging
import threading
import warnings
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_MAX_FILE_SIZE = 1 * 1024 * 1024  # 1 MB – skip snippets for oversized files


@lru_cache(maxsize=128)
def _read_file_lines(path: str) -> tuple[str, ...] | None:
    """Read and cache file lines for snippet extraction.

    Returns a tuple of lines (hashable for lru_cache), or None on error.
    Files exceeding *_MAX_FILE_SIZE* bytes are skipped to keep memory bounded.
    """
    try:
        p = Path(path)
        if p.stat().st_size > _MAX_FILE_SIZE:
            return None
        return tuple(p.read_text(errors="replace").splitlines())
    except Exception:
        return None


SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS seq_repo_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_file_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_node_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_edge_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_usage_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_lineage_id START 1;

CREATE TABLE IF NOT EXISTS repos (
    repo_id     INTEGER PRIMARY KEY DEFAULT nextval('seq_repo_id'),
    name        TEXT NOT NULL UNIQUE,
    path        TEXT NOT NULL,
    repo_type   TEXT NOT NULL DEFAULT 'sql',
    last_commit TEXT,
    last_branch TEXT,
    indexed_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS files (
    file_id     INTEGER PRIMARY KEY DEFAULT nextval('seq_file_id'),
    repo_id     INTEGER NOT NULL,  -- logical FK to repos(repo_id)
    path        TEXT NOT NULL,
    language    TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    indexed_at  TIMESTAMP DEFAULT now(),
    UNIQUE(repo_id, path)
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id     INTEGER PRIMARY KEY DEFAULT nextval('seq_node_id'),
    file_id     INTEGER,  -- NULL for phantom nodes; logical FK to files(file_id)
    kind        TEXT NOT NULL,
    name        TEXT NOT NULL,
    schema      TEXT,
    language    TEXT NOT NULL,
    line_start  INTEGER,
    line_end    INTEGER,
    metadata    JSON,
    UNIQUE(file_id, kind, name, schema)
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id       INTEGER PRIMARY KEY DEFAULT nextval('seq_edge_id'),
    source_id     INTEGER NOT NULL,  -- logical FK to nodes(node_id)
    target_id     INTEGER NOT NULL,  -- logical FK to nodes(node_id)
    relationship  TEXT NOT NULL,
    context       TEXT,
    metadata      JSON
);

CREATE TABLE IF NOT EXISTS column_usage (
    usage_id    INTEGER PRIMARY KEY DEFAULT nextval('seq_usage_id'),
    node_id     INTEGER NOT NULL,   -- logical FK to nodes(node_id)
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    usage_type  TEXT NOT NULL,
    alias       TEXT,
    transform   TEXT,
    file_id     INTEGER NOT NULL    -- logical FK to files(file_id)
);

CREATE TABLE IF NOT EXISTS column_lineage (
    lineage_id      INTEGER PRIMARY KEY DEFAULT nextval('seq_lineage_id'),
    file_id         INTEGER NOT NULL,  -- logical FK to files(file_id)
    output_node     TEXT NOT NULL,
    output_column   TEXT NOT NULL,
    chain_index     INTEGER NOT NULL DEFAULT 0,
    hop_index       INTEGER NOT NULL,
    hop_column      TEXT NOT NULL,
    hop_table       TEXT NOT NULL,
    hop_expression  TEXT
);

CREATE SEQUENCE IF NOT EXISTS seq_column_id START 1;

CREATE TABLE IF NOT EXISTS columns (
    column_id   INTEGER PRIMARY KEY DEFAULT nextval('seq_column_id'),
    node_id     INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    data_type   TEXT,
    position    INTEGER,
    source      TEXT NOT NULL,
    description TEXT,
    UNIQUE(node_id, column_name)
);

"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(name);
CREATE INDEX IF NOT EXISTS idx_nodes_kind ON nodes(kind);
CREATE INDEX IF NOT EXISTS idx_nodes_file ON nodes(file_id);
CREATE INDEX IF NOT EXISTS idx_nodes_kind_name ON nodes(kind, name);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
CREATE INDEX IF NOT EXISTS idx_edges_relationship ON edges(relationship);
CREATE INDEX IF NOT EXISTS idx_col_table ON column_usage(table_name);
CREATE INDEX IF NOT EXISTS idx_col_column ON column_usage(column_name);
CREATE INDEX IF NOT EXISTS idx_col_table_column ON column_usage(table_name, column_name);
CREATE INDEX IF NOT EXISTS idx_col_usage_type ON column_usage(usage_type);
CREATE INDEX IF NOT EXISTS idx_lineage_output ON column_lineage(output_node, output_column);
CREATE INDEX IF NOT EXISTS idx_lineage_hop ON column_lineage(hop_table, hop_column);
CREATE INDEX IF NOT EXISTS idx_lineage_file ON column_lineage(file_id);
CREATE INDEX IF NOT EXISTS idx_nodes_schema ON nodes(schema);
CREATE INDEX IF NOT EXISTS idx_columns_node ON columns(node_id);
CREATE INDEX IF NOT EXISTS idx_columns_name ON columns(column_name);
"""


class GraphDB:
    """DuckDB-backed knowledge graph storage.

    The sole storage layer for the SQL indexer. Manages a DuckDB database
    containing repos, files, nodes, edges, column usage, and column lineage
    tables. Provides insert/upsert methods for the indexer and query methods
    consumed by the MCP tool layer.

    Thread-safety (read/write separation):
        Write operations are serialised through ``_write_lock`` (a
        ``threading.RLock``).  Read operations use a fresh cursor and
        require no lock -- DuckDB MVCC ensures snapshot isolation.

        The ``write_transaction()`` context manager holds the lock for the
        full ``BEGIN .. COMMIT`` scope so no other thread can interleave
        write statements.  ``asyncio.to_thread()`` callers are safe because
        reads are lock-free and writes acquire the lock internally.
    """

    def __init__(self, db_path: str | Path | None = None):
        """Initialise the database.

        Args:
            db_path: Path to DuckDB file. None for in-memory (testing).
        """
        self.db_path = str(db_path) if db_path else ":memory:"
        self.conn = duckdb.connect(self.db_path)
        self._write_lock = threading.RLock()
        # Thread-local flag: only the thread holding _write_lock inside
        # write_transaction() sets this to True.  _execute_read checks it
        # to decide whether to use the main connection (to see uncommitted
        # writes) or a fresh cursor (snapshot isolation).
        self._tlocal = threading.local()
        self._init_schema()
        self._has_pgq = False
        self._init_pgq()

    def _init_schema(self) -> None:
        """Create tables and indices if they don't exist."""
        with self._write_lock:
            self._execute_write(SCHEMA_SQL)
            self._execute_write(INDEX_SQL)
            self._migrate()

    def _migrate(self) -> None:
        """Run idempotent schema migrations for existing databases."""
        # v1.0.1: add repo_type column (DuckDB ALTER doesn't support NOT NULL)
        self._execute_write(
            "ALTER TABLE repos ADD COLUMN IF NOT EXISTS "
            "repo_type TEXT DEFAULT 'sql'"
        )

    def _init_pgq(self) -> None:
        """Try to load DuckPGQ, installing if needed. Non-fatal if unavailable.

        Tries ``LOAD`` first (fast, no network). Only falls back to
        ``INSTALL FROM community`` (network call) if the load fails.
        Both ``INSTALL`` and ``LOAD`` are individually idempotent in DuckDB.
        """
        try:
            with self._write_lock:
                try:
                    self._execute_write("LOAD duckpgq")
                except Exception:
                    self._execute_write("INSTALL duckpgq FROM community")
                    self._execute_write("LOAD duckpgq")
            self._has_pgq = True
            self._create_property_graph()
        except Exception as e:
            logger.debug("DuckPGQ unavailable: %s", e)
            self._has_pgq = False

    def _create_property_graph(self) -> None:
        """Create or replace the DuckPGQ property graph from nodes/edges.

        Note: DuckPGQ does not support views as vertex tables, so phantom
        nodes (``file_id IS NULL``) are included. Graph query consumers
        should filter phantoms in their result processing if needed.
        """
        if not self._has_pgq:
            return
        try:
            with self._write_lock:
                self._execute_write(
                    "CREATE OR REPLACE PROPERTY GRAPH sqlprism_graph "
                    "VERTEX TABLES (nodes) "
                    "EDGE TABLES (edges SOURCE KEY (source_id) REFERENCES nodes (node_id) "
                    "DESTINATION KEY (target_id) REFERENCES nodes (node_id))"
                )
        except Exception as e:
            logger.warning("Failed to create property graph: %s", e)
            self._has_pgq = False

    def _execute_read(self, sql: str, params=None):
        """Execute a read-only SQL statement.

        When called **outside** a write transaction, creates a fresh cursor
        for snapshot isolation so reads never block writes.

        When called **inside** a write transaction (``_in_transaction`` is
        ``True``), uses the main connection so the read can see uncommitted
        writes from the current transaction.
        """
        if getattr(self._tlocal, "in_transaction", False):
            # Inside a write transaction — must read from the same
            # connection to see uncommitted data.
            if params:
                return self.conn.execute(sql, params)
            return self.conn.execute(sql)
        cursor = self.conn.cursor()
        try:
            if params:
                return cursor.execute(sql, params)
            return cursor.execute(sql)
        except Exception:
            cursor.close()
            raise

    def _execute_write(self, sql: str, params=None):
        """Execute a write SQL statement on the main connection.

        The caller **must** already hold ``_write_lock`` (either directly
        or via ``write_transaction()``).  Uses ``self.conn.execute()``
        directly so that writes participate in the current transaction.
        """
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        with self._write_lock:
            self.conn.close()

    def __enter__(self) -> "GraphDB":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def refresh_property_graph(self) -> None:
        """Refresh the property graph after data changes (e.g., reindex)."""
        self._create_property_graph()

    @property
    def has_pgq(self) -> bool:
        """Whether DuckPGQ is available for graph queries."""
        return self._has_pgq

    @staticmethod
    def clear_snippet_cache() -> None:
        """Clear the cached file contents used for snippet extraction.

        Should be called after reindex to avoid serving stale content.
        """
        _read_file_lines.cache_clear()

    @contextmanager
    def write_transaction(self):
        """Context manager that holds ``_write_lock`` for a full transaction.

        Acquires the write lock, issues ``BEGIN TRANSACTION``, yields, then
        ``COMMIT`` on success or ``ROLLBACK`` on exception.

        Re-entrant: if the current thread already holds the lock and is
        inside a transaction, yields without starting a nested one (DuckDB
        does not support nested transactions).
        """
        if getattr(self._tlocal, "in_transaction", False):
            yield
            return
        with self._write_lock:
            self.conn.execute("BEGIN TRANSACTION")
            self._tlocal.in_transaction = True
            try:
                yield
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            finally:
                self._tlocal.in_transaction = False

    @contextmanager
    def transaction(self):
        """Backward-compatible alias for :meth:`write_transaction`.

        .. deprecated:: 0.6
            Use :meth:`write_transaction` instead.
        """
        warnings.warn(
            "transaction() is deprecated, use write_transaction() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        with self.write_transaction():
            yield

    # ── Repo management ──

    def upsert_repo(self, name: str, path: str, repo_type: str = "sql") -> int:
        """Create or update a repo entry.

        Updates the stored path and repo_type if changed.

        Args:
            name: Unique repo name used as the identifier across the index.
            path: Absolute filesystem path to the repo root.
            repo_type: One of ``'sql'``, ``'dbt'``, ``'sqlmesh'``.

        Returns:
            The ``repo_id`` (existing or newly created).
        """
        with self._write_lock:
            existing = self._execute_write(
                "SELECT repo_id, path, repo_type FROM repos WHERE name = ?", [name],
            ).fetchone()
            if existing:
                if existing[1] != str(path) or existing[2] != repo_type:
                    self._execute_write(
                        "UPDATE repos SET path = ?, repo_type = ? WHERE repo_id = ?",
                        [str(path), repo_type, existing[0]],
                    )
                return existing[0]
            result = self._execute_write(
                "INSERT INTO repos (name, path, repo_type) VALUES (?, ?, ?) RETURNING repo_id",
                [name, str(path), repo_type],
            ).fetchone()
            return result[0]

    def update_repo_metadata(self, repo_id: int, commit: str | None = None, branch: str | None = None) -> None:
        """Update the last indexed commit/branch for a repo."""
        with self._write_lock:
            self._execute_write(
                "UPDATE repos SET last_commit = ?, last_branch = ?, indexed_at = now() WHERE repo_id = ?",
                [commit, branch, repo_id],
            )

    def delete_repo(self, repo_id: int) -> None:
        """Delete a repo and all associated data (manual cascade).

        DuckDB does not support ``ON DELETE CASCADE``, so child rows are
        deleted in dependency order: lineage, column_usage, edges, nodes,
        files, then the repo itself.

        Args:
            repo_id: ID of the repo to delete.
        """
        with self.write_transaction():
            self._delete_repo_impl(repo_id)

    def _delete_repo_impl(self, repo_id: int) -> None:
        """Inner impl -- caller must hold ``_write_lock`` (via write_transaction)."""
        # Delete column_lineage for all files in repo
        self._execute_write(
            "DELETE FROM column_lineage WHERE file_id IN (SELECT file_id FROM files WHERE repo_id = ?)",
            [repo_id],
        )
        # Delete column_usage for all nodes in repo's files
        self._execute_write(
            "DELETE FROM column_usage WHERE file_id IN (SELECT file_id FROM files WHERE repo_id = ?)",
            [repo_id],
        )
        # Delete columns for all nodes in repo's files
        self._execute_write(
            "DELETE FROM columns WHERE node_id IN "
            "(SELECT node_id FROM nodes WHERE file_id IN "
            "(SELECT file_id FROM files WHERE repo_id = ?))",
            [repo_id],
        )
        # Delete edges referencing repo's nodes
        self._execute_write(
            "DELETE FROM edges WHERE source_id IN "
            "(SELECT node_id FROM nodes WHERE file_id IN "
            "(SELECT file_id FROM files WHERE repo_id = ?))",
            [repo_id],
        )
        self._execute_write(
            "DELETE FROM edges WHERE target_id IN "
            "(SELECT node_id FROM nodes WHERE file_id IN "
            "(SELECT file_id FROM files WHERE repo_id = ?))",
            [repo_id],
        )
        # Delete nodes
        self._execute_write(
            "DELETE FROM nodes WHERE file_id IN (SELECT file_id FROM files WHERE repo_id = ?)",
            [repo_id],
        )
        # Delete files
        self._execute_write("DELETE FROM files WHERE repo_id = ?", [repo_id])
        # Delete repo
        self._execute_write("DELETE FROM repos WHERE repo_id = ?", [repo_id])

    def get_all_repos(self) -> list[tuple[int, str, str, str]]:
        """Return all repos as (repo_id, name, path, repo_type) tuples."""
        return self._execute_read(
            "SELECT repo_id, name, path, repo_type FROM repos"
        ).fetchall()

    def get_file_checksum(self, repo_id: int, path: str) -> str | None:
        """Get the stored checksum for a single file in a repo."""
        row = self._execute_read(
            "SELECT checksum FROM files WHERE repo_id = ? AND path = ?",
            [repo_id, path],
        ).fetchone()
        return row[0] if row else None

    def find_node_name_by_file(self, repo_id: int, rel_path: str) -> str | None:
        """Find the primary node name for a file path in a repo.

        Used by sqlmesh reindex to resolve file paths to model names.
        Returns the first table/view node name found, or ``None``.
        """
        row = self._execute_read(
            "SELECT n.name FROM nodes n "
            "JOIN files f ON n.file_id = f.file_id "
            "WHERE f.repo_id = ? AND f.path = ? AND n.kind IN ('table', 'view') "
            "ORDER BY n.kind, n.name LIMIT 1",
            [repo_id, rel_path],
        ).fetchone()
        return row[0] if row else None

    def find_file_paths_by_stem(self, repo_id: int, stem: str) -> list[str]:
        """Find stored file paths whose filename stem matches.

        Used by dbt/sqlmesh on-save reindex to map filesystem paths
        (e.g. ``models/orders.sql``) to stored paths that may differ
        (e.g. ``staging/orders.sql`` for dbt, ``catalog/schema/orders.sql``
        for sqlmesh).

        Args:
            repo_id: Repo to search within.
            stem: Filename stem without extension (e.g. ``"orders"``).

        Returns:
            List of matching stored file paths.
        """
        rows = self._execute_read(
            "SELECT path FROM files WHERE repo_id = ? "
            "AND (path = ? OR path LIKE ?)",
            [repo_id, stem + ".sql", "%/" + stem + ".sql"],
        ).fetchall()
        return [r[0] for r in rows]

    # ── File management ──

    def get_file_checksums(self, repo_id: int) -> dict[str, str]:
        """Get {path: checksum} for all files in a repo."""
        rows = self._execute_read("SELECT path, checksum FROM files WHERE repo_id = ?", [repo_id]).fetchall()
        return {path: checksum for path, checksum in rows}

    def delete_file_data(self, repo_id: int, path: str) -> None:
        """Delete all data for a file (nodes, edges, column_usage, file record).

        Nodes that have inbound edges from OTHER files are converted to phantom
        nodes (file_id=NULL) instead of being deleted, so that cross-file edges
        survive incremental reindex. cleanup_phantoms() will later merge these
        phantoms with the newly-inserted real nodes.

        Wraps in a write_transaction if not already inside one.
        """
        with self.write_transaction():
            self._delete_file_data_impl(repo_id, path)

    def _delete_file_data_impl(self, repo_id: int, path: str) -> None:
        """Inner impl -- caller must hold ``_write_lock`` (via write_transaction)."""
        file_row = self._execute_write(
            "SELECT file_id FROM files WHERE repo_id = ? AND path = ?",
            [repo_id, path],
        ).fetchone()
        if not file_row:
            return
        file_id = file_row[0]

        self._execute_write("DELETE FROM column_lineage WHERE file_id = ?", [file_id])
        self._execute_write("DELETE FROM column_usage WHERE file_id = ?", [file_id])
        self._execute_write(
            "DELETE FROM columns WHERE node_id IN (SELECT node_id FROM nodes WHERE file_id = ?)",
            [file_id],
        )

        # Delete edges where source is in this file's nodes (outbound from this file)
        self._execute_write(
            "DELETE FROM edges WHERE source_id IN (SELECT node_id FROM nodes WHERE file_id = ?)",
            [file_id],
        )

        # Find nodes in this file that have inbound edges from OTHER files' nodes.
        # These must be preserved as phantoms so cross-file edges survive.
        nodes_with_cross_file_edges = self._execute_write(
            "SELECT DISTINCT n.node_id FROM nodes n "
            "JOIN edges e ON e.target_id = n.node_id "
            "JOIN nodes src ON e.source_id = src.node_id "
            "WHERE n.file_id = ? AND (src.file_id IS NULL OR src.file_id != ?)",
            [file_id, file_id],
        ).fetchall()

        if nodes_with_cross_file_edges:
            # Convert these nodes to phantoms (preserve for edge continuity)
            phantom_ids = [row[0] for row in nodes_with_cross_file_edges]
            placeholders = ",".join(["?"] * len(phantom_ids))
            self._execute_write(
                f"UPDATE nodes SET file_id = NULL, line_start = NULL, line_end = NULL "
                f"WHERE node_id IN ({placeholders})",
                phantom_ids,
            )

        # Delete edges where target is in this file's non-phantom nodes
        # (edges from other files now point to phantoms, so they're safe)
        self._execute_write(
            "DELETE FROM edges WHERE target_id IN (SELECT node_id FROM nodes WHERE file_id = ?)",
            [file_id],
        )

        # Delete remaining (non-phantom) nodes for this file
        self._execute_write("DELETE FROM nodes WHERE file_id = ?", [file_id])
        self._execute_write("DELETE FROM files WHERE file_id = ?", [file_id])

    def insert_file(self, repo_id: int, path: str, language: str, checksum: str) -> int:
        """Insert a file record.

        Args:
            repo_id: Owning repo.
            path: Relative file path within the repo.
            language: Language identifier (e.g. ``"sql"``).
            checksum: SHA-256 hex digest of the file content.

        Returns:
            The newly assigned ``file_id``.
        """
        with self._write_lock:
            result = self._execute_write(
                "INSERT INTO files (repo_id, path, language, checksum) VALUES (?, ?, ?, ?) RETURNING file_id",
                [repo_id, path, language, checksum],
            ).fetchone()
            return result[0]

    # ── Node management ──

    def insert_node(
        self,
        file_id: int | None,
        kind: str,
        name: str,
        language: str,
        line_start: int | None = None,
        line_end: int | None = None,
        metadata: dict | None = None,
        schema: str | None = None,
    ) -> int:
        """Insert a single node.

        Args:
            file_id: Owning file, or ``None`` for phantom nodes.
            kind: Node kind (e.g. ``"table"``, ``"view"``, ``"cte"``).
            name: Unqualified entity name.
            language: Language identifier.
            line_start: First source line, if known.
            line_end: Last source line, if known.
            metadata: Arbitrary JSON-serialisable metadata.
            schema: Database schema qualifier (e.g. ``"staging"``).

        Returns:
            The newly assigned ``node_id``.
        """
        with self._write_lock:
            result = self._execute_write(
                "INSERT INTO nodes (file_id, kind, name, language, "
                "line_start, line_end, metadata, schema) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING node_id",
                [
                    file_id,
                    kind,
                    name,
                    language,
                    line_start,
                    line_end,
                    json.dumps(metadata) if metadata else None,
                    schema,
                ],
            ).fetchone()
            return result[0]

    def resolve_node(
        self,
        name: str,
        kind: str,
        repo_id: int | None = None,
        schema: str | None = None,
    ) -> int | None:
        """Find a node by name and kind.

        Matches on short name (e.g. ``"orders"``) which covers both
        unqualified references and qualified ones (stored as short name
        plus schema column). Search order: same repo first, then cross-repo.

        Args:
            name: Unqualified entity name.
            kind: Node kind to match.
            repo_id: Prefer nodes from this repo. Falls back to cross-repo
                search if not found.
            schema: Optional schema qualifier. When provided, only nodes
                with a matching ``schema`` column are returned.

        Returns:
            The ``node_id`` if found, otherwise ``None``.
        """
        schema_clause = ""
        schema_params: list = []
        if schema is not None:
            schema_clause = " AND n.schema = ?"
            schema_params = [schema]

        if repo_id:
            row = self._execute_read(
                "SELECT n.node_id FROM nodes n "
                "JOIN files f ON n.file_id = f.file_id "
                "WHERE n.name = ? AND n.kind = ? AND f.repo_id = ?" + schema_clause + " LIMIT 1",
                [name, kind, repo_id] + schema_params,
            ).fetchone()
            if row:
                return row[0]

        # Cross-repo search (use alias so schema_clause referencing 'n.' works)
        row = self._execute_read(
            "SELECT n.node_id FROM nodes n WHERE n.name = ? AND n.kind = ?" + schema_clause + " LIMIT 1",
            [name, kind] + schema_params,
        ).fetchone()
        return row[0] if row else None

    def get_or_create_phantom(self, name: str, kind: str, language: str) -> int:
        """Get an existing phantom node or create one. Returns node_id."""
        with self._write_lock:
            row = self._execute_write(
                "SELECT node_id FROM nodes WHERE name = ? AND kind = ? AND file_id IS NULL LIMIT 1",
                [name, kind],
            ).fetchone()
            if row:
                return row[0]
            # insert_node acquires _write_lock (RLock is re-entrant)
            return self.insert_node(file_id=None, kind=kind, name=name, language=language)

    def cleanup_phantoms(self) -> int:
        """Repoint edges from phantom nodes to real counterparts, then delete phantoms.

        A phantom node (file_id IS NULL) can be replaced when a real node with
        the same name+kind exists. Edges pointing to/from the phantom are updated
        to reference the real node, then the phantom is deleted.

        Returns the number of phantom nodes cleaned up.
        """
        with self._write_lock:
            # Find phantoms that have a real counterpart
            phantoms = self._execute_write(
                "SELECT p.node_id AS phantom_id, r.node_id AS real_id "
                "FROM nodes p "
                "JOIN nodes r ON p.name = r.name AND p.kind = r.kind "
                "AND COALESCE(p.schema, '') = COALESCE(r.schema, '') "
                "WHERE p.file_id IS NULL AND r.file_id IS NOT NULL"
            ).fetchall()

            if not phantoms:
                # Still check for orphaned phantoms (no edges at all)
                orphaned = self._execute_write(
                    "SELECT node_id FROM nodes "
                    "WHERE file_id IS NULL "
                    "AND node_id NOT IN (SELECT source_id FROM edges) "
                    "AND node_id NOT IN (SELECT target_id FROM edges)"
                ).fetchall()
                # Also find stale phantoms: phantoms whose only inbound edges
                # come from other phantoms (no real node references them).
                stale = self._execute_write(
                    "SELECT p.node_id FROM nodes p "
                    "WHERE p.file_id IS NULL "
                    "AND p.node_id IN (SELECT target_id FROM edges) "
                    "AND NOT EXISTS ("
                    "  SELECT 1 FROM edges e "
                    "  JOIN nodes src ON e.source_id = src.node_id "
                    "  WHERE e.target_id = p.node_id AND src.file_id IS NOT NULL"
                    ")"
                ).fetchall()
                to_delete = {row[0] for row in orphaned} | {row[0] for row in stale}
                if to_delete:
                    delete_ids = list(to_delete)
                    placeholders = ",".join(["?"] * len(delete_ids))
                    # Remove edges referencing stale phantoms before deleting nodes
                    self._execute_write(
                        f"DELETE FROM edges WHERE source_id IN ({placeholders}) OR target_id IN ({placeholders})",
                        delete_ids + delete_ids,
                    )
                    self._execute_write(
                        f"DELETE FROM nodes WHERE node_id IN ({placeholders})",
                        delete_ids,
                    )
                    return len(to_delete)
                return 0

            # Batch repoint edges: single UPDATE per direction using a mapping table
            # instead of O(phantoms) individual UPDATEs.
            mapping_values = ", ".join([f"({phantom_id}, {real_id})" for phantom_id, real_id in phantoms])
            self._execute_write(
                f"UPDATE edges SET source_id = m.real_id "
                f"FROM (VALUES {mapping_values}) AS m(phantom_id, real_id) "
                f"WHERE edges.source_id = m.phantom_id"
            )
            self._execute_write(
                f"UPDATE edges SET target_id = m.real_id "
                f"FROM (VALUES {mapping_values}) AS m(phantom_id, real_id) "
                f"WHERE edges.target_id = m.phantom_id"
            )

            # Delete all phantoms that had real counterparts
            phantom_ids = [p[0] for p in phantoms]
            placeholders = ",".join(["?"] * len(phantom_ids))
            self._execute_write(
                f"DELETE FROM nodes WHERE node_id IN ({placeholders})",
                phantom_ids,
            )

            # Clean up orphaned phantoms: phantom nodes with no edges at all
            orphaned = self._execute_write(
                "SELECT node_id FROM nodes "
                "WHERE file_id IS NULL "
                "AND node_id NOT IN (SELECT source_id FROM edges) "
                "AND node_id NOT IN (SELECT target_id FROM edges)"
            ).fetchall()

            if orphaned:
                orphan_ids = [row[0] for row in orphaned]
                placeholders = ",".join(["?"] * len(orphan_ids))
                self._execute_write(
                    f"DELETE FROM nodes WHERE node_id IN ({placeholders})",
                    orphan_ids,
                )

            return len(phantoms) + len(orphaned)

    # ── Edge management ──

    def insert_edge(
        self,
        source_id: int,
        target_id: int,
        relationship: str,
        context: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        """Insert an edge. Returns edge_id."""
        with self._write_lock:
            result = self._execute_write(
                "INSERT INTO edges (source_id, target_id, relationship, context, metadata) "
                "VALUES (?, ?, ?, ?, ?) RETURNING edge_id",
                [
                    source_id,
                    target_id,
                    relationship,
                    context,
                    json.dumps(metadata) if metadata else None,
                ],
            ).fetchone()
            return result[0]

    # ── Batch inserts ──

    def insert_nodes_batch(
        self,
        rows: list[tuple],
    ) -> list[int]:
        """Batch insert nodes.

        Args:
            rows: List of tuples, each containing
                ``(file_id, kind, name, language, line_start, line_end, metadata_json, schema)``.

        Returns:
            ``node_id`` values in insertion order.
        """
        if not rows:
            return []
        chunk_size = 200
        all_ids = []
        with self._write_lock:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(chunk))
                flat = [v for row in chunk for v in row]
                result = self.conn.execute(
                    "INSERT INTO nodes (file_id, kind, name, language, "
                    "line_start, line_end, metadata, schema) "
                    f"VALUES {placeholders} RETURNING node_id",
                    flat,
                ).fetchall()
                all_ids.extend(r[0] for r in result)
        return all_ids

    def insert_edges_batch(self, rows: list[tuple]) -> None:
        """Batch insert edges.

        Args:
            rows: List of tuples, each containing
                ``(source_id, target_id, relationship, context, metadata_json)``.
        """
        if not rows:
            return
        with self._write_lock:
            self.conn.executemany(
                "INSERT INTO edges (source_id, target_id, relationship, context, metadata) VALUES (?, ?, ?, ?, ?)",
                rows,
            )

    def insert_column_usage_batch(self, rows: list[tuple]) -> None:
        """Batch insert column usage records.

        Args:
            rows: List of tuples, each containing
                ``(node_id, table_name, column_name, usage_type, file_id, alias, transform)``.
        """
        if not rows:
            return
        with self._write_lock:
            self.conn.executemany(
                "INSERT INTO column_usage (node_id, table_name, column_name, "
                "usage_type, file_id, alias, transform) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

    def insert_column_lineage_batch(self, rows: list[tuple]) -> None:
        """Batch insert column lineage hops.

        Args:
            rows: List of tuples, each containing
                ``(file_id, output_node, output_column, chain_index,
                hop_index, hop_column, hop_table, hop_expression)``.
        """
        if not rows:
            return
        with self._write_lock:
            self.conn.executemany(
                "INSERT INTO column_lineage "
                "(file_id, output_node, output_column, chain_index, "
                "hop_index, hop_column, hop_table, hop_expression) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

    # ── Columns (schema-level) ──

    def insert_columns_batch(self, rows: list[tuple]) -> int:
        """Batch insert/upsert column definitions.

        Args:
            rows: List of tuples, each containing
                ``(node_id, column_name, data_type, position, source,
                description)``.

        Returns:
            Number of rows processed (inserts + upserts).
        """
        if not rows:
            return 0
        with self._write_lock:
            self.conn.executemany(
                "INSERT INTO columns "
                "(node_id, column_name, data_type, position, source, description) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (node_id, column_name) DO UPDATE SET "
                "data_type = COALESCE(EXCLUDED.data_type, columns.data_type), "
                "position = COALESCE(EXCLUDED.position, columns.position), "
                "source = EXCLUDED.source, "
                "description = COALESCE(EXCLUDED.description, columns.description)",
                rows,
            )
        return len(rows)

    # ── Column usage ──

    def insert_column_usage(
        self,
        node_id: int,
        table_name: str,
        column_name: str,
        usage_type: str,
        file_id: int,
        alias: str | None = None,
        transform: str | None = None,
    ) -> None:
        """Insert a column usage record."""
        with self._write_lock:
            self._execute_write(
                "INSERT INTO column_usage (node_id, table_name, column_name, "
                "usage_type, file_id, alias, transform) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [node_id, table_name, column_name, usage_type, file_id, alias, transform],
            )

    # ── Column lineage ──

    def insert_column_lineage(
        self,
        file_id: int,
        output_node: str,
        output_column: str,
        hop_index: int,
        hop_column: str,
        hop_table: str,
        hop_expression: str | None = None,
        chain_index: int = 0,
    ) -> None:
        """Insert a single hop in a column lineage chain."""
        with self._write_lock:
            self._execute_write(
                "INSERT INTO column_lineage "
                "(file_id, output_node, output_column, chain_index, "
                "hop_index, hop_column, hop_table, hop_expression) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    file_id,
                    output_node,
                    output_column,
                    chain_index,
                    hop_index,
                    hop_column,
                    hop_table,
                    hop_expression,
                ],
            )

    def query_column_lineage(
        self,
        table: str | None = None,
        column: str | None = None,
        output_node: str | None = None,
        repo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """Query column lineage chains.

        Can search by:

        - ``output_node`` + ``column``: "where does this output column come from?"
        - ``table`` + ``column`` at any hop: "where does this source column flow to?"

        ``limit`` applies to chain count (distinct
        ``output_node``/``output_column``/``chain_index`` combinations),
        not raw hop rows.

        Args:
            table: Filter by hop table name.
            column: Filter by column name (output or any hop, depending
                on whether ``output_node`` is also set).
            output_node: Filter by the node that produces the output column.
            repo: Filter by repo name.
            limit: Maximum number of lineage chains to return.
            offset: Pagination offset (in chains).

        Returns:
            Dict with keys ``"chains"`` (list of chain dicts, each with
            ``output_node``, ``output_column``, ``chain_index``, ``hops``,
            ``file``, and ``repo``) and ``"total_count"`` (int).
        """
        # Build WHERE clauses for both outer (cl) and inner (cl2) aliases
        outer_where: list[str] = []
        inner_where: list[str] = []
        params: list = []

        if output_node:
            outer_where.append("cl.output_node = ?")
            inner_where.append("cl2.output_node = ?")
            params.append(output_node)
        if column:
            if output_node:
                outer_where.append("cl.output_column = ?")
                inner_where.append("cl2.output_column = ?")
                params.append(column)
            else:
                outer_where.append("(cl.output_column = ? OR cl.hop_column = ?)")
                inner_where.append("(cl2.output_column = ? OR cl2.hop_column = ?)")
                params.extend([column, column])
        if table:
            outer_where.append("cl.hop_table = ?")
            inner_where.append("cl2.hop_table = ?")
            params.append(table)
        if repo:
            outer_where.append("r.name = ?")
            inner_where.append("r2.name = ?")
            params.append(repo)

        if not outer_where:
            return {"chains": [], "total_count": 0}

        # True total count of matching chains (before pagination)
        count_sql = (
            "SELECT COUNT(*) FROM ("
            "  SELECT DISTINCT cl2.output_node, cl2.output_column, cl2.chain_index "
            "  FROM column_lineage cl2 "
            "  JOIN files f2 ON cl2.file_id = f2.file_id "
            "  JOIN repos r2 ON f2.repo_id = r2.repo_id "
            f"  WHERE {' AND '.join(inner_where)} "
            ")"
        )
        total_count = self._execute_read(count_sql, params).fetchone()[0]

        # Subquery selects distinct chains with LIMIT, then outer query
        # fetches all hops for those chains. This ensures LIMIT counts chains,
        # not individual hop rows.
        sql = (
            "SELECT cl.output_node, cl.output_column, cl.chain_index, cl.hop_index, "
            "cl.hop_column, cl.hop_table, cl.hop_expression, "
            "f.path, r.name as repo_name "
            "FROM column_lineage cl "
            "JOIN files f ON cl.file_id = f.file_id "
            "JOIN repos r ON f.repo_id = r.repo_id "
            "WHERE (cl.output_node, cl.output_column, cl.chain_index) IN ("
            "  SELECT DISTINCT cl2.output_node, cl2.output_column, cl2.chain_index "
            "  FROM column_lineage cl2 "
            "  JOIN files f2 ON cl2.file_id = f2.file_id "
            "  JOIN repos r2 ON f2.repo_id = r2.repo_id "
            f"  WHERE {' AND '.join(inner_where)} "
            "  ORDER BY cl2.output_node, cl2.output_column, cl2.chain_index "
            "  LIMIT ? OFFSET ?"
            ") "
            "ORDER BY cl.output_node, cl.output_column, cl.chain_index, cl.hop_index"
        )

        # params duplicated: once for inner subquery, once not needed for outer
        # (outer filters via the IN subquery)
        rows = self._execute_read(sql, params + [limit, offset]).fetchall()

        # Group by (output_node, output_column, chain_index) into chains
        chains: dict[tuple[str, str, int], dict] = {}
        for r in rows:
            key = (r[0], r[1], r[2])
            if key not in chains:
                chains[key] = {
                    "output_node": r[0],
                    "output_column": r[1],
                    "chain_index": r[2],
                    "hops": [],
                    "file": r[7],
                    "repo": r[8],
                }
            chains[key]["hops"].append(
                {
                    "index": r[3],
                    "column": r[4],
                    "table": r[5],
                    "expression": r[6],
                }
            )

        return {"chains": list(chains.values()), "total_count": total_count}

    # ── Schema catalog ──

    def get_table_columns(self, repo_id: int | None = None) -> dict[str, dict[str, str]]:
        """Build a schema catalog from indexed columns and column usage.

        Prefers the authoritative ``columns`` table (joined with ``nodes``)
        which carries real ``data_type`` information.  Falls back to
        ``column_usage`` for any additional columns not present in the
        ``columns`` table, assigning them the default type ``"TEXT"``.

        When the ``columns`` table is empty the behaviour is identical to
        the previous column-usage-only implementation.

        Suitable for passing to ``sqlglot.optimizer.qualify_columns`` or
        ``sqlglot.lineage``.

        Args:
            repo_id: Restrict to columns from this repo. ``None`` returns
                columns across all repos.

        Returns:
            ``{table_name: {column_name: data_type, ...}}`` mapping.
        """
        schema: dict[str, dict[str, str]] = {}

        # 1. Authoritative columns from columns table (with real types).
        # Note: phantom nodes (file_id IS NULL) are intentionally excluded
        # since they lack a verified repo association.
        if repo_id is not None:
            col_rows = self._execute_read(
                "SELECT n.name, c.column_name, c.data_type "
                "FROM columns c "
                "JOIN nodes n ON c.node_id = n.node_id "
                "JOIN files f ON n.file_id = f.file_id "
                "WHERE f.repo_id = ?",
                [repo_id],
            ).fetchall()
        else:
            col_rows = self._execute_read(
                "SELECT n.name, c.column_name, c.data_type "
                "FROM columns c "
                "JOIN nodes n ON c.node_id = n.node_id"
            ).fetchall()

        for table, col, dtype in col_rows:
            if table not in schema:
                schema[table] = {}
            schema[table][col] = dtype or "TEXT"

        # 2. Fallback: fill gaps from column_usage
        if repo_id is not None:
            usage_rows = self._execute_read(
                "SELECT DISTINCT cu.table_name, cu.column_name "
                "FROM column_usage cu "
                "JOIN files f ON cu.file_id = f.file_id "
                "WHERE f.repo_id = ? AND cu.column_name != '*'",
                [repo_id],
            ).fetchall()
        else:
            usage_rows = self._execute_read(
                "SELECT DISTINCT table_name, column_name FROM column_usage WHERE column_name != '*'"
            ).fetchall()

        for table, col in usage_rows:
            if table not in schema:
                schema[table] = {}
            if col not in schema[table]:  # Don't overwrite columns table entries
                schema[table][col] = "TEXT"

        return schema

    def query_schema(
        self,
        name: str,
        repo: str | None = None,
    ) -> dict:
        """Return the full schema for a named table or model.

        Includes column definitions with types, descriptions, and the
        node's upstream/downstream dependencies.

        Args:
            name: Entity name to look up.
            repo: Optional repo name filter for disambiguation.

        Returns:
            Dict with ``name``, ``kind``, ``file``, ``repo``, ``columns``,
            ``upstream``, and ``downstream`` keys.  Returns an ``error``
            key when the entity is not found.
        """
        # 1. Find the node — ORDER BY prefers real nodes over phantoms
        if repo:
            node_rows = self._execute_read(
                "SELECT n.node_id, n.kind, f.path, r.name "
                "FROM nodes n "
                "LEFT JOIN files f ON n.file_id = f.file_id "
                "LEFT JOIN repos r ON f.repo_id = r.repo_id "
                "WHERE n.name = ? AND r.name = ? "
                "ORDER BY (n.file_id IS NULL), n.node_id DESC",
                [name, repo],
            ).fetchall()
        else:
            node_rows = self._execute_read(
                "SELECT n.node_id, n.kind, f.path, r.name "
                "FROM nodes n "
                "LEFT JOIN files f ON n.file_id = f.file_id "
                "LEFT JOIN repos r ON f.repo_id = r.repo_id "
                "WHERE n.name = ? AND n.file_id IS NOT NULL "
                "ORDER BY n.node_id DESC",
                [name],
            ).fetchall()

        if not node_rows:
            return {"error": f"Model '{name}' not found in the index."}

        # Use the first match (real node preferred) for all queries
        first = node_rows[0]
        node_id = first[0]
        node_kind = first[1]
        file_path = first[2]
        repo_name = first[3]

        # 2. Get columns
        col_rows = self._execute_read(
            "SELECT column_name, data_type, position, source, description "
            "FROM columns WHERE node_id = ? ORDER BY position",
            [node_id],
        ).fetchall()

        columns = [
            {
                "name": r[0],
                "type": r[1] or "UNKNOWN",
                "position": r[2],
                "source": r[3],
                "description": r[4],
            }
            for r in col_rows
        ]

        # 3. Get upstream (outbound edges — what this node references)
        upstream_rows = self._execute_read(
            "SELECT DISTINCT n2.name, n2.kind "
            "FROM edges e "
            "JOIN nodes n2 ON e.target_id = n2.node_id "
            "WHERE e.source_id = ?",
            [node_id],
        ).fetchall()

        upstream = [{"name": r[0], "kind": r[1]} for r in upstream_rows]

        # 4. Get downstream (inbound edges — what depends on this node)
        downstream_rows = self._execute_read(
            "SELECT DISTINCT n2.name, n2.kind "
            "FROM edges e "
            "JOIN nodes n2 ON e.source_id = n2.node_id "
            "WHERE e.target_id = ?",
            [node_id],
        ).fetchall()

        downstream = [{"name": r[0], "kind": r[1]} for r in downstream_rows]

        result = {
            "name": name,
            "kind": node_kind,
            "file": file_path,
            "repo": repo_name,
            "columns": columns,
            "upstream": upstream,
            "downstream": downstream,
        }
        if len(node_rows) > 1:
            result["matches"] = len(node_rows)
        return result

    def query_check_impact(
        self,
        model: str,
        changes: list[dict],
        repo: str | None = None,
    ) -> dict:
        """Analyze downstream impact of column changes on a model.

        For each proposed change (column removal, rename, or addition),
        queries the ``column_usage`` table to classify downstream models
        as **breaking**, **warning**, or **safe**.

        Note: ``add_column`` does not account for ``SELECT *`` usage —
        downstream models using wildcard selects may still be affected.

        The ``repo`` filter restricts both model lookup and downstream
        consumer discovery to the same repo.

        Args:
            model: The model/table name whose columns are changing.
            changes: List of change dicts.  Supported actions:
                - ``{"action": "remove_column", "column": "col"}``
                - ``{"action": "rename_column", "old": "old", "new": "new"}``
                - ``{"action": "add_column", "column": "col"}``
            repo: Optional repo name filter.

        Returns:
            Dict with ``model``, ``model_found``, ``changes_analyzed``,
            ``impacts`` (one entry per change with ``breaking``,
            ``warnings``, and ``safe`` lists), and a ``summary`` with
            totals.
        """
        breaking_types = {"select", "join_on", "insert", "update"}
        warning_types = {
            "where", "group_by", "order_by", "having",
            "partition_by", "window_order", "qualify",
        }

        # Pre-fetch source node IDs — exclude phantoms (file_id IS NULL)
        if repo:
            node_rows = self._execute_read(
                "SELECT n.node_id FROM nodes n "
                "JOIN files f ON n.file_id = f.file_id "
                "JOIN repos r ON f.repo_id = r.repo_id "
                "WHERE n.name = ? AND r.name = ?",
                [model, repo],
            ).fetchall()
        else:
            node_rows = self._execute_read(
                "SELECT n.node_id FROM nodes n "
                "WHERE n.name = ? AND n.file_id IS NOT NULL",
                [model],
            ).fetchall()

        if not node_rows:
            return {
                "model": model,
                "model_found": False,
                "changes_analyzed": len(changes),
                "impacts": [],
                "summary": {"total_breaking": 0, "total_warnings": 0, "total_safe": 0},
            }

        node_ids = [r[0] for r in node_rows]
        placeholders = ", ".join("?" for _ in node_ids)

        # Fetch downstream models via edges, excluding the model itself
        ds_rows = self._execute_read(
            "SELECT DISTINCT n2.name, n2.kind "
            "FROM edges e "
            "JOIN nodes n2 ON e.source_id = n2.node_id "
            f"WHERE e.target_id IN ({placeholders}) "
            f"AND e.source_id NOT IN ({placeholders})",
            node_ids + node_ids,
        ).fetchall()
        all_downstream = [{"name": r[0], "kind": r[1]} for r in ds_rows]

        impacts: list[dict] = []
        total_breaking = 0
        total_warnings = 0
        total_safe = 0

        for change in changes:
            action = change.get("action", "")

            # Determine the column name to check
            if action == "add_column":
                # Always safe — no downstream references exist yet
                # (does not account for SELECT * usage)
                impacts.append({
                    "change": change,
                    "breaking": [],
                    "warnings": [],
                    "safe": [{"model": d["name"], "kind": d["kind"]} for d in all_downstream],
                })
                total_safe += len(all_downstream)
                continue
            elif action == "rename_column":
                col_name = change.get("old", "")
            elif action == "remove_column":
                col_name = change.get("column", "")
            else:
                # Unknown action — record as skipped so len(impacts) == changes_analyzed
                impacts.append({
                    "change": change,
                    "skipped": True,
                    "reason": f"unknown action '{action}'",
                    "breaking": [],
                    "warnings": [],
                    "safe": [],
                })
                continue

            # Query column_usage for downstream references to this column
            where = ["cu.table_name = ?", "cu.column_name = ?"]
            params: list = [model, col_name]
            if repo:
                where.append("r.name = ?")
                params.append(repo)

            usage_sql = (
                "SELECT DISTINCT n.name AS node_name, n.kind AS node_kind, cu.usage_type "
                "FROM column_usage cu "
                "JOIN nodes n ON cu.node_id = n.node_id "
                "LEFT JOIN files f ON cu.file_id = f.file_id "
                "LEFT JOIN repos r ON f.repo_id = r.repo_id "
                f"WHERE {' AND '.join(where)}"
            )
            usage_rows = self._execute_read(usage_sql, params).fetchall()

            # Group usage_types per downstream model
            model_usage: dict[tuple[str, str], list[str]] = {}
            for r in usage_rows:
                key = (r[0], r[1])  # (node_name, node_kind)
                model_usage.setdefault(key, []).append(r[2])

            breaking: list[dict] = []
            warnings: list[dict] = []
            affected_names: set[str] = set()

            for (name, kind), usage_types in model_usage.items():
                affected_names.add(name)
                types_set = set(usage_types)
                if types_set & breaking_types:
                    breaking.append({
                        "model": name,
                        "kind": kind,
                        "usage_types": sorted(types_set),
                    })
                elif types_set & warning_types:
                    warnings.append({
                        "model": name,
                        "kind": kind,
                        "usage_types": sorted(types_set),
                    })

            # Safe = downstream models not in breaking or warning
            safe = [
                {"model": d["name"], "kind": d["kind"]}
                for d in all_downstream
                if d["name"] not in affected_names
            ]

            impacts.append({
                "change": change,
                "breaking": breaking,
                "warnings": warnings,
                "safe": safe,
            })
            total_breaking += len(breaking)
            total_warnings += len(warnings)
            total_safe += len(safe)

        return {
            "model": model,
            "model_found": True,
            "changes_analyzed": len(changes),
            "impacts": impacts,
            "summary": {
                "total_breaking": total_breaking,
                "total_warnings": total_warnings,
                "total_safe": total_safe,
            },
        }

    def query_find_path(
        self,
        from_model: str,
        to_model: str,
        max_hops: int = 10,
    ) -> dict:
        """Find the shortest path between two models using DuckPGQ.

        Uses ``ANY SHORTEST`` for path-length discovery, then a BFS CTE
        to recover intermediate node names.

        Args:
            from_model: Source model name.
            to_model:   Target model name.
            max_hops:   Maximum edge traversals (clamped to 1..10).

        Returns:
            Dict with ``path_found``, ``path`` (list of node names),
            and ``length``; or an ``error`` key when DuckPGQ is missing.
        """
        if not self.has_pgq:
            return {
                "error": (
                    "DuckPGQ not installed. "
                    "Install with: INSTALL duckpgq FROM community"
                ),
            }

        max_hops = max(min(max_hops, 10), 1)

        # Resolve model names to node_ids via parameterized query
        # (avoids string interpolation into GRAPH_TABLE SQL)
        from_row = self._execute_read(
            "SELECT node_id FROM nodes WHERE name = ? LIMIT 1",
            [from_model],
        ).fetchone()
        to_row = self._execute_read(
            "SELECT node_id FROM nodes WHERE name = ? LIMIT 1",
            [to_model],
        ).fetchone()
        if not from_row or not to_row:
            return {
                "from": from_model,
                "to": to_model,
                "path_found": False,
                "path": [],
                "length": 0,
            }

        from_id, to_id = from_row[0], to_row[0]

        # Step 1: Find shortest path length via PGQ ANY SHORTEST
        # DuckPGQ does not support bind parameters inside GRAPH_TABLE,
        # so we interpolate integer node_ids (safe, no escaping needed).
        try:
            rows = self._execute_read(
                f"FROM GRAPH_TABLE (sqlprism_graph "
                f"MATCH p = ANY SHORTEST "
                f"(src:nodes WHERE src.node_id = {from_id})"
                f"-[e:edges]->{{1,{max_hops}}}"
                f"(dst:nodes WHERE dst.node_id = {to_id}) "
                f"COLUMNS (path_length(p) AS hops))",
            ).fetchall()
        except duckdb.Error as e:
            logger.warning("DuckPGQ find_path failed: %s", e)
            return {"error": f"Graph query failed: {e}"}

        if not rows:
            return {
                "from": from_model,
                "to": to_model,
                "path_found": False,
                "path": [],
                "length": 0,
            }

        path_length = int(rows[0][0])

        # Step 2: Recover intermediate nodes via BFS CTE
        # No file_id filter — BFS must traverse the same topology as PGQ
        path_cte = f"""
        WITH RECURSIVE path_bfs AS (
            SELECT n.node_id, n.name, 0 as depth,
                   ARRAY[n.name] as path_names
            FROM nodes n
            WHERE n.node_id = ?
            UNION ALL
            SELECT n2.node_id, n2.name, pb.depth + 1,
                   array_append(pb.path_names, n2.name)
            FROM edges e
            JOIN nodes n2 ON e.target_id = n2.node_id
            JOIN path_bfs pb ON e.source_id = pb.node_id
            WHERE pb.depth < {path_length}
            AND NOT array_contains(pb.path_names, n2.name)
        )
        SELECT path_names FROM path_bfs
        WHERE node_id = ? AND depth = {path_length}
        LIMIT 1
        """
        path_rows = self._execute_read(
            path_cte, [from_id, to_id]
        ).fetchall()

        if path_rows:
            path_names = list(path_rows[0][0])
        else:
            # BFS could not reconstruct the path (e.g., topology mismatch)
            path_names = []

        return {
            "from": from_model,
            "to": to_model,
            "path_found": True,
            "path": path_names,
            "length": path_length,
        }

    def query_context(self, name: str, repo: str | None = None) -> dict:
        """Return comprehensive context for a model.

        Composes schema info, column usage summary, a code snippet,
        and optional graph metrics into a single response.

        Args:
            name: Entity name to look up.
            repo: Optional repo name filter for disambiguation.

        Returns:
            Dict with ``model``, ``columns``, ``upstream``, ``downstream``,
            ``column_usage_summary``, ``snippet``, and optionally
            ``graph_metrics`` keys.
        """
        # 1. Schema lookup
        schema_result = self.query_schema(name, repo)
        if "error" in schema_result:
            return schema_result

        # 2. Column usage summary
        # Note: repo filter scopes to consumers *within* that repo.
        # Cross-repo usage (consumer in repo X referencing model in repo Y)
        # is excluded when a repo filter is applied.
        if repo:
            usage_sql = (
                "SELECT cu.column_name, cu.usage_type, COUNT(*) as cnt "
                "FROM column_usage cu "
                "JOIN files f ON cu.file_id = f.file_id "
                "JOIN repos r ON f.repo_id = r.repo_id "
                "WHERE cu.table_name = ? AND r.name = ? "
                "GROUP BY cu.column_name, cu.usage_type "
                "ORDER BY cnt DESC"
            )
            usage_rows = self._execute_read(usage_sql, [name, repo]).fetchall()
        else:
            usage_sql = (
                "SELECT cu.column_name, cu.usage_type, COUNT(*) as cnt "
                "FROM column_usage cu "
                "WHERE cu.table_name = ? "
                "GROUP BY cu.column_name, cu.usage_type "
                "ORDER BY cnt DESC"
            )
            usage_rows = self._execute_read(usage_sql, [name]).fetchall()

        # Aggregate total usage per column for top-10
        col_totals: dict[str, int] = {}
        join_keys: list[str] = []
        aggregations: list[str] = []
        seen_join: set[str] = set()
        seen_agg: set[str] = set()

        for col_name, usage_type, cnt in usage_rows:
            col_totals[col_name] = col_totals.get(col_name, 0) + cnt
            if usage_type == "join_on" and col_name not in seen_join:
                join_keys.append(col_name)
                seen_join.add(col_name)
            if usage_type in ("group_by", "partition_by") and col_name not in seen_agg:
                aggregations.append(col_name)
                seen_agg.add(col_name)

        most_used = sorted(col_totals, key=lambda c: col_totals[c], reverse=True)[:10]

        # 3. Code snippet (first 30 lines)
        snippet = self._read_snippet(
            schema_result["repo"],
            schema_result["file"],
            1,
            None,
            context_lines=0,
            max_lines=30,
        )

        # 4. Optional graph metrics (edge-based downstream_count, not usage-based)
        graph_metrics = None
        if self.has_pgq:
            try:
                repo_name = schema_result["repo"]
                if repo_name:
                    pr_rows = self._execute_read(
                        "SELECT pr.pagerank FROM pagerank(sqlprism_graph, nodes, edges) pr "
                        "JOIN nodes n ON n.node_id = pr.node_id "
                        "JOIN files f ON n.file_id = f.file_id "
                        "JOIN repos r ON f.repo_id = r.repo_id "
                        "WHERE n.name = ? AND r.name = ?",
                        [name, repo_name],
                    ).fetchall()
                else:
                    pr_rows = self._execute_read(
                        "SELECT pr.pagerank FROM pagerank(sqlprism_graph, nodes, edges) pr "
                        "JOIN nodes n ON n.node_id = pr.node_id WHERE n.name = ?",
                        [name],
                    ).fetchall()
                raw_score = pr_rows[0][0] if pr_rows else None
                graph_metrics = {
                    "importance": round(raw_score, 6) if raw_score is not None else None,
                    "downstream_count": len(schema_result.get("downstream", [])),
                }
            except (duckdb.Error, RuntimeError) as e:
                logger.debug("PageRank query failed: %s", e)
                graph_metrics = None

        # 5. Compose result
        result: dict = {
            "model": {
                "name": name,
                "kind": schema_result["kind"],
                "file": schema_result["file"],
                "repo": schema_result["repo"],
            },
            "columns": schema_result["columns"],
            "upstream": schema_result["upstream"],
            "downstream": schema_result["downstream"],
            "column_usage_summary": {
                "most_used_columns": most_used,
                "downstream_join_keys": join_keys,
                "downstream_aggregations": aggregations,
            },
            "snippet": snippet,
        }
        if graph_metrics is not None:
            result["graph_metrics"] = graph_metrics
        return result

    # ── Snippet helper ──

    def _read_snippet(
        self,
        repo_name: str | None,
        file_path: str | None,
        line_start: int | None,
        line_end: int | None,
        context_lines: int = 2,
        max_lines: int = 20,
    ) -> str | None:
        """Read a code snippet from the source file.

        Args:
            repo_name: Repo name to look up the base path
            file_path: Relative file path within the repo
            line_start: First line of the entity
            line_end: Last line of the entity
            context_lines: Extra lines before/after to include
            max_lines: Cap total snippet length
        """
        if file_path is None or line_start is None:
            return None

        # Get repo base path
        if repo_name:
            row = self._execute_read("SELECT path FROM repos WHERE name = ?", [repo_name]).fetchone()
            if not row:
                return None
            base = Path(row[0])
        else:
            return None

        full_path = base / file_path
        if not full_path.exists():
            return None

        lines = _read_file_lines(str(full_path))
        if lines is None:
            return None

        start = max(0, line_start - 1 - context_lines)
        end_line = line_end or line_start
        end = min(len(lines), end_line + context_lines)

        # Cap to max_lines
        if end - start > max_lines:
            end = start + max_lines

        snippet_lines = lines[start:end]
        # Add line numbers
        numbered = [f"{start + i + 1:4d} | {line}" for i, line in enumerate(snippet_lines)]
        return "\n".join(numbered)

    # ── Query methods (used by MCP tools) ──

    def query_references(
        self,
        name: str,
        kind: str | None = None,
        schema: str | None = None,
        repo: str | None = None,
        direction: str = "both",
        include_snippets: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """Find all references to/from a named entity.

        Args:
            name: Entity name to look up.
            kind: Optional node kind filter.
            schema: Optional database schema filter.
            repo: Optional repo name filter.
            direction: ``"both"``, ``"inbound"``, or ``"outbound"``.
            include_snippets: Attach source code snippets when ``True``.
            limit: Maximum edges per direction.
            offset: Pagination offset.

        Returns:
            Dict with keys ``"entity"`` (list of matched node dicts or
            ``None``), ``"inbound"`` (list of referencing entities), and
            ``"outbound"`` (list of referenced entities). Each entry
            contains ``name``, ``kind``, ``relationship``, ``context``,
            ``file``, ``repo``, ``line``, and optionally ``snippet``.
        """
        # Find the target node(s)
        where_clauses = ["n.name = ?"]
        params: list = [name]
        if kind:
            where_clauses.append("n.kind = ?")
            params.append(kind)
        if schema:
            where_clauses.append("n.schema = ?")
            params.append(schema)

        where_str = " AND ".join(where_clauses)
        node_query = f"SELECT n.node_id, n.kind, n.name FROM nodes n WHERE {where_str}"
        target_nodes = self._execute_read(node_query, params).fetchall()

        if not target_nodes:
            return {"entity": None, "inbound": [], "outbound": []}

        node_ids = [row[0] for row in target_nodes]
        placeholders = ",".join(["?"] * len(node_ids))

        result = {
            "entity": [{"node_id": r[0], "kind": r[1], "name": r[2]} for r in target_nodes],
            "inbound": [],
            "outbound": [],
        }

        if direction in ("both", "inbound"):
            inbound_sql = (
                f"SELECT n2.name, n2.kind, e.relationship, e.context, "
                f"f2.path, r2.name as repo_name, n2.line_start, n2.line_end "
                f"FROM edges e "
                f"JOIN nodes n2 ON e.source_id = n2.node_id "
                f"LEFT JOIN files f2 ON n2.file_id = f2.file_id "
                f"LEFT JOIN repos r2 ON f2.repo_id = r2.repo_id "
                f"WHERE e.target_id IN ({placeholders}) "
                f"LIMIT ? OFFSET ?"
            )
            for r in self._execute_read(inbound_sql, node_ids + [limit, offset]).fetchall():
                entry = {
                    "name": r[0],
                    "kind": r[1],
                    "relationship": r[2],
                    "context": r[3],
                    "file": r[4],
                    "repo": r[5],
                    "line": r[6],
                }
                if include_snippets:
                    snippet = self._read_snippet(r[5], r[4], r[6], r[7])
                    if snippet:
                        entry["snippet"] = snippet
                result["inbound"].append(entry)

        if direction in ("both", "outbound"):
            outbound_sql = (
                f"SELECT n2.name, n2.kind, e.relationship, e.context, "
                f"f2.path, r2.name as repo_name, n2.line_start, n2.line_end "
                f"FROM edges e "
                f"JOIN nodes n2 ON e.target_id = n2.node_id "
                f"LEFT JOIN files f2 ON n2.file_id = f2.file_id "
                f"LEFT JOIN repos r2 ON f2.repo_id = r2.repo_id "
                f"WHERE e.source_id IN ({placeholders}) "
                f"LIMIT ? OFFSET ?"
            )
            for r in self._execute_read(outbound_sql, node_ids + [limit, offset]).fetchall():
                entry = {
                    "name": r[0],
                    "kind": r[1],
                    "relationship": r[2],
                    "context": r[3],
                    "file": r[4],
                    "repo": r[5],
                    "line": r[6],
                }
                if include_snippets:
                    snippet = self._read_snippet(r[5], r[4], r[6], r[7])
                    if snippet:
                        entry["snippet"] = snippet
                result["outbound"].append(entry)

        return result

    def query_column_usage(
        self,
        table: str,
        column: str | None = None,
        usage_type: str | None = None,
        repo: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """Find column usage records for a table.

        Args:
            table: Table name to search for.
            column: Optional column name filter.
            usage_type: Optional usage type filter (e.g. ``"select"``, ``"where"``).
            repo: Optional repo name filter.
            limit: Maximum records to return.
            offset: Pagination offset.

        Returns:
            Dict with keys ``"usage"`` (list of usage dicts with ``table``,
            ``column``, ``usage_type``, ``alias``, ``node_name``,
            ``node_kind``, ``file``, ``repo``, ``line``, and optionally
            ``transform``), ``"summary"`` (dict mapping usage_type to count),
            and ``"total_count"`` (int).
        """
        where = ["cu.table_name = ?"]
        params: list = [table]
        if column:
            where.append("cu.column_name = ?")
            params.append(column)
        if usage_type:
            where.append("cu.usage_type = ?")
            params.append(usage_type)

        joins = (
            "JOIN nodes n ON cu.node_id = n.node_id "
            "JOIN files f ON cu.file_id = f.file_id "
            "JOIN repos r ON f.repo_id = r.repo_id"
        )
        if repo:
            where.append("r.name = ?")
            params.append(repo)

        sql = (
            f"SELECT cu.table_name, cu.column_name, cu.usage_type, cu.alias, "
            f"n.name as node_name, n.kind as node_kind, f.path, r.name as repo_name, n.line_start, "
            f"cu.transform "
            f"FROM column_usage cu {joins} "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY cu.table_name, cu.column_name, cu.usage_type "
            f"LIMIT ? OFFSET ?"
        )

        rows = self._execute_read(sql, params + [limit, offset]).fetchall()

        usage = []
        for r in rows:
            entry = {
                "table": r[0],
                "column": r[1],
                "usage_type": r[2],
                "alias": r[3],
                "node_name": r[4],
                "node_kind": r[5],
                "file": r[6],
                "repo": r[7],
                "line": r[8],
            }
            if r[9]:
                entry["transform"] = r[9]
            usage.append(entry)

        # True total count (before pagination)
        count_sql = f"SELECT COUNT(*) FROM column_usage cu {joins} WHERE {' AND '.join(where)}"
        total_count = self._execute_read(count_sql, params).fetchone()[0]

        # Summary by usage_type
        summary: dict[str, int] = {}
        for u in usage:
            summary[u["usage_type"]] = summary.get(u["usage_type"], 0) + 1

        return {"usage": usage, "summary": summary, "total_count": total_count}

    def query_search(
        self,
        pattern: str,
        kind: str | None = None,
        language: str | None = None,
        schema: str | None = None,
        repo: str | None = None,
        limit: int = 20,
        offset: int = 0,
        include_snippets: bool = True,
    ) -> dict:
        """Search nodes by name pattern (case-insensitive ``ILIKE``).

        Args:
            pattern: Substring to match against node names.
            kind: Filter by node kind (e.g. ``"table"``, ``"view"``).
            language: Filter by language (e.g. ``"sql"``).
            schema: Filter by database schema.
            repo: Filter by repo name.
            limit: Maximum number of matches to return.
            offset: Number of matches to skip (for pagination).
            include_snippets: If ``True``, attach source code snippets to results.

        Returns:
            Dict with keys ``"matches"`` (list of match dicts with ``name``,
            ``kind``, ``language``, ``file``, ``repo``, ``line_start``,
            ``line_end``, and optionally ``snippet``) and ``"total_count"``
            (int, total matching nodes before pagination).
        """
        escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where = ["n.name ILIKE ? ESCAPE '\\'"]
        params: list = [f"%{escaped}%"]
        if kind:
            where.append("n.kind = ?")
            params.append(kind)
        if language:
            where.append("n.language = ?")
            params.append(language)
        if schema:
            where.append("n.schema = ?")
            params.append(schema)

        joins = "LEFT JOIN files f ON n.file_id = f.file_id LEFT JOIN repos r ON f.repo_id = r.repo_id"
        if repo:
            where.append("r.name = ?")
            params.append(repo)

        count_sql = f"SELECT COUNT(*) FROM nodes n {joins} WHERE {' AND '.join(where)}"
        total = self._execute_read(count_sql, params).fetchone()[0]

        sql = (
            f"SELECT n.name, n.kind, n.language, f.path, r.name as repo_name, "
            f"n.line_start, n.line_end "
            f"FROM nodes n {joins} "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY n.name "
            f"LIMIT ? OFFSET ?"
        )
        rows = self._execute_read(sql, params + [limit, offset]).fetchall()

        matches = []
        for r in rows:
            match = {
                "name": r[0],
                "kind": r[1],
                "language": r[2],
                "file": r[3],
                "repo": r[4],
                "line_start": r[5],
                "line_end": r[6],
            }
            if include_snippets:
                snippet = self._read_snippet(r[4], r[3], r[5], r[6])
                if snippet:
                    match["snippet"] = snippet
            matches.append(match)

        return {"matches": matches, "total_count": total}

    def query_trace(
        self,
        name: str,
        kind: str | None = None,
        direction: str = "downstream",
        max_depth: int = 3,
        repo: str | None = None,
        include_snippets: bool = False,
        limit: int = 100,
        exclude_edges: set[tuple[str, str]] | None = None,
    ) -> dict:
        """Trace multi-hop dependency chains via DuckPGQ or recursive CTE.

        Args:
            name: Starting entity name.
            kind: Optional node kind filter for the starting node.
            direction: ``"downstream"``, ``"upstream"``, or ``"both"``.
            max_depth: Maximum hops to follow (capped at 10).
            repo: Optional repo name filter.
            include_snippets: Attach source code snippets when ``True``.
            limit: Maximum result rows.
            exclude_edges: Optional set of ``(source_name, target_name)``
                tuples.  Any edge whose source and target names match a
                tuple in this set will be excluded from traversal.  Used
                by PR-impact v2 to approximate a base-commit graph by
                removing newly-added edges from the HEAD graph.

        Returns:
            Dict with keys ``"root"`` (starting node dict or ``None``),
            ``"paths"`` (list of path-step dicts with ``name``, ``kind``,
            ``language``, ``relationship``, ``context``, ``depth``,
            ``file``, ``repo``, and optionally ``snippet``),
            ``"depth_summary"`` (``{depth: count}``), and
            ``"repos_affected"`` (sorted list of repo names). When
            ``direction="both"``, paths are split into ``"downstream"``
            and ``"upstream"`` keys instead of a single ``"paths"``.
        """
        max_depth = max(min(max_depth, 10), 1)
        # Find starting node(s)
        where = ["name = ?"]
        params: list = [name]
        if kind:
            where.append("kind = ?")
            params.append(kind)

        start_nodes = self._execute_read(
            f"SELECT node_id, name, kind FROM nodes WHERE {' AND '.join(where)}",
            params,
        ).fetchall()

        if not start_nodes:
            return {"root": None, "paths": [], "depth_summary": {}, "repos_affected": []}

        start_id = start_nodes[0][0]

        # "both" — run downstream and upstream separately and merge
        if direction == "both":
            down = self.query_trace(
                name,
                kind,
                "downstream",
                max_depth,
                repo,
                include_snippets,
                limit,
                exclude_edges,
            )
            up = self.query_trace(
                name,
                kind,
                "upstream",
                max_depth,
                repo,
                include_snippets,
                limit,
                exclude_edges,
            )
            return {
                "root": down["root"],
                "downstream": down["paths"],
                "upstream": up["paths"],
                "depth_summary": {
                    depth: down["depth_summary"].get(depth, 0) + up["depth_summary"].get(depth, 0)
                    for depth in set(down["depth_summary"]) | set(up["depth_summary"])
                },
                "repos_affected": list(set(down["repos_affected"] + up["repos_affected"])),
            }

        # Dispatch to PGQ or CTE
        use_pgq = self.has_pgq and exclude_edges is None
        if use_pgq:
            paths = self._trace_pgq(
                start_id, name, direction, max_depth, limit, include_snippets
            )
        else:
            paths = self._trace_cte(
                start_id, direction, max_depth, limit, include_snippets, exclude_edges
            )

        depth_summary: dict[int, int] = {}
        repos_affected: set[str] = set()
        for p in paths:
            depth_summary[p["depth"]] = depth_summary.get(p["depth"], 0) + 1
            if p["repo"]:
                repos_affected.add(p["repo"])

        return {
            "root": {"name": start_nodes[0][1], "kind": start_nodes[0][2]},
            "paths": paths,
            "depth_summary": depth_summary,
            "repos_affected": sorted(repos_affected),
        }

    def _trace_cte(
        self,
        start_id: int,
        direction: str,
        max_depth: int,
        limit: int,
        include_snippets: bool,
        exclude_edges: set[tuple[str, str]] | None = None,
    ) -> list[dict]:
        """Trace dependencies using recursive CTE (fallback when DuckPGQ unavailable)."""
        if direction == "downstream":
            source_col, target_col = "source_id", "target_id"
        else:
            source_col, target_col = "target_id", "source_id"

        # Pre-resolve exclude_edges name pairs to ID pairs
        exclude_clause = ""
        if exclude_edges:
            excluded_id_pairs: set[tuple[int, int]] = set()
            for src_name, tgt_name in exclude_edges:
                rows_ex = self._execute_read(
                    "SELECT s.node_id, t.node_id FROM nodes s, nodes t WHERE s.name = ? AND t.name = ?",
                    [src_name, tgt_name],
                ).fetchall()
                for row_ex in rows_ex:
                    excluded_id_pairs.add((row_ex[0], row_ex[1]))
            if excluded_id_pairs:
                pairs_sql = ", ".join(f"({s}, {t})" for s, t in excluded_id_pairs)
                exclude_clause = f"AND (e.source_id, e.target_id) NOT IN (VALUES {pairs_sql})"

        recursive_sql = f"""
        WITH RECURSIVE trace AS (
            SELECT
                e.{target_col} as node_id,
                e.relationship,
                e.context,
                1 as depth,
                ARRAY[e.{source_col}] as path
            FROM edges e
            WHERE e.{source_col} = ?
            {exclude_clause}

            UNION ALL

            SELECT
                e.{target_col},
                e.relationship,
                e.context,
                t.depth + 1,
                array_append(t.path, e.{source_col})
            FROM edges e
            JOIN trace t ON e.{source_col} = t.node_id
            WHERE t.depth < ?
            AND NOT array_contains(t.path, e.{target_col})
            {exclude_clause}
        )
        SELECT DISTINCT
            t.node_id, t.relationship, t.context, t.depth,
            n.name, n.kind, n.language,
            f.path as file_path, r.name as repo_name,
            n.line_start, n.line_end
        FROM trace t
        JOIN nodes n ON t.node_id = n.node_id
        LEFT JOIN files f ON n.file_id = f.file_id
        LEFT JOIN repos r ON f.repo_id = r.repo_id
        ORDER BY t.depth, n.name
        LIMIT ?
        """

        rows = self._execute_read(recursive_sql, [start_id, max_depth, limit]).fetchall()

        paths: list[dict] = []
        for r in rows:
            entry = {
                "name": r[4],
                "kind": r[5],
                "language": r[6],
                "relationship": r[1],
                "context": r[2],
                "depth": r[3],
                "file": r[7],
                "repo": r[8],
            }
            if include_snippets:
                snippet = self._read_snippet(r[8], r[7], r[9], r[10])
                if snippet:
                    entry["snippet"] = snippet
            paths.append(entry)

        return paths

    def _trace_pgq(
        self,
        start_id: int,
        name: str,
        direction: str,
        max_depth: int,
        limit: int,
        include_snippets: bool,
    ) -> list[dict]:
        """Trace dependencies using DuckPGQ GRAPH_TABLE bounded traversal.

        Note: PGQ bounded traversal does not provide per-hop depth or
        per-hop edge attributes. Depth is recovered via a lightweight
        CTE after node discovery. Relationship defaults to the edge's
        actual value when a direct edge exists.
        """
        # Edge direction: source_id -> target_id in our model.
        # -> follows source-to-target. <- follows target-to-source.
        # Downstream (what does start_id reach) = follow -> (outgoing).
        # Upstream (what reaches start_id) = follow <- (incoming).
        if direction == "downstream":
            edge_pattern = f"(a:nodes WHERE a.node_id = ?)-[e:edges]->{{1,{max_depth}}}"
        else:
            edge_pattern = f"(a:nodes WHERE a.node_id = ?)<-[e:edges]-{{1,{max_depth}}}"

        # Step 1: PGQ bounded traversal to discover reachable node_ids
        pgq_sql = (
            f"SELECT DISTINCT node_id FROM ("
            f"FROM GRAPH_TABLE (sqlprism_graph "
            f"MATCH {edge_pattern}(b:nodes) "
            f"COLUMNS (b.node_id))) LIMIT ?"
        )
        try:
            node_ids = [
                r[0]
                for r in self._execute_read(pgq_sql, [start_id, limit]).fetchall()
            ]
        except duckdb.Error as e:
            logger.warning("DuckPGQ trace failed for %s: %s, falling back to CTE", name, e)
            return self._trace_cte(
                start_id, direction, max_depth, limit, include_snippets
            )

        if not node_ids:
            return []

        # Step 2: Recover depth via lightweight CTE on discovered nodes only
        if direction == "downstream":
            source_col, target_col = "source_id", "target_id"
        else:
            source_col, target_col = "target_id", "source_id"

        placeholders = ",".join("?" for _ in node_ids)
        depth_sql = f"""
        WITH RECURSIVE depth_trace AS (
            SELECT e.{target_col} as node_id, 1 as depth
            FROM edges e
            WHERE e.{source_col} = ?
            AND e.{target_col} IN ({placeholders})

            UNION ALL

            SELECT e.{target_col}, dt.depth + 1
            FROM edges e
            JOIN depth_trace dt ON e.{source_col} = dt.node_id
            WHERE dt.depth < ?
            AND e.{target_col} IN ({placeholders})
        )
        SELECT node_id, MIN(depth) as min_depth FROM depth_trace GROUP BY node_id
        """
        depth_params = [start_id] + node_ids + [max_depth] + node_ids
        depth_rows = self._execute_read(depth_sql, depth_params).fetchall()
        depth_map: dict[int, int] = {r[0]: r[1] for r in depth_rows}

        # Step 3: Enrich with metadata (file, repo, edge relationship)
        # Include start_id in edge source lookup so depth-1 nodes get real relationship
        source_ids = [start_id] + node_ids
        source_ph = ",".join("?" for _ in source_ids)
        enrich_sql = (
            f"SELECT n.node_id, n.name, n.kind, n.language, "
            f"n.line_start, n.line_end, f.path, r.name, "
            f"e.relationship, e.context "
            f"FROM nodes n "
            f"LEFT JOIN files f ON n.file_id = f.file_id "
            f"LEFT JOIN repos r ON f.repo_id = r.repo_id "
            f"LEFT JOIN edges e ON e.{target_col} = n.node_id "
            f"AND e.{source_col} IN ({source_ph}) "
            f"WHERE n.node_id IN ({placeholders}) "
            f"AND n.file_id IS NOT NULL "
            f"ORDER BY n.name"
        )
        enrich_params = source_ids + node_ids
        rows = self._execute_read(enrich_sql, enrich_params).fetchall()

        seen: set[int] = set()
        paths: list[dict] = []
        for r in rows:
            nid, node_name, node_kind, language, line_start, line_end, fp, rn, rel, ctx = r
            if nid in seen:
                continue
            seen.add(nid)
            entry: dict = {
                "name": node_name,
                "kind": node_kind,
                "language": language,
                "relationship": rel or "references",
                "context": ctx,
                "depth": depth_map.get(nid, 1),
                "file": fp,
                "repo": rn,
            }
            if include_snippets:
                snippet = self._read_snippet(rn, fp, line_start, line_end)
                if snippet:
                    entry["snippet"] = snippet
            paths.append(entry)

        return paths

    def get_index_status(self) -> dict:
        """Return a summary of the current index state.

        Returns:
            Dict with keys ``"repos"`` (list of repo summaries with
            ``name``, ``path``, ``last_commit``, ``last_branch``,
            ``indexed_at``, ``file_count``, ``node_count``),
            ``"totals"`` (aggregate counts for ``files``, ``nodes``,
            ``edges``, ``column_usage_records``,
            ``column_lineage_chains``), ``"phantom_nodes"`` (int), and
            ``"schema_version"`` (str).
        """
        repos = self._execute_read(
            "SELECT r.name, r.path, r.last_commit, r.last_branch, r.indexed_at, "
            "COUNT(DISTINCT f.file_id) as file_count, "
            "COUNT(DISTINCT n.node_id) as node_count "
            "FROM repos r "
            "LEFT JOIN files f ON r.repo_id = f.repo_id "
            "LEFT JOIN nodes n ON f.file_id = n.file_id "
            "GROUP BY r.repo_id, r.name, r.path, r.last_commit, r.last_branch, r.indexed_at"
        ).fetchall()

        totals = self._execute_read(
            "SELECT "
            "(SELECT COUNT(*) FROM files), "
            "(SELECT COUNT(*) FROM nodes), "
            "(SELECT COUNT(*) FROM edges), "
            "(SELECT COUNT(*) FROM column_usage), "
            "(SELECT COUNT(*) FROM nodes WHERE file_id IS NULL), "
            "(SELECT COUNT(DISTINCT output_node || '.' || output_column) FROM column_lineage)"
        ).fetchone()

        return {
            "repos": [
                {
                    "name": r[0],
                    "path": r[1],
                    "last_commit": r[2],
                    "last_branch": r[3],
                    "indexed_at": str(r[4]) if r[4] else None,
                    "file_count": r[5],
                    "node_count": r[6],
                }
                for r in repos
            ],
            "totals": {
                "files": totals[0],
                "nodes": totals[1],
                "edges": totals[2],
                "column_usage_records": totals[3],
                "column_lineage_chains": totals[5],
            },
            "phantom_nodes": totals[4],
            "schema_version": "1.0",
        }
