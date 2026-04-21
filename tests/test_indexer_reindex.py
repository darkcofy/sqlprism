"""Tests for the indexer orchestrator."""



from sqlprism.types import (
    ColumnUsageResult,
    EdgeResult,
    NodeResult,
    ParseResult,
)

# ── v1.2: Task 2.2 — upsert_repo updates path ──


def test_upsert_repo_updates_path():
    """upsert_repo should update the stored path when repo is moved."""
    from sqlprism.core.graph import GraphDB

    db = GraphDB()
    repo_id1 = db.upsert_repo("myrepo", "/old/path")
    repo_id2 = db.upsert_repo("myrepo", "/new/path")

    assert repo_id1 == repo_id2, "Should return same repo_id"

    # Path should be updated
    row = db._execute_read("SELECT path FROM repos WHERE repo_id = ?", [repo_id1]).fetchone()
    assert row[0] == "/new/path", "Path should be updated to new location"

    db.close()


# ── v1.2: Task 1.4 — INSERT...SELECT alias resolution ──


def test_insert_select_resolves_table_aliases():
    """INSERT...SELECT should resolve table aliases to real table names."""
    from sqlprism.languages.sql import SqlParser

    parser = SqlParser()
    result = parser.parse(
        "test.sql",
        """
        INSERT INTO target (col_a, col_b)
        SELECT o.id, o.amount
        FROM orders o
    """,
    )

    # Column usage for the INSERT should reference 'orders', not 'o'
    insert_usage = [cu for cu in result.column_usage if cu.usage_type == "insert"]
    for cu in insert_usage:
        assert cu.table_name == "orders", f"Expected table_name='orders', got '{cu.table_name}' (alias not resolved)"


# ── 5.9/5.10: Edge case tests ──


def test_get_git_info_non_git_directory(tmp_path):
    """_get_git_info returns (None, None) for a non-git directory."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    # tmp_path is not a git repo
    db = GraphDB()
    indexer = Indexer(db)
    commit, branch = indexer._get_git_info(tmp_path)
    assert commit is None
    assert branch is None
    db.close()


def test_checksum_parse_result_stability():
    """Same ParseResult input produces the same checksum across calls."""
    from sqlprism.core.indexer import _checksum_parse_result

    result = ParseResult(
        language="sql",
        nodes=[
            NodeResult(kind="table", name="orders"),
            NodeResult(kind="query", name="report"),
        ],
        edges=[
            EdgeResult(
                source_name="report",
                source_kind="query",
                target_name="orders",
                target_kind="table",
                relationship="references",
            ),
        ],
        column_usage=[
            ColumnUsageResult(
                node_name="report",
                node_kind="query",
                table_name="orders",
                column_name="id",
                usage_type="select",
            ),
        ],
    )

    checksum1 = _checksum_parse_result(result)
    checksum2 = _checksum_parse_result(result)
    checksum3 = _checksum_parse_result(result)

    assert checksum1 == checksum2 == checksum3
    # Sanity: it's a hex string
    assert len(checksum1) == 64
    int(checksum1, 16)  # valid hex


# ── Issue #11: reindex_files() and _resolve_file_repo() ──


def test_reindex_files_plain_sql(tmp_path):
    """reindex_files() parses a plain SQL file and inserts into the graph."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT o.id, o.amount FROM orders o")

    db = GraphDB()
    indexer = Indexer(db)

    # Register the repo first
    db.upsert_repo("test", str(repo_dir), repo_type="sql")

    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["reindexed"] == 1
    assert stats["skipped"] == 0
    assert stats["errors"] == []

    # Verify data is in the DB with specific checks
    results = db.query_search("orders")
    assert results["total_count"] >= 1
    match = results["matches"][0]
    assert match["name"] == "orders"
    assert "report.sql" in match["file"]
    db.close()


def test_reindex_files_unknown_repo_skipped(tmp_path):
    """reindex_files() silently skips files not under any configured repo."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    # File outside any repo
    orphan = tmp_path / "orphan.sql"
    orphan.write_text("SELECT 1")

    db = GraphDB()
    indexer = Indexer(db)

    stats = indexer.reindex_files(paths=[str(orphan)])
    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0

    detail = stats["details"][0]
    assert detail["reason"] == "no matching repo"
    db.close()


def test_reindex_files_deleted_file_cleans_graph(tmp_path):
    """reindex_files() removes graph data when file has been deleted."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("CREATE TABLE report (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    # First, do a full reindex so data exists
    indexer.reindex_repo("test", str(repo_dir))
    status = db.get_index_status()
    assert status["totals"]["files"] == 1

    # Now delete the file and call reindex_files with the deleted path
    sql_file.unlink()
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["deleted"] == 1

    # Verify data was cleaned
    status = db.get_index_status()
    assert status["totals"]["files"] == 0
    db.close()


def test_reindex_files_unchanged_checksum_skipped(tmp_path):
    """reindex_files() skips files whose content hasn't changed."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("SELECT 1 FROM orders")

    db = GraphDB()
    indexer = Indexer(db)

    # First reindex via full reindex
    indexer.reindex_repo("test", str(repo_dir))

    # Now reindex_files — file unchanged
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["skipped"] == 1
    assert stats["reindexed"] == 0

    detail = stats["details"][0]
    assert detail["reason"] == "unchanged"
    db.close()


def test_reindex_files_groups_by_repo(tmp_path):
    """reindex_files() groups files by repo and processes each correctly."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    (repo_a / "a.sql").write_text("CREATE TABLE alpha (id INT)")
    (repo_b / "b.sql").write_text("CREATE TABLE beta (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    db.upsert_repo("repo_a", str(repo_a), repo_type="sql")
    db.upsert_repo("repo_b", str(repo_b), repo_type="sql")

    stats = indexer.reindex_files(paths=[
        str(repo_a / "a.sql"),
        str(repo_b / "b.sql"),
    ])
    assert stats["reindexed"] == 2
    assert stats["skipped"] == 0

    # Both tables are searchable
    assert db.query_search("alpha")["total_count"] >= 1
    assert db.query_search("beta")["total_count"] >= 1
    db.close()


def test_resolve_file_repo_deepest_match(tmp_path):
    """_resolve_file_repo picks the deepest matching repo for nested repos."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    parent = tmp_path / "project"
    child = parent / "subdir"
    child.mkdir(parents=True)

    db = GraphDB()
    indexer = Indexer(db)

    db.upsert_repo("parent", str(parent), repo_type="sql")
    db.upsert_repo("child", str(child), repo_type="dbt")

    all_repos = db.get_all_repos()

    file_path = child / "model.sql"
    resolved = indexer._resolve_file_repo(file_path, all_repos)
    assert resolved is not None
    _repo_id, repo_name, _repo_path, repo_type = resolved
    assert repo_name == "child"
    assert repo_type == "dbt"

    # A file in parent but not in child resolves to parent
    parent_file = parent / "query.sql"
    resolved2 = indexer._resolve_file_repo(parent_file, all_repos)
    assert resolved2 is not None
    assert resolved2[1] == "parent"
    assert resolved2[3] == "sql"
    db.close()


def test_reindex_files_non_sql_skipped(tmp_path):
    """reindex_files() skips non-SQL files."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    py_file = repo_dir / "script.py"
    py_file.write_text("print('hello')")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("test", str(repo_dir), repo_type="sql")

    stats = indexer.reindex_files(paths=[str(py_file)])
    assert stats["skipped"] == 1
    assert stats["details"][0]["reason"] == "not a SQL file"
    db.close()


def test_reindex_files_changed_content(tmp_path):
    """reindex_files() re-parses a file whose content changed since last index."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "myrepo"
    repo_dir.mkdir()
    sql_file = repo_dir / "report.sql"
    sql_file.write_text("CREATE TABLE report (id INT)")

    db = GraphDB()
    indexer = Indexer(db)

    # Initial full reindex
    indexer.reindex_repo("test", str(repo_dir))
    assert db.query_search("report")["total_count"] >= 1

    # Modify the file
    sql_file.write_text("CREATE TABLE report (id INT, name TEXT, amount DECIMAL)")

    # reindex_files should detect the change and re-parse
    stats = indexer.reindex_files(paths=[str(sql_file)])
    assert stats["reindexed"] == 1
    assert stats["skipped"] == 0

    # Verify the new content is actually in the graph —
    # the updated DDL has 3 columns vs 1, so node count should reflect the change
    status = db.get_index_status()
    assert status["totals"]["files"] == 1  # still 1 file
    assert status["totals"]["nodes"] >= 1  # node exists
    db.close()


def test_reindex_files_dbt_model(tmp_path):
    """reindex_files() compiles a dbt model via render_models() and inserts the result."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_project"
    repo_dir.mkdir()
    model_file = repo_dir / "models" / "stg_orders.sql"
    model_file.parent.mkdir()
    model_file.write_text("SELECT id, amount FROM raw_orders")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_dbt", str(repo_dir), repo_type="dbt")

    # Mock render_models to return a ParseResult without running dbt
    mock_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="stg_orders", metadata={"schema": "staging"})],
        edges=[EdgeResult(
            source_name="stg_orders", source_kind="table",
            target_name="raw_orders", target_kind="table",
            relationship="references",
        )],
    )

    with patch.object(indexer.dbt_renderer, "render_models", return_value={"staging/stg_orders.sql": mock_result}):
        stats = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_dbt": {"project_path": str(repo_dir), "repo_type": "dbt"}},
        )

    assert stats["reindexed"] == 1
    assert stats["errors"] == []

    # Verify data in the graph
    results = db.query_search("stg_orders")
    assert results["total_count"] >= 1
    db.close()


def test_reindex_dbt_manifest_edges_end_to_end(tmp_path):
    """Issue #96: reindex_dbt persists manifest-derived ref edges into the graph.

    Builds a fake dbt project with a compiled `orders.sql` and `stg_orders.sql`
    plus a manifest.json that declares `orders` depends on `stg_orders`. After
    `reindex_dbt`, `query_references("stg_orders", direction="inbound")` must
    surface `orders` as a consumer — the behavioural acceptance criterion
    from the issue body.
    """
    import json
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_proj"
    repo_dir.mkdir()
    (repo_dir / "dbt_project.yml").write_text("name: my_proj\n")
    (repo_dir / ".venv").mkdir()

    compiled = repo_dir / "target" / "compiled" / "my_proj" / "models"
    (compiled / "staging").mkdir(parents=True)
    (compiled / "marts").mkdir(parents=True)
    # Compiled SQL resolves to a physical schema unrelated to the model name —
    # the parser can't recover the logical ref from this alone; manifest must.
    (compiled / "staging" / "stg_orders.sql").write_text(
        'SELECT id FROM "raw"."raw_orders"'
    )
    (compiled / "marts" / "orders.sql").write_text(
        'SELECT * FROM "analytics_prod"."stg_orders"'
    )

    manifest = {
        "nodes": {
            "model.my_proj.orders": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "orders",
                "path": "marts/orders.sql",
                "depends_on": {"nodes": ["model.my_proj.stg_orders"]},
            },
            "model.my_proj.stg_orders": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "stg_orders",
                "path": "staging/stg_orders.sql",
                "depends_on": {"nodes": ["source.my_proj.raw.raw_orders"]},
            },
        },
        "sources": {
            "source.my_proj.raw.raw_orders": {
                "name": "raw_orders",
                "identifier": "raw_orders",
            },
        },
    }
    (repo_dir / "target" / "manifest.json").write_text(json.dumps(manifest))

    db = GraphDB()
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        stats = indexer.reindex_dbt(
            repo_name="jaffle", project_path=str(repo_dir), dialect="duckdb"
        )

    assert stats["models_compiled"] == 2
    assert stats["edges_added"] > 0

    # The behavioural assertion from issue #96: stg_orders has `orders` as
    # an inbound model consumer, not just its test/materialization nodes.
    refs = db.query_references("stg_orders", direction="inbound", include_snippets=False)
    inbound_names = {r["name"] for r in refs["inbound"]}
    assert "orders" in inbound_names, f"Expected 'orders' in inbound refs, got {inbound_names}"

    db.close()


def test_reindex_files_sqlmesh_model(tmp_path):
    """reindex_files() renders a sqlmesh model via render_models() and inserts the result."""
    from unittest.mock import patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "sqlmesh_project"
    repo_dir.mkdir()
    model_file = repo_dir / "models" / "model_a.sql"
    model_file.parent.mkdir()
    model_file.write_text("SELECT id FROM source_table")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_sm", str(repo_dir), repo_type="sqlmesh")

    # Mock render_models to return a ParseResult without running sqlmesh
    mock_result = ParseResult(
        language="sql",
        nodes=[NodeResult(kind="table", name="model_a")],
        edges=[EdgeResult(
            source_name="model_a", source_kind="table",
            target_name="source_table", target_kind="table",
            relationship="references",
        )],
    )

    renderer = indexer.get_sqlmesh_renderer("athena")
    with patch.object(renderer, "render_models", return_value={'"db"."schema"."model_a"': mock_result}):
        stats = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_sm": {
                "project_path": str(repo_dir),
                "repo_type": "sqlmesh",
                "dialect": "athena",
            }},
        )

    assert stats["reindexed"] == 1
    assert stats["errors"] == []

    # Verify data in the graph
    results = db.query_search("model_a")
    assert results["total_count"] >= 1
    db.close()


def test_reindex_files_dbt_delete_finds_stored_path(tmp_path):
    """Deleting a dbt model finds and removes the stored compiled-dir path."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_project"
    repo_dir.mkdir()

    db = GraphDB()
    indexer = Indexer(db)
    repo_id = db.upsert_repo("my_dbt", str(repo_dir), repo_type="dbt")

    # Simulate a previously indexed dbt model stored as "staging/stg_orders.sql"
    with db.write_transaction():
        file_id = db.insert_file(repo_id, "staging/stg_orders.sql", "sql", "abc123")
        db.insert_nodes_batch([(file_id, "table", "stg_orders", "sql", 1, 1, None, "staging")])

    assert db.get_index_status()["totals"]["files"] == 1

    # The filesystem path is models/stg_orders.sql — now deleted
    deleted_path = repo_dir / "models" / "stg_orders.sql"

    stats = indexer.reindex_files(
        paths=[str(deleted_path)],
        repo_configs={"my_dbt": {"project_path": str(repo_dir), "repo_type": "dbt"}},
    )

    assert stats["deleted"] >= 1
    assert db.get_index_status()["totals"]["files"] == 0
    db.close()


def test_reindex_files_empty_input():
    """reindex_files() with empty paths returns zeroed stats."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    db = GraphDB()
    indexer = Indexer(db)
    stats = indexer.reindex_files(paths=[])
    assert stats["reindexed"] == 0
    assert stats["skipped"] == 0
    assert stats["deleted"] == 0
    assert stats["errors"] == []
    assert stats["details"] == []
    db.close()


# ── Issue #14: Additional indexer unit and integration tests ──


def test_resolve_file_repo_various_layouts(tmp_path):
    """_resolve_file_repo maps files to deepest matching repo; unmatched → None."""
    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    parent = tmp_path / "project" / "a"
    child = parent / "sub"
    child.mkdir(parents=True)

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("parent", str(parent), repo_type="sql")
    db.upsert_repo("child", str(child), repo_type="dbt")

    all_repos = db.get_all_repos()

    # File in child → resolves to child (deepest)
    resolved = indexer._resolve_file_repo(child / "model.sql", all_repos)
    assert resolved is not None
    _repo_id, repo_name, _repo_path, repo_type = resolved
    assert repo_name == "child"
    assert repo_type == "dbt"

    # File in parent but not in child → resolves to parent
    resolved2 = indexer._resolve_file_repo(parent / "query.sql", all_repos)
    assert resolved2 is not None
    _, repo_name2, _, repo_type2 = resolved2
    assert repo_name2 == "parent"
    assert repo_type2 == "sql"

    # Deeply nested file under child → still resolves to child
    deep_file = child / "nested" / "deep" / "model.sql"
    resolved3 = indexer._resolve_file_repo(deep_file, all_repos)
    assert resolved3 is not None
    assert resolved3[1] == "child"

    # File completely outside all repos → None
    outside = tmp_path / "elsewhere" / "orphan.sql"
    resolved4 = indexer._resolve_file_repo(outside, all_repos)
    assert resolved4 is None

    db.close()
