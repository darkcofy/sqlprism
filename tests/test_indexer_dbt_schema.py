"""Tests for the indexer orchestrator."""





# ── #125: dbt ColumnDefResult persistence ──


def _build_minimal_dbt_project(
    repo_dir,
    *,
    manifest_nodes: dict | None = None,
    manifest_sources: dict | None = None,
    compiled_models: dict[str, str] | None = None,
    schema_yml_models: str | None = None,
    schema_yml_sources: str | None = None,
):
    """Scaffold a fake compiled dbt project so reindex_dbt can run end-to-end
    without a real dbt install. Returns the repo_dir."""
    import json

    repo_dir.mkdir(parents=True, exist_ok=True)
    (repo_dir / "dbt_project.yml").write_text("name: my_proj\n")
    (repo_dir / ".venv").mkdir(exist_ok=True)

    compiled = repo_dir / "target" / "compiled" / "my_proj" / "models"
    compiled.mkdir(parents=True, exist_ok=True)
    for rel, sql in (compiled_models or {}).items():
        target = compiled / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(sql)

    manifest = {
        "nodes": manifest_nodes or {},
        "sources": manifest_sources or {},
    }
    (repo_dir / "target" / "manifest.json").write_text(json.dumps(manifest))

    models_dir = repo_dir / "models"
    models_dir.mkdir(exist_ok=True)
    if schema_yml_models:
        (models_dir / "schema.yml").write_text(schema_yml_models)
    if schema_yml_sources:
        (models_dir / "sources.yml").write_text(schema_yml_sources)

    return repo_dir


def test_reindex_dbt_persists_schema_yml_columns(tmp_path):
    """schema.yml types and descriptions land in the `columns` table for
    dbt-indexed models, beating the inferred projection emitted by the CTAS
    wrap so documented types don't get shadowed."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            "staging/orders.sql": "SELECT customer_id, order_id FROM raw.orders",
        },
        manifest_nodes={
            "model.my_proj.orders": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "orders",
                "path": "staging/orders.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_models=(
            "version: 2\n"
            "models:\n"
            "  - name: orders\n"
            "    columns:\n"
            "      - name: customer_id\n"
            "        data_type: bigint\n"
            "        description: The customer reference\n"
            "      - name: order_id\n"
            "        data_type: bigint\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    rows = db._execute_read(
        "SELECT n.name, c.column_name, c.data_type, c.source, c.description "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ? "
        "ORDER BY c.column_name",
        ["proj", "orders"],
    ).fetchall()

    assert rows == [
        ("orders", "customer_id", "bigint", "schema_yml", "The customer reference"),
        ("orders", "order_id", "bigint", "schema_yml", None),
    ], rows

    db.close()


def test_dbt_columns_visible_in_schema_catalog(tmp_path):
    """GraphDB.get_table_columns returns real schema.yml types for dbt
    models; CTAS-only columns still appear, but with the `TEXT` fallback
    since no schema.yml documents them."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            "staging/orders.sql": "SELECT customer_id, order_id FROM raw.orders",
            "staging/plain.sql": "SELECT only_col FROM raw.foo",
        },
        manifest_nodes={
            "model.my_proj.orders": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "orders",
                "path": "staging/orders.sql",
                "depends_on": {"nodes": []},
            },
            "model.my_proj.plain": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "plain",
                "path": "staging/plain.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_models=(
            "version: 2\n"
            "models:\n"
            "  - name: orders\n"
            "    columns:\n"
            "      - name: customer_id\n"
            "        data_type: bigint\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    repo_id = db._execute_read(
        "SELECT repo_id FROM repos WHERE name = ?", ["proj"]
    ).fetchone()[0]
    catalog = db.get_table_columns(repo_id)

    assert catalog.get("orders", {}).get("customer_id") == "bigint", catalog
    # CTAS-only column without schema.yml docs falls back to TEXT in the
    # catalog — pin the fallback value so the contract can't drift.
    assert catalog.get("plain", {}).get("only_col") == "TEXT", catalog
    db.close()


def test_reindex_dbt_captures_source_columns(tmp_path):
    """schema.yml source entries attach to the source table node a model
    references — even without compiled SQL for the source itself, the
    `columns` table carries its documented columns under source='schema_yml'."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            # A model that references the raw.customers source — gives the
            # parser a table node the source column defs can attach to.
            "staging/stg_customers.sql": 'SELECT id FROM "raw"."customers"',
        },
        manifest_nodes={
            "model.my_proj.stg_customers": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "stg_customers",
                "path": "staging/stg_customers.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_sources=(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    tables:\n"
            "      - name: customers\n"
            "        columns:\n"
            "          - name: id\n"
            "            data_type: bigint\n"
            "          - name: email\n"
            "            data_type: varchar\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    rows = db._execute_read(
        "SELECT c.column_name, c.data_type, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ? "
        "ORDER BY c.column_name",
        ["proj", "customers"],
    ).fetchall()

    assert rows == [
        ("email", "varchar", "schema_yml"),
        ("id", "bigint", "schema_yml"),
    ], rows

    db.close()


def test_reindex_dbt_drops_stale_schema_yml_sources(tmp_path):
    """If a `sources:` entry disappears from schema.yml, its rows must be
    cleared on the next reindex — the synthetic source file is deleted
    unconditionally so stale columns don't persist after a YAML edit."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            "staging/stg_customers.sql": 'SELECT id FROM "raw"."customers"',
        },
        manifest_nodes={
            "model.my_proj.stg_customers": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "stg_customers",
                "path": "staging/stg_customers.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_sources=(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    tables:\n"
            "      - name: customers\n"
            "        columns:\n"
            "          - name: id\n            data_type: bigint\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    before = db._execute_read(
        "SELECT count(*) FROM columns c "
        "JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ? AND c.source = ?",
        ["proj", "customers", "schema_yml"],
    ).fetchone()[0]
    assert before == 1, "source row should exist before removal"

    # Drop the source from schema.yml and re-index.
    (repo_dir / "models" / "sources.yml").write_text(
        "version: 2\nsources: []\n"
    )
    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    after = db._execute_read(
        "SELECT count(*) FROM columns c "
        "JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ? AND c.source = ?",
        ["proj", "customers", "schema_yml"],
    ).fetchone()[0]
    assert after == 0, f"stale source rows should be gone, found {after}"

    db.close()


def test_reindex_files_dbt_merges_schema_yml(tmp_path):
    """reindex_files() (the on-save fast path) must also merge schema.yml
    into dbt ParseResults — otherwise a single-file save would silently
    drop documented types and regress back to CTAS-inferred rows only."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            "staging/orders.sql": "SELECT customer_id FROM raw.orders",
        },
        manifest_nodes={
            "model.my_proj.orders": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "orders",
                "path": "staging/orders.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_models=(
            "version: 2\n"
            "models:\n"
            "  - name: orders\n"
            "    columns:\n"
            "      - name: customer_id\n"
            "        data_type: bigint\n"
        ),
    )

    # The on-save path expects the user to point at a source file — the
    # dbt reindex path looks up the stem against the compiled tree.
    src_file = repo_dir / "models" / "staging" / "orders.sql"
    src_file.parent.mkdir(parents=True, exist_ok=True)
    src_file.write_text("SELECT customer_id FROM {{ source('raw', 'orders') }}")

    db = GraphDB(":memory:")
    indexer = Indexer(db)
    db.upsert_repo("proj", str(repo_dir), repo_type="dbt")

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        stats = indexer.reindex_files(
            paths=[str(src_file)],
            repo_configs={"proj": {
                "project_path": str(repo_dir),
                "repo_type": "dbt",
                "dialect": "duckdb",
            }},
        )

    assert stats["reindexed"] == 1, stats
    rows = db._execute_read(
        "SELECT c.column_name, c.data_type, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ?",
        ["proj", "orders"],
    ).fetchall()
    # schema.yml row beats the inferred row — bigint survives.
    assert ("customer_id", "bigint", "schema_yml") in rows, rows
    db.close()


def test_reindex_dbt_source_identifier_and_schema_keyed_separately(tmp_path):
    """Two sources sharing a bare identifier across different schemas must
    attach their columns to distinct graph nodes. The fix qualifies the
    node_name with the source schema, so they no longer collapse onto one
    `users` row via the UNIQUE upsert."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            # Two staging models each reference a differently-schema'd
            # source table that both happen to be called `users`.
            "staging/a.sql": 'SELECT id FROM "raw"."users"',
            "staging/b.sql": 'SELECT id FROM "prod"."users"',
        },
        manifest_nodes={
            "model.my_proj.a": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "a",
                "path": "staging/a.sql",
                "depends_on": {"nodes": []},
            },
            "model.my_proj.b": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "b",
                "path": "staging/b.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_sources=(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    schema: raw\n"
            "    tables:\n"
            "      - name: users\n"
            "        columns:\n"
            "          - name: raw_only\n            data_type: text\n"
            "  - name: prod\n"
            "    schema: prod\n"
            "    tables:\n"
            "      - name: users\n"
            "        columns:\n"
            "          - name: prod_only\n            data_type: text\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    rows = db._execute_read(
        "SELECT n.name, n.schema, c.column_name, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ? "
        "ORDER BY n.schema, c.column_name",
        ["proj", "users"],
    ).fetchall()

    by_schema: dict[str | None, set[str]] = {}
    for _name, schema, col_name, src in rows:
        assert src == "schema_yml", rows
        by_schema.setdefault(schema, set()).add(col_name)

    # raw_only must only live under the raw users node; prod_only under prod.
    assert by_schema.get("raw") == {"raw_only"}, by_schema
    assert by_schema.get("prod") == {"prod_only"}, by_schema
    db.close()


def test_reindex_dbt_source_identifier_override(tmp_path):
    """``sources.tables[].identifier`` overrides the logical ``name`` —
    the physical table the compiled SQL references is what graph nodes
    are keyed by, so columns must attach to the identifier."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            # Compiled SQL references the PHYSICAL name (customers_raw),
            # not the logical source entry name (customers).
            "staging/stg_customers.sql": 'SELECT id FROM "raw"."customers_raw"',
        },
        manifest_nodes={
            "model.my_proj.stg_customers": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "stg_customers",
                "path": "staging/stg_customers.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_sources=(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    schema: raw\n"
            "    tables:\n"
            "      - name: customers\n"
            "        identifier: customers_raw\n"
            "        columns:\n"
            "          - name: id\n            data_type: bigint\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    # Columns attached to the physical identifier, not the logical name.
    phys_rows = db._execute_read(
        "SELECT c.column_name, c.data_type, c.source "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ?",
        ["proj", "customers_raw"],
    ).fetchall()
    assert phys_rows == [("id", "bigint", "schema_yml")], phys_rows

    # And nothing landed on a phantom `customers` node.
    logical_rows = db._execute_read(
        "SELECT c.column_name "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ?",
        ["proj", "customers"],
    ).fetchall()
    assert logical_rows == [], logical_rows
    db.close()


def test_reindex_dbt_model_and_source_same_bare_name_disambiguated(tmp_path):
    """A model named ``customers`` and a ``raw.customers`` source entry must
    not cross-pollinate: model columns attach to the compiled model node,
    source columns to the source (via its physical schema)."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = _build_minimal_dbt_project(
        tmp_path / "proj",
        compiled_models={
            # The model `customers` references the source — which gives
            # the parser a table node for the source too.
            "marts/customers.sql": 'SELECT id FROM "raw"."customers"',
        },
        manifest_nodes={
            "model.my_proj.customers": {
                "resource_type": "model",
                "package_name": "my_proj",
                "name": "customers",
                "path": "marts/customers.sql",
                "depends_on": {"nodes": []},
            },
        },
        schema_yml_models=(
            "version: 2\n"
            "models:\n"
            "  - name: customers\n"
            "    columns:\n"
            "      - name: id\n"
            "        data_type: bigint\n"
            "        description: model-level id\n"
        ),
        schema_yml_sources=(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            "    schema: raw\n"
            "    tables:\n"
            "      - name: customers\n"
            "        columns:\n"
            "          - name: id\n"
            "            data_type: text\n"
            "            description: raw-source id\n"
        ),
    )

    db = GraphDB(":memory:")
    indexer = Indexer(db)

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        indexer.reindex_dbt(
            repo_name="proj", project_path=str(repo_dir), dialect="duckdb",
        )

    rows = db._execute_read(
        "SELECT n.name, n.schema, c.column_name, c.data_type, c.description "
        "FROM columns c JOIN nodes n ON c.node_id = n.node_id "
        "JOIN files f ON n.file_id = f.file_id "
        "JOIN repos r ON f.repo_id = r.repo_id "
        "WHERE r.name = ? AND n.name = ?",
        ["proj", "customers"],
    ).fetchall()

    descriptions_by_schema = {r[1]: r[4] for r in rows}
    types_by_schema = {r[1]: r[3] for r in rows}

    # The source row under schema=raw stays distinct from the model row
    # (which has no schema or a dbt-assigned path-based schema).
    assert descriptions_by_schema.get("raw") == "raw-source id", rows
    assert types_by_schema.get("raw") == "text", rows
    # The model-level row must carry the model description, NOT the source.
    model_rows = [r for r in rows if r[1] != "raw"]
    assert model_rows, f"expected at least one non-raw row, got {rows}"
    assert all(r[4] == "model-level id" for r in model_rows), model_rows
    db.close()
