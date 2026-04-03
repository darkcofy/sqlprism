"""Tests for dbt and sqlmesh renderer utilities (Phase 5.2)."""

import pytest

from sqlprism.languages.dbt import DbtRenderer
from sqlprism.languages.sqlmesh import _validate_command

# ── DbtRenderer._get_project_name ──


def test_dbt_get_project_name(tmp_path):
    """_get_project_name reads name from dbt_project.yml."""
    yml = tmp_path / "dbt_project.yml"
    yml.write_text("name: my_project\nversion: '1.0.0'\n")
    renderer = DbtRenderer()
    assert renderer._get_project_name(tmp_path) == "my_project"


def test_dbt_get_project_name_with_quotes(tmp_path):
    """_get_project_name strips surrounding quotes from name value."""
    yml = tmp_path / "dbt_project.yml"
    yml.write_text("name: 'quoted_project'\nversion: '1.0.0'\n")
    renderer = DbtRenderer()
    assert renderer._get_project_name(tmp_path) == "quoted_project"


def test_dbt_get_project_name_missing(tmp_path):
    """_get_project_name raises FileNotFoundError when dbt_project.yml is absent."""
    renderer = DbtRenderer()
    with pytest.raises(FileNotFoundError):
        renderer._get_project_name(tmp_path)


# ── _validate_command error messages ──


def test_validate_command_error_message():
    """_validate_command rejects shell metacharacters with a clear error."""
    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("python; rm -rf /", {"python"})


def test_validate_command_rejects_pipe():
    """_validate_command rejects pipe characters."""
    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("python | cat", {"python"})


def test_validate_command_rejects_backtick():
    """_validate_command rejects backtick substitution."""
    with pytest.raises(ValueError, match="disallowed shell characters"):
        _validate_command("python `whoami`", {"python"})


def test_validate_command_rejects_unknown_base():
    """_validate_command rejects commands not in the allowlist."""
    with pytest.raises(ValueError, match="not in allowlist"):
        _validate_command("bash -c 'echo hi'", {"python", "uv"})


def test_validate_command_accepts_valid():
    """_validate_command passes for a valid command."""
    _validate_command("uv run python", {"uv", "python"})


def test_validate_command_empty():
    """_validate_command rejects empty command."""
    with pytest.raises(ValueError, match="Empty command"):
        _validate_command("", {"python"})


# ── DbtRenderer.render_project mocked tests ──

import json  # noqa: E402
import subprocess  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

from sqlprism.languages.sqlmesh import SqlMeshRenderer  # noqa: E402


def test_dbt_render_project_command_construction(tmp_path):
    """Verify dbt compile command is constructed with correct args, cwd, env, and timeout."""
    # Set up project structure
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Call render_project — it will fail reading compiled dir, but we only
        # care about the subprocess call
        renderer.render_project(
            project_path=tmp_path,
            profiles_dir=tmp_path,
            target="dev",
            dbt_command="uv run dbt",
        )

        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args
        cmd = call_kwargs[1].get("cmd", call_kwargs[0][0]) if call_kwargs[0] else call_kwargs[1]["cmd"]
        # Access positional arg
        cmd = call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1]["cmd"]

        assert cmd[0] == "uv"
        assert cmd[1] == "run"
        assert cmd[2] == "dbt"
        assert "compile" in cmd
        assert "--project-dir" in cmd
        assert "--profiles-dir" in cmd
        assert "--target" in cmd
        assert "dev" in cmd

        assert call_kwargs[1]["cwd"] == tmp_path.resolve()
        assert call_kwargs[1]["capture_output"] is True
        assert call_kwargs[1]["text"] is True
        assert call_kwargs[1]["timeout"] == 300


def test_dbt_render_project_env_propagation(tmp_path):
    """Verify env vars from env_file are passed to the subprocess."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    env_file = tmp_path / ".env"
    env_file.write_text("MY_SECRET=hunter2\nDB_HOST=localhost\n")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        renderer.render_project(
            project_path=tmp_path,
            env_file=str(env_file),
        )

        mock_run.assert_called_once()
        passed_env = mock_run.call_args[1]["env"]
        assert passed_env["MY_SECRET"] == "hunter2", "env_file vars should be in subprocess env"
        assert passed_env["DB_HOST"] == "localhost", "env_file vars should be in subprocess env"


def test_dbt_render_project_nonzero_exit(tmp_path):
    """Verify RuntimeError is raised with stderr when dbt compile fails."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: Profile not found",
        )

        with pytest.raises(RuntimeError, match="Profile not found"):
            renderer.render_project(project_path=tmp_path)


def test_dbt_render_project_timeout(tmp_path):
    """Verify TimeoutExpired propagates when dbt compile takes too long."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["uv", "run", "dbt", "compile"], timeout=300)

        with pytest.raises(subprocess.TimeoutExpired):
            renderer.render_project(project_path=tmp_path)


def test_dbt_render_project_success(tmp_path):
    """Verify ParseResults are returned with correct node names from compiled SQL."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    # Create compiled directory structure as dbt would produce
    compiled_dir = tmp_path / "target" / "compiled" / "test_proj" / "models"
    compiled_dir.mkdir(parents=True)

    (compiled_dir / "orders.sql").write_text("SELECT id, customer_id FROM raw.orders")
    staging = compiled_dir / "staging"
    staging.mkdir()
    (staging / "stg_customers.sql").write_text("SELECT id, name FROM raw.customers")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        results = renderer.render_project(project_path=tmp_path)

    assert "orders.sql" in results, "Should contain top-level model"
    assert "staging/stg_customers.sql" in results, "Should contain nested model"

    # Check that nodes have dbt metadata
    for rel_path, parse_result in results.items():
        assert len(parse_result.nodes) > 0, f"Model {rel_path} should have nodes"
        for node in parse_result.nodes:
            assert node.metadata is not None, "Nodes should have metadata"
            assert "dbt_model" in node.metadata, "Nodes should have dbt_model metadata"
            assert node.metadata["dbt_model"] == rel_path


# ── SqlMeshRenderer.render_project mocked tests ──


def test_sqlmesh_render_project_command_construction(tmp_path):
    """Verify the inline Python script is passed correctly to subprocess."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"rendered": {}, "errors": []}),
            stderr="",
        )

        renderer.render_project(
            project_path=tmp_path,
            dialect="athena",
            gateway="local",
            variables={"GRACE_PERIOD": 7},
            sqlmesh_command="uv run python",
        )

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]

        assert cmd[0] == "uv"
        assert cmd[1] == "run"
        assert cmd[2] == "python"
        assert cmd[3] == "-c"
        # The script text should be the 5th element
        assert "sqlmesh" in cmd[4], "Inline script should reference sqlmesh"
        assert "Context" in cmd[4], "Inline script should use sqlmesh Context"
        # Positional args: project_path, dialect, gateway, variables json, model_filter
        assert cmd[5] == str(tmp_path.resolve())
        assert cmd[6] == "athena"
        assert cmd[7] == "local"
        assert json.loads(cmd[8]) == {"GRACE_PERIOD": 7}
        assert json.loads(cmd[9]) == [], "render_project passes empty model filter"

        assert mock_run.call_args[1]["timeout"] == 600


def test_sqlmesh_render_project_nonzero_exit(tmp_path):
    """Verify RuntimeError is raised with stderr when sqlmesh render fails."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="ModuleNotFoundError: No module named 'sqlmesh'",
        )

        with pytest.raises(RuntimeError, match="No module named"):
            renderer.render_project(project_path=tmp_path)


def test_sqlmesh_render_project_timeout(tmp_path):
    """Verify TimeoutExpired propagates when sqlmesh render takes too long."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["uv", "run", "python", "-c", "..."], timeout=600)

        with pytest.raises(subprocess.TimeoutExpired):
            renderer.render_project(project_path=tmp_path)


def test_sqlmesh_render_project_success(tmp_path):
    """Verify ParseResults are created correctly from rendered model JSON."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered_models = {
        '"db"."schema"."orders"': "SELECT id, customer_id FROM raw.orders",
        '"db"."schema"."customers"': "SELECT id, name FROM raw.customers",
    }
    stdout_json = json.dumps({"rendered": rendered_models, "errors": []})

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")

        results = renderer.render_project(project_path=tmp_path)

    assert len(results) == 2, "Should have two model results"

    for model_name, parse_result in results.items():
        assert model_name in rendered_models, "Key should be original model name"
        assert len(parse_result.nodes) > 0, f"Model {model_name} should have nodes"
        for node in parse_result.nodes:
            assert node.metadata is not None, "Nodes should have metadata"
            assert "sqlmesh_model" in node.metadata, "Nodes should have sqlmesh_model metadata"
            assert node.metadata["sqlmesh_model"] == model_name


def test_sqlmesh_render_project_bad_json(tmp_path):
    """Verify error when subprocess returns non-JSON stdout."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="this is not valid json {{{",
            stderr="",
        )

        with pytest.raises(json.JSONDecodeError):
            renderer.render_project(project_path=tmp_path)


# ── Phase 1: dbt model name derivation (1.4c-d) ──


def test_dbt_model_name_from_subdirectory(tmp_path):
    """dbt model at models/staging/orders.sql -> node 'orders' with schema 'staging' (1.4c)."""
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: test_project\nversion: '1.0.0'\n")

    # Create compiled output in expected location
    compiled_dir = project_dir / "target" / "compiled" / "test_project" / "models" / "staging"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "orders.sql").write_text("SELECT id, amount FROM raw_orders")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.DbtRenderer._run_dbt_compile"):
        results = renderer.render_project(project_path=project_dir)

    assert "staging/orders.sql" in results
    result = results["staging/orders.sql"]
    # The node should be named "orders" not "staging__orders"
    node_names = {n.name for n in result.nodes}
    assert "orders" in node_names, f"Expected 'orders' in {node_names}"
    assert "staging__orders" not in node_names, "Should not use flattened name"


def test_dbt_get_project_name_nested_name_before_toplevel(tmp_path):
    """_get_project_name ignores nested name: keys and finds top-level name (2.7b)."""
    yml = tmp_path / "dbt_project.yml"
    yml.write_text("models:\n  name: nested_wrong\nname: correct_project\nversion: '1.0.0'\n")
    renderer = DbtRenderer()
    assert renderer._get_project_name(tmp_path) == "correct_project"


def test_dbt_model_name_no_subdirectory(tmp_path):
    """dbt model at models/orders.sql -> node 'orders' with no schema (1.4d)."""
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: test_project\nversion: '1.0.0'\n")

    compiled_dir = project_dir / "target" / "compiled" / "test_project" / "models"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "orders.sql").write_text("SELECT id, amount FROM raw_orders")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.DbtRenderer._run_dbt_compile"):
        results = renderer.render_project(project_path=project_dir)

    assert "orders.sql" in results
    result = results["orders.sql"]
    node_names = {n.name for n in result.nodes}
    assert "orders" in node_names


# ── DbtRenderer.render_models tests (#9) ──


def test_dbt_render_models_single(tmp_path):
    """render_models compiles a single model via --select and returns only that model."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    # Create compiled directory with multiple models (simulating dbt output)
    compiled_dir = tmp_path / "target" / "compiled" / "test_proj" / "models"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "stg_orders.sql").write_text("SELECT id FROM raw.orders")
    (compiled_dir / "stg_payments.sql").write_text("SELECT id FROM raw.payments")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        results = renderer.render_models(
            project_path=tmp_path,
            model_names=["stg_orders"],
        )

    # Should only contain the selected model
    assert "stg_orders.sql" in results
    assert "stg_payments.sql" not in results

    # Verify --select was passed
    cmd = mock_run.call_args[0][0]
    assert "--select" in cmd
    assert "stg_orders" in cmd


def test_dbt_render_models_multiple(tmp_path):
    """render_models compiles multiple models in a single dbt compile --select call."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    compiled_dir = tmp_path / "target" / "compiled" / "test_proj" / "models"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "stg_orders.sql").write_text("SELECT id FROM raw.orders")
    (compiled_dir / "stg_payments.sql").write_text("SELECT id FROM raw.payments")
    (compiled_dir / "stg_customers.sql").write_text("SELECT id FROM raw.customers")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        results = renderer.render_models(
            project_path=tmp_path,
            model_names=["stg_orders", "stg_payments"],
        )

    assert "stg_orders.sql" in results
    assert "stg_payments.sql" in results
    assert "stg_customers.sql" not in results

    # Both models passed to single --select call
    cmd = mock_run.call_args[0][0]
    select_idx = cmd.index("--select")
    assert "stg_orders" in cmd[select_idx + 1 :]
    assert "stg_payments" in cmd[select_idx + 1 :]


def test_dbt_render_models_partial_failure(tmp_path):
    """render_models returns error from dbt compile when a model has errors."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Compilation Error in model stg_orders",
        )

        with pytest.raises(RuntimeError, match="Compilation Error"):
            renderer.render_models(
                project_path=tmp_path,
                model_names=["stg_orders"],
            )


def test_dbt_render_project_unchanged(tmp_path):
    """render_project still works without --select (no regression)."""
    (tmp_path / "dbt_project.yml").write_text("name: test_proj\n")
    (tmp_path / ".venv").mkdir()

    compiled_dir = tmp_path / "target" / "compiled" / "test_proj" / "models"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "orders.sql").write_text("SELECT id FROM raw.orders")

    renderer = DbtRenderer()

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        results = renderer.render_project(project_path=tmp_path)

    # No --select in command
    cmd = mock_run.call_args[0][0]
    assert "--select" not in cmd
    assert "orders.sql" in results


# ── SqlMeshRenderer.render_models tests (#10) ──


def test_sqlmesh_render_models_single(tmp_path):
    """render_models renders a single model via model filter."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered = {'"db"."schema"."model_a"': "SELECT id FROM raw.a"}
    stdout_json = json.dumps({"rendered": rendered, "errors": []})

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")

        results = renderer.render_models(
            project_path=tmp_path,
            model_names=["model_a"],
        )

    assert len(results) == 1
    assert '"db"."schema"."model_a"' in results

    # Verify model filter was passed as last subprocess arg
    cmd = mock_run.call_args[0][0]
    assert json.loads(cmd[-1]) == ["model_a"]


def test_sqlmesh_render_models_multiple(tmp_path):
    """render_models renders multiple models in a single subprocess call."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered = {
        '"db"."schema"."model_a"': "SELECT id FROM raw.a",
        '"db"."schema"."model_b"': "SELECT id FROM raw.b",
    }
    stdout_json = json.dumps({"rendered": rendered, "errors": []})

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")

        results = renderer.render_models(
            project_path=tmp_path,
            model_names=["model_a", "model_b"],
        )

    assert len(results) == 2

    # Both models in filter arg
    cmd = mock_run.call_args[0][0]
    filter_arg = json.loads(cmd[-1])
    assert set(filter_arg) == {"model_a", "model_b"}


def test_sqlmesh_render_models_partial_failure(tmp_path):
    """render_models returns successful models even when some fail."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered = {'"db"."schema"."model_a"': "SELECT id FROM raw.a"}
    errors = [{"model": "model_b", "error": "syntax error"}]
    stdout_json = json.dumps({"rendered": rendered, "errors": errors})

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")

        results = renderer.render_models(
            project_path=tmp_path,
            model_names=["model_a", "model_b"],
        )

    # model_a succeeded, model_b had error but process didn't crash
    assert len(results) == 1
    assert '"db"."schema"."model_a"' in results


def test_sqlmesh_render_project_unchanged(tmp_path):
    """render_project passes empty model filter (no regression)."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    stdout_json = json.dumps({"rendered": {}, "errors": []})

    with (
        patch.object(renderer, "_list_models", side_effect=RuntimeError("no sqlmesh")),
        patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        renderer.render_project(project_path=tmp_path)

    # render_project passes empty model_filter (render all)
    cmd = mock_run.call_args[0][0]
    assert json.loads(cmd[-1]) == [], "render_project should pass empty filter"


# ── Parallel subprocess rendering tests (#92) ──


def test_inline_scripts_syntax():
    """Both inline scripts are valid Python syntax."""
    import ast

    from sqlprism.languages.sqlmesh import _LIST_MODELS_SCRIPT, _RENDER_SCRIPT
    ast.parse(_LIST_MODELS_SCRIPT)
    ast.parse(_RENDER_SCRIPT)


def test_batch_splitting():
    """Models are split into balanced batches."""
    from sqlprism.languages.sqlmesh import _split_into_batches

    models = [f"model_{i}" for i in range(10)]
    batches = _split_into_batches(models, num_batches=3)
    assert len(batches) == 3
    flat = [m for batch in batches for m in batch]
    assert sorted(flat) == sorted(models)
    sizes = [len(b) for b in batches]
    assert max(sizes) - min(sizes) <= 1


def test_batch_splitting_edge_cases():
    """Batch splitting handles edge cases correctly."""
    from sqlprism.languages.sqlmesh import _split_into_batches

    # Empty list
    assert _split_into_batches([], 3) == []

    # num_batches=0 returns single batch
    assert _split_into_batches(["a", "b"], 0) == [["a", "b"]]

    # num_batches > len(items) produces len(items) batches
    batches = _split_into_batches(["a", "b"], 5)
    assert len(batches) == 2
    assert sorted(m for b in batches for m in b) == ["a", "b"]

    # Single item
    assert _split_into_batches(["a"], 3) == [["a"]]


# ── Integration tests: Indexer → Renderer → Graph (#14) ──


def test_reindex_dbt_triggers_select_compile(tmp_path):
    """reindex_files for a dbt model invokes dbt compile --select and updates graph."""
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "dbt_project"
    repo_dir.mkdir()
    (repo_dir / "dbt_project.yml").write_text("name: test_proj\n")
    (repo_dir / ".venv").mkdir()

    models_dir = repo_dir / "models"
    models_dir.mkdir()
    model_file = models_dir / "stg_orders.sql"
    model_file.write_text("SELECT id, amount FROM {{ ref('raw_orders') }}")

    compiled_dir = repo_dir / "target" / "compiled" / "test_proj" / "models"
    compiled_dir.mkdir(parents=True)
    (compiled_dir / "stg_orders.sql").write_text("SELECT id, amount FROM raw_schema.raw_orders")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_dbt", str(repo_dir), repo_type="dbt")

    with patch("sqlprism.languages.dbt.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        stats = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_dbt": {
                "project_path": str(repo_dir),
                "repo_type": "dbt",
            }},
        )

    assert stats["reindexed"] == 1
    assert stats["errors"] == []

    # Verify dbt compile was invoked exactly once with --select
    assert mock_run.call_count == 1
    cmd = mock_run.call_args[0][0]
    assert "--select" in cmd
    assert "stg_orders" in cmd

    # Verify graph was updated with file-backed node
    results = db.query_search("stg_orders")
    assert results["total_count"] >= 1
    file_backed = [m for m in results["matches"] if m.get("file")]
    assert len(file_backed) >= 1

    # Verify edge to referenced table was also inserted
    ref_results = db.query_search("raw_orders")
    assert ref_results["total_count"] >= 1

    db.close()


def test_reindex_sqlmesh_triggers_filtered_render(tmp_path):
    """reindex_files for a sqlmesh model passes model filter and updates graph."""
    import json
    from unittest.mock import MagicMock, patch

    from sqlprism.core.graph import GraphDB
    from sqlprism.core.indexer import Indexer

    repo_dir = tmp_path / "sqlmesh_project"
    repo_dir.mkdir()
    (repo_dir / ".venv").mkdir()

    models_dir = repo_dir / "models"
    models_dir.mkdir()
    model_file = models_dir / "model_a.sql"
    model_file.write_text("SELECT id FROM source_table")

    db = GraphDB()
    indexer = Indexer(db)
    db.upsert_repo("my_sm", str(repo_dir), repo_type="sqlmesh")

    rendered_v1 = {'"db"."schema"."model_a"': "SELECT id FROM raw.source_table"}
    stdout_v1 = json.dumps({"rendered": rendered_v1, "errors": []})

    # First: index the model so it has a stored node name (exercises primary path)
    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_v1, stderr="")
        stats1 = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_sm": {
                "project_path": str(repo_dir),
                "repo_type": "sqlmesh",
                "dialect": "athena",
            }},
        )
    assert stats1["reindexed"] == 1

    # Verify model filter was passed on the first call (fallback path: file stem)
    cmd1 = mock_run.call_args[0][0]
    filter1 = json.loads(cmd1[-1])
    assert "model_a" in filter1

    # Now modify the file and reindex again — this exercises the primary
    # _resolve_model_names_by_stem path (lookup stored node name, not fallback).
    # The primary path resolves stored table/view node names for this file,
    # which may differ from the file stem.
    model_file.write_text("SELECT id, name FROM source_table")
    rendered_v2 = {'"db"."schema"."model_a"': "SELECT id, name FROM raw.source_table"}
    stdout_v2 = json.dumps({"rendered": rendered_v2, "errors": []})

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_v2, stderr="")

        stats2 = indexer.reindex_files(
            paths=[str(model_file)],
            repo_configs={"my_sm": {
                "project_path": str(repo_dir),
                "repo_type": "sqlmesh",
                "dialect": "athena",
            }},
        )

    assert stats2["reindexed"] == 1
    assert stats2["errors"] == []

    # Verify subprocess was called with a non-empty model filter
    cmd2 = mock_run.call_args[0][0]
    filter2 = json.loads(cmd2[-1])
    assert len(filter2) >= 1, "Primary path should resolve at least one model name"

    # Verify graph was updated with file-backed node
    results = db.query_search("model_a")
    assert results["total_count"] >= 1
    file_backed = [m for m in results["matches"] if m.get("file")]
    assert len(file_backed) >= 1

    db.close()


# ── SqlMesh column schema extraction tests (#25) ──


from sqlprism.types import ColumnDefResult  # noqa: E402


def test_sqlmesh_column_schema_explicit(tmp_path):
    """Model with explicit column types produces ColumnDefResult entries with source='sqlmesh_schema'."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered = {'"db"."staging"."orders"': "SELECT order_id, status FROM raw.orders"}
    column_schemas = {
        '"db"."staging"."orders"': {"order_id": "INT", "status": "TEXT"},
    }
    stdout_json = json.dumps({
        "rendered": rendered,
        "errors": [],
        "column_schemas": column_schemas,
    })

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        results = renderer.render_project(project_path=tmp_path)

    result = results['"db"."staging"."orders"']
    assert len(result.columns) == 2, f"Expected 2 column defs, got {len(result.columns)}"

    col_map = {c.column_name: c for c in result.columns}
    assert "order_id" in col_map
    assert "status" in col_map

    assert col_map["order_id"].data_type == "INT"
    assert col_map["status"].data_type == "TEXT"
    assert col_map["order_id"].source == "sqlmesh_schema"
    assert col_map["status"].source == "sqlmesh_schema"
    assert col_map["order_id"].position == 0
    assert col_map["status"].position == 1
    assert col_map["order_id"].node_name == '"db"."staging"."orders"'


def test_sqlmesh_column_schema_none(tmp_path):
    """Model without column definitions produces no ColumnDefResult entries and no error."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    rendered = {'"db"."staging"."orders"': "SELECT order_id, status FROM raw.orders"}
    stdout_json = json.dumps({
        "rendered": rendered,
        "errors": [],
        "column_schemas": {},
    })

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        results = renderer.render_project(project_path=tmp_path)

    result = results['"db"."staging"."orders"']
    assert len(result.columns) == 0, "Model without columns_to_types should have no column defs"


def test_sqlmesh_columns_to_types_extraction(tmp_path):
    """Renderer correctly handles column_schemas payload with mixed presence across models."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    # Simulate subprocess output where columns_to_types was available
    rendered = {
        '"db"."staging"."orders"': "SELECT order_id, status FROM raw.orders",
        '"db"."staging"."customers"': "SELECT id, name FROM raw.customers",
    }
    column_schemas = {
        '"db"."staging"."orders"': {"order_id": "INT", "status": "TEXT"},
        # customers has no column schema (columns_to_types was None/empty)
    }
    stdout_json = json.dumps({
        "rendered": rendered,
        "errors": [],
        "column_schemas": column_schemas,
    })

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        results = renderer.render_project(project_path=tmp_path)

    # orders should have column defs
    orders = results['"db"."staging"."orders"']
    assert len(orders.columns) == 2
    assert all(isinstance(c, ColumnDefResult) for c in orders.columns)
    assert all(c.source == "sqlmesh_schema" for c in orders.columns)

    # customers should have no column defs
    customers = results['"db"."staging"."customers"']
    assert len(customers.columns) == 0


def test_sqlmesh_column_schema_absent_key(tmp_path):
    """Subprocess JSON without column_schemas key still works (backwards compatible)."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    # Old-format output without column_schemas key
    rendered = {'"db"."staging"."orders"': "SELECT order_id FROM raw.orders"}
    stdout_json = json.dumps({
        "rendered": rendered,
        "errors": [],
    })

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        results = renderer.render_project(project_path=tmp_path)

    result = results['"db"."staging"."orders"']
    assert len([c for c in result.columns if c.source == "sqlmesh_schema"]) == 0


def test_build_column_defs_realistic_types():
    """_build_column_defs handles realistic SQL type strings."""
    from sqlprism.languages.sqlmesh import _build_column_defs

    cols = _build_column_defs("my_model", {
        "id": "INT64",
        "name": "VARCHAR(255)",
        "amount": "DECIMAL(10, 2)",
        "created_at": "TIMESTAMP",
    })
    assert len(cols) == 4
    assert cols[0].column_name == "id"
    assert cols[0].data_type == "INT64"
    assert cols[1].data_type == "VARCHAR(255)"
    assert cols[2].data_type == "DECIMAL(10, 2)"
    assert cols[3].data_type == "TIMESTAMP"
    assert cols[0].position == 0
    assert cols[3].position == 3
    assert all(c.source == "sqlmesh_schema" for c in cols)


# ── DbtRenderer.extract_schema_yml tests (#24) ──


def _write_yaml(path, content):
    """Helper to write YAML content to a file, creating parent dirs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_extract_schema_yml_standard(tmp_path):
    """Extract columns from a standard schema.yml with descriptions."""
    models_dir = tmp_path / "models"
    _write_yaml(
        models_dir / "schema.yml",
        """\
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        description: "Primary key"
      - name: status
        description: "Order status"
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)

    assert "stg_orders" in result
    cols = result["stg_orders"]
    assert len(cols) == 2

    assert cols[0].column_name == "order_id"
    assert cols[0].description == "Primary key"
    assert cols[0].source == "schema_yml"
    assert cols[0].node_name == "stg_orders"
    assert cols[0].position == 0

    assert cols[1].column_name == "status"
    assert cols[1].description == "Order status"
    assert cols[1].position == 1


def test_extract_schema_yml_multiple_files(tmp_path):
    """Extract columns from multiple YAML files across subdirectories."""
    _write_yaml(
        tmp_path / "models" / "staging" / "schema.yml",
        """\
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        description: "PK"
""",
    )
    _write_yaml(
        tmp_path / "models" / "marts" / "schema.yml",
        """\
version: 2

models:
  - name: fct_revenue
    columns:
      - name: revenue
        description: "Total revenue"
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)

    assert "stg_orders" in result
    assert "fct_revenue" in result
    assert result["stg_orders"][0].column_name == "order_id"
    assert result["fct_revenue"][0].column_name == "revenue"


def test_extract_schema_yml_no_columns(tmp_path):
    """Model with no columns key produces no ColumnDefResult entries."""
    _write_yaml(
        tmp_path / "models" / "schema.yml",
        """\
version: 2

models:
  - name: stg_orders
    description: "Staging orders model"
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)

    assert "stg_orders" not in result


def test_extract_schema_yml_tests_no_desc(tmp_path):
    """Columns with tests but no description produce entries with description=None."""
    _write_yaml(
        tmp_path / "models" / "schema.yml",
        """\
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - not_null
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)

    assert "stg_orders" in result
    cols = result["stg_orders"]
    assert len(cols) == 2
    assert cols[0].column_name == "order_id"
    assert cols[0].description is None
    assert cols[1].column_name == "status"
    assert cols[1].description is None


def test_extract_schema_yml_non_standard_names(tmp_path):
    """Non-standard YAML filenames like _sources.yml and _models.yml are parsed."""
    _write_yaml(
        tmp_path / "models" / "_sources.yml",
        """\
version: 2

models:
  - name: src_payments
    columns:
      - name: payment_id
        description: "Payment PK"
""",
    )
    _write_yaml(
        tmp_path / "models" / "staging" / "_models.yaml",
        """\
version: 2

models:
  - name: stg_customers
    columns:
      - name: customer_id
        description: "Customer PK"
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)

    assert "src_payments" in result
    assert result["src_payments"][0].column_name == "payment_id"

    assert "stg_customers" in result
    assert result["stg_customers"][0].column_name == "customer_id"


def test_extract_schema_yml_no_models_dir(tmp_path):
    """Missing models/ directory returns empty dict."""
    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)
    assert result == {}


def test_extract_schema_yml_malformed_yaml(tmp_path):
    """Malformed YAML files are skipped without raising."""
    _write_yaml(
        tmp_path / "models" / "broken.yml",
        "!!invalid: [yaml\n  bad: {indent",
    )
    _write_yaml(
        tmp_path / "models" / "good.yml",
        """\
version: 2

models:
  - name: good_model
    columns:
      - name: id
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)
    # Broken file skipped, good file still parsed
    assert "good_model" in result
    assert result["good_model"][0].column_name == "id"


def test_extract_schema_yml_duplicate_model_across_files(tmp_path):
    """Same model in two files: columns merged with offset positions."""
    _write_yaml(
        tmp_path / "models" / "a.yml",
        """\
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
      - name: status
""",
    )
    _write_yaml(
        tmp_path / "models" / "b.yml",
        """\
version: 2

models:
  - name: stg_orders
    columns:
      - name: amount
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)
    assert "stg_orders" in result
    cols = result["stg_orders"]
    assert len(cols) == 3
    names = [c.column_name for c in cols]
    assert "order_id" in names
    assert "status" in names
    assert "amount" in names
    # Positions should not collide — second file offset by first file's count
    positions = [c.position for c in cols]
    assert len(set(positions)) == 3  # all unique


def test_parallel_parse_matches_sequential():
    """Parallel parsing produces identical results to sequential."""
    from sqlprism.languages.sqlmesh import SqlMeshRenderer

    renderer = SqlMeshRenderer()

    models = {
        "model_a": "SELECT id, name FROM raw.users WHERE active = true",
        "model_b": "SELECT o.id, u.name FROM raw.orders o JOIN raw.users u ON o.user_id = u.id",
        "model_c": "WITH cte AS (SELECT * FROM raw.events) SELECT * FROM cte",
        "model_d": "SELECT count(*) as cnt, status FROM raw.orders GROUP BY status",
        "model_e": "SELECT a.id FROM raw.a a LEFT JOIN raw.b b ON a.id = b.a_id WHERE b.id IS NULL",
    }
    column_schemas = {}

    # Use the same code path for both to ensure apples-to-apples comparison
    sequential_results = renderer._parse_models_sequential(models, column_schemas, schema_catalog=None)
    parallel_results = renderer._parse_models_parallel(models, column_schemas, schema_catalog=None)

    for name in models:
        seq = sequential_results[name]
        par = parallel_results[name]
        assert len(seq.nodes) == len(par.nodes), f"{name}: node count mismatch"
        assert len(seq.edges) == len(par.edges), f"{name}: edge count mismatch"
        assert len(seq.column_usage) == len(par.column_usage), f"{name}: column_usage mismatch"


def test_extract_schema_yml_sources(tmp_path):
    """sources: blocks with tables and columns are extracted."""
    _write_yaml(
        tmp_path / "models" / "sources.yml",
        """\
version: 2

sources:
  - name: raw
    tables:
      - name: orders
        columns:
          - name: order_id
            description: "PK"
          - name: total
      - name: customers
        columns:
          - name: customer_id
""",
    )

    renderer = DbtRenderer()
    result = renderer.extract_schema_yml(tmp_path)
    assert "raw.orders" in result
    assert len(result["raw.orders"]) == 2
    assert result["raw.orders"][0].column_name == "order_id"
    assert result["raw.orders"][0].description == "PK"
    assert result["raw.orders"][0].source == "schema_yml"
    assert "raw.customers" in result
    assert result["raw.customers"][0].column_name == "customer_id"
