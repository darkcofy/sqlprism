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
            assert "dbt_model" in node.metadata, "Nodes should have dbt_model metadata"
            assert node.metadata["dbt_model"] == rel_path


# ── SqlMeshRenderer.render_project mocked tests ──


def test_sqlmesh_render_project_command_construction(tmp_path):
    """Verify the inline Python script is passed correctly to subprocess."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
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

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
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

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
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

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")

        results = renderer.render_project(project_path=tmp_path)

    assert len(results) == 2, "Should have two model results"

    for model_name, parse_result in results.items():
        assert model_name in rendered_models, "Key should be original model name"
        assert len(parse_result.nodes) > 0, f"Model {model_name} should have nodes"
        for node in parse_result.nodes:
            assert "sqlmesh_model" in node.metadata, "Nodes should have sqlmesh_model metadata"
            assert node.metadata["sqlmesh_model"] == model_name


def test_sqlmesh_render_project_bad_json(tmp_path):
    """Verify error when subprocess returns non-JSON stdout."""
    (tmp_path / ".venv").mkdir()

    renderer = SqlMeshRenderer()

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
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

    with patch("sqlprism.languages.sqlmesh.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=stdout_json, stderr="")
        renderer.render_project(project_path=tmp_path)

    # render_project passes empty model_filter (render all)
    cmd = mock_run.call_args[0][0]
    assert json.loads(cmd[-1]) == [], "render_project should pass empty filter"
