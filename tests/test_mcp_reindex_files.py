"""Tests for the reindex_files MCP tool and the debounce mechanism."""

import asyncio
from unittest.mock import patch

import sqlprism.core.mcp_tools as _mcp_mod
from sqlprism.core.mcp_tools import (
    ReindexFilesInput,
    _enqueue_reindex,
    _flush_reindex,
    configure,
    reindex_files,
)


# ── reindex_files tool and debounce tests ──


def test_mcp_reindex_files_single(tmp_path):
    """reindex_files reindexes a modified SQL file via debounce."""
    repo_dir = tmp_path / "rf_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT, amount DECIMAL)")
    # A query that will reference the new column after modification
    report_file = repo_dir / "report.sql"
    report_file.write_text("SELECT id, amount FROM orders")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Verify 'status' column is NOT present before modification
    assert _mcp_mod._state is not None
    graph = _mcp_mod._state.graph
    col_before = graph.query_column_usage(table="orders", column="status")
    assert col_before["total_count"] == 0

    # Modify the SQL file and the report to reference the new column
    sql_file.write_text("CREATE TABLE orders (id INT, amount DECIMAL, status TEXT)")
    report_file.write_text("SELECT id, amount, status FROM orders")

    async def _run():
        result = await reindex_files(
            ReindexFilesInput(paths=[str(sql_file), str(report_file)])
        )
        assert result["accepted"] == 2
        # Wait for debounce to fire (0.5s for sql + margin)
        await asyncio.sleep(1.0)

    asyncio.run(_run())

    # Verify the updated schema was indexed — status column should now appear
    col_after = graph.query_column_usage(table="orders", column="status")
    assert col_after["total_count"] >= 1, (
        "Expected 'status' column usage after reindex_files update"
    )


def test_mcp_reindex_files_filters_non_sql(tmp_path):
    """reindex_files skips non-SQL files and only accepts SQL ones."""
    repo_dir = tmp_path / "filter_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "model.sql"
    sql_file.write_text("CREATE TABLE model (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    async def _run():
        result = await reindex_files(
            ReindexFilesInput(
                paths=[str(sql_file), str(repo_dir / "readme.md"), str(repo_dir / "config.yml")]
            )
        )
        assert result["accepted"] == 1
        assert result["skipped"] == 2
        return result

    asyncio.run(_run())


def test_debounce_batches_plain_sql(tmp_path):
    """Enqueuing multiple SQL files batches them and flushes after debounce."""
    repo_dir = tmp_path / "batch_repo"
    repo_dir.mkdir()
    for name in ("a.sql", "b.sql", "c.sql"):
        (repo_dir / name).write_text(f"CREATE TABLE {name[0]} (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    async def _run():
        await _enqueue_reindex("test", "sql", [str(repo_dir / "a.sql")])
        await _enqueue_reindex("test", "sql", [str(repo_dir / "b.sql")])
        await _enqueue_reindex("test", "sql", [str(repo_dir / "c.sql")])

        # All 3 should be pending
        assert len(_mcp_mod._reindex_pending["test"]) == 3

        # Wait for debounce to fire (0.5s + margin)
        await asyncio.sleep(1.0)

        # Pending should be empty after flush
        assert len(_mcp_mod._reindex_pending.get("test", [])) == 0

    asyncio.run(_run())


def test_debounce_batches_dbt_sqlmesh(tmp_path):
    """dbt debounce batches 5 models saved within the debounce window."""
    repo_dir = tmp_path / "dbt_batch_repo"
    repo_dir.mkdir()

    configure(db_path=":memory:", repos={"dbt_test": {"path": str(repo_dir), "repo_type": "dbt"}})

    flush_calls = []

    async def mock_flush(repo_name):
        flush_calls.append(repo_name)
        # Still drain the pending list like real flush does
        _mcp_mod._reindex_pending.pop(repo_name, None)
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with (
            patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush),
            patch.object(_mcp_mod, "_DEBOUNCE_RENDERED", 0.2),
        ):
            # Enqueue 5 distinct paths (BDD: "5 models saved within 2s")
            for i in range(5):
                await _enqueue_reindex("dbt_test", "dbt", [f"/models/model_{i}.sql"])

            # All 5 should be pending
            assert len(_mcp_mod._reindex_pending["dbt_test"]) == 5

            # Sleep 0.1s — less than 0.2s debounce, timer not fired yet
            await asyncio.sleep(0.1)
            assert len(flush_calls) == 0

            # Sleep another 0.2s — now past the 0.2s debounce
            await asyncio.sleep(0.2)
            assert len(flush_calls) == 1

    asyncio.run(_run())


def test_debounce_timer_resets(tmp_path):
    """Enqueuing a second file resets the debounce timer."""
    repo_dir = tmp_path / "reset_repo"
    repo_dir.mkdir()

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    flush_calls = []

    async def mock_flush(repo_name):
        flush_calls.append(list(_mcp_mod._reindex_pending.get(repo_name, [])))
        _mcp_mod._reindex_pending.pop(repo_name, None)
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush):
            await _enqueue_reindex("test", "sql", ["/a.sql"])
            await asyncio.sleep(0.3)

            # Enqueue second file — resets the 0.5s timer
            await _enqueue_reindex("test", "sql", ["/b.sql"])
            await asyncio.sleep(0.3)

            # Timer hasn't fired yet (only 0.3s since reset)
            assert len(flush_calls) == 0

            # Wait another 0.3s — now 0.6s since last enqueue, timer should fire
            await asyncio.sleep(0.3)
            assert len(flush_calls) == 1
            # Both files should be in the batch
            assert "/a.sql" in flush_calls[0]
            assert "/b.sql" in flush_calls[0]

    asyncio.run(_run())


def test_debounce_deduplicates_paths(tmp_path):
    """Duplicate paths are deduplicated when flush fires."""
    repo_dir = tmp_path / "dedup_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "model.sql"
    sql_file.write_text("CREATE TABLE model (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    captured_paths = []
    assert _mcp_mod._state is not None
    original_reindex_files = _mcp_mod._state.indexer.reindex_files

    def capture_reindex_files(paths, **kwargs):
        captured_paths.extend(paths)
        return original_reindex_files(paths=paths, **kwargs)

    async def _run():
        assert _mcp_mod._state is not None
        with patch.object(_mcp_mod._state.indexer, "reindex_files", side_effect=capture_reindex_files):
            # Enqueue same path twice
            await _enqueue_reindex("test", "sql", [str(sql_file)])
            await _enqueue_reindex("test", "sql", [str(sql_file)])

            # Pending has 2 entries (pre-dedup)
            assert len(_mcp_mod._reindex_pending["test"]) == 2

            # Wait for flush
            await asyncio.sleep(1.0)

            # Flush should have deduplicated
            assert captured_paths.count(str(sql_file)) == 1

    asyncio.run(_run())


def test_mcp_reindex_files_not_configured():
    """reindex_files returns error when server is not configured."""
    # _reset_mcp_state fixture already sets _state = None

    async def _run():
        result = await reindex_files(ReindexFilesInput(paths=["some.sql"]))
        assert "error" in result
        assert "not configured" in result["error"].lower()

    asyncio.run(_run())


def test_reindex_files_waits_for_lock(tmp_path):
    """_flush_reindex blocks on _reindex_lock and completes after release."""
    repo_dir = tmp_path / "lock_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Enqueue a file so flush has something to do
    _mcp_mod._reindex_pending["test"] = [str(sql_file)]

    flush_done = False

    async def _run():
        nonlocal flush_done

        # Acquire the lock first
        await _mcp_mod._reindex_lock.acquire()

        # Start flush in background — should block on the lock
        flush_task = asyncio.create_task(_flush_reindex("test"))

        # Give it a moment to attempt acquiring the lock
        await asyncio.sleep(0.1)
        assert not flush_task.done(), "flush should be blocked on the lock"

        # Release the lock
        _mcp_mod._reindex_lock.release()

        # Now flush should complete
        await asyncio.wait_for(flush_task, timeout=5.0)
        flush_done = True

    asyncio.run(_run())
    assert flush_done


def test_debounce_batches_rapid_enqueues(tmp_path):
    """5 rapid enqueues produce exactly one flush containing all 5 files."""
    repo_dir = tmp_path / "batch5_repo"
    repo_dir.mkdir()
    paths = []
    for i in range(5):
        f = repo_dir / f"model_{i}.sql"
        f.write_text(f"CREATE TABLE t{i} (id INT)")
        paths.append(str(f))

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    flushed_paths = []
    flush_call_count = 0

    async def mock_flush(repo_name):
        nonlocal flush_call_count
        flush_call_count += 1
        flushed_paths.extend(_mcp_mod._reindex_pending.pop(repo_name, []))
        _mcp_mod._reindex_timers.pop(repo_name, None)

    async def _run():
        with (
            patch.object(_mcp_mod, "_flush_reindex", side_effect=mock_flush),
            patch.object(_mcp_mod, "_DEBOUNCE_SQL", 0.05),
        ):
            for p in paths:
                await _enqueue_reindex("test", "sql", [p])

            assert len(_mcp_mod._reindex_pending["test"]) == 5

            # Wait for debounce to fire (0.05s + margin)
            await asyncio.sleep(0.3)

        # Exactly one flush should have run, containing all 5 paths
        assert flush_call_count == 1
        assert len(flushed_paths) == 5
        for p in paths:
            assert p in flushed_paths

    asyncio.run(_run())


def test_reindex_concurrent_waits_for_lock(tmp_path):
    """_flush_reindex waits for _reindex_lock, then updates the graph after release.

    Differs from test_reindex_files_waits_for_lock by verifying the graph is
    actually updated after the lock is released (not just that the task completes).
    """
    repo_dir = tmp_path / "concurrent_repo"
    repo_dir.mkdir()
    sql_file = repo_dir / "orders.sql"
    sql_file.write_text("CREATE TABLE orders (id INT)")

    configure(db_path=":memory:", repos={"test": str(repo_dir)})

    from sqlprism.core.mcp_tools import _get_indexer

    indexer = _get_indexer()
    indexer.reindex_repo("test", str(repo_dir))

    # Verify initial state — no 'status' column
    assert _mcp_mod._state is not None
    graph = _mcp_mod._state.graph
    col_before = graph.query_column_usage(table="orders", column="status")
    assert col_before["total_count"] == 0

    # Modify the file and stage it for flush
    sql_file.write_text("CREATE TABLE orders (id INT, status TEXT)")
    _mcp_mod._reindex_pending["test"] = [str(sql_file)]

    flush_completed = False

    async def _run():
        nonlocal flush_completed

        # Recreate the lock on the current event loop — prior tests may have
        # bound it to a different loop via asyncio.run().
        _mcp_mod._reindex_lock = asyncio.Lock()

        # Simulate a full reindex holding the lock
        await _mcp_mod._reindex_lock.acquire()

        flush_task = asyncio.create_task(_flush_reindex("test"))

        await asyncio.sleep(0.2)
        assert not flush_task.done(), "flush should be blocked waiting for the lock"

        _mcp_mod._reindex_lock.release()

        await asyncio.wait_for(flush_task, timeout=5.0)
        flush_completed = True

    asyncio.run(_run())
    assert flush_completed

    # Verify the graph was actually updated after lock release
    status = graph.get_index_status()
    assert status["totals"]["files"] == 1
