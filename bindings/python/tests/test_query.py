import asyncio
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pytest

from sombra import Database
import sombra.query as query
from sombra.query import (
    _literal_value,
    eq,
    ErrorCode,
    SombraError,
    AnalyzerError,
    JsonError,
    IoError,
    CorruptionError,
    ConflictError,
    SnapshotTooOldError,
    CancelledError,
    InvalidArgError,
    NotFoundError,
    ClosedError,
    wrap_native_error,
)


def temp_db_path() -> str:
    tmp_dir = Path(tempfile.mkdtemp())
    return str(tmp_dir / "db")


def test_execute_query_returns_rows() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    rows = db.query().nodes("User").where(eq("name", "Ada")).execute()
    assert len(rows) == 1
    record = rows[0]["n0"]
    assert isinstance(record, dict)
    assert record["_id"] > 0
    assert isinstance(record["props"], dict)


def test_execute_with_meta_returns_envelope() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    payload = (
        db.query()
        .nodes("User")
        .request_id("req-py-meta")
        .select("name")
        .execute(with_meta=True)
    )
    rows = payload.rows()
    assert len(rows) >= 1
    assert payload.request_id() == "req-py-meta"


def test_stream_iterates_results() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    async def collect():
        results = []
        async for row in db.query().nodes("User").stream():
            results.append(row)
        return results

    rows = asyncio.run(collect())
    assert len(rows) >= 3
    assert all("n0" in row and isinstance(row["n0"], dict) for row in rows)


def test_stream_close_stops_iteration() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    async def run() -> None:
        stream = db.query().nodes("User").stream()
        first = await stream.__anext__()
        assert "n0" in first
        stream.close()
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()

    asyncio.run(run())


def test_explain_plan_shape() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    plan = db.query().match("User").where("FOLLOWS", "User").select(["n0", "n1"]).explain()
    assert isinstance(plan["plan"], list)
    assert plan["plan"][0]["op"] == "Project"


def test_request_id_round_trip() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    plan = db.query().nodes("User").request_id("req-py").where(eq("name", "Ada")).explain()
    assert plan["request_id"] == "req-py"


def test_mutate_crud_helpers() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    node_id = db.create_node("User", {"name": "PyBench"})
    assert node_id is not None

    db.update_node(node_id, set_props={"bio": "updated"})
    db.delete_node(node_id, cascade=True)


def test_mutate_many_batches_ops() -> None:
    db = Database.open(temp_db_path())
    summary = db.mutate_many(
        [
            {"op": "createNode", "labels": ["User"], "props": {"name": "BatchA"}},
            {"op": "createNode", "labels": ["User"], "props": {"name": "BatchB"}},
        ]
    )
    created = summary.get("createdNodes") or []
    assert len(created) == 2


def test_mutate_batched_chunks_ops() -> None:
    db = Database.open(temp_db_path())
    ops = [
        {"op": "createNode", "labels": ["User"], "props": {"name": "ChunkA"}},
        {"op": "createNode", "labels": ["User"], "props": {"name": "ChunkB"}},
        {"op": "createNode", "labels": ["User"], "props": {"name": "ChunkC"}},
    ]
    summary = db.mutate_batched(ops, batch_size=2)
    created = summary.get("createdNodes") or []
    assert len(created) == 3


def test_parallel_read_and_write_share_single_handle() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    async def run() -> tuple[list[dict[str, Any]], Optional[int]]:
        async def do_read() -> list[dict[str, Any]]:
            await asyncio.sleep(0)
            return db.query().nodes("User").execute()

        async def do_write() -> Optional[int]:
            await asyncio.sleep(0)
            return db.create_node("User", {"name": "Concurrent"})

        return await asyncio.gather(do_read(), do_write())

    rows, new_id = asyncio.run(run())
    assert isinstance(rows, list)
    assert new_id is None or isinstance(new_id, int)


def test_create_builder_handle_refs() -> None:
    db = Database.open(temp_db_path())
    builder = db.create()
    alice = builder.node("User", {"name": "Alice"})
    bob = builder.node(["User"], {"name": "Bob"})
    builder.edge(alice, "KNOWS", bob, {"since": 2020})
    summary = builder.execute()
    assert len(summary["nodes"]) == 2
    assert len(summary["edges"]) == 1
    assert summary.alias("$missing") is None


def test_create_builder_alias_chain() -> None:
    db = Database.open(temp_db_path())
    summary = (
        db.create()
        .node("User", {"name": "alice"}, "$alice")
        .node("User", {"name": "bob"}, "$bob")
        .node("Company", {"name": "Acme Inc"}, "$company")
        .edge("$alice", "FOLLOWS", "$bob")
        .edge("$alice", "WORKS_AT", "$company", {"role": "Engineer"})
        .execute()
    )
    assert len(summary["nodes"]) == 3
    assert len(summary["edges"]) == 2
    assert summary["aliases"]["$alice"] > 0
    assert summary.alias("$alice") == summary["aliases"]["$alice"]


def test_transaction_collects_ops() -> None:
    db = Database.open(temp_db_path())

    def builder(tx: Any) -> str:
        tx.create_node("User", {"name": "TxPy"})
        tx.create_node("User", {"name": "TxPy2"})
        return "done"

    result, summary = db.transaction(builder)
    created = summary.get("createdNodes") or []
    assert len(created) == 2
    assert result == "done"


def test_pragma_round_trip() -> None:
    db = Database.open(temp_db_path())
    db.pragma("synchronous", "normal")
    assert db.pragma("synchronous") == "normal"
    db.pragma("autocheckpoint_ms", 7)
    assert db.pragma("autocheckpoint_ms") == 7


def test_property_projections_return_scalars() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    rows = db.query().nodes("User").select("name").execute()
    assert len(rows) > 0
    assert isinstance(rows[0]["name"], str)


def test_literal_value_datetime_supports_timezone() -> None:
    aware = datetime(2020, 1, 1, tzinfo=timezone.utc)
    literal = _literal_value(aware)
    assert literal["t"] == "DateTime"
    assert isinstance(literal["v"], int)

    naive = datetime(2020, 1, 1)
    try:
        _literal_value(naive)
    except ValueError as exc:
        assert "timezone" in str(exc)
    else:
        raise AssertionError("expected ValueError for naive datetime")


def test_runtime_schema_validation_rejects_unknown_property() -> None:
    db = Database.open(temp_db_path(), schema={"User": {"name": {"type": "string"}}})
    db.seed_demo()

    db.query().nodes("User").where(eq("name", "Ada"))

    try:
        db.query().nodes("User").where(eq("unknown_prop", "x"))
    except ValueError as exc:
        assert "Unknown property 'unknown_prop'" in str(exc)
    else:
        raise AssertionError("expected ValueError for invalid predicate property")

    try:
        db.query().nodes("User").select("bogus")
    except ValueError as exc:
        assert "Unknown property 'bogus'" in str(exc)
    else:
        raise AssertionError("expected ValueError for invalid projection property")


# ============================================================================
# Database Lifecycle Tests
# ============================================================================


def test_close_marks_database_as_closed() -> None:
    db = Database.open(temp_db_path())
    assert db.is_closed is False
    db.close()
    assert db.is_closed is True


def test_close_is_idempotent() -> None:
    db = Database.open(temp_db_path())
    db.close()
    db.close()  # Should not raise
    db.close()
    assert db.is_closed is True


def test_operations_on_closed_database_raise_error() -> None:
    db = Database.open(temp_db_path())
    db.close()

    with pytest.raises(ClosedError, match="database is closed"):
        db.seed_demo()

    with pytest.raises(ClosedError, match="database is closed"):
        db.query()

    with pytest.raises(ClosedError, match="database is closed"):
        db.create()

    with pytest.raises(ClosedError, match="database is closed"):
        db.mutate({"ops": []})

    with pytest.raises(ClosedError, match="database is closed"):
        db.pragma("synchronous")


def test_create_builder_execute_on_closed_db() -> None:
    db = Database.open(temp_db_path())
    builder = db.create()
    builder.node("User", {"name": "Test"})
    db.close()

    with pytest.raises(ClosedError, match="database is closed"):
        builder.execute()


def test_context_manager_closes_database() -> None:
    path = temp_db_path()
    with Database.open(path) as db:
        assert db.is_closed is False
        db.seed_demo()
        rows = db.query().nodes("User").execute()
        assert len(rows) >= 1
    # After exiting, database should be closed
    assert db.is_closed is True


def test_context_manager_closes_on_exception() -> None:
    path = temp_db_path()
    db = None
    try:
        with Database.open(path) as db:
            db.seed_demo()
            raise ValueError("intentional error")
    except ValueError:
        pass
    assert db is not None
    assert db.is_closed is True


def test_native_errors_are_wrapped(monkeypatch: Any) -> None:
    db = Database.open(temp_db_path())

    def boom_execute(handle: Any, spec: Any) -> Any:
        raise RuntimeError("[IO] boom")

    monkeypatch.setattr(query._native, "database_execute", boom_execute)
    with pytest.raises(IoError):
        db.query().nodes("User").execute()

    def boom_mutate(handle: Any, script: Any) -> Any:
        raise RuntimeError("[CONFLICT] nope")

    monkeypatch.setattr(query._native, "database_mutate", boom_mutate)
    with pytest.raises(ConflictError):
        db.create_node("User", {"name": "x"})

    def boom_stream(handle: Any, spec: Any) -> Any:
        raise RuntimeError("[CORRUPTION] broken")

    monkeypatch.setattr(query._native, "database_stream", boom_stream)
    with pytest.raises(CorruptionError):
        db.query().nodes("User").stream()


# ============================================================================
# Error Code Tests
# ============================================================================


def test_error_code_constants_defined() -> None:
    assert ErrorCode.UNKNOWN == "UNKNOWN"
    assert ErrorCode.MESSAGE == "MESSAGE"
    assert ErrorCode.ANALYZER == "ANALYZER"
    assert ErrorCode.JSON == "JSON"
    assert ErrorCode.IO == "IO"
    assert ErrorCode.CORRUPTION == "CORRUPTION"
    assert ErrorCode.CONFLICT == "CONFLICT"
    assert ErrorCode.SNAPSHOT_TOO_OLD == "SNAPSHOT_TOO_OLD"
    assert ErrorCode.CANCELLED == "CANCELLED"
    assert ErrorCode.INVALID_ARG == "INVALID_ARG"
    assert ErrorCode.NOT_FOUND == "NOT_FOUND"
    assert ErrorCode.CLOSED == "CLOSED"


# ============================================================================
# Error Class Hierarchy Tests
# ============================================================================


def test_sombra_error_defaults() -> None:
    err = SombraError("test message")
    assert str(err) == "test message"
    assert err.code == ErrorCode.UNKNOWN
    assert isinstance(err, Exception)


def test_sombra_error_custom_code() -> None:
    err = SombraError("test", ErrorCode.IO)
    assert err.code == ErrorCode.IO


def test_analyzer_error_code() -> None:
    err = AnalyzerError("bad query")
    assert err.code == ErrorCode.ANALYZER
    assert isinstance(err, SombraError)


def test_json_error_code() -> None:
    err = JsonError("parse failed")
    assert err.code == ErrorCode.JSON
    assert isinstance(err, SombraError)


def test_io_error_code() -> None:
    err = IoError("disk error")
    assert err.code == ErrorCode.IO
    assert isinstance(err, SombraError)


def test_corruption_error_code() -> None:
    err = CorruptionError("data corrupt")
    assert err.code == ErrorCode.CORRUPTION
    assert isinstance(err, SombraError)


def test_conflict_error_code() -> None:
    err = ConflictError("write conflict")
    assert err.code == ErrorCode.CONFLICT
    assert isinstance(err, SombraError)


def test_snapshot_too_old_error_code() -> None:
    err = SnapshotTooOldError("snapshot evicted")
    assert err.code == ErrorCode.SNAPSHOT_TOO_OLD
    assert isinstance(err, SombraError)


def test_cancelled_error_code() -> None:
    err = CancelledError("request cancelled")
    assert err.code == ErrorCode.CANCELLED
    assert isinstance(err, SombraError)


def test_invalid_arg_error_code() -> None:
    err = InvalidArgError("bad argument")
    assert err.code == ErrorCode.INVALID_ARG
    assert isinstance(err, SombraError)


def test_not_found_error_code() -> None:
    err = NotFoundError("not found")
    assert err.code == ErrorCode.NOT_FOUND
    assert isinstance(err, SombraError)


def test_closed_error_code() -> None:
    err = ClosedError("db closed")
    assert err.code == ErrorCode.CLOSED
    assert isinstance(err, SombraError)


# ============================================================================
# wrap_native_error Tests
# ============================================================================


def test_wrap_native_error_parses_analyzer() -> None:
    err = wrap_native_error(RuntimeError("[ANALYZER] invalid syntax"))
    assert isinstance(err, AnalyzerError)
    assert err.code == ErrorCode.ANALYZER
    assert str(err) == "invalid syntax"


def test_wrap_native_error_parses_io() -> None:
    err = wrap_native_error(RuntimeError("[IO] file not found"))
    assert isinstance(err, IoError)
    assert err.code == ErrorCode.IO
    assert str(err) == "file not found"


def test_wrap_native_error_parses_corruption() -> None:
    err = wrap_native_error(RuntimeError("[CORRUPTION] page checksum mismatch"))
    assert isinstance(err, CorruptionError)
    assert err.code == ErrorCode.CORRUPTION
    assert str(err) == "page checksum mismatch"


def test_wrap_native_error_parses_conflict() -> None:
    err = wrap_native_error(RuntimeError("[CONFLICT] write-write conflict"))
    assert isinstance(err, ConflictError)
    assert err.code == ErrorCode.CONFLICT
    assert str(err) == "write-write conflict"


def test_wrap_native_error_parses_snapshot_too_old() -> None:
    err = wrap_native_error(RuntimeError("[SNAPSHOT_TOO_OLD] reader evicted"))
    assert isinstance(err, SnapshotTooOldError)
    assert err.code == ErrorCode.SNAPSHOT_TOO_OLD
    assert str(err) == "reader evicted"


def test_wrap_native_error_parses_cancelled() -> None:
    err = wrap_native_error(RuntimeError("[CANCELLED] operation cancelled"))
    assert isinstance(err, CancelledError)
    assert err.code == ErrorCode.CANCELLED
    assert str(err) == "operation cancelled"


def test_wrap_native_error_parses_invalid_arg() -> None:
    err = wrap_native_error(RuntimeError("[INVALID_ARG] bad parameter"))
    assert isinstance(err, InvalidArgError)
    assert err.code == ErrorCode.INVALID_ARG
    assert str(err) == "bad parameter"


def test_wrap_native_error_parses_not_found() -> None:
    err = wrap_native_error(RuntimeError("[NOT_FOUND] node does not exist"))
    assert isinstance(err, NotFoundError)
    assert err.code == ErrorCode.NOT_FOUND
    assert str(err) == "node does not exist"


def test_wrap_native_error_parses_closed() -> None:
    err = wrap_native_error(RuntimeError("[CLOSED] database closed"))
    assert isinstance(err, ClosedError)
    assert err.code == ErrorCode.CLOSED
    assert str(err) == "database closed"


def test_wrap_native_error_parses_json() -> None:
    err = wrap_native_error(RuntimeError("[JSON] invalid json"))
    assert isinstance(err, JsonError)
    assert err.code == ErrorCode.JSON
    assert str(err) == "invalid json"


def test_wrap_native_error_unknown_code() -> None:
    err = wrap_native_error(RuntimeError("[UNKNOWN] something went wrong"))
    assert isinstance(err, SombraError)
    assert err.code == ErrorCode.UNKNOWN
    assert str(err) == "something went wrong"


def test_wrap_native_error_no_prefix() -> None:
    err = wrap_native_error(RuntimeError("no prefix here"))
    assert isinstance(err, SombraError)
    assert err.code == ErrorCode.UNKNOWN
    assert str(err) == "no prefix here"


def test_wrap_native_error_preserves_sombra_error() -> None:
    original = IoError("already typed")
    wrapped = wrap_native_error(original)
    assert wrapped is original
