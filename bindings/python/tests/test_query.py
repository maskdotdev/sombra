import asyncio
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sombra import Database
from sombra.query import _literal_value, eq


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
