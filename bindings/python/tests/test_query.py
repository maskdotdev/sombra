import asyncio
import tempfile
from pathlib import Path
from typing import Any

from sombra_py import Database


def temp_db_path() -> str:
    tmp_dir = Path(tempfile.mkdtemp())
    return str(tmp_dir / "db")


def test_execute_query_returns_rows() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    rows = db.query().match("User").where_prop("a", "name", "=", "Ada").select(["a"]).execute()
    assert len(rows) == 1
    assert rows[0]["a"] > 0


def test_stream_iterates_results() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    async def collect():
        results = []
        async for row in db.query().match("User").select(["a"]).stream():
            results.append(row)
        return results

    rows = asyncio.run(collect())
    assert len(rows) >= 3


def test_explain_plan_shape() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    plan = db.query().match("User").where("FOLLOWS", "User").select(["a", "b"]).explain()
    assert plan["plan"]["op"] == "Project"


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
    db.pragma("autocheckpoint_ms", None)
    assert db.pragma("autocheckpoint_ms") is None
