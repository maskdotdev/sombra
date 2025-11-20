import tempfile
from pathlib import Path
from typing import TypedDict

import pytest
from typing_extensions import Literal

from sombra.typed import NodeSchema, SombraDB, TypedGraphSchema


class PersonProps(TypedDict):
    name: str
    age: int


class CompanyProps(TypedDict):
    name: str


class PersonNode(NodeSchema):
    properties: PersonProps


class CompanyNode(NodeSchema):
    properties: CompanyProps


class GraphNodes(TypedDict):
    Person: PersonNode
    Company: CompanyNode


class WorksAtProps(TypedDict):
    role: str


WorksAtEdge = TypedDict(
    "WorksAtEdge",
    {
        "from": Literal["Person"],
        "to": Literal["Company"],
        "properties": WorksAtProps,
    },
)


class GraphEdges(TypedDict):
    WORKS_AT: WorksAtEdge


class DemoGraphSchema(TypedGraphSchema):
    nodes: GraphNodes
    edges: GraphEdges


SCHEMA: DemoGraphSchema = {
    "nodes": {
        "Person": {"properties": {"name": "", "age": 0}},
        "Company": {"properties": {"name": ""}},
    },
    "edges": {
        "WORKS_AT": {
            "from": "Person",
            "to": "Company",
            "properties": {"role": ""},
        }
    },
}


def temp_db_path() -> str:
    tmp_dir = Path(tempfile.mkdtemp())
    return str(tmp_dir / "typed.db")


def test_typed_facade_crud_and_traversal() -> None:
    db = SombraDB(temp_db_path(), schema=SCHEMA)

    alice = db.add_node("Person", {"name": "Alice", "age": 32})
    acme = db.add_node("Company", {"name": "Acme"})
    db.add_edge(alice, acme, "WORKS_AT", {"role": "Engineer"})

    stored = db.get_node(alice, "Person")
    assert stored is not None
    assert stored["properties"]["name"] == "Alice"

    lookup = db.find_node_by_property("Person", "name", "Alice")
    assert lookup == alice

    outgoing = db.get_outgoing_neighbors(alice, "WORKS_AT")
    assert acme in outgoing

    incoming = db.get_incoming_neighbors(acme, "WORKS_AT")
    assert alice in incoming

    result = db.query().start_from_label("Person").traverse(["WORKS_AT"]).get_ids()
    assert acme in result["node_ids"]

    assert db.count_nodes_with_label("Person") == 1
    assert db.count_edges_with_type("WORKS_AT") == 1

    with pytest.raises(ValueError):
        db.add_node("Unknown", {"name": "Ghost"})

