"""Typed SombraDB facade demo for Python users."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from typing_extensions import Literal

from sombra.typed import NodeSchema, SombraDB, TypedGraphSchema

DB_PATH = Path(__file__).with_name("typed-example.db")


class PersonProps(TypedDict):
    name: str
    age: int


class CompanyProps(TypedDict):
    name: str
    employees: int


class CityProps(TypedDict):
    name: str
    state: str


class WorksAtProps(TypedDict):
    role: str


class LivesInProps(TypedDict, total=False):
    pass


class PersonNode(NodeSchema):
    properties: PersonProps


class CompanyNode(NodeSchema):
    properties: CompanyProps


class CityNode(NodeSchema):
    properties: CityProps


class GraphNodes(TypedDict):
    Person: PersonNode
    Company: CompanyNode
    City: CityNode


WorksAtEdge = TypedDict(
    "WorksAtEdge",
    {
        "from": Literal["Person"],
        "to": Literal["Company"],
        "properties": WorksAtProps,
    },
)

LivesInEdge = TypedDict(
    "LivesInEdge",
    {
        "from": Literal["Person"],
        "to": Literal["City"],
        "properties": LivesInProps,
    },
)


class GraphEdges(TypedDict):
    WORKS_AT: WorksAtEdge
    LIVES_IN: LivesInEdge


class DemoGraphSchema(TypedGraphSchema):
    nodes: GraphNodes
    edges: GraphEdges


def reset_db(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def build_schema() -> DemoGraphSchema:
    return {
        "nodes": {
            "Person": {"properties": {"name": "", "age": 0}},
            "Company": {"properties": {"name": "", "employees": 0}},
            "City": {"properties": {"name": "", "state": ""}},
        },
        "edges": {
            "WORKS_AT": {
                "from": "Person",
                "to": "Company",
                "properties": {"role": ""},
            },
            "LIVES_IN": {"from": "Person", "to": "City", "properties": {}},
        },
    }


def main() -> None:
    reset_db(DB_PATH)
    schema = build_schema()
    db = SombraDB(str(DB_PATH), schema=schema)

    print("=== Typed SombraDB Demo ===\n")

    fabian = db.add_node("Person", {"name": "Fabian", "age": 32})
    michelle = db.add_node("Person", {"name": "Michelle", "age": 33})
    aurora = db.add_node("Company", {"name": "AuroraTech", "employees": 250})
    austin = db.add_node("City", {"name": "Austin", "state": "TX"})

    db.add_edge(fabian, aurora, "WORKS_AT", {"role": "Staff Software Engineer"})
    db.add_edge(michelle, aurora, "WORKS_AT", {"role": "Product Manager"})
    db.add_edge(fabian, austin, "LIVES_IN", {})
    db.add_edge(michelle, austin, "LIVES_IN", {})

    print("1. Find a company by typed property lookup:")
    aurora_id = db.find_node_by_property("Company", "name", "AuroraTech")
    if aurora_id:
        record = db.get_node(aurora_id, "Company")
        if record:
            props = record["properties"]
            print(f"   Found {props['name']} with {props['employees']} employees\n")

    print("2. Collect employees via typed edges:")
    employees = db.get_incoming_neighbors(aurora, "WORKS_AT")
    for idx, emp_id in enumerate(employees, start=1):
        person = db.get_node(emp_id, "Person")
        if person:
            props = person["properties"]
            print(f"   {idx}. {props['name']} (age {props['age']})")
    print()

    print("3. Run the typed BFS helper:")
    traversal = db.query().start_from_label("Person").traverse(["LIVES_IN"], "out", 1).get_ids()
    print(f"   Reached {len(traversal['node_ids'])} unique cities\n")

    print("Done! Inspect typed-example.db to explore the data.")


if __name__ == "__main__":
    main()

