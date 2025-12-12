"""Bulk load example using the typed Python facade.

This script demonstrates how to use `SombraDB.bulk_load_nodes` and
`SombraDB.bulk_load_edges` to ingest a small code-graph-like dataset
in chunked, non-atomic fashion.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

from typing_extensions import Literal

from sombra.typed import NodeSchema, SombraDB, TypedGraphSchema

DB_PATH = Path(__file__).with_name("bulk-load-typed.db")


class FunctionProps(TypedDict):
    name: str
    filePath: str


class FileProps(TypedDict):
    path: str


class CallsProps(TypedDict):
    weight: int


class FunctionNode(NodeSchema):
    properties: FunctionProps


class FileNode(NodeSchema):
    properties: FileProps


class GraphNodes(TypedDict):
    Function: FunctionNode
    File: FileNode


CallsEdge = TypedDict(
    "CallsEdge",
    {
        "from": Literal["Function"],
        "to": Literal["Function"],
        "properties": CallsProps,
    },
)


class GraphEdges(TypedDict):
    CALLS: CallsEdge


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
            "Function": {"properties": {"name": "", "filePath": ""}},
            "File": {"properties": {"path": ""}},
        },
        "edges": {
            "CALLS": {
                "from": "Function",
                "to": "Function",
                "properties": {"weight": 0},
            }
        },
    }


def main() -> None:
    reset_db(DB_PATH)
    schema = build_schema()
    db = SombraDB(str(DB_PATH), schema=schema)

    print("=== Typed bulk-load demo ===\n")

    # Prepare a small synthetic code graph: a few files and functions.
    functions = [
        {"name": "main", "filePath": "src/main.py"},
        {"name": "helper", "filePath": "src/util.py"},
        {"name": "parse", "filePath": "src/parser.py"},
        {"name": "analyze", "filePath": "src/analyzer.py"},
    ]

    # Bulk load functions as nodes.
    function_ids = db.bulk_load_nodes("Function", functions, chunk_size=2)
    print(f"Loaded {len(function_ids)} Function nodes")

    # Create a simple CALLS graph using the assigned IDs.
    edges = []
    if len(function_ids) >= 2:
        edges.append((function_ids[0], function_ids[1], "CALLS", {"weight": 1}))
    if len(function_ids) >= 3:
        edges.append((function_ids[1], function_ids[2], "CALLS", {"weight": 2}))
    if len(function_ids) >= 4:
        edges.append((function_ids[2], function_ids[3], "CALLS", {"weight": 3}))

    edge_ids = db.bulk_load_edges(edges, chunk_size=2)
    print(f"Loaded {len(edge_ids)} CALLS edges")

    # Sanity-check: count nodes and edges via typed helpers.
    total_functions = db.count_nodes_with_label("Function")
    total_calls = db.count_edges_with_type("CALLS")
    print(f"Total Function nodes: {total_functions}")
    print(f"Total CALLS edges: {total_calls}")

    print("\nDone! Inspect bulk-load-typed.db to explore the data.")


if __name__ == "__main__":
    main()
