"""Schema types and normalization helpers for the typed Sombra facade."""

from __future__ import annotations

from typing import Any, Dict, Mapping, TypeVar

from typing_extensions import NotRequired, TypedDict


class NodeSchema(TypedDict, total=False):
    """Node definition container used within TypedGraphSchema."""

    properties: Mapping[str, Any]


EdgeSchema = TypedDict(
    "EdgeSchema",
    {
        "from": str,
        "to": str,
        "properties": NotRequired[Mapping[str, Any]],
    },
)


class TypedGraphSchema(TypedDict):
    nodes: Mapping[str, NodeSchema]
    edges: Mapping[str, EdgeSchema]


class RuntimeNodeDefinition(TypedDict, total=False):
    properties: Dict[str, Any]


class RuntimeEdgeDefinition(TypedDict):
    from_label: str
    to_label: str
    properties: Dict[str, Any]


class NormalizedGraphSchema(TypedDict):
    nodes: Dict[str, RuntimeNodeDefinition]
    edges: Dict[str, RuntimeEdgeDefinition]


SchemaT = TypeVar("SchemaT", bound="TypedGraphSchema")


def normalize_graph_schema(schema: TypedGraphSchema) -> NormalizedGraphSchema:
    if not isinstance(schema, Mapping):
        raise TypeError("graph schema must be a mapping")

    raw_nodes = schema.get("nodes")
    raw_edges = schema.get("edges")
    if raw_nodes is None or raw_edges is None:
        raise TypeError("graph schema must include 'nodes' and 'edges'")
    if not isinstance(raw_nodes, Mapping):
        raise TypeError("'nodes' must be a mapping from label -> node definition")
    if not isinstance(raw_edges, Mapping):
        raise TypeError("'edges' must be a mapping from label -> edge definition")

    normalized_nodes: Dict[str, RuntimeNodeDefinition] = {}
    for label, definition in raw_nodes.items():
        if not isinstance(label, str) or not label.strip():
            raise ValueError("node labels must be non-empty strings")
        if not isinstance(definition, Mapping):
            raise TypeError(f"definition for node '{label}' must be a mapping")
        props = definition.get("properties") or {}
        if not isinstance(props, Mapping):
            raise TypeError(f"properties for node '{label}' must be a mapping")
        normalized_nodes[label] = {"properties": dict(props)}

    normalized_edges: Dict[str, RuntimeEdgeDefinition] = {}
    for label, definition in raw_edges.items():
        if not isinstance(label, str) or not label.strip():
            raise ValueError("edge labels must be non-empty strings")
        if not isinstance(definition, Mapping):
            raise TypeError(f"definition for edge '{label}' must be a mapping")
        from_label = definition.get("from") or definition.get("from_")
        to_label = definition.get("to")
        if not isinstance(from_label, str) or not from_label.strip():
            raise ValueError(f"edge '{label}' must include a non-empty 'from' label")
        if not isinstance(to_label, str) or not to_label.strip():
            raise ValueError(f"edge '{label}' must include a non-empty 'to' label")
        if from_label not in normalized_nodes or to_label not in normalized_nodes:
            raise ValueError(
                f"edge '{label}' references unknown nodes '{from_label}' -> '{to_label}'"
            )
        props = definition.get("properties") or {}
        if not isinstance(props, Mapping):
            raise TypeError(f"properties for edge '{label}' must be a mapping")
        normalized_edges[label] = {
            "from_label": from_label,
            "to_label": to_label,
            "properties": dict(props),
        }

    return {"nodes": normalized_nodes, "edges": normalized_edges}


def extract_runtime_node_schema(schema: NormalizedGraphSchema) -> Dict[str, Dict[str, Any]]:
    runtime: Dict[str, Dict[str, Any]] = {}
    for label, definition in schema["nodes"].items():
        runtime[label] = dict(definition.get("properties") or {})
    return runtime
