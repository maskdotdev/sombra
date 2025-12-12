"""Typed Python facade mirroring the ergonomic Node.js surface."""

from __future__ import annotations

from typing import Any, Dict, Generic, List, Mapping, Optional, Sequence, Set, Tuple, TypeVar, Union, cast

from typing_extensions import Literal, TypedDict

from ..query import Database
from .schema import (
    EdgeSchema,
    NormalizedGraphSchema,
    TypedGraphSchema,
    extract_runtime_node_schema,
    normalize_graph_schema,
)

SchemaT = TypeVar("SchemaT", bound=TypedGraphSchema)
NodeLabelT = TypeVar("NodeLabelT", bound=str)
EdgeLabelT = TypeVar("EdgeLabelT", bound=str)
Direction = Literal["out", "in", "both"]


class NodeId(int, Generic[NodeLabelT]):
    """Branded node identifier that carries the originating label."""


class TypedQueryResult(TypedDict):
    node_ids: List[int]


class SombraDB(Generic[SchemaT]):
    """Schema-aware facade that wraps the raw Database binding."""

    def __init__(
        self,
        path_or_db: Union[str, Database],
        *,
        schema: Optional[SchemaT] = None,
        **connect_options: Any,
    ) -> None:
        if isinstance(path_or_db, Database):
            if connect_options:
                raise TypeError("connect options are not allowed when wrapping an existing Database")
            self._db = path_or_db
        elif isinstance(path_or_db, str):
            self._db = Database.open(path_or_db, **connect_options)
        else:
            raise TypeError("SombraDB requires a file path or Database instance")

        self._schema: Optional[NormalizedGraphSchema] = (
            normalize_graph_schema(schema) if schema is not None else None
        )
        if self._schema is not None:
            self._db.with_schema(extract_runtime_node_schema(self._schema))

    @classmethod
    def open(
        cls,
        path: str,
        *,
        schema: Optional[SchemaT] = None,
        **connect_options: Any,
    ) -> SombraDB[SchemaT]:
        return cls(path, schema=schema, **connect_options)

    @classmethod
    def from_database(
        cls,
        db: Database,
        *,
        schema: Optional[SchemaT] = None,
    ) -> SombraDB[SchemaT]:
        return cls(db, schema=schema)

    def raw(self) -> Database:
        return self._db

    def add_node(self, label: NodeLabelT, props: Optional[Mapping[str, Any]] = None) -> NodeId[NodeLabelT]:
        normalized = self._assert_node_label(label, "add_node")
        values = dict(props or {})
        self._validate_node_props(normalized, values)
        node_id = self._db.create_node(normalized, values)
        if node_id is None:
            raise RuntimeError("unable to create node")
        return cast(NodeId[NodeLabelT], int(node_id))

    def add_edge(
        self,
        src: NodeId[str],
        dst: NodeId[str],
        edge_type: EdgeLabelT,
        props: Optional[Mapping[str, Any]] = None,
    ) -> int:
        normalized_edge = self._assert_edge_label(edge_type, "add_edge")
        values = dict(props or {})
        self._validate_edge_props(normalized_edge, values)
        if self._schema:
            definition = self._schema["edges"].get(normalized_edge)
            if definition:
                expected_src = definition.get("from_label")
                expected_dst = definition.get("to_label")
                if expected_src:
                    self._ensure_node_matches_label(int(src), expected_src, "add_edge source")
                if expected_dst:
                    self._ensure_node_matches_label(int(dst), expected_dst, "add_edge target")
        edge_id = self._db.create_edge(int(src), int(dst), normalized_edge, values)
        if edge_id is None:
            raise RuntimeError("unable to create edge")
        return int(edge_id)

    def bulk_load_nodes(
        self,
        label: NodeLabelT,
        nodes: Sequence[Mapping[str, Any]],
        *,
        chunk_size: int = 10_000,
    ) -> List[NodeId[NodeLabelT]]:
        """Bulk load many nodes for a single label.

        This API is explicitly non-atomic: each chunk is committed
        independently using the underlying JSON `create` path.
        """
        normalized = self._assert_node_label(label, "bulk_load_nodes")
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        nodes_list = list(nodes)
        results: List[NodeId[NodeLabelT]] = []
        if not nodes_list:
            return results
        for i in range(0, len(nodes_list), chunk_size):
            chunk = nodes_list[i : i + chunk_size]
            ops: List[Dict[str, Any]] = []
            for props in chunk:
                values = dict(props or {})
                self._validate_node_props(normalized, values)
                ops.append({"op": "createNode", "labels": [normalized], "props": values})
            summary = self._db.mutate({"ops": ops})
            created = summary.get("createdNodes") or []
            for node_id in created:
                results.append(cast(NodeId[NodeLabelT], int(node_id)))
        return results

    def bulk_load_edges(
        self,
        edges: Sequence[Tuple[NodeId[str], NodeId[str], EdgeLabelT, Mapping[str, Any]]],
        *,
        chunk_size: int = 100_000,
    ) -> List[int]:
        """Bulk load many edges.

        This API is explicitly non-atomic: each chunk is committed
        independently using the underlying JSON `create` path.
        """
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        edges_list = list(edges)
        results: List[int] = []
        if not edges_list:
            return results
        for i in range(0, len(edges_list), chunk_size):
            chunk = edges_list[i : i + chunk_size]
            ops: List[Dict[str, Any]] = []
            for src, dst, edge_type, props in chunk:
                normalized_edge = self._assert_edge_label(edge_type, "bulk_load_edges")
                values = dict(props or {})
                self._validate_edge_props(normalized_edge, values)
                if self._schema:
                    definition = self._schema["edges"].get(normalized_edge)
                    if definition:
                        expected_src = definition.get("from_label")
                        expected_dst = definition.get("to_label")
                        if expected_src:
                            self._ensure_node_matches_label(int(src), expected_src, "bulk_load_edges source")
                        if expected_dst:
                            self._ensure_node_matches_label(int(dst), expected_dst, "bulk_load_edges target")
                ops.append(
                    {
                        "op": "createEdge",
                        "src": int(src),
                        "dst": int(dst),
                        "ty": normalized_edge,
                        "props": values,
                    }
                )
            summary = self._db.mutate({"ops": ops})
            created = summary.get("createdEdges") or []
            results.extend(int(edge_id) for edge_id in created)
        return results

    def get_node(
        self,
        node_id: NodeId[NodeLabelT],
        expected_label: Optional[NodeLabelT] = None,
    ) -> Optional[Dict[str, Any]]:
        record = self._db.get_node_record(int(node_id))
        if record is None:
            return None
        labels = record.get("labels")
        resolved_label = expected_label
        if not resolved_label and isinstance(labels, list) and labels:
            candidate = labels[0]
            if isinstance(candidate, str):
                resolved_label = cast(NodeLabelT, candidate)
        properties = record.get("properties")
        props_dict = properties if isinstance(properties, Mapping) else {}
        return {
            "id": cast(NodeId[NodeLabelT], int(node_id)),
            "label": resolved_label,
            "properties": dict(props_dict),
        }

    def find_node_by_property(
        self,
        label: NodeLabelT,
        prop: str,
        value: Any,
    ) -> Optional[NodeId[NodeLabelT]]:
        candidates = self.list_nodes_with_label(label)
        for node_id in candidates:
            record = self.get_node(node_id, label)
            if not record:
                continue
            props = record.get("properties") or {}
            if props.get(prop) == value:
                return cast(NodeId[NodeLabelT], node_id)
        return None

    def list_nodes_with_label(self, label: NodeLabelT) -> List[NodeId[NodeLabelT]]:
        normalized = self._assert_node_label(label, "list_nodes_with_label")
        values = self._db.list_nodes_with_label(normalized)
        return [cast(NodeId[NodeLabelT], int(value)) for value in values]

    def get_incoming_neighbors(
        self,
        node_id: NodeId[str],
        edge_type: Optional[EdgeLabelT] = None,
        *,
        distinct: bool = True,
    ) -> List[NodeId[str]]:
        normalized_edge = self._maybe_assert_edge(edge_type, "get_incoming_neighbors")
        neighbors = self._db.neighbors(
            int(node_id),
            direction="in",
            edge_type=normalized_edge,
            distinct=distinct,
        )
        return cast(List[NodeId[str]], self._extract_neighbor_ids(neighbors))

    def get_outgoing_neighbors(
        self,
        node_id: NodeId[str],
        edge_type: Optional[EdgeLabelT] = None,
        *,
        distinct: bool = True,
    ) -> List[NodeId[str]]:
        normalized_edge = self._maybe_assert_edge(edge_type, "get_outgoing_neighbors")
        neighbors = self._db.neighbors(
            int(node_id),
            direction="out",
            edge_type=normalized_edge,
            distinct=distinct,
        )
        return cast(List[NodeId[str]], self._extract_neighbor_ids(neighbors))

    def get_neighbors(
        self,
        node_id: NodeId[Any],
        *,
        direction: Direction = "both",
        edge_type: Optional[EdgeLabelT] = None,
        distinct: bool = True,
    ) -> List[int]:
        if direction not in ("out", "in", "both"):
            raise ValueError("direction must be 'out', 'in', or 'both'")
        normalized_edge = self._maybe_assert_edge(edge_type, "get_neighbors")
        neighbors = self._db.neighbors(
            int(node_id),
            direction=direction,
            edge_type=normalized_edge,
            distinct=distinct,
        )
        return [int(entry.get("node_id", -1)) for entry in neighbors if isinstance(entry, Mapping)]

    def count_nodes_with_label(self, label: NodeLabelT) -> int:
        normalized = self._assert_node_label(label, "count_nodes_with_label")
        return int(self._db.count_nodes_with_label(normalized))

    def count_edges_with_type(self, edge_type: EdgeLabelT) -> int:
        normalized = self._assert_edge_label(edge_type, "count_edges_with_type")
        return int(self._db.count_edges_with_type(normalized))

    def bfs_traversal(
        self,
        node_id: NodeId[Any],
        max_depth: int,
        *,
        direction: Direction = "out",
        edge_types: Optional[Sequence[EdgeLabelT]] = None,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, int]]:
        normalized_types = (
            [self._assert_edge_label(edge, "bfs_traversal") for edge in edge_types] if edge_types else None
        )
        return self._db.bfs_traversal(
            int(node_id),
            int(max_depth),
            direction=direction,
            edge_types=normalized_types,
            max_results=max_results,
        )

    def query(self) -> TypedQueryBuilder[SchemaT]:
        return TypedQueryBuilder(self)

    def flush(self) -> SombraDB[SchemaT]:
        return self

    def _assert_node_label(self, label: str, ctx: str) -> str:
        if not isinstance(label, str) or not label.strip():
            raise TypeError(f"{ctx} requires a non-empty node label")
        if self._schema and label not in self._schema["nodes"]:
            raise ValueError(f"{ctx} refers to unknown label '{label}'")
        return label

    def _assert_edge_label(self, edge_type: str, ctx: str) -> str:
        if not isinstance(edge_type, str) or not edge_type.strip():
            raise TypeError(f"{ctx} requires a non-empty edge label")
        if self._schema and edge_type not in self._schema["edges"]:
            raise ValueError(f"{ctx} refers to unknown edge type '{edge_type}'")
        return edge_type

    def _maybe_assert_edge(self, edge_type: Optional[str], ctx: str) -> Optional[str]:
        if edge_type is None:
            return None
        return self._assert_edge_label(edge_type, ctx)

    def _validate_node_props(self, label: str, props: Mapping[str, Any]) -> None:
        if not self._schema or not props:
            return
        definition = self._schema["nodes"].get(label)
        if not definition:
            return
        allowed = set((definition.get("properties") or {}).keys())
        for key in props.keys():
            if key not in allowed:
                raise ValueError(f"unknown property '{key}' for node '{label}'")

    def _validate_edge_props(self, edge_type: str, props: Mapping[str, Any]) -> None:
        if not self._schema or not props:
            return
        definition = self._schema["edges"].get(edge_type)
        if not definition:
            return
        allowed = set((definition.get("properties") or {}).keys())
        for key in props.keys():
            if key not in allowed:
                raise ValueError(f"unknown property '{key}' for edge '{edge_type}'")

    def _extract_neighbor_ids(self, entries: Sequence[Mapping[str, Any]]) -> List[NodeId[Any]]:
        results: List[NodeId[Any]] = []
        for entry in entries:
            node_value = entry.get("node_id")
            if isinstance(node_value, int):
                results.append(cast(NodeId[Any], node_value))
        return results

    def _ensure_node_matches_label(self, node_id: int, label: str, ctx: str) -> None:
        record = self._db.get_node_record(node_id)
        if not record:
            raise ValueError(f"{ctx} requires an existing node (id {node_id})")
        labels = record.get("labels")
        if isinstance(labels, list) and label in labels:
            return
        raise ValueError(f"{ctx} expected node with label '{label}'")


class TypedQueryBuilder(Generic[SchemaT]):
    def __init__(self, db: SombraDB[SchemaT]):
        self._db = db
        self._start_label: Optional[str] = None
        self._edge_types: List[str] = []
        self._direction: Direction = "out"
        self._depth = 1

    def start_from_label(self, label: NodeLabelT) -> TypedQueryBuilder[SchemaT]:
        self._db._assert_node_label(label, "query.start_from_label")
        self._start_label = label
        return self

    def traverse(
        self,
        edge_types: Sequence[EdgeLabelT],
        direction: Direction = "out",
        depth: int = 1,
    ) -> TypedQueryBuilder[SchemaT]:
        if not edge_types:
            raise ValueError("traverse requires at least one edge type")
        normalized = [self._db._assert_edge_label(edge, "query.traverse") for edge in edge_types]
        if direction not in ("out", "in", "both"):
            raise ValueError("direction must be 'out', 'in', or 'both'")
        if not isinstance(depth, int) or depth <= 0:
            raise ValueError("depth must be a positive integer")
        self._edge_types = normalized
        self._direction = direction
        self._depth = depth
        return self

    def get_ids(self) -> TypedQueryResult:
        if not self._start_label:
            raise RuntimeError("start_from_label() must be called before get_ids()")
        start_nodes = self._db.list_nodes_with_label(self._start_label)
        seen: Set[int] = set()
        for node_id in start_nodes:
            visits = self._db.bfs_traversal(
                node_id,
                self._depth,
                direction=self._direction,
                edge_types=self._edge_types,
            )
            for visit in visits:
                neighbor = visit.get("node_id")
                depth = visit.get("depth")
                if isinstance(depth, int) and depth > 0 and isinstance(neighbor, int):
                    seen.add(neighbor)
        return TypedQueryResult(node_ids=list(seen))
