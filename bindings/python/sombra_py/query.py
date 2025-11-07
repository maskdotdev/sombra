"""Stage 8 fluent query builder for Python bindings."""

from __future__ import annotations

import copy
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from . import _native

LiteralInput = Optional[Union[str, int, float, bool]]
ProjectionField = Union[
    str,
    Dict[str, Optional[str]],
    Dict[str, str],
]


def _auto_var_name(idx: int) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    letter = alphabet[idx % len(alphabet)]
    if idx < len(alphabet):
        return letter
    return f"{letter}{idx // len(alphabet)}"


def _normalize_target(
    target: Union[str, Dict[str, Optional[str]]], fallback: str
) -> Dict[str, Optional[str]]:
    if isinstance(target, str):
        return {"var": fallback, "label": target}
    if isinstance(target, dict):
        var_name = target.get("var") or fallback
        label = target.get("label")
        return {"var": var_name, "label": label}
    raise ValueError("target must be a string or dict with keys 'var' and optional 'label'")


def _literal_spec(value: LiteralInput) -> Dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": value}
    if isinstance(value, float):
        if not (value == value and value not in (float("inf"), float("-inf"))):
            raise ValueError("float literal must be finite")
        if value.is_integer():
            return {"type": "int", "value": int(value)}
        return {"type": "float", "value": value}
    raise ValueError(f"unsupported literal type: {type(value)!r}")


def _included_bound(value: LiteralInput) -> Dict[str, Any]:
    return {"kind": "included", "value": _literal_spec(value)}


def _excluded_bound(value: LiteralInput) -> Dict[str, Any]:
    return {"kind": "excluded", "value": _literal_spec(value)}


def _clone(obj: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(obj)


def _normalize_labels(labels: Union[str, Sequence[str]]) -> List[str]:
    if isinstance(labels, str):
        return [labels]
    if isinstance(labels, Sequence):
        result: List[str] = []
        for label in labels:
            if not isinstance(label, str):
                raise ValueError("node labels must be strings")
            result.append(label)
        return result
    raise ValueError("labels must be a string or sequence of strings")


_PRAGMA_SENTINEL = object()


class CreateSummaryResult(dict):
    def __init__(self, summary: Mapping[str, Any]):
        nodes = list(summary.get("nodes") or [])
        edges = list(summary.get("edges") or [])
        aliases = dict(summary.get("aliases") or {})
        super().__init__({"nodes": nodes, "edges": edges, "aliases": aliases})

    def alias(self, name: str) -> Optional[int]:
        if not isinstance(name, str) or not name:
            raise ValueError("alias lookup requires a non-empty string name")
        aliases = self.get("aliases") or {}
        value = aliases.get(name)
        return int(value) if value is not None else None


class _MutationBatch:
    def __init__(self) -> None:
        self._ops: List[Dict[str, Any]] = []
        self._sealed = False

    def _ensure_mutable(self) -> None:
        if self._sealed:
            raise RuntimeError("transaction already committed")

    def _queue(self, op: Dict[str, Any]) -> "_MutationBatch":
        self._ensure_mutable()
        self._ops.append(op)
        return self

    def queue(self, op: Mapping[str, Any]) -> "_MutationBatch":
        return self._queue(dict(op))

    def create_node(
        self, labels: Union[str, Sequence[str]], props: Optional[Mapping[str, Any]] = None
    ) -> "_MutationBatch":
        label_list = [labels] if isinstance(labels, str) else list(labels)
        return self._queue({"op": "createNode", "labels": label_list, "props": dict(props or {})})

    def update_node(
        self,
        node_id: int,
        *,
        set_props: Optional[Mapping[str, Any]] = None,
        unset: Optional[Sequence[str]] = None,
    ) -> "_MutationBatch":
        return self._queue(
            {"op": "updateNode", "id": int(node_id), "set": dict(set_props or {}), "unset": list(unset or [])}
        )

    def delete_node(self, node_id: int, cascade: bool = False) -> "_MutationBatch":
        return self._queue({"op": "deleteNode", "id": int(node_id), "cascade": cascade})

    def create_edge(
        self, src: int, dst: int, ty: str, props: Optional[Mapping[str, Any]] = None
    ) -> "_MutationBatch":
        return self._queue(
            {"op": "createEdge", "src": int(src), "dst": int(dst), "ty": ty, "props": dict(props or {})}
        )

    def update_edge(
        self,
        edge_id: int,
        *,
        set_props: Optional[Mapping[str, Any]] = None,
        unset: Optional[Sequence[str]] = None,
    ) -> "_MutationBatch":
        return self._queue(
            {"op": "updateEdge", "id": int(edge_id), "set": dict(set_props or {}), "unset": list(unset or [])}
        )

    def delete_edge(self, edge_id: int) -> "_MutationBatch":
        return self._queue({"op": "deleteEdge", "id": int(edge_id)})

    def drain(self) -> List[Dict[str, Any]]:
        self._sealed = True
        ops = list(self._ops)
        self._ops.clear()
        return ops


class _QueryStream:
    def __init__(self, handle: _native.StreamHandle):
        self._handle = handle

    def __aiter__(self) -> "_QueryStream":
        return self

    async def __anext__(self) -> Any:
        value = _native.stream_next(self._handle)
        if value is None:
            raise StopAsyncIteration
        return value


class _CreateHandle:
    def __init__(self, builder: "CreateBuilder", index: int) -> None:
        self._builder = builder
        self._index = index

    @property
    def index(self) -> int:
        return self._index

    def node(self, labels: Union[str, Sequence[str]], props: Optional[Mapping[str, Any]] = None, alias: Optional[str] = None) -> "_CreateHandle":
        return self._builder.node(labels, props, alias)

    def node_with_alias(
        self, labels: Union[str, Sequence[str]], alias: str, props: Optional[Mapping[str, Any]] = None
    ) -> "_CreateHandle":
        return self._builder.node_with_alias(labels, alias, props)

    def edge(
        self,
        src: Union["_CreateHandle", str, int],
        ty: str,
        dst: Union["_CreateHandle", str, int],
        props: Optional[Mapping[str, Any]] = None,
    ) -> "CreateBuilder":
        return self._builder.edge(src, ty, dst, props)

    def execute(self) -> Dict[str, Any]:
        return self._builder.execute()


class CreateBuilder:
    def __init__(self, db: "Database"):
        self._db = db
        self._nodes: List[Dict[str, Any]] = []
        self._edges: List[Dict[str, Any]] = []
        self._sealed = False

    def node(
        self,
        labels: Union[str, Sequence[str]],
        props: Optional[Mapping[str, Any]] = None,
        alias: Optional[str] = None,
    ) -> _CreateHandle:
        self._ensure_mutable()
        label_list = _normalize_labels(labels)
        entry: Dict[str, Any] = {
            "labels": label_list,
            "props": dict(props or {}),
        }
        if alias is not None:
            if not isinstance(alias, str) or not alias:
                raise ValueError("alias must be a non-empty string")
            entry["alias"] = alias
        self._nodes.append(entry)
        return _CreateHandle(self, len(self._nodes) - 1)

    def node_with_alias(
        self,
        labels: Union[str, Sequence[str]],
        alias: str,
        props: Optional[Mapping[str, Any]] = None,
    ) -> _CreateHandle:
        if not isinstance(alias, str) or not alias:
            raise ValueError("alias must be a non-empty string")
        return self.node(labels, props, alias)

    def edge(
        self,
        src: Union[_CreateHandle, str, int],
        ty: str,
        dst: Union[_CreateHandle, str, int],
        props: Optional[Mapping[str, Any]] = None,
    ) -> "CreateBuilder":
        self._ensure_mutable()
        if not isinstance(ty, str) or not ty:
            raise ValueError("edge type must be a non-empty string")
        self._edges.append(
            {
                "src": self._encode_ref(src),
                "ty": ty,
                "dst": self._encode_ref(dst),
                "props": dict(props or {}),
            }
        )
        return self

    def execute(self) -> Dict[str, Any]:
        self._ensure_mutable()
        self._sealed = True
        script_nodes: List[Dict[str, Any]] = []
        for node in self._nodes:
            spec = {"labels": node["labels"], "props": dict(node["props"])}
            if "alias" in node:
                spec["alias"] = node["alias"]
            script_nodes.append(spec)
        script_edges = [
            {
                "src": dict(edge["src"]),
                "ty": edge["ty"],
                "dst": dict(edge["dst"]),
                "props": dict(edge["props"]),
            }
            for edge in self._edges
        ]
        script = {
            "nodes": script_nodes,
            "edges": script_edges,
        }
        summary = _native.database_create(self._db._handle, script)
        return CreateSummaryResult(summary)

    def _encode_ref(self, value: Union[_CreateHandle, str, int]) -> Dict[str, Any]:
        if isinstance(value, _CreateHandle):
            return {"kind": "handle", "index": value.index}
        if isinstance(value, str):
            if not value:
                raise ValueError("alias references must be non-empty strings")
            return {"kind": "alias", "alias": value}
        if isinstance(value, int):
            if value < 0:
                raise ValueError("node id references must be non-negative")
            return {"kind": "id", "id": int(value)}
        raise TypeError("edge endpoints must be node handles, alias strings, or numeric ids")

    def _ensure_mutable(self) -> None:
        if self._sealed:
            raise RuntimeError("builder already executed")


class Database:
    """Connection handle wrapped around the native database."""

    def __init__(self, handle: _native.DatabaseHandle):
        self._handle = handle

    @classmethod
    def open(cls, path: str, **options: Any) -> "Database":
        handle = _native.open_database(path, options or None)
        return cls(handle)

    def query(self) -> "QueryBuilder":
        return QueryBuilder(self)

    def create(self) -> "CreateBuilder":
        return CreateBuilder(self)

    def intern(self, name: str) -> int:
        return _native.database_intern(self._handle, name)

    def seed_demo(self) -> "Database":
        _native.database_seed_demo(self._handle)
        return self

    def mutate(self, script: Mapping[str, Any]) -> Dict[str, Any]:
        return _native.database_mutate(self._handle, script)

    def mutate_many(self, ops: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        return self.mutate({"ops": [dict(op) for op in ops]})

    def transaction(self, fn: Callable[["_MutationBatch"], Any]) -> Tuple[Any, Dict[str, Any]]:
        batch = _MutationBatch()
        result = fn(batch)
        if hasattr(result, "__await__"):
            raise RuntimeError("async transactions are not supported")
        ops = batch.drain()
        summary = self.mutate({"ops": ops})
        return result, summary

    def pragma(self, name: str, value: Any = _PRAGMA_SENTINEL) -> Any:
        if value is _PRAGMA_SENTINEL:
            return _native.database_pragma_get(self._handle, name)
        return _native.database_pragma_set(self._handle, name, value)

    def create_node(
        self,
        labels: Union[str, Sequence[str]],
        props: Optional[Mapping[str, Any]] = None,
    ) -> Optional[int]:
        label_list = [labels] if isinstance(labels, str) else list(labels)
        summary = self.mutate({"ops": [{"op": "createNode", "labels": label_list, "props": props or {}}]})
        created = summary.get("createdNodes") or []
        return int(created[-1]) if created else None

    def update_node(
        self,
        node_id: int,
        *,
        set_props: Optional[Mapping[str, Any]] = None,
        unset: Optional[Sequence[str]] = None,
    ) -> "Database":
        self.mutate(
            {
                "ops": [
                    {
                        "op": "updateNode",
                        "id": int(node_id),
                        "set": dict(set_props or {}),
                        "unset": list(unset or []),
                    }
                ]
            }
        )
        return self

    def delete_node(self, node_id: int, cascade: bool = False) -> "Database":
        self.mutate({"ops": [{"op": "deleteNode", "id": int(node_id), "cascade": cascade}]})
        return self

    def create_edge(
        self,
        src: int,
        dst: int,
        ty: str,
        props: Optional[Mapping[str, Any]] = None,
    ) -> Optional[int]:
        summary = self.mutate(
            {
                "ops": [
                    {
                        "op": "createEdge",
                        "src": int(src),
                        "dst": int(dst),
                        "ty": ty,
                        "props": props or {},
                    }
                ]
            }
        )
        created = summary.get("createdEdges") or []
        return int(created[-1]) if created else None

    def delete_edge(self, edge_id: int) -> "Database":
        self.mutate({"ops": [{"op": "deleteEdge", "id": int(edge_id)}]})
        return self

    def _execute(self, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _native.database_execute(self._handle, spec)

    def _explain(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        return _native.database_explain(self._handle, spec)

    def _stream(self, spec: Dict[str, Any]) -> _native.StreamHandle:
        return _native.database_stream(self._handle, spec)


def open_database(path: str, **options: Any) -> Database:
    """Convenience helper mirroring Database.open."""
    return Database.open(path, **options)


class QueryBuilder:
    """Fluent query builder mirroring the Stage 8 TypeScript surface."""

    def __init__(self, db: Database):
        self._db = db
        self._matches: List[Dict[str, Optional[str]]] = []
        self._edges: List[Dict[str, Any]] = []
        self._predicates: List[Dict[str, Any]] = []
        self._projections: List[Dict[str, Any]] = []
        self._distinct = False
        self._last_var: Optional[str] = None
        self._next_var_idx = 0
        self._pending_direction = "out"

    def match(self, target: Union[str, Dict[str, Optional[str]]]) -> "QueryBuilder":
        fallback = self._next_auto_var()
        normalized = _normalize_target(target, fallback)
        self._ensure_match(normalized["var"], normalized.get("label"))
        self._last_var = normalized["var"]
        return self

    def where(
        self,
        edge_type: Optional[str],
        target: Union[str, Dict[str, Optional[str]]],
    ) -> "QueryBuilder":
        if not self._last_var:
            raise ValueError("where requires a preceding match clause")
        fallback = self._next_auto_var()
        normalized = _normalize_target(target, fallback)
        self._ensure_match(normalized["var"], normalized.get("label"))
        edge = {
            "from": self._last_var,
            "to": normalized["var"],
            "edge_type": edge_type,
            "direction": self._pending_direction,
        }
        self._edges.append(edge)
        self._last_var = normalized["var"]
        self._pending_direction = "out"
        return self

    def direction(self, direction: str) -> "QueryBuilder":
        if direction not in ("out", "in", "both"):
            raise ValueError(f"invalid direction: {direction}")
        self._pending_direction = direction
        return self

    def bidirectional(self) -> "QueryBuilder":
        return self.direction("both")

    def where_prop(
        self,
        var_name: str,
        prop: str,
        op: str,
        value: LiteralInput,
        value2: LiteralInput = None,
    ) -> "QueryBuilder":
        op_lower = op.lower()
        if op_lower in ("=", "eq"):
            self._predicates.append(
                {"kind": "eq", "var": var_name, "prop": prop, "value": _literal_spec(value)}
            )
        elif op_lower == "between":
            if value2 is None:
                raise ValueError("between operator requires two values")
            self._predicates.append(
                {
                    "kind": "range",
                    "var": var_name,
                    "prop": prop,
                    "lower": _included_bound(value),
                    "upper": _included_bound(value2),
                }
            )
        elif op_lower in (">", "gt"):
            self._predicates.append(
                {
                    "kind": "range",
                    "var": var_name,
                    "prop": prop,
                    "lower": _excluded_bound(value),
                    "upper": {"kind": "unbounded"},
                }
            )
        elif op_lower in (">=", "ge"):
            self._predicates.append(
                {
                    "kind": "range",
                    "var": var_name,
                    "prop": prop,
                    "lower": _included_bound(value),
                    "upper": {"kind": "unbounded"},
                }
            )
        elif op_lower in ("<", "lt"):
            self._predicates.append(
                {
                    "kind": "range",
                    "var": var_name,
                    "prop": prop,
                    "lower": {"kind": "unbounded"},
                    "upper": _excluded_bound(value),
                }
            )
        elif op_lower in ("<=", "le"):
            self._predicates.append(
                {
                    "kind": "range",
                    "var": var_name,
                    "prop": prop,
                    "lower": {"kind": "unbounded"},
                    "upper": _included_bound(value),
                }
            )
        else:
            raise ValueError(f"unsupported property operator: {op}")
        return self

    def distinct(self, _on: Optional[str] = None) -> "QueryBuilder":
        self._distinct = True
        return self

    def select(self, fields: Sequence[ProjectionField]) -> "QueryBuilder":
        projections: List[Dict[str, Any]] = []
        for field in fields:
            if isinstance(field, str):
                projections.append({"kind": "var", "var": field, "alias": None})
            elif isinstance(field, dict):
                if "expr" in field:
                    expr = field["expr"]
                    alias = field.get("as")
                    if not isinstance(expr, str) or not isinstance(alias, str):
                        raise ValueError("projection expression requires string expr and alias")
                    projections.append({"kind": "expr", "expr": expr, "alias": alias})
                elif "var" in field:
                    var_name = field["var"]
                    alias = field.get("as")
                    projections.append({"kind": "var", "var": var_name, "alias": alias})
                else:
                    raise ValueError("projection dict must contain 'expr' or 'var'")
            else:
                raise ValueError("unsupported projection field")
        self._projections = projections
        return self

    def explain(self) -> Dict[str, Any]:
        return self._db._explain(self._build())

    def execute(self) -> List[Dict[str, Any]]:
        return self._db._execute(self._build())

    def stream(self) -> AsyncIterator[Any]:
        handle = self._db._stream(self._build())
        return _QueryStream(handle)

    def _ensure_match(self, var_name: str, label: Optional[str]) -> None:
        for match in self._matches:
            if match["var"] == var_name:
                if label is not None and match.get("label") is None:
                    match["label"] = label
                return
        self._matches.append({"var": var_name, "label": label})

    def _next_auto_var(self) -> str:
        name = _auto_var_name(self._next_var_idx)
        self._next_var_idx += 1
        return name

    def _build(self) -> Dict[str, Any]:
        return {
            "matches": [
                {"var": clause["var"], "label": clause.get("label")}
                for clause in self._matches
            ],
            "edges": [
                {
                    "from": edge["from"],
                    "to": edge["to"],
                    "edge_type": edge.get("edge_type"),
                    "direction": edge["direction"],
                }
                for edge in self._edges
            ],
            "predicates": [_clone(pred) for pred in self._predicates],
            "distinct": self._distinct,
            "projections": [_clone(proj) for proj in self._projections],
        }
