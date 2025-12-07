"""Stage 8 fluent query builder for Python bindings."""

from __future__ import annotations

import base64
import copy
import math
import re
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union

from . import _native


# Error code regex: [CODE_NAME] message
_ERROR_CODE_REGEX = re.compile(r'^\[([A-Z_]+)\]\s*')


class ErrorCode:
    """Error codes returned by the Sombra database engine."""
    UNKNOWN = "UNKNOWN"
    MESSAGE = "MESSAGE"
    ANALYZER = "ANALYZER"
    JSON = "JSON"
    IO = "IO"
    CORRUPTION = "CORRUPTION"
    CONFLICT = "CONFLICT"
    SNAPSHOT_TOO_OLD = "SNAPSHOT_TOO_OLD"
    CANCELLED = "CANCELLED"
    INVALID_ARG = "INVALID_ARG"
    NOT_FOUND = "NOT_FOUND"
    CLOSED = "CLOSED"


class SombraError(Exception):
    """Base exception class for all Sombra database errors."""
    
    def __init__(self, message: str, code: str = ErrorCode.UNKNOWN):
        super().__init__(message)
        self.code = code


class AnalyzerError(SombraError):
    """Error raised when a query analysis fails."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.ANALYZER)


class JsonError(SombraError):
    """Error raised when JSON serialization/deserialization fails."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.JSON)


class IoError(SombraError):
    """Error raised when an I/O operation fails."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.IO)


class CorruptionError(SombraError):
    """Error raised when data corruption is detected."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.CORRUPTION)


class ConflictError(SombraError):
    """Error raised when a transaction conflict occurs (write-write conflict)."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.CONFLICT)


class SnapshotTooOldError(SombraError):
    """Error raised when a snapshot is too old for an MVCC read."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.SNAPSHOT_TOO_OLD)


class CancelledError(SombraError):
    """Error raised when an operation is cancelled."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.CANCELLED)


class InvalidArgError(SombraError):
    """Error raised when an invalid argument is provided."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.INVALID_ARG)


class NotFoundError(SombraError):
    """Error raised when a requested resource is not found."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.NOT_FOUND)


class ClosedError(SombraError):
    """Error raised when operations are attempted on a closed database."""
    
    def __init__(self, message: str):
        super().__init__(message, ErrorCode.CLOSED)


# Map of error code strings to their corresponding exception classes
_ERROR_CLASS_MAP: Dict[str, Type[SombraError]] = {
    ErrorCode.UNKNOWN: SombraError,
    ErrorCode.MESSAGE: SombraError,
    ErrorCode.ANALYZER: AnalyzerError,
    ErrorCode.JSON: JsonError,
    ErrorCode.IO: IoError,
    ErrorCode.CORRUPTION: CorruptionError,
    ErrorCode.CONFLICT: ConflictError,
    ErrorCode.SNAPSHOT_TOO_OLD: SnapshotTooOldError,
    ErrorCode.CANCELLED: CancelledError,
    ErrorCode.INVALID_ARG: InvalidArgError,
    ErrorCode.NOT_FOUND: NotFoundError,
    ErrorCode.CLOSED: ClosedError,
}


def wrap_native_error(err: BaseException) -> SombraError:
    """Parse an error from the native layer and return a typed exception.
    
    Native errors have format: "[CODE_NAME] actual message"
    
    Args:
        err: The exception from the native layer
        
    Returns:
        A typed SombraError subclass instance
    """
    message = str(err)
    match = _ERROR_CODE_REGEX.match(message)
    
    if match:
        code = match.group(1)
        clean_message = message[match.end():]
        error_class = _ERROR_CLASS_MAP.get(code, SombraError)
        return error_class(clean_message)
    
    # No code prefix, return as generic SombraError
    if isinstance(err, SombraError):
        return err
    return SombraError(message, ErrorCode.UNKNOWN)


def _wrap_native_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Invoke a native function and re-raise with typed errors."""
    try:
        return fn(*args, **kwargs)
    except BaseException as err:  # noqa: BLE001 - surface native error codes
        raise wrap_native_error(err)


class QueryResult(dict):
    """Envelope returned by execute()/explain() with convenience helpers."""

    def rows(self) -> List[Dict[str, Any]]:
        rows = self.get("rows") or []
        if not isinstance(rows, list):
            raise TypeError("result rows must be a list")
        return rows

    def request_id(self) -> Optional[str]:
        rid = self.get("request_id")
        if rid is not None and not isinstance(rid, str):
            raise TypeError("request_id must be a string when present")
        return rid

    def features(self) -> List[Any]:
        feats = self.get("features") or []
        if not isinstance(feats, list):
            raise TypeError("features must be a list")
        return feats

    def plan(self) -> List[Dict[str, Any]]:
        plan = self.get("plan")
        if plan is None:
            return []
        if isinstance(plan, list):
            return plan
        if isinstance(plan, dict):
            return [plan]
        raise TypeError("plan must be a list of plan nodes when present")

    def plan_hash(self) -> Optional[str]:
        value = self.get("plan_hash")
        if value is not None and not isinstance(value, str):
            raise TypeError("plan_hash must be a string when present")
        return value

LiteralInput = Optional[Union[str, int, float, bool, datetime, bytes, bytearray, memoryview]]
ProjectionField = Union[
    str,
    Dict[str, Optional[str]],
    Dict[str, str],
]


def _auto_var_name(idx: int) -> str:
    return f"n{idx}"


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


def _normalize_runtime_schema(
    schema: Optional[Mapping[str, Mapping[str, Any]]]
) -> Optional[Dict[str, Dict[str, Any]]]:
    if schema is None:
        return None
    if not isinstance(schema, Mapping):
        raise TypeError("schema must be a mapping of label -> property metadata")
    normalized: Dict[str, Dict[str, Any]] = {}
    for label, props in schema.items():
        if not isinstance(label, str) or not label.strip():
            raise ValueError("schema labels must be non-empty strings")
        if not isinstance(props, Mapping):
            raise TypeError(f"schema entry for label '{label}' must be a mapping of properties")
        normalized_props: Dict[str, Any] = {}
        for prop_name in props.keys():
            if not isinstance(prop_name, str) or not prop_name.strip():
                raise ValueError(f"schema for label '{label}' contains an invalid property name")
            normalized_props[prop_name] = props[prop_name]
        normalized[label] = normalized_props
    return normalized


_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MIN_DATETIME = datetime(1900, 1, 1, tzinfo=timezone.utc)
_MAX_DATETIME = datetime(2100, 1, 1, tzinfo=timezone.utc)
_NANOS_PER_SECOND = 1_000_000_000


def _encode_bytes_literal(value: Union[bytes, bytearray, memoryview]) -> str:
    buf = bytes(value)
    return base64.b64encode(buf).decode("ascii")


def _literal_value(value: LiteralInput) -> Dict[str, Any]:
    if value is None:
        return {"t": "Null"}
    if isinstance(value, bool):
        return {"t": "Bool", "v": value}
    if isinstance(value, datetime):
        return {"t": "DateTime", "v": _datetime_to_ns(value)}
    if isinstance(value, int):
        if value < _I64_MIN or value > _I64_MAX:
            raise ValueError("integer literal must fit within signed 64-bit range")
        return {"t": "Int", "v": value}
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError("float literal must be finite")
        return {"t": "Float", "v": value}
    if isinstance(value, str):
        return {"t": "String", "v": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"t": "Bytes", "v": _encode_bytes_literal(value)}
    raise ValueError(f"unsupported literal type: {type(value)!r}")


def _clone(obj: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(obj)


def _empty_mutation_summary() -> Dict[str, Any]:
    return {
        "createdNodes": [],
        "createdEdges": [],
        "updatedNodes": 0,
        "updatedEdges": 0,
        "deletedNodes": 0,
        "deletedEdges": 0,
    }


def _merge_mutation_summaries(lhs: Dict[str, Any], rhs: Dict[str, Any]) -> Dict[str, Any]:
    left = lhs or _empty_mutation_summary()
    right = rhs or _empty_mutation_summary()
    return {
        "createdNodes": list(left.get("createdNodes") or []) + list(right.get("createdNodes") or []),
        "createdEdges": list(left.get("createdEdges") or []) + list(right.get("createdEdges") or []),
        "updatedNodes": int(left.get("updatedNodes") or 0) + int(right.get("updatedNodes") or 0),
        "updatedEdges": int(left.get("updatedEdges") or 0) + int(right.get("updatedEdges") or 0),
        "deletedNodes": int(left.get("deletedNodes") or 0) + int(right.get("deletedNodes") or 0),
        "deletedEdges": int(left.get("deletedEdges") or 0) + int(right.get("deletedEdges") or 0),
    }


def _normalize_envelope(payload: Dict[str, Any], *, expect_plan: bool = False) -> Dict[str, Any]:
    if "request_id" not in payload:
        payload["request_id"] = None
    if expect_plan:
        plan = payload.get("plan")
        if plan is None:
            payload["plan"] = []
        elif isinstance(plan, list):
            pass
        elif isinstance(plan, dict):
            payload["plan"] = [plan]
        else:
            raise TypeError("plan must be an object or list when present")
    return payload


def _normalize_prop_name(prop: str) -> str:
    if not isinstance(prop, str) or not prop.strip():
        raise ValueError("property name must be a non-empty string")
    return prop


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


class Expr:
    """Opaque expression tree produced by the helper functions below."""

    __slots__ = ("_node",)

    def __init__(self, node: Dict[str, Any]):
        if not isinstance(node, dict):
            raise TypeError("expression node must be a dict")
        self._node = node


def _wrap_expr(node: Dict[str, Any]) -> Expr:
    return Expr(node)


def _ensure_expr(value: Any, ctx: str) -> Dict[str, Any]:
    if isinstance(value, Expr):
        return value._node
    raise TypeError(f"{ctx} must be created via the sombra query helpers")


def _ensure_expr_prop(prop: Any, ctx: str) -> str:
    if not isinstance(prop, str) or not prop.strip():
        raise ValueError(f"{ctx} requires a non-empty property name")
    return prop


def _ensure_scalar_literal(value: Any, ctx: str) -> None:
    if isinstance(value, (list, tuple, set, dict)):
        raise TypeError(f"{ctx} does not accept nested arrays or objects")
    if isinstance(value, (datetime, bytes, bytearray, memoryview)):
        return
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    raise TypeError(f"{ctx} requires scalar literal values")


def and_(*exprs: Expr) -> Expr:
    if not exprs:
        raise ValueError("and_() requires at least one expression")
    nodes = [_ensure_expr(expr, f"and_[{idx}]") for idx, expr in enumerate(exprs)]
    return _wrap_expr({"op": "and", "args": nodes})


def or_(*exprs: Expr) -> Expr:
    if not exprs:
        raise ValueError("or_() requires at least one expression")
    nodes = [_ensure_expr(expr, f"or_[{idx}]") for idx, expr in enumerate(exprs)]
    return _wrap_expr({"op": "or", "args": nodes})


def not_(expr: Expr) -> Expr:
    node = _ensure_expr(expr, "not_() argument")
    return _wrap_expr({"op": "not", "args": [node]})


def eq(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "eq", "prop": _ensure_expr_prop(prop, "eq"), "value": value})


def ne(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "ne", "prop": _ensure_expr_prop(prop, "ne"), "value": value})


def lt(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "lt", "prop": _ensure_expr_prop(prop, "lt"), "value": value})


def le(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "le", "prop": _ensure_expr_prop(prop, "le"), "value": value})


def gt(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "gt", "prop": _ensure_expr_prop(prop, "gt"), "value": value})


def ge(prop: str, value: LiteralInput) -> Expr:
    return _wrap_expr({"op": "ge", "prop": _ensure_expr_prop(prop, "ge"), "value": value})


def between(
    prop: str,
    low: LiteralInput,
    high: LiteralInput,
    *,
    inclusive: Optional[Sequence[bool]] = None,
) -> Expr:
    if inclusive is not None:
        if (
            not isinstance(inclusive, Sequence)
            or len(inclusive) != 2
            or not all(isinstance(flag, bool) for flag in inclusive)
        ):
            raise ValueError("inclusive must be a two-element sequence of booleans")
        flags: Optional[List[bool]] = [bool(inclusive[0]), bool(inclusive[1])]
    else:
        flags = None
    return _wrap_expr(
        {
            "op": "between",
            "prop": _ensure_expr_prop(prop, "between"),
            "low": low,
            "high": high,
            "inclusive": flags,
        }
    )


def in_list(prop: str, values: Sequence[LiteralInput]) -> Expr:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise TypeError("in_list() requires a sequence of literal values")
    items = list(values)
    if not items:
        raise ValueError("in_list() requires at least one literal")
    for idx, item in enumerate(items):
        _ensure_scalar_literal(item, f"in_list()[{idx}]")
    return _wrap_expr({"op": "in", "prop": _ensure_expr_prop(prop, "in_list"), "values": items})


def exists(prop: str) -> Expr:
    return _wrap_expr({"op": "exists", "prop": _ensure_expr_prop(prop, "exists")})


def is_null(prop: str) -> Expr:
    return _wrap_expr({"op": "isNull", "prop": _ensure_expr_prop(prop, "is_null")})


def is_not_null(prop: str) -> Expr:
    return _wrap_expr({"op": "isNotNull", "prop": _ensure_expr_prop(prop, "is_not_null")})


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
        self._closed = False

    def __aiter__(self) -> "_QueryStream":
        return self

    async def __anext__(self) -> Any:
        if self._closed:
            raise StopAsyncIteration
        value = _wrap_native_call(_native.stream_next, self._handle)
        if value is None:
            self.close()
            raise StopAsyncIteration
        return value

    async def __aenter__(self) -> "_QueryStream":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_fn = getattr(_native, "stream_close", None)
        if close_fn is None:
            return
        try:
            _wrap_native_call(close_fn, self._handle)
        except BaseException as err:  # noqa: BLE001 - propagate typed native errors
            raise wrap_native_error(err)

    def __del__(self) -> None:
        if not self._closed:
            try:
                close_fn = getattr(_native, "stream_close", None)
                if close_fn is not None:
                    _wrap_native_call(close_fn, self._handle)
            except BaseException:
                pass
            self._closed = True


class _PredicateBuilder:
    def __init__(
        self,
        parent: Optional["QueryBuilder"],
        var: str,
        mode: str = "and",
        validator: Optional[Callable[[str], str]] = None,
    ) -> None:
        if not isinstance(var, str) or not var:
            raise ValueError("where_var() requires a non-empty variable name")
        self._parent = parent
        self._var = var
        self._mode = mode
        self._validator = validator
        self._exprs: List[Dict[str, Any]] = []
        self._sealed = False

    def _ensure_active(self) -> None:
        if self._sealed:
            raise RuntimeError("predicate builder already finalized")

    def _push(self, expr: Dict[str, Any]) -> "_PredicateBuilder":
        self._ensure_active()
        self._exprs.append(expr)
        return self

    def _finalize_expr(self) -> Dict[str, Any]:
        self._ensure_active()
        if not self._exprs:
            raise ValueError("predicate builder must emit at least one predicate")
        self._sealed = True
        if len(self._exprs) == 1:
            return self._exprs[0]
        return {"op": self._mode, "args": list(self._exprs)}

    def done(self) -> "QueryBuilder":
        if self._parent is None:
            raise RuntimeError("cannot finalize nested predicate group directly")
        expr = self._finalize_expr()
        self._parent._append_predicate(expr)
        return self._parent

    def _normalize_prop(self, prop: str) -> str:
        if self._validator is not None:
            return self._validator(prop)
        return _normalize_prop_name(prop)

    def _comparison(self, op: str, prop: str, extra: Dict[str, Any]) -> "_PredicateBuilder":
        spec = {"op": op, "var": self._var, "prop": self._normalize_prop(prop)}
        spec.update(extra)
        return self._push(spec)

    def eq(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        return self._comparison("eq", prop, {"value": _literal_value(value)})

    def ne(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        return self._comparison("ne", prop, {"value": _literal_value(value)})

    def lt(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        literal = _literal_value(value)
        if literal["t"] == "Null":
            raise ValueError("lt() does not accept null literals")
        return self._comparison("lt", prop, {"value": literal})

    def le(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        literal = _literal_value(value)
        if literal["t"] == "Null":
            raise ValueError("le() does not accept null literals")
        return self._comparison("le", prop, {"value": literal})

    def gt(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        literal = _literal_value(value)
        if literal["t"] == "Null":
            raise ValueError("gt() does not accept null literals")
        return self._comparison("gt", prop, {"value": literal})

    def ge(self, prop: str, value: LiteralInput) -> "_PredicateBuilder":
        literal = _literal_value(value)
        if literal["t"] == "Null":
            raise ValueError("ge() does not accept null literals")
        return self._comparison("ge", prop, {"value": literal})

    def between(
        self,
        prop: str,
        low: LiteralInput,
        high: LiteralInput,
        inclusive: Optional[Sequence[bool]] = None,
    ) -> "_PredicateBuilder":
        low_literal = _literal_value(low)
        high_literal = _literal_value(high)
        if low_literal["t"] == "Null" or high_literal["t"] == "Null":
            raise ValueError("between() does not accept null bounds")
        flags = [True, True]
        if inclusive is not None:
            if (
                not isinstance(inclusive, Sequence)
                or len(inclusive) != 2
                or not all(isinstance(flag, bool) for flag in inclusive)
            ):
                raise ValueError("inclusive must be a two-element sequence of booleans")
            flags = [bool(inclusive[0]), bool(inclusive[1])]
        return self._push(
            {
                "op": "between",
                "var": self._var,
                "prop": self._normalize_prop(prop),
                "low": low_literal,
                "high": high_literal,
                "inclusive": flags,
            }
        )

    def in_(self, prop: str, values: Sequence[LiteralInput]) -> "_PredicateBuilder":
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
            raise TypeError("in_() requires a sequence of literal values")
        items = list(values)
        if not items:
            raise ValueError("in_() requires at least one literal")
        tagged: List[Dict[str, Any]] = []
        for value in items:
            if isinstance(value, (list, tuple, set, dict)):
                raise TypeError("in_() does not accept nested collections")
            tagged.append(_literal_value(value))
        exemplar = next((entry for entry in tagged if entry["t"] != "Null"), None)
        if exemplar is not None:
            for entry in tagged:
                if entry["t"] != "Null" and entry["t"] != exemplar["t"]:
                    raise ValueError("in_() requires all literals to share the same type")
        return self._push(
            {
                "op": "in",
                "var": self._var,
                "prop": self._normalize_prop(prop),
                "values": tagged,
            }
        )

    def exists(self, prop: str) -> "_PredicateBuilder":
        return self._push({"op": "exists", "var": self._var, "prop": self._normalize_prop(prop)})

    def is_null(self, prop: str) -> "_PredicateBuilder":
        return self._push({"op": "isNull", "var": self._var, "prop": self._normalize_prop(prop)})

    def is_not_null(self, prop: str) -> "_PredicateBuilder":
        return self._push(
            {"op": "isNotNull", "var": self._var, "prop": self._normalize_prop(prop)}
        )

    def and_(self, callback: Callable[["_PredicateBuilder"], None]) -> "_PredicateBuilder":
        return self._group("and", callback)

    def or_(self, callback: Callable[["_PredicateBuilder"], None]) -> "_PredicateBuilder":
        return self._group("or", callback)

    def not_(self, callback: Callable[["_PredicateBuilder"], None]) -> "_PredicateBuilder":
        if not callable(callback):
            raise TypeError("not_() requires a callback")
        nested = _PredicateBuilder(None, self._var, "and", self._validator)
        callback(nested)
        expr = nested._finalize_expr()
        return self._push({"op": "not", "args": [expr]})

    def _group(
        self, mode: str, callback: Callable[["_PredicateBuilder"], None]
    ) -> "_PredicateBuilder":
        if not callable(callback):
            raise TypeError(f"{mode}_() requires a callback")
        nested = _PredicateBuilder(None, self._var, mode, self._validator)
        callback(nested)
        expr = nested._finalize_expr()
        return self._push(expr)


class _NodeScope:
    def __init__(self, builder: "QueryBuilder", var_name: str):
        self._builder = builder
        self._var = var_name

    def where(self, expr: Union[Expr, Callable[["_NodeScope"], Expr]]) -> "_NodeScope":
        stamped = _stamp_expr_for_var(self._builder, self._var, expr, "where()", self)
        self._builder._append_predicate(stamped, combinator="and")
        return self

    def and_where(self, expr: Union[Expr, Callable[["_NodeScope"], Expr]]) -> "_NodeScope":
        stamped = _stamp_expr_for_var(self._builder, self._var, expr, "and_where()", self)
        self._builder._append_predicate(stamped, combinator="and")
        return self

    def or_where(self, expr: Union[Expr, Callable[["_NodeScope"], Expr]]) -> "_NodeScope":
        stamped = _stamp_expr_for_var(self._builder, self._var, expr, "or_where()", self)
        self._builder._append_predicate(stamped, combinator="or")
        return self

    def select(self, *keys: str) -> "_NodeScope":
        if not keys:
            raise ValueError("select() requires at least one property name")
        self._builder._select_props(self._var, keys)
        return self

    def distinct(self) -> "_NodeScope":
        self._builder.distinct()
        return self

    def direction(self, direction: str) -> "_NodeScope":
        self._builder.direction(direction)
        return self

    def bidirectional(self) -> "_NodeScope":
        self._builder.bidirectional()
        return self

    def request_id(self, value: Optional[str]) -> "_NodeScope":
        self._builder.request_id(value)
        return self

    def explain(self, *, redact_literals: bool = False) -> QueryResult:
        return self._builder.explain(redact_literals=redact_literals)

    def execute(self, *, with_meta: bool = False) -> Union[List[Dict[str, Any]], QueryResult]:
        return self._builder.execute(with_meta=with_meta)

    def stream(self) -> AsyncIterator[Any]:
        return self._builder.stream()

    def _as_expr(self, expr: Union[Expr, Callable[["_NodeScope"], Expr]], ctx: str) -> Expr:
        if callable(expr):
            result = expr(self)
        else:
            result = expr
        if not isinstance(result, Expr):
            raise TypeError(f"{ctx} must return an Expr built via the sombra query helpers")
        return result


def _stamp_expr_for_var(
    builder: "QueryBuilder",
    var_name: str,
    expr: Union[Expr, Callable[["_NodeScope"], Expr]],
    ctx: str,
    scope: Optional[_NodeScope] = None,
) -> Dict[str, Any]:
    active_scope = scope or _NodeScope(builder, var_name)
    resolved = active_scope._as_expr(expr, ctx)
    node = _ensure_expr(resolved, ctx)
    builder._require_label(var_name)
    return _translate_expr_node(builder, var_name, node, ctx)


def _translate_expr_node(
    builder: "QueryBuilder",
    var_name: str,
    node: Dict[str, Any],
    ctx: str,
    validator: Optional[Callable[[str], str]] = None,
) -> Dict[str, Any]:
    if not isinstance(node, dict):
        raise TypeError(f"{ctx} must be built via the sombra query helpers")
    op = node.get("op")
    if op in ("and", "or"):
        args = node.get("args")
        if not isinstance(args, list) or not args:
            raise ValueError(f"{ctx} {op}_() requires at least one expression")
        return {
            "op": op,
            "args": [
                _translate_expr_node(builder, var_name, child, f"{ctx}.{op}[{idx}]", validator)
                for idx, child in enumerate(args)
            ],
        }
    if op == "not":
        args = node.get("args")
        if not isinstance(args, list) or len(args) != 1:
            raise ValueError(f"{ctx} not_() requires exactly one child expression")
        return {
            "op": "not",
            "args": [_translate_expr_node(builder, var_name, args[0], f"{ctx}.not", validator)],
        }
    prop_validator = validator or builder._make_prop_validator(var_name)
    return _translate_comparison_node(builder, var_name, node, ctx, prop_validator)


def _translate_comparison_node(
    builder: "QueryBuilder",
    var_name: str,
    node: Dict[str, Any],
    ctx: str,
    validator: Callable[[str], str],
) -> Dict[str, Any]:
    prop = validator(_ensure_expr_prop(node.get("prop"), ctx))
    op = node.get("op")
    if op in {"eq", "ne", "lt", "le", "gt", "ge"}:
        if "value" not in node:
            raise ValueError(f"{ctx} {op}() requires a value")
        literal = _literal_value(node["value"])
        if op in {"lt", "le", "gt", "ge"} and literal["t"] == "Null":
            raise ValueError(f"{ctx} {op}() does not accept null literals")
        return {"op": op, "var": var_name, "prop": prop, "value": literal}
    if op == "between":
        if "low" not in node or "high" not in node:
            raise ValueError(f"{ctx} between() requires low and high bounds")
        low_literal = _literal_value(node["low"])
        high_literal = _literal_value(node["high"])
        if low_literal["t"] == "Null" or high_literal["t"] == "Null":
            raise ValueError(f"{ctx} between() does not accept null bounds")
        inclusive = _normalize_inclusive_tuple(node.get("inclusive"))
        return {
            "op": "between",
            "var": var_name,
            "prop": prop,
            "low": low_literal,
            "high": high_literal,
            "inclusive": inclusive,
        }
    if op == "in":
        raw_values = node.get("values")
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"{ctx} in_list() requires at least one literal")
        tagged = _convert_in_list_values(raw_values)
        return {"op": "in", "var": var_name, "prop": prop, "values": tagged}
    if op in {"exists", "isNull", "isNotNull"}:
        return {"op": op, "var": var_name, "prop": prop}
    raise ValueError(f"unsupported expression operator '{op}'")


def _normalize_inclusive_tuple(value: Any) -> List[bool]:
    if value is None:
        return [True, True]
    if (
        isinstance(value, Sequence)
        and len(value) == 2
        and all(isinstance(flag, bool) for flag in value)
    ):
        return [bool(value[0]), bool(value[1])]
    raise ValueError("between().inclusive must be a two-element sequence of booleans")


def _convert_in_list_values(values: Sequence[Any]) -> List[Dict[str, Any]]:
    tagged: List[Dict[str, Any]] = []
    for idx, entry in enumerate(values):
        _ensure_scalar_literal(entry, f"in_list()[{idx}]")
        tagged.append(_literal_value(entry))
    exemplar = next((item for item in tagged if item["t"] != "Null"), None)
    if exemplar is not None:
        for entry in tagged:
            if entry["t"] != "Null" and entry["t"] != exemplar["t"]:
                raise ValueError("in_list() requires all literals to share the same type")
    return tagged


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
        self._db._assert_open()
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
        summary = _wrap_native_call(_native.database_create, self._db._handle, script)
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
        self._schema: Optional[Dict[str, Dict[str, Any]]] = None
        self._closed = False

    @classmethod
    def open(cls, path: str, **options: Any) -> "Database":
        schema = options.pop("schema", None)
        handle = _wrap_native_call(_native.open_database, path, options or None)
        db = cls(handle)
        if schema is not None:
            db.with_schema(schema)
        return db

    def close(self) -> None:
        """Close the database, releasing all resources.
        
        After calling close(), all subsequent operations on this instance will fail.
        Calling close() multiple times is safe (subsequent calls are no-ops).
        """
        if self._closed:
            return
        _wrap_native_call(_native.database_close, self._handle)
        self._closed = True

    @property
    def is_closed(self) -> bool:
        """Returns True if the database has been closed."""
        return self._closed

    def _assert_open(self) -> None:
        """Raises ClosedError if the database is closed."""
        if self._closed:
            raise ClosedError("database is closed")

    def __enter__(self) -> "Database":
        """Context manager entry - returns self."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes the database."""
        self.close()

    def query(self) -> "QueryBuilder":
        self._assert_open()
        return QueryBuilder(self)

    def create(self) -> "CreateBuilder":
        self._assert_open()
        return CreateBuilder(self)

    def intern(self, name: str) -> int:
        self._assert_open()
        return _wrap_native_call(_native.database_intern, self._handle, name)

    def seed_demo(self) -> "Database":
        self._assert_open()
        _wrap_native_call(_native.database_seed_demo, self._handle)
        return self

    def mutate(self, script: Mapping[str, Any]) -> Dict[str, Any]:
        self._assert_open()
        return _wrap_native_call(_native.database_mutate, self._handle, script)

    def mutate_many(self, ops: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        self._assert_open()
        return self.mutate({"ops": [dict(op) for op in ops]})

    def mutate_batched(
        self,
        ops: Sequence[Mapping[str, Any]],
        *,
        batch_size: int = 1024,
    ) -> Dict[str, Any]:
        self._assert_open()
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if isinstance(ops, (str, bytes)):
            raise TypeError("mutate_batched requires a sequence of operations")
        ops_list = list(ops)
        if not ops_list:
            return _empty_mutation_summary()
        summary = _empty_mutation_summary()
        for i in range(0, len(ops_list), batch_size):
            chunk = [dict(op) for op in ops_list[i : i + batch_size]]
            part = self.mutate({"ops": chunk})
            summary = _merge_mutation_summaries(summary, part)
        return summary

    def transaction(self, fn: Callable[["_MutationBatch"], Any]) -> Tuple[Any, Dict[str, Any]]:
        self._assert_open()
        batch = _MutationBatch()
        result = fn(batch)
        if hasattr(result, "__await__"):
            raise RuntimeError("async transactions are not supported")
        ops = batch.drain()
        summary = self.mutate({"ops": ops})
        return result, summary

    def pragma(self, name: str, value: Any = _PRAGMA_SENTINEL) -> Any:
        self._assert_open()
        if value is _PRAGMA_SENTINEL:
            return _wrap_native_call(_native.database_pragma_get, self._handle, name)
        return _wrap_native_call(_native.database_pragma_set, self._handle, name, value)

    def cancel_request(self, request_id: str) -> bool:
        self._assert_open()
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("cancel_request requires a non-empty request id string")
        return _wrap_native_call(_native.database_cancel_request, self._handle, request_id)

    def neighbors(
        self,
        node_id: int,
        *,
        direction: str = "out",
        edge_type: Optional[str] = None,
        distinct: bool = True,
    ) -> List[Dict[str, int]]:
        self._assert_open()
        if not isinstance(node_id, int) or node_id < 0:
            raise ValueError("neighbors() requires a non-negative node id")
        if direction not in ("out", "in", "both"):
            raise ValueError("direction must be 'out', 'in', or 'both'")
        options: Dict[str, Any] = {"direction": direction, "distinct": bool(distinct)}
        if edge_type is not None:
            if not isinstance(edge_type, str) or not edge_type.strip():
                raise ValueError("edge_type must be a non-empty string when provided")
            options["edge_type"] = edge_type
        return _wrap_native_call(_native.database_neighbors, self._handle, int(node_id), options)

    def bfs_traversal(
        self,
        node_id: int,
        max_depth: int,
        *,
        direction: str = "out",
        edge_types: Optional[Sequence[str]] = None,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, int]]:
        self._assert_open()
        if not isinstance(node_id, int) or node_id < 0:
            raise ValueError("bfs_traversal() requires a non-negative node id")
        if not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("bfs_traversal() requires a non-negative integer max_depth")
        if direction not in ("out", "in", "both"):
            raise ValueError("direction must be 'out', 'in', or 'both'")
        options: Dict[str, Any] = {"direction": direction}
        if edge_types is not None:
            values: List[str] = []
            for ty in edge_types:
                if not isinstance(ty, str) or not ty.strip():
                    raise ValueError("edge_types entries must be non-empty strings")
                values.append(ty)
            options["edge_types"] = values
        if max_results is not None:
            if not isinstance(max_results, int) or max_results <= 0:
                raise ValueError("max_results must be a positive integer when provided")
            options["max_results"] = max_results
        return _wrap_native_call(
            _native.database_bfs_traversal, self._handle, int(node_id), int(max_depth), options
        )

    def with_schema(self, schema: Optional[Mapping[str, Mapping[str, Any]]]) -> "Database":
        self._assert_open()
        self._schema = _normalize_runtime_schema(schema)
        return self

    def get_node_record(self, node_id: int) -> Optional[Dict[str, Any]]:
        self._assert_open()
        record = _wrap_native_call(_native.database_get_node, self._handle, int(node_id))
        if record is None:
            return None
        if not isinstance(record, dict):
            raise TypeError("node record must be a mapping when present")
        return record

    def get_edge_record(self, edge_id: int) -> Optional[Dict[str, Any]]:
        self._assert_open()
        record = _wrap_native_call(_native.database_get_edge, self._handle, int(edge_id))
        if record is None:
            return None
        if not isinstance(record, dict):
            raise TypeError("edge record must be a mapping when present")
        return record

    def count_nodes_with_label(self, label: str) -> int:
        self._assert_open()
        if not isinstance(label, str) or not label.strip():
            raise ValueError("count_nodes_with_label requires a non-empty string label")
        return int(_wrap_native_call(_native.database_count_nodes_with_label, self._handle, label))

    def count_edges_with_type(self, edge_type: str) -> int:
        self._assert_open()
        if not isinstance(edge_type, str) or not edge_type.strip():
            raise ValueError("count_edges_with_type requires a non-empty edge type string")
        return int(_wrap_native_call(_native.database_count_edges_with_type, self._handle, edge_type))

    def list_nodes_with_label(self, label: str) -> List[int]:
        self._assert_open()
        if not isinstance(label, str) or not label.strip():
            raise ValueError("list_nodes_with_label requires a non-empty string label")
        values = _wrap_native_call(_native.database_list_nodes_with_label, self._handle, label)
        return [int(value) for value in values]

    def create_node(
        self,
        labels: Union[str, Sequence[str]],
        props: Optional[Mapping[str, Any]] = None,
    ) -> Optional[int]:
        self._assert_open()
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
        self._assert_open()
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
        self._assert_open()
        self.mutate({"ops": [{"op": "deleteNode", "id": int(node_id), "cascade": cascade}]})
        return self

    def create_edge(
        self,
        src: int,
        dst: int,
        ty: str,
        props: Optional[Mapping[str, Any]] = None,
    ) -> Optional[int]:
        self._assert_open()
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
        self._assert_open()
        self.mutate({"ops": [{"op": "deleteEdge", "id": int(edge_id)}]})
        return self

    def _execute(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        self._assert_open()
        payload = _wrap_native_call(_native.database_execute, self._handle, spec)
        return _normalize_envelope(payload)

    def _explain(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        self._assert_open()
        payload = _wrap_native_call(_native.database_explain, self._handle, spec)
        return _normalize_envelope(payload, expect_plan=True)

    def _stream(self, spec: Dict[str, Any]) -> _native.StreamHandle:
        self._assert_open()
        return _wrap_native_call(_native.database_stream, self._handle, spec)


def open_database(path: str, **options: Any) -> Database:
    """Convenience helper mirroring Database.open."""
    return Database.open(path, **options)


class QueryBuilder:
    """Fluent query builder mirroring the Stage 8 TypeScript surface."""

    def __init__(self, db: Database):
        self._db = db
        self._schema = getattr(db, "_schema", None)
        self._matches: List[Dict[str, Optional[str]]] = []
        self._edges: List[Dict[str, Any]] = []
        self._predicate: Optional[Dict[str, Any]] = None
        self._projections: List[Dict[str, Any]] = []
        self._distinct = False
        self._last_var: Optional[str] = None
        self._next_var_idx = 0
        self._pending_direction = "out"
        self._request_id: Optional[str] = None

    def nodes(self, label: str) -> _NodeScope:
        if not isinstance(label, str) or not label:
            raise ValueError("nodes() requires a non-empty label name")
        var_name = self._next_auto_var()
        self._ensure_match(var_name, label)
        self._last_var = var_name
        return _NodeScope(self, var_name)

    def match(self, target: Union[str, Dict[str, Optional[str]]]) -> "QueryBuilder":
        if isinstance(target, Mapping) and "var" not in target and "label" not in target:
            return self._match_map(target)
        fallback = self._next_auto_var()
        normalized = _normalize_target(target, fallback)
        self._ensure_match(normalized["var"], normalized.get("label"))
        self._last_var = normalized["var"]
        return self

    def on(self, var_name: str, scope: Callable[[_NodeScope], None]) -> "QueryBuilder":
        if not isinstance(var_name, str) or not var_name:
            raise ValueError("on() requires a non-empty variable name")
        if not callable(scope):
            raise TypeError("on() requires a callback function")
        self._assert_match(var_name)
        self._require_label(var_name)
        scope(_NodeScope(self, var_name))
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

    def where_var(
        self,
        var_name: str,
        build: Optional[Callable[["_PredicateBuilder"], None]] = None,
    ) -> Union["_PredicateBuilder", "QueryBuilder"]:
        if not isinstance(var_name, str) or not var_name:
            raise ValueError("where_var() requires a non-empty variable name")
        self._assert_match(var_name)
        validator = self._make_prop_validator(var_name)
        builder = _PredicateBuilder(self, var_name, validator=validator)
        if build is not None:
            if not callable(build):
                raise TypeError("where_var() callback must be callable")
            build(builder)
            return builder.done()
        return builder

    def direction(self, direction: str) -> "QueryBuilder":
        if direction not in ("out", "in", "both"):
            raise ValueError(f"invalid direction: {direction}")
        self._pending_direction = direction
        return self

    def bidirectional(self) -> "QueryBuilder":
        return self.direction("both")

    def distinct(self, _on: Optional[str] = None) -> "QueryBuilder":
        self._distinct = True
        return self

    def request_id(self, value: Optional[str]) -> "QueryBuilder":
        if value is None:
            self._request_id = None
            return self
        if not isinstance(value, str):
            raise ValueError("request_id must be provided as a string")
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("request_id must be a non-empty string")
        self._request_id = trimmed
        return self

    def select(self, fields: Sequence[ProjectionField]) -> "QueryBuilder":
        projections: List[Dict[str, Any]] = []
        for field in fields:
            if isinstance(field, str):
                self._assert_match(field)
                projections.append({"kind": "var", "var": field, "alias": None})
            elif isinstance(field, dict):
                if "prop" in field:
                    var_name = field.get("var")
                    prop = field["prop"]
                    alias = field.get("as")
                    if not isinstance(var_name, str) or not var_name:
                        raise ValueError("property projection requires a variable name")
                    if not isinstance(prop, str) or not prop:
                        raise ValueError("property projection requires a property name")
                    if alias is not None and not isinstance(alias, str):
                        raise ValueError("property projection alias must be a string when provided")
                    self._assert_match(var_name)
                    validator = self._make_prop_validator(var_name)
                    normalized_prop = validator(prop)
                    projections.append({"kind": "prop", "var": var_name, "prop": normalized_prop, "alias": alias})
                elif "var" in field:
                    var_name = field["var"]
                    alias = field.get("as")
                    self._assert_match(var_name)
                    projections.append({"kind": "var", "var": var_name, "alias": alias})
                elif "expr" in field:
                    raise ValueError("expression projections are not supported; use property projections instead")
                else:
                    raise ValueError("projection dict must contain 'prop' or 'var'")
            else:
                raise ValueError("unsupported projection field")
        self._projections = projections
        return self

    def explain(self, *, redact_literals: bool = False) -> QueryResult:
        spec = self._build()
        if redact_literals:
            spec["redact_literals"] = True
        payload = self._db._explain(spec)
        return QueryResult(payload)

    def execute(self, *, with_meta: bool = False) -> Union[List[Dict[str, Any]], QueryResult]:
        payload = QueryResult(self._db._execute(self._build()))
        if with_meta:
            return payload
        return payload.rows()

    def stream(self) -> AsyncIterator[Any]:
        handle = self._db._stream(self._build())
        return _QueryStream(handle)

    def _match_map(self, mapping: Mapping[str, Any]) -> "QueryBuilder":
        entries = list(mapping.items())
        if not entries:
            raise ValueError("match({...}) requires at least one entry")
        for var_name, value in entries:
            if not isinstance(var_name, str) or not var_name:
                raise ValueError("match({...}) keys must be non-empty strings")
            if isinstance(value, str):
                normalized = {"var": var_name, "label": value}
            elif value is None:
                normalized = {"var": var_name, "label": None}
            elif isinstance(value, Mapping):
                normalized = _normalize_target({**value, "var": var_name}, var_name)
            else:
                raise ValueError("match({...}) values must be labels or dicts with optional 'label'")
            self._ensure_match(normalized["var"], normalized.get("label"))
        self._last_var = entries[-1][0]
        return self

    def _ensure_match(self, var_name: str, label: Optional[str]) -> None:
        for match in self._matches:
            if match["var"] == var_name:
                if label is not None and match.get("label") is None:
                    match["label"] = label
                return
        self._matches.append({"var": var_name, "label": label})

    def _assert_match(self, var_name: str) -> None:
        if not any(match["var"] == var_name for match in self._matches):
            raise ValueError(f"unknown variable '{var_name}' - call match() first")

    def _append_predicate(self, expr: Dict[str, Any], *, combinator: str = "and") -> None:
        if self._predicate is None:
            self._predicate = expr
            return
        if combinator == "and":
            if self._predicate.get("op") == "and":
                self._predicate["args"].append(expr)
            else:
                self._predicate = {"op": "and", "args": [self._predicate, expr]}
            return
        if combinator == "or":
            if self._predicate.get("op") == "or":
                self._predicate["args"].append(expr)
            else:
                self._predicate = {"op": "or", "args": [self._predicate, expr]}
            return
        raise ValueError(f"unsupported predicate combinator '{combinator}'")

    def _next_auto_var(self) -> str:
        name = _auto_var_name(self._next_var_idx)
        self._next_var_idx += 1
        return name

    def _build(self) -> Dict[str, Any]:
        projections = (
            self._projections
            if self._projections
            else [{"kind": "var", "var": clause["var"], "alias": None} for clause in self._matches]
        )
        spec: Dict[str, Any] = {
            "$schemaVersion": 1,
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
            "distinct": self._distinct,
            "projections": [_clone(proj) for proj in projections],
        }
        if self._predicate is not None:
            spec["predicate"] = _clone(self._predicate)
        if self._request_id is not None:
            spec["request_id"] = self._request_id
        return spec

    def _label_for_var(self, var_name: str) -> Optional[str]:
        for clause in self._matches:
            if clause["var"] == var_name:
                label = clause.get("label")
                if isinstance(label, str) and label:
                    return label
                return None
        return None

    def _require_label(self, var_name: str) -> str:
        label = self._label_for_var(var_name)
        if not label:
            raise ValueError(f"variable '{var_name}' requires a label before applying predicates")
        return label

    def _select_props(self, var_name: str, keys: Sequence[str]) -> None:
        self._assert_match(var_name)
        validator = self._make_prop_validator(var_name)
        for key in keys:
            normalized = validator(key)
            self._projections.append({"kind": "prop", "var": var_name, "prop": normalized, "alias": None})

    def _make_prop_validator(self, var_name: str) -> Callable[[str], str]:
        schema = self._schema
        if not schema:
            return _normalize_prop_name
        label = self._label_for_var(var_name)
        if not label:
            return _normalize_prop_name
        label_schema = schema.get(label)
        if not isinstance(label_schema, Mapping):
            return _normalize_prop_name

        def validator(prop: str) -> str:
            normalized = _normalize_prop_name(prop)
            if normalized not in label_schema:
                raise ValueError(f"Unknown property '{normalized}' on label '{label}'")
            return normalized

        return validator
def _datetime_to_ns(value: datetime) -> int:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("datetime literal must include timezone info")
    normalized = value.astimezone(timezone.utc)
    if normalized < _MIN_DATETIME or normalized > _MAX_DATETIME:
        raise ValueError("datetime literal must be between 1900-01-01 and 2100-01-01 UTC")
    delta = normalized - _EPOCH
    total_seconds = delta.days * 86_400 + delta.seconds
    nanos = total_seconds * _NANOS_PER_SECOND + delta.microseconds * 1_000
    return nanos
