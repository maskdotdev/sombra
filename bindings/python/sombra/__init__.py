"""Python bindings for the Sombra database (Stage 8 query surface)."""

from . import typed
from ._native import version as _native_version
from .query import (
    CreateBuilder,
    Database,
    QueryBuilder,
    QueryResult,
    open_database,
    # Error types
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

__all__ = [
    "version",
    "Database",
    "QueryBuilder",
    "CreateBuilder",
    "QueryResult",
    "open_database",
    "typed",
    # Error types
    "ErrorCode",
    "SombraError",
    "AnalyzerError",
    "JsonError",
    "IoError",
    "CorruptionError",
    "ConflictError",
    "SnapshotTooOldError",
    "CancelledError",
    "InvalidArgError",
    "NotFoundError",
    "ClosedError",
    "wrap_native_error",
]


def version() -> str:
    """Return the current stub version string."""
    return _native_version()
