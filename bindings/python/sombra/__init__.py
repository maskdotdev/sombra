"""Python bindings for the Sombra database (Stage 8 query surface)."""

from ._native import version as _native_version
from .query import CreateBuilder, Database, QueryBuilder, QueryResult, open_database

__all__ = ["version", "Database", "QueryBuilder", "CreateBuilder", "QueryResult", "open_database"]


def version() -> str:
    """Return the current stub version string."""
    return _native_version()
