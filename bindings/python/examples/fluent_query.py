"""End-to-end fluent query example aligned with the canonical Stage 8 payload."""

from __future__ import annotations

import tempfile
from pathlib import Path

from sombra_py import Database
from sombra_py.query import between, eq


def temp_db_path() -> str:
    directory = Path(tempfile.mkdtemp())
    return str(directory / "fluent-query.db")


def main() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    result = (
        db.query()
        .match({"source": "User", "target": "User"})
        .where("FOLLOWS", {"var": "target", "label": "User"})
        .on("source", lambda scope: scope.where(eq("country", "US")))
        .on("target", lambda scope: scope.where(between("name", "Ada", "Grace")))
        .select(["source", "target"])
        .distinct()
        .execute()
    )

    for row in result.rows():
        src = row["source"]
        dst = row["target"]
        print(
            f"source={src['_id']} ({src['props'].get('name')}) "
            f"-> target={dst['_id']} ({dst['props'].get('name')})"
        )


if __name__ == "__main__":
    main()
