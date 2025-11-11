"""End-to-end fluent query example aligned with the canonical Stage 8 payload."""

from __future__ import annotations

import tempfile
from pathlib import Path

from sombra_py import Database


def temp_db_path() -> str:
    directory = Path(tempfile.mkdtemp())
    return str(directory / "fluent-query.db")


def main() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    result = (
        db.query()
        .match({"var": "a", "label": "User"})
        .where("FOLLOWS", {"var": "b", "label": "User"})
        .where_var("a", lambda pred: pred.eq("country", "US"))
        .where_var("b", lambda pred: pred.between("name", "Ada", "Grace"))
        .select(["a", "b"])
        .distinct()
        .execute()
    )

    for row in result.rows():
        src = row["a"]
        dst = row["b"]
        print(
            f"source={src['_id']} ({src['props'].get('name')}) "
            f"-> target={dst['_id']} ({dst['props'].get('name')})"
        )


if __name__ == "__main__":
    main()
