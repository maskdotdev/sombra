"""End-to-end fluent query example aligned with the canonical Stage 8 payload."""

from __future__ import annotations

import tempfile
from pathlib import Path

from sombra import Database
from sombra.query import and_, between, eq, in_list, not_


def temp_db_path() -> str:
    directory = Path(tempfile.mkdtemp())
    return str(directory / "fluent-query.db")


def main() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    scalar_rows = (
        db.query()
        .nodes("User")
        .where(and_(in_list("name", ["Ada", "Grace", "Alan"]), not_(eq("name", "Alan"))))
        .select("name")
        .execute()
    )
    print("names returned by nodes() scope:")
    for row in scalar_rows:
        print(f"- {row['name']}")

    payload = (
        db.query()
        .match({"followee": "User", "follower": "User"})
        .where("FOLLOWS", {"var": "followee", "label": "User"})
        .on("follower", lambda scope: scope.where(eq("name", "Ada")))
        .on("followee", lambda scope: scope.where(between("name", "Ada", "Grace")))
        .select(["follower", "followee"])
        .request_id("fluent-query")
        .distinct()
        .execute(with_meta=True)
    )

    print("follow relationships returned by match().on() scopes:")
    for row in payload.rows():
        src = row["follower"]
        dst = row["followee"]
        print(
            f"source={src['_id']} ({src['props'].get('name')}) "
            f"-> target={dst['_id']} ({dst['props'].get('name')})"
        )
    print(f"request id for previous query: {payload.request_id()}")


if __name__ == "__main__":
    main()
