"""Simple CRUD walkthrough for the Python bindings."""

from __future__ import annotations

import tempfile
from pathlib import Path

from sombra import Database
from sombra.query import eq


def temp_db_path() -> str:
    directory = Path(tempfile.mkdtemp())
    return str(directory / "db")


def main() -> None:
    db = Database.open(temp_db_path())
    db.seed_demo()

    user_id = db.create_node("User", {"name": "Example User", "bio": "hello"})
    print("Created user:", user_id)

    db.update_node(user_id, set_props={"bio": "Updated from Python"})

    rows = db.query().nodes("User").where(eq("name", "Example User")).execute()
    print("Query rows:", rows)

    db.delete_node(user_id, cascade=True)
    print("Deleted user:", user_id)


if __name__ == "__main__":
    main()
