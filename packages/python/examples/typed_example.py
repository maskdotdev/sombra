#!/usr/bin/env python3
"""
Basic Typed API Example

This example demonstrates the basic usage of the typed Sombra API with schema definitions.
"""

import os
import sys
from typing_extensions import TypedDict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from sombra.typed import SombraDB


class PersonProps(TypedDict):
    name: str
    age: int
    email: str


class PostProps(TypedDict):
    title: str
    content: str
    views: int


class NodeSchema(TypedDict):
    Person: PersonProps
    Post: PostProps


class AuthoredEdgeProps(TypedDict):
    created_at: int


class EdgeSchema(TypedDict):
    AUTHORED: AuthoredEdgeProps


class BlogSchema(TypedDict):
    nodes: NodeSchema
    edges: EdgeSchema


def main():
    db_path = "typed_example.db"

    if os.path.exists(db_path):
        os.unlink(db_path)

    print("Typed API Example\n")

    db: SombraDB[BlogSchema] = SombraDB(db_path)

    print("1. Creating a Person node with type-safe properties...")
    alice_id = db.add_node(
        "Person", {"name": "Alice Johnson", "age": 30, "email": "alice@example.com"}
    )
    print(f"Created Person: {alice_id}")

    print("\n2. Creating a Post node...")
    post_id = db.add_node(
        "Post",
        {
            "title": "Introduction to Graph Databases",
            "content": "Graph databases are amazing for connected data...",
            "views": 42,
        },
    )
    print(f"Created Post: {post_id}")

    print("\n3. Creating an AUTHORED edge...")
    import time

    edge_id = db.add_edge(
        alice_id, post_id, "AUTHORED", {"created_at": int(time.time())}
    )
    print(f"Created edge: {edge_id}")

    print("\n4. Using typed transaction...")
    tx = db.begin_transaction()

    bob_id = tx.add_node(
        "Person", {"name": "Bob Smith", "age": 35, "email": "bob@example.com"}
    )

    post2_id = tx.add_node(
        "Post",
        {
            "title": "Advanced Graph Queries",
            "content": "Let's explore complex graph traversals...",
            "views": 128,
        },
    )

    tx.add_edge(bob_id, post2_id, "AUTHORED", {"created_at": int(time.time())})

    tx.commit()
    print(f"Created Person {bob_id} and Post {post2_id} in transaction")

    print("\n5. Querying nodes by label...")
    all_people = db.get_nodes_by_label("Person")
    all_posts = db.get_nodes_by_label("Post")

    print(f"Total people: {len(all_people)}")
    print(f"Total posts: {len(all_posts)}")

    print("\n6. Inspecting a node...")
    alice = db.get_node(alice_id)
    print(f"Person {alice.id}:")
    print(f"  Labels: {alice.labels}")
    print(f"  Properties: {alice.properties}")

    print("\n✅ Typed API example complete!")

    os.unlink(db_path)


if __name__ == "__main__":
    main()
