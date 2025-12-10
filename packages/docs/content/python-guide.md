# Python Binding Guide

This guide covers using Sombra from Python, including installation, basic operations, and integration with the Python ecosystem.

## Installation

### From PyPI (Recommended)

```bash
pip install sombra
```

### From Source

```bash
git clone https://github.com/sombra-db/sombra
cd sombra/bindings/python
pip install .
```

### Development Installation with uv

```bash
git clone https://github.com/sombra-db/sombra
cd sombra/bindings/python
uv venv
source .venv/bin/activate
maturin develop
```

## Quick Start

```python
from sombra import Database
from sombra.query import eq

# Open a database (creates if missing by default)
db = Database.open("./example.db", create_if_missing=True)

# Create nodes
alice_id = db.create_node("User", {"name": "Alice", "age": 30})
bob_id = db.create_node("User", {"name": "Bob", "age": 25})

# Create an edge
db.create_edge(alice_id, bob_id, "FOLLOWS", {"since": 2024})

# Query nodes
rows = db.query().nodes("User").where(eq("name", "Alice")).execute()
print(rows)

# Close when done
db.close()
```

## Opening and Closing Databases

```python
from sombra import Database

# Open with options
db = Database.open(
    "./data.db",
    create_if_missing=True,        # Create database if it doesn't exist
    page_size=4096,                # Page size in bytes
    cache_pages=1024,              # Number of pages to cache
    synchronous="normal",          # "full", "normal", or "off"
    autocheckpoint_ms=30000,       # Auto-checkpoint interval (None to disable)
)

# Check if database is closed
print(db.is_closed)  # False

# Using context manager (auto-closes)
with Database.open("./data.db") as db:
    db.seed_demo()
    # ... operations ...
# Database is automatically closed here

# Or close explicitly
db = Database.open("./data.db")
db.close()
print(db.is_closed)  # True

# Operations on closed database raise ClosedError
db.query()  # raises ClosedError
```

## Basic Operations

### Creating Nodes

```python
# Single label
user_id = db.create_node("User", {
    "username": "john_doe",
    "email": "john@example.com",
    "active": True,
    "score": 95.5,
})

# Multiple labels
admin_id = db.create_node(["User", "Admin"], {
    "username": "admin",
    "level": 10,
})

# Using the create builder (returns IDs)
result = db.create() \
    .node("User", {"name": "Alice"}, "$alice") \
    .node("User", {"name": "Bob"}, "$bob") \
    .edge("$alice", "FOLLOWS", "$bob") \
    .execute()

print(result["nodes"])           # [1, 2]
print(result["edges"])           # [1]
print(result.alias("$alice"))    # 1
```

### Updating Nodes

```python
# Set properties
db.update_node(user_id, set_props={"score": 98.2, "verified": True})

# Unset properties
db.update_node(user_id, unset=["score"])

# Both at once
db.update_node(
    user_id,
    set_props={"last_login": 1640995200},
    unset=["temporary_flag"],
)
```

### Deleting Nodes

```python
# Delete node (fails if has edges)
db.delete_node(user_id)

# Delete node and all connected edges (cascade)
db.delete_node(user_id, cascade=True)
```

### Creating and Deleting Edges

```python
# Create an edge
edge_id = db.create_edge(alice_id, bob_id, "FOLLOWS", {"since": 2021})

# Delete an edge
db.delete_edge(edge_id)
```

### Reading Nodes and Edges

```python
# Get node record
node = db.get_node_record(user_id)
# {"labels": ["User"], "properties": {"name": "Alice", "age": 30}}

# Get edge record
edge = db.get_edge_record(edge_id)
# {"src": 1, "dst": 2, "type": "FOLLOWS", "properties": {"since": 2021}}

# List nodes by label
user_ids = db.list_nodes_with_label("User")

# Count nodes/edges
user_count = db.count_nodes_with_label("User")
follows_count = db.count_edges_with_type("FOLLOWS")
```

## Query Builder

Sombra provides a powerful fluent query builder for complex graph queries.

### Simple Node Queries

```python
from sombra.query import eq, and_, or_, not_, between, in_list

# Find users by name
rows = db.query().nodes("User").where(eq("name", "Alice")).execute()

# Complex predicates
rows = db.query().nodes("User").where(
    and_(
        in_list("name", ["Alice", "Bob", "Charlie"]),
        between("age", 18, 65),
        not_(eq("status", "inactive")),
    )
).execute()

# Select specific properties (returns scalar values)
rows = db.query().nodes("User").select("name").execute()
# [{"name": "Alice"}, {"name": "Bob"}]
```

### Pattern Matching with match()

```python
# Match nodes connected by edges
rows = db.query() \
    .match({"user": "User", "follower": "User"}) \
    .where("FOLLOWS", {"var": "user", "label": "User"}) \
    .on("user", lambda scope: scope.where(eq("name", "Alice"))) \
    .select([
        {"var": "follower", "as": "follower"},
        {"var": "user", "as": "user"},
    ]) \
    .execute()
```

### Predicate Functions

| Function                   | Description           | Example                                    |
| -------------------------- | --------------------- | ------------------------------------------ |
| `eq(prop, value)`          | Equal to              | `eq("name", "Alice")`                      |
| `ne(prop, value)`          | Not equal to          | `ne("status", "deleted")`                  |
| `lt(prop, value)`          | Less than             | `lt("age", 30)`                            |
| `le(prop, value)`          | Less than or equal    | `le("score", 100)`                         |
| `gt(prop, value)`          | Greater than          | `gt("followers", 1000)`                    |
| `ge(prop, value)`          | Greater than or equal | `ge("rating", 4.0)`                        |
| `between(prop, low, high)` | Range (inclusive)     | `between("age", 18, 65)`                   |
| `in_list(prop, values)`    | In list               | `in_list("status", ["active", "pending"])` |
| `exists(prop)`             | Property exists       | `exists("email")`                          |
| `is_null(prop)`            | Is null               | `is_null("deleted_at")`                    |
| `is_not_null(prop)`        | Is not null           | `is_not_null("verified_at")`               |
| `and_(*exprs)`             | Logical AND           | `and_(eq("a", 1), eq("b", 2))`             |
| `or_(*exprs)`              | Logical OR            | `or_(eq("a", 1), eq("a", 2))`              |
| `not_(expr)`               | Logical NOT           | `not_(eq("status", "deleted"))`            |

### Query Execution Options

```python
# Basic execute - returns list of rows
rows = db.query().nodes("User").execute()

# Execute with metadata
result = db.query() \
    .nodes("User") \
    .request_id("my-query-id") \
    .execute(with_meta=True)

print(result.rows())        # List of row dicts
print(result.request_id())  # "my-query-id"
print(result.features())    # Query features used

# Streaming for large results (async)
async for row in db.query().nodes("User").stream():
    print(row)

# Explain query plan
plan = db.query() \
    .nodes("User") \
    .where(eq("name", "Alice")) \
    .explain()

print(plan.plan())  # Query execution plan
```

### Query Direction

```python
# Follow edges in specific direction
rows = db.query() \
    .match("User") \
    .where("FOLLOWS", "User") \
    .direction("out") \
    .execute()

rows = db.query() \
    .match("User") \
    .where("FOLLOWS", "User") \
    .direction("in") \
    .execute()

# Bidirectional traversal
rows = db.query() \
    .match("User") \
    .where("FOLLOWS", "User") \
    .bidirectional() \
    .execute()
```

## Graph Traversal

### Neighbors

```python
# Get neighbors with options
neighbors = db.neighbors(
    user_id,
    direction="out",      # "out", "in", or "both"
    edge_type="FOLLOWS",  # Optional filter
    distinct=True,        # Deduplicate
)
# [{"node_id": 2, "edge_id": 1, "type_id": 5}, ...]
```

### BFS Traversal

```python
# Breadth-first traversal
visited = db.bfs_traversal(
    start_node_id,
    max_depth=3,
    direction="out",
    edge_types=["FOLLOWS", "KNOWS"],
    max_results=100,
)

for record in visited:
    print(f"Node {record['node_id']} at depth {record['depth']}")
```

## Mutations

### Mutation Script

```python
summary = db.mutate({
    "ops": [
        {"op": "createNode", "labels": ["User"], "props": {"name": "Alice"}},
        {"op": "createNode", "labels": ["User"], "props": {"name": "Bob"}},
        {"op": "createEdge", "src": 1, "dst": 2, "ty": "FOLLOWS", "props": {}},
        {"op": "updateNode", "id": 1, "set": {"verified": True}},
        {"op": "deleteEdge", "id": 1},
        {"op": "deleteNode", "id": 2, "cascade": True},
    ]
})

print(summary["createdNodes"])  # [1, 2]
print(summary["createdEdges"])  # [1]
print(summary["deletedNodes"])  # 1
```

### Batched Mutations

```python
# Multiple operations in one commit
summary = db.mutate_many([
    {"op": "createNode", "labels": ["User"], "props": {"name": "User1"}},
    {"op": "createNode", "labels": ["User"], "props": {"name": "User2"}},
])

# Batched with chunk size (for very large operations)
ops = [
    {"op": "createNode", "labels": ["User"], "props": user}
    for user in users_data
]
summary = db.mutate_batched(ops, batch_size=1000)
```

### Transactions

```python
result, summary = db.transaction(lambda tx: (
    tx.create_node("User", {"name": "Alice"}),
    tx.create_node("User", {"name": "Bob"}),
    tx.create_edge(1, 2, "FOLLOWS", {}),
    "success"
)[-1])

print(summary["createdNodes"])  # [1, 2]
print(result)                   # "success"
```

## Error Handling

Sombra provides typed exception classes for different failure modes:

```python
from sombra import (
    SombraError,
    AnalyzerError,
    IoError,
    CorruptionError,
    ConflictError,
    ClosedError,
    NotFoundError,
    ErrorCode,
)
from sombra.query import eq

try:
    db.query().nodes("User").where(eq("invalid", "query")).execute()
except AnalyzerError as err:
    print(f"Query syntax error: {err}")
except IoError as err:
    print(f"Database I/O error: {err}")
except ClosedError as err:
    print(f"Database was closed: {err}")
except SombraError as err:
    print(f"Error [{err.code}]: {err}")

# Error codes
ErrorCode.ANALYZER        # Query analysis failed
ErrorCode.IO              # I/O operation failed
ErrorCode.CORRUPTION      # Data corruption detected
ErrorCode.CONFLICT        # Write-write conflict
ErrorCode.SNAPSHOT_TOO_OLD # MVCC snapshot evicted
ErrorCode.CANCELLED       # Operation cancelled
ErrorCode.INVALID_ARG     # Invalid argument
ErrorCode.NOT_FOUND       # Resource not found
ErrorCode.CLOSED          # Database closed
```

## Database Configuration

### Pragmas

```python
# Get pragma value
sync_mode = db.pragma("synchronous")

# Set pragma value
db.pragma("synchronous", "normal")

# Available pragmas
db.pragma("synchronous")          # "full", "normal", or "off"
db.pragma("autocheckpoint_ms")    # int or None
```

### Request Cancellation

```python
import asyncio

# Start a query with a request ID
async def run_query():
    return db.query() \
        .nodes("User") \
        .request_id("slow-query") \
        .execute()

# Cancel it from another context
def cancel_query():
    db.cancel_request("slow-query")
```

## Typed API (Experimental)

Sombra provides a type-safe wrapper API for Python using `TypedDict` for schema definitions:

```python
from typing_extensions import TypedDict
from sombra.typed import SombraDB

class PersonProps(TypedDict):
    name: str
    age: int

class CompanyProps(TypedDict):
    name: str
    employees: int

# Create a typed database instance
db = SombraDB("./typed.db")

# Type-safe node creation
person_id = db.add_node("Person", {"name": "Alice", "age": 30})
company_id = db.add_node("Company", {"name": "ACME", "employees": 100})

# Create edges
edge_id = db.add_edge(person_id, company_id, "WORKS_AT", {"role": "Engineer"})

# Type-safe queries
person = db.get_node(person_id)
print(f"{person.properties['name']} is {person.properties['age']} years old")

# Find by property
found = db.find_node_by_property("Company", "name", "ACME")

# Traversal
employees = db.get_incoming_neighbors(company_id, "WORKS_AT")
```

## Integration with Python Ecosystem

### Pandas Integration

```python
import pandas as pd
from sombra import Database

db = Database.open("./social.db")

# Nodes to DataFrame
user_ids = db.list_nodes_with_label("User")
node_data = []

for node_id in user_ids:
    node = db.get_node_record(node_id)
    if node:
        row = {
            "id": node_id,
            "labels": ",".join(node.get("labels", [])),
            **node.get("properties", {}),
        }
        node_data.append(row)

df_nodes = pd.DataFrame(node_data)
print(df_nodes.head())
```

### NetworkX Integration

```python
import networkx as nx
from sombra import Database

db = Database.open("./social.db")

# Create NetworkX graph from Sombra data
G = nx.DiGraph()

# Add nodes
user_ids = db.list_nodes_with_label("User")
for node_id in user_ids:
    node = db.get_node_record(node_id)
    if node:
        G.add_node(
            node_id,
            labels=node.get("labels", []),
            **node.get("properties", {}),
        )

# Add edges via neighbors
for node_id in user_ids:
    neighbors = db.neighbors(node_id, direction="out", edge_type="FOLLOWS")
    for neighbor in neighbors:
        G.add_edge(node_id, neighbor["node_id"])

# NetworkX analysis
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")
print(f"Density: {nx.density(G):.4f}")

# Centrality measures
centrality = nx.betweenness_centrality(G)
top_nodes = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
print("Top 5 nodes by betweenness centrality:")
for node_id, score in top_nodes:
    print(f"  Node {node_id}: {score:.4f}")
```

## Testing with pytest

```python
import pytest
import tempfile
from pathlib import Path
from sombra import Database
from sombra.query import eq

@pytest.fixture
def test_db():
    """Fixture providing a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = Database.open(str(db_path))
        yield db
        db.close()

def test_create_and_retrieve_node(test_db):
    user_id = test_db.create_node("User", {"name": "Alice", "age": 30})
    node = test_db.get_node_record(user_id)

    assert node is not None
    assert "User" in node["labels"]
    assert node["properties"]["name"] == "Alice"
    assert node["properties"]["age"] == 30

def test_query_with_predicates(test_db):
    test_db.create_node("User", {"name": "Alice", "age": 30})
    test_db.create_node("User", {"name": "Bob", "age": 25})

    rows = test_db.query() \
        .nodes("User") \
        .where(eq("name", "Alice")) \
        .execute()

    assert len(rows) == 1

def test_graph_traversal(test_db):
    alice = test_db.create_node("User", {"name": "Alice"})
    bob = test_db.create_node("User", {"name": "Bob"})
    test_db.create_edge(alice, bob, "FOLLOWS", {})

    neighbors = test_db.neighbors(alice, direction="out", edge_type="FOLLOWS")

    assert len(neighbors) == 1
    assert neighbors[0]["node_id"] == bob

def test_data_persists_after_close():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "persist.db"

        # Create and close
        db1 = Database.open(str(db_path))
        db1.create_node("User", {"name": "Alice"})
        db1.close()

        # Reopen and verify
        db2 = Database.open(str(db_path), create_if_missing=False)
        users = db2.query().nodes("User").execute()
        assert len(users) == 1
        db2.close()
```

## Performance Tips

1. **Use the create builder for batch inserts**: Group related creates together

    ```python
    db.create() \
        .node("User", {"name": "A"}) \
        .node("User", {"name": "B"}) \
        .edge(...) \
        .execute()  # Single commit
    ```

2. **Use mutate_batched for large operations**: Chunk large imports

    ```python
    db.mutate_batched(thousands_of_ops, batch_size=1000)
    ```

3. **Stream large result sets**: Avoid loading everything into memory

    ```python
    async for row in db.query().nodes("User").stream():
        process_row(row)
    ```

4. **Use context managers**: Ensure databases are properly closed

    ```python
    with Database.open("./data.db") as db:
        # operations
    ```

5. **Use request IDs for cancellation**: Cancel long-running queries
    ```python
    db.query().request_id("my-query").execute()
    db.cancel_request("my-query")
    ```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic concepts
- Check the [Configuration Guide](configuration.md) for tuning options
- Review the [Architecture Guide](architecture.md) for internals
- Browse the [examples](https://github.com/sombra-db/sombra/tree/main/bindings/python/examples) directory for complete applications
