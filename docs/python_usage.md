# Python Usage

Sombra exposes a high-performance Python binding built with [PyO3](https://pyo3.rs). The interface mirrors the TypeScript API while translating graph properties to native Python types (strings, integers, floats, booleans, and bytes).

## Prerequisites

- Python 3.8 or newer
- Rust toolchain (via `rustup`)
- [`maturin`](https://github.com/PyO3/maturin) for building wheels: `pip install maturin`

## Installing the Extension

Use `maturin` to compile and install the extension into your current Python environment:

```bash
pip install maturin
maturin develop --release -F python
```

- `develop` installs the module in editable mode, rebuilding automatically when sources change.
- `-F python` enables the PyO3 bindings in the Rust crate.

To publish a wheel:

```bash
maturin build --release -F python
# `pip install` the generated wheel in `target/wheels/`
```

## Quick Start

```python
from sombra import SombraDB

db = SombraDB("./example.db")

alice = db.add_node(["Person"], {"name": "Alice", "age": 30})
bob = db.add_node(["Person"], {"name": "Bob", "age": 25})

db.add_edge(alice, bob, "KNOWS", {"since": 2020})

node = db.get_node(alice)
print(node.labels)        # ['Person']
print(node.properties)    # {'name': 'Alice', 'age': 30}

neighbors = db.get_neighbors(alice)
print(neighbors)          # [bob]
```

## Property Mapping

Python values map directly onto Sombra's property types:

| Python Value | Stored As               |
|--------------|-------------------------|
| `str`        | `PropertyValue::String` |
| `int`        | `PropertyValue::Int`    |
| `float`      | `PropertyValue::Float`  |
| `bool`       | `PropertyValue::Bool`   |
| `bytes` / `bytearray` | `PropertyValue::Bytes` |

If you need binary data, pass `bytes` or `bytearray`. All other types raise a `TypeError`.

## Transactions

Transactions behave the same way as in Rust and TypeScript—explicitly commit to persist, otherwise roll back:

```python
tx = db.begin_transaction()
try:
    charlie = tx.add_node(["Person"], {"name": "Charlie"})
    tx.add_edge(alice, charlie, "KNOWS")
    tx.commit()
except Exception:
    tx.rollback()
    raise
```

After `commit` or `rollback` the transaction object is marked finished; calling either method a second time raises `ValueError`.

## Graph Queries

### Traversal Operations

All traversal helpers are exposed:

```python
edges = db.get_outgoing_edges(alice)
incoming = db.get_incoming_edges(bob)
neighbors = db.get_neighbors(alice)
incoming_neighbors = db.get_incoming_neighbors(alice)
two_hops = db.get_neighbors_two_hops(alice)
bfs = db.bfs_traversal(start_node_id=alice, max_depth=2)

for result in bfs:
    print(result.node_id, result.depth)
```

`SombraNode` and `SombraEdge` instances expose `id`, `labels`/`type_name`, and a lazily constructed `properties` dictionary each time you access it, so mutations to the returned dict do not change the stored data.

### Query Builder (Chainable API)

Sombra provides a fluent, chainable query builder for complex graph queries:

```python
result = db.query() \
    .start_from_label("Function") \
    .traverse(["CALLS"], "outgoing", 2) \
    .limit(10) \
    .execute()

print(f"Found {len(result.nodes)} nodes")
print(f"Start nodes: {result.start_nodes}")
print(f"All node IDs: {result.node_ids}")
```

**Starting Points:**

```python
# Start from specific node IDs
result = db.query() \
    .start_from([node_id1, node_id2]) \
    .execute()

# Start from all nodes with a label
result = db.query() \
    .start_from_label("Function") \
    .execute()

# Start from nodes matching a property
result = db.query() \
    .start_from_property("Function", "name", "main") \
    .execute()
```

**Traversal:**

```python
# Traverse outgoing edges
result = db.query() \
    .start_from_label("Function") \
    .traverse(["CALLS"], "outgoing", depth=2) \
    .execute()

# Traverse incoming edges
result = db.query() \
    .start_from([func_id]) \
    .traverse(["CONTAINS"], "incoming", depth=1) \
    .execute()

# Traverse both directions
result = db.query() \
    .start_from([node_id]) \
    .traverse(["KNOWS"], "both", depth=2) \
    .execute()
```

**Limiting Results:**

```python
# Limit the number of results
result = db.query() \
    .start_from_label("Person") \
    .limit(100) \
    .execute()
```

**Query Result:**

The `QueryResult` object contains:
- `start_nodes`: List of starting node IDs
- `node_ids`: List of all discovered node IDs
- `nodes`: List of `SombraNode` objects (lazily loaded)
- `edges`: List of `SombraEdge` objects (lazily loaded)
- `limited`: Boolean indicating if results were truncated

```python
result = db.query() \
    .start_from_label("Function") \
    .traverse(["CALLS"], "outgoing", 3) \
    .limit(50) \
    .execute()

if result.limited:
    print("Results were truncated to 50 nodes")

for node in result.nodes:
    print(f"Node {node.id}: {node.properties}")

for edge in result.edges:
    print(f"Edge {edge.source_node_id} -> {edge.target_node_id}: {edge.type_name}")
```

## Hierarchy Traversal

Sombra provides powerful methods for traversing hierarchical relationships in your graph. These are particularly useful for code analysis, organizational charts, file systems, and any tree-like structures.

### Find Ancestor by Label

Find the nearest ancestor with a specific label:

```python
file_node = db.find_ancestor_by_label(statement_node, "File", "PARENT")

function_node = db.find_ancestor_by_label(block_node, "Function", "PARENT")

if file_node is None:
    print("No ancestor with label 'File' found")
```

### Get All Ancestors

Retrieve all ancestors up to a specified depth:

```python
all_ancestors = db.get_ancestors(node_id, "PARENT")

limited_ancestors = db.get_ancestors(node_id, "PARENT", max_depth=3)
```

### Get All Descendants

Retrieve all descendants up to a specified depth:

```python
all_descendants = db.get_descendants(node_id, "PARENT")

limited_descendants = db.get_descendants(node_id, "PARENT", max_depth=2)
```

### Get Containing File

Convenience method to find the File node containing a given node:

```python
file_id = db.get_containing_file(statement_id)
print(f"Statement is in file: {db.get_node(file_id)}")
```

### Example: Code Analysis Hierarchy

```python
file = db.add_node(["File"], {"name": "main.py"})
func = db.add_node(["Function"], {"name": "process_data"})
block = db.add_node(["Block"], {"name": "if-block"})
stmt = db.add_node(["Statement"], {"name": "return"})

db.add_edge(func, file, "PARENT")
db.add_edge(block, func, "PARENT")
db.add_edge(stmt, block, "PARENT")

ancestors = db.get_ancestors(stmt, "PARENT")

function_node = db.find_ancestor_by_label(stmt, "Function", "PARENT")
file_node = db.get_containing_file(stmt)
```

## Pattern Matching

Sombra provides a declarative pattern matching API for querying graph structures. This is ideal for code analysis (finding call patterns, import chains), dependency tracking, and complex relationship queries.

### Basic Pattern Matching

Find nodes and edges matching a specific pattern:

```python
from sombra import Pattern, NodePattern, EdgePattern, PropertyFilters

pattern = Pattern(
    nodes=[
        NodePattern(
            var_name="call",
            labels=["CallExpr"],
            properties=PropertyFilters(
                equals={"callee": "foo"},
                not_equals={},
                ranges=[]
            )
        ),
        NodePattern(
            var_name="func",
            labels=["Function"],
            properties=PropertyFilters(
                equals={"name": "foo"},
                not_equals={},
                ranges=[]
            )
        )
    ],
    edges=[
        EdgePattern(
            from_var="call",
            to_var="func",
            types=["CALLS"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
            direction="outgoing"
        )
    ]
)

matches = db.match_pattern(pattern)
for match in matches:
    print(f"Call node: {match.node_bindings['call']}")
    print(f"Function node: {match.node_bindings['func']}")
    print(f"Edge IDs: {match.edge_ids}")
```

### Property Filters

Pattern matching supports three types of property filters:

#### Equals Filter
```python
PropertyFilters(
    equals={"status": "active", "count": 5},
    not_equals={},
    ranges=[]
)
```

#### Not-Equals Filter
```python
PropertyFilters(
    equals={},
    not_equals={"visibility": "private"},
    ranges=[]
)
```

#### Range Filter
```python
from sombra import PropertyRangeFilter, PropertyBound

PropertyFilters(
    equals={},
    not_equals={},
    ranges=[
        PropertyRangeFilter(
            "age",
            PropertyBound(30, True),  # min: value=30, inclusive=True
            PropertyBound(40, True)   # max: value=40, inclusive=True
        )
    ]
)
```

`PropertyBound` takes two arguments: `value` (the boundary value) and `inclusive` (boolean).

### Edge Directions

Edges can match in three directions:

```python
EdgePattern(..., direction="outgoing")

EdgePattern(..., direction="incoming")

EdgePattern(..., direction="both")
```

### Multi-Hop Patterns

Match paths through multiple nodes:

```python
pattern = Pattern(
    nodes=[
        NodePattern(
            var_name="a",
            labels=["Person"],
            properties=PropertyFilters(
                equals={"name": "Alice"},
                not_equals={},
                ranges=[]
            )
        ),
        NodePattern(
            var_name="b",
            labels=["Person"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[])
        ),
        NodePattern(
            var_name="c",
            labels=["Person"],
            properties=PropertyFilters(
                equals={"name": "Charlie"},
                not_equals={},
                ranges=[]
            )
        )
    ],
    edges=[
        EdgePattern(
            from_var="a",
            to_var="b",
            types=["KNOWS"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
            direction="outgoing"
        ),
        EdgePattern(
            from_var="b",
            to_var="c",
            types=["KNOWS"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
            direction="outgoing"
        )
    ]
)

matches = db.match_pattern(pattern)
```

### Example: Code Analysis Queries

Find all function calls to a specific function:

```python
call_pattern = Pattern(
    nodes=[
        NodePattern(
            var_name="call",
            labels=["CallExpr"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[])
        ),
        NodePattern(
            var_name="func",
            labels=["Function"],
            properties=PropertyFilters(
                equals={"name": "dangerous_api"},
                not_equals={},
                ranges=[]
            )
        )
    ],
    edges=[
        EdgePattern(
            from_var="call",
            to_var="func",
            types=["CALLS"],
            properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
            direction="outgoing"
        )
    ]
)

dangerous_calls = db.match_pattern(call_pattern)
print(f"Found {len(dangerous_calls)} calls to dangerous_api")
```

Find import chains:

```python
import_chain_pattern = Pattern(
    nodes=[
        NodePattern(var_name="file1", labels=["File"], 
                   properties=PropertyFilters(equals={}, not_equals={}, ranges=[])),
        NodePattern(var_name="file2", labels=["File"],
                   properties=PropertyFilters(equals={}, not_equals={}, ranges=[])),
        NodePattern(var_name="file3", labels=["File"],
                   properties=PropertyFilters(equals={}, not_equals={}, ranges=[]))
    ],
    edges=[
        EdgePattern(from_var="file1", to_var="file2", types=["IMPORTS"],
                   properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
                   direction="outgoing"),
        EdgePattern(from_var="file2", to_var="file3", types=["IMPORTS"],
                   properties=PropertyFilters(equals={}, not_equals={}, ranges=[]),
                   direction="outgoing")
    ]
)

import_chains = db.match_pattern(import_chain_pattern)
```

### Performance Considerations

- Specify labels and property filters to reduce the search space
- More selective patterns (with specific property values) will be faster
- The algorithm has O(P * (V + E)) worst-case complexity where P is pattern size, V is nodes, E is edges
- Use property indexes for frequently queried properties to improve performance


