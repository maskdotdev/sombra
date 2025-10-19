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
