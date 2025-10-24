# Sombra - Python Bindings

High-performance graph database for Python, powered by Rust.

> **Note:** This is alpha software under active development. APIs may change between minor versions.

## Installation

```bash
pip install sombra
```

## Features

- **Property Graph Model**: Nodes, edges, and flexible properties
- **ACID Transactions**: Full transactional support with rollback
- **Native Performance**: Rust implementation with PyO3 bindings
- **Type Hints**: Full type annotations and IDE support
- **Cross-Platform**: Pre-built wheels for Linux, macOS, and Windows

## Quick Start

```python
from sombra import GraphDB

db = GraphDB("my_graph.db")

user = db.add_node()
db.set_node_label(user, "User")
db.set_node_property(user, "name", "Alice")

post = db.add_node()
db.set_node_label(post, "Post")

db.add_edge(user, post, "AUTHORED")

neighbors = db.get_neighbors(user)
print(f"User {user} authored {len(neighbors)} posts")
```

## Transactions

```python
from sombra import GraphDB

db = GraphDB("my_graph.db")

tx = db.begin_transaction()
try:
    user = tx.add_node()
    post = tx.add_node()
    tx.add_edge(user, post, "AUTHORED")
    tx.commit()
except Exception as e:
    tx.rollback()
    raise
```

## Documentation

- [Getting Started Guide](https://github.com/maskdotdev/sombra/blob/main/docs/python-guide.md)
- [Main Documentation](https://github.com/maskdotdev/sombra)

## Building from Source

```bash
pip install maturin
maturin develop
pytest
```

## Repository

[GitHub](https://github.com/maskdotdev/sombra)

## License

MIT
