# Getting Started with Sombra

Welcome to Sombra, a high-performance embedded graph database for Rust, Python, and Node.js applications.

## Installation

### Rust

Add Sombra to your `Cargo.toml`:

```toml
[dependencies]
sombra = "0.1"
```

### Python

Install from PyPI:

```bash
pip install sombra
```

Or from source:

```bash
git clone https://github.com/sombra-db/sombra
cd sombra
pip install .
```

### Node.js

Install from npm:

```bash
npm install sombra
```

Or from source:

```bash
git clone https://github.com/sombra-db/sombra
cd sombra
npm install
npm run build
```

## Quick Start

### Rust

```rust
use sombra::{GraphDB, Node, Edge, PropertyValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a database
    let mut db = GraphDB::open("my_graph.db")?;
    
    // Start a transaction
    let mut tx = db.begin_transaction()?;
    
    // Create nodes
    let alice = tx.create_node("Person", vec![
        ("name".into(), PropertyValue::String("Alice".into())),
        ("age".into(), PropertyValue::Integer(30)),
    ])?;
    
    let bob = tx.create_node("Person", vec![
        ("name".into(), PropertyValue::String("Bob".into())),
        ("age".into(), PropertyValue::Integer(25)),
    ])?;
    
    // Create an edge
    tx.create_edge(alice, bob, "KNOWS", vec![
        ("since".into(), PropertyValue::Integer(2020)),
    ])?;
    
    // Commit the transaction
    tx.commit()?;
    
    println!("Graph created successfully!");
    Ok(())
}
```

### Python

```python
import sombra

# Open or create a database
db = sombra.GraphDB("my_graph.db")

# Start a transaction
tx = db.begin_transaction()

# Create nodes
alice = tx.create_node("Person", {
    "name": "Alice",
    "age": 30
})

bob = tx.create_node("Person", {
    "name": "Bob", 
    "age": 25
})

# Create an edge
tx.create_edge(alice, bob, "KNOWS", {
    "since": 2020
})

# Commit the transaction
tx.commit()

print("Graph created successfully!")
```

### Node.js/TypeScript

```typescript
import { GraphDB } from 'sombra';

async function main() {
    // Open or create a database
    const db = new GraphDB('my_graph.db');
    
    // Start a transaction
    const tx = db.beginTransaction();
    
    // Create nodes
    const alice = await tx.createNode('Person', {
        name: 'Alice',
        age: 30
    });
    
    const bob = await tx.createNode('Person', {
        name: 'Bob',
        age: 25
    });
    
    // Create an edge
    await tx.createEdge(alice, bob, 'KNOWS', {
        since: 2020
    });
    
    // Commit the transaction
    await tx.commit();
    
    console.log('Graph created successfully!');
}

main().catch(console.error);
```

## Basic CRUD Operations

### Creating Nodes

```rust
// Create a node with properties
let node = tx.create_node("User", vec![
    ("username".into(), PropertyValue::String("john_doe".into())),
    ("email".into(), PropertyValue::String("john@example.com".into())),
    ("active".into(), PropertyValue::Boolean(true)),
])?;
```

### Reading Nodes

```rust
// Get node by ID
let node = tx.get_node(node_id)?;

// Get node properties
let properties = tx.get_node_properties(node_id)?;
if let Some(name) = properties.get("name") {
    println!("Node name: {:?}", name);
}

// Find nodes by label
let users = tx.find_nodes_by_label("User")?;
```

### Updating Nodes

```rust
// Update node properties
tx.update_node_properties(node_id, vec![
    ("last_login".into(), PropertyValue::Integer(timestamp)),
])?;
```

### Deleting Nodes

```rust
// Delete a node (also deletes connected edges)
tx.delete_node(node_id)?;
```

### Working with Edges

```rust
// Create an edge
tx.create_edge(from_node, to_node, "FOLLOWS", vec![
    ("since".into(), PropertyValue::Integer(2023)),
])?;

// Get edges from a node
let outgoing = tx.get_outgoing_edges(node_id)?;
let incoming = tx.get_incoming_edges(node_id)?;

// Delete an edge
tx.delete_edge(edge_id)?;
```

## Graph Traversal

### Basic Traversal

```rust
// Find all friends of a user
let user_node = tx.find_nodes_by_property("User", "username", &PropertyValue::String("alice".into()))?
    .into_iter().next()
    .ok_or("User not found")?;

let friends = tx.traverse()
    .from(user_node)
    .outgoing("FRIENDS_WITH")
    .collect::<Result<Vec<_>, _>>()?;

println!("Alice has {} friends", friends.len());
```

### Multi-hop Traversal

```rust
// Find friends of friends (2 hops)
let fofs = tx.traverse()
    .from(user_node)
    .outgoing("FRIENDS_WITH")
    .outgoing("FRIENDS_WITH")
    .collect::<Result<Vec<_>, _>>()?;
```

### Conditional Traversal

```rust
// Find active users who follow the target user
let active_followers = tx.traverse()
    .from(user_node)
    .incoming("FOLLOWS")
    .filter(|node| {
        let props = tx.get_node_properties(node.id)?;
        Ok(props.get("active")
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    })
    .collect::<Result<Vec<_>, _>>()?;
```

## Running Tests

### Rust Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture

# Run benchmarks
cargo bench
```

### Python Tests

```bash
# Install test dependencies
pip install pytest

# Run tests
pytest tests/

# Run with coverage
pytest --cov=sombra tests/
```

### Node.js Tests

```bash
# Run tests
npm test

# Run with coverage
npm run test:coverage
```

## Next Steps

- Read the [Configuration Guide](configuration.md) to learn about performance tuning
- Check the [Operations Guide](operations.md) for production deployment
- Explore the [Python Guide](python-guide.md) for Python-specific features
- See the [Node.js Guide](nodejs-guide.md) for TypeScript patterns
- Browse the [examples](../examples/) directory for complete applications

## Getting Help

- 📖 [Documentation](https://docs.sombra.dev)
- 🐛 [Issue Tracker](https://github.com/sombra-db/sombra/issues)
- 💬 [Discussions](https://github.com/sombra-db/sombra/discussions)
- 📧 [Email](mailto:support@sombra.dev)