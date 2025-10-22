# Getting Started with Sombra

Welcome to Sombra, a high-performance embedded graph database for Rust, Python, and Node.js applications.

## Installation

### Rust

Add Sombra to your `Cargo.toml`:

```toml
[dependencies]
sombra = "0.3"
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
    let mut alice = Node::new(1);
    alice.labels.push("Person".to_string());
    alice.properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    alice.properties.insert("age".to_string(), PropertyValue::Int(30));
    let alice_id = tx.add_node(alice)?;
    
    let mut bob = Node::new(2);
    bob.labels.push("Person".to_string());
    bob.properties.insert("name".to_string(), PropertyValue::String("Bob".to_string()));
    bob.properties.insert("age".to_string(), PropertyValue::Int(25));
    let bob_id = tx.add_node(bob)?;
    
    // Create an edge
    let mut edge = Edge::new(1, alice_id, bob_id, "KNOWS");
    edge.properties.insert("since".to_string(), PropertyValue::Int(2020));
    tx.add_edge(edge)?;
    
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
db = sombra.SombraDB("my_graph.db")

# Start a transaction
tx = db.begin_transaction()

# Create nodes
alice = tx.add_node(["Person"], {
    "name": "Alice",
    "age": 30
})

bob = tx.add_node(["Person"], {
    "name": "Bob", 
    "age": 25
})

# Create an edge
tx.add_edge(alice, bob, "KNOWS", {
    "since": 2020
})

# Commit the transaction
tx.commit()

print("Graph created successfully!")
```

### Node.js/TypeScript

```typescript
import { SombraDB } from 'sombra';

async function main() {
    // Open or create a database
    const db = new SombraDB('my_graph.db');
    
    // Start a transaction
    const tx = db.beginTransaction();
    
    // Create nodes
    const alice = tx.addNode(['Person'], {
        name: { type: 'string', value: 'Alice' },
        age: { type: 'int', value: 30 }
    });
    
    const bob = tx.addNode(['Person'], {
        name: { type: 'string', value: 'Bob' },
        age: { type: 'int', value: 25 }
    });
    
    // Create an edge
    tx.addEdge(alice, bob, 'KNOWS', {
        since: { type: 'int', value: 2020 }
    });
    
    // Commit the transaction
    tx.commit();
    
    console.log('Graph created successfully!');
}

main().catch(console.error);
```

## Basic CRUD Operations

### Creating Nodes

```rust
use std::collections::BTreeMap;

// Create a node with properties
let mut node = Node::new(1);
node.labels.push("User".to_string());
node.properties.insert("username".to_string(), PropertyValue::String("john_doe".to_string()));
node.properties.insert("email".to_string(), PropertyValue::String("john@example.com".to_string()));
node.properties.insert("active".to_string(), PropertyValue::Bool(true));

let node_id = tx.add_node(node)?;
```

### Reading Nodes

```rust
// Get node by ID
let node = tx.get_node(node_id)?;

// Get node properties
let name = node.properties.get("name");
if let Some(PropertyValue::String(name_str)) = name {
    println!("Node name: {}", name_str);
}

// Find nodes by label
let users = tx.get_nodes_by_label("User")?;
```

### Updating Nodes

```rust
// Update node properties (outside transaction)
db.set_node_property(node_id, "last_login".to_string(), PropertyValue::Int(timestamp))?;
```

### Deleting Nodes

```rust
// Delete a node (also deletes connected edges)
tx.delete_node(node_id)?;
```

### Working with Edges

```rust
// Create an edge
let edge = Edge::new(1, from_node, to_node, "FOLLOWS");
tx.add_edge(edge)?;

// Get edges from a node
let outgoing = db.get_outgoing_edges(node_id)?;
let incoming = db.get_incoming_edges(node_id)?;

// Delete an edge
tx.delete_edge(edge_id)?;
```

## Graph Traversal

### Basic Traversal

```rust
use sombra::EdgeDirection;

// Find all friends of a user
let result = db.query()
    .start_from_label("User")
    .filter_nodes(|node| {
        matches!(
            node.properties.get("username"),
            Some(PropertyValue::String(name)) if name == "alice"
        )
    })
    .traverse(&["FRIENDS_WITH"], EdgeDirection::Outgoing, 1)
    .execute()?;

println!("Alice has {} friends", result.node_ids.len() - 1);
```

### Multi-hop Traversal

```rust
// Find friends of friends (2 hops)
let result = db.query()
    .start_from_label("User")
    .filter_nodes(|node| {
        matches!(
            node.properties.get("username"),
            Some(PropertyValue::String(name)) if name == "alice"
        )
    })
    .traverse(&["FRIENDS_WITH"], EdgeDirection::Outgoing, 2)
    .execute()?;
```

### Conditional Traversal

```rust
// Find active users who follow the target user
let result = db.query()
    .start_from_property("User", "username", &PropertyValue::String("alice".to_string()))
    .traverse(&["FOLLOWS"], EdgeDirection::Incoming, 1)
    .filter_nodes(|node| {
        matches!(
            node.properties.get("active"),
            Some(PropertyValue::Bool(true))
        )
    })
    .execute()?;
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