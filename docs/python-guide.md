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
cd sombra
pip install .
```

### Development Installation

```bash
git clone https://github.com/sombra-db/sombra
cd sombra
pip install -e .
```

## Quick Start

```python
import sombra

# Open or create a database
db = sombra.SombraDB("example.db")

# Start a transaction
tx = db.begin_transaction()

# Create nodes
alice_id = tx.add_node(["Person"], {
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
})

bob_id = tx.add_node(["Person"], {
    "name": "Bob",
    "age": 25,
    "email": "bob@example.com"
})

# Create an edge
edge_id = tx.add_edge(alice_id, bob_id, "KNOWS", {
    "since": 2020,
    "relationship": "friend"
})

# Commit the transaction
tx.commit()

print("Graph created successfully!")
```

## Basic Operations

### Working with Nodes

```python
# Create a node
user_id = tx.add_node(["User"], {
    "username": "john_doe",
    "email": "john@example.com",
    "active": True,
    "created_at": 1640995200  # Unix timestamp
})

# Get node by ID
node = tx.get_node(user_id)
print(f"Node {node.id}: {node.labels}")
print(f"Properties: {node.properties}")

# Update a node property
tx.set_node_property(user_id, "last_login", 1640995300)
tx.set_node_property(user_id, "login_count", 5)

# Remove a node property
tx.remove_node_property(user_id, "last_login")

# Find nodes by label
users = tx.get_nodes_by_label("User")
print(f"Found {len(users)} users")

# Delete a node
tx.delete_node(user_id)
```

### Working with Edges

```python
# Create an edge
follow_edge_id = tx.add_edge(alice_id, bob_id, "FOLLOWS", {
    "since": 2021,
    "strength": 0.8
})

# Get edge by ID
edge = tx.get_edge(follow_edge_id)
print(f"Edge {edge.id}: {edge.source_node_id} -> {edge.target_node_id}")
print(f"Type: {edge.type_name}")
print(f"Properties: {edge.properties}")

# Get edges from a node
outgoing = tx.get_outgoing_edges(alice_id)
incoming = tx.get_incoming_edges(bob_id)

print(f"Alice follows {len(outgoing)} users")
print(f"Bob has {len(incoming)} followers")

# Count edges
out_count = tx.count_outgoing_edges(alice_id)
in_count = tx.count_incoming_edges(bob_id)

# Delete an edge
tx.delete_edge(follow_edge_id)
```

### Graph Traversal

```python
# Get direct neighbors
neighbors = tx.get_neighbors(alice_id)
print(f"Alice's neighbors: {neighbors}")

# Get incoming neighbors only
incoming_neighbors = tx.get_incoming_neighbors(alice_id)

# Multi-hop traversal
two_hop_neighbors = tx.get_neighbors_two_hops(alice_id)
three_hop_neighbors = tx.get_neighbors_three_hops(alice_id)

# BFS traversal
results = tx.bfs_traversal(alice_id, max_depth=3)
for result in results:
    print(f"Node {result.node_id} at depth {result.depth}")
```

## Range Queries

```python
# Get nodes in ID range
nodes = db.get_nodes_in_range(100, 200)

# Get nodes from a starting ID
nodes = db.get_nodes_from(100)

# Get nodes up to an ending ID
nodes = db.get_nodes_to(200)

# Get first/last nodes
first = db.get_first_node()
last = db.get_last_node()

# Get first/last N nodes
first_10 = db.get_first_n_nodes(10)
last_10 = db.get_last_n_nodes(10)

# Get all node IDs ordered
all_nodes = db.get_all_node_ids_ordered()
```

## Transactions

### Manual Transaction Management

```python
# Begin a transaction
tx = db.begin_transaction()

try:
    # Perform operations
    user_id = tx.add_node(["User"], {"name": "Alice"})
    friend_id = tx.add_node(["User"], {"name": "Bob"})
    tx.add_edge(user_id, friend_id, "KNOWS", {})
    
    # Commit on success
    tx.commit()
except Exception as e:
    # Rollback on error
    tx.rollback()
    raise
```

### Batch Operations

```python
# Batch node creation
user_ids = []
tx = db.begin_transaction()

for i in range(1000):
    user_id = tx.add_node(["User"], {
        "name": f"User{i}",
        "index": i
    })
    user_ids.append(user_id)

tx.commit()

# Batch edge creation
tx = db.begin_transaction()

for i in range(len(user_ids) - 1):
    tx.add_edge(user_ids[i], user_ids[i + 1], "FOLLOWS", {"order": i})

tx.commit()
```

## Integration with Python Ecosystem

### Pandas Integration

Convert graph data to pandas DataFrames:

```python
import pandas as pd

# Nodes to DataFrame
node_ids = db.get_nodes_by_label("User")
node_data = []

tx = db.begin_transaction()
for node_id in node_ids:
    node = tx.get_node(node_id)
    row = {
        "id": node.id,
        "labels": ",".join(node.labels),
        **node.properties
    }
    node_data.append(row)
tx.commit()

df_nodes = pd.DataFrame(node_data)
print(df_nodes.head())

# Edges to DataFrame
edge_data = []
for node_id in node_ids:
    outgoing = tx.get_outgoing_edges(node_id)
    for edge_id in outgoing:
        edge = tx.get_edge(edge_id)
        row = {
            "id": edge.id,
            "source": edge.source_node_id,
            "target": edge.target_node_id,
            "type": edge.type_name,
            **edge.properties
        }
        edge_data.append(row)

df_edges = pd.DataFrame(edge_data)
print(df_edges.head())
```

### NetworkX Integration

Use NetworkX for graph analysis:

```python
import networkx as nx

# Create NetworkX graph from Sombra data
G = nx.DiGraph()

tx = db.begin_transaction()

# Add nodes
node_ids = db.get_nodes_by_label("User")
for node_id in node_ids:
    node = tx.get_node(node_id)
    G.add_node(node.id, labels=node.labels, **node.properties)

# Add edges
for node_id in node_ids:
    outgoing = tx.get_outgoing_edges(node_id)
    for edge_id in outgoing:
        edge = tx.get_edge(edge_id)
        G.add_edge(edge.source_node_id, edge.target_node_id, 
                   type=edge.type_name, **edge.properties)

tx.commit()

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

### Matplotlib Visualization

Visualize graph data with Matplotlib:

```python
import matplotlib.pyplot as plt
import networkx as nx

# Create a subgraph for visualization (assuming G is already created)
subgraph_nodes = list(G.nodes())[:20]  # First 20 nodes
H = G.subgraph(subgraph_nodes)

# Draw the graph
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(H)

# Draw nodes
nx.draw_networkx_nodes(H, pos, node_color='lightblue', node_size=500)
nx.draw_networkx_labels(H, pos)

# Draw edges
nx.draw_networkx_edges(H, pos, edge_color='gray', arrows=True)

plt.title("Social Network Subgraph")
plt.axis('off')
plt.show()
```

## Error Handling

```python
import sombra

# Handle database errors
try:
    db = sombra.SombraDB("/invalid/path/db.db")
except IOError as e:
    print(f"Cannot open database: {e}")

# Handle transaction errors
try:
    tx = db.begin_transaction()
    node_id = tx.add_node(["User"], {"name": "Alice"})
    # Try to create invalid edge
    tx.add_edge(node_id, 999999, "KNOWS", {})  # Invalid target
    tx.commit()
except ValueError as e:
    print(f"Invalid operation: {e}")
    tx.rollback()
except Exception as e:
    print(f"Error: {e}")
    tx.rollback()
```

## Performance Optimization

### Bulk Operations

```python
def bulk_insert_users(db, users_data):
    """Efficiently insert many users in batches"""
    batch_size = 1000
    
    for i in range(0, len(users_data), batch_size):
        batch = users_data[i:i + batch_size]
        
        tx = db.begin_transaction()
        for user_data in batch:
            tx.add_node(["User"], user_data)
        tx.commit()

# Usage
users_data = [
    {"name": f"User{i}", "email": f"user{i}@example.com"}
    for i in range(10000)
]

bulk_insert_users(db, users_data)
```

### Checkpointing

```python
# Periodically checkpoint to ensure durability
db.checkpoint()

# Flush pending writes
db.flush()
```

## Testing

### Unit Testing

```python
import unittest
import tempfile
import os
import sombra

class TestSombraOperations(unittest.TestCase):
    def setUp(self):
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        self.db = sombra.SombraDB(self.temp_db.name)
    
    def tearDown(self):
        os.unlink(self.temp_db.name)
    
    def test_create_and_retrieve_node(self):
        tx = self.db.begin_transaction()
        user_id = tx.add_node(["User"], {"name": "Alice"})
        tx.commit()
        
        tx = self.db.begin_transaction()
        retrieved = tx.get_node(user_id)
        self.assertIn("User", retrieved.labels)
        self.assertEqual(retrieved.properties["name"], "Alice")
        tx.commit()
    
    def test_neighbors(self):
        tx = self.db.begin_transaction()
        alice_id = tx.add_node(["User"], {"name": "Alice"})
        bob_id = tx.add_node(["User"], {"name": "Bob"})
        tx.add_edge(alice_id, bob_id, "KNOWS", {})
        tx.commit()
        
        tx = self.db.begin_transaction()
        neighbors = tx.get_neighbors(alice_id)
        self.assertEqual(len(neighbors), 1)
        self.assertEqual(neighbors[0], bob_id)
        tx.commit()

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing

```python
import pytest
import sombra
import tempfile
import os

@pytest.fixture
def test_db():
    """Fixture providing a temporary database for testing"""
    temp_db = tempfile.NamedTemporaryFile(delete=False)
    temp_db.close()
    
    db = sombra.SombraDB(temp_db.name)
    yield db
    
    os.unlink(temp_db.name)

def test_social_network_scenario(test_db):
    """Test a complete social network scenario"""
    tx = test_db.begin_transaction()
    
    # Create users
    alice_id = tx.add_node(["User"], {"name": "Alice", "age": 30})
    bob_id = tx.add_node(["User"], {"name": "Bob", "age": 25})
    charlie_id = tx.add_node(["User"], {"name": "Charlie", "age": 35})
    
    # Create friendships
    tx.add_edge(alice_id, bob_id, "FRIENDS", {"since": 2020})
    tx.add_edge(bob_id, charlie_id, "FRIENDS", {"since": 2021})
    tx.add_edge(alice_id, charlie_id, "FRIENDS", {"since": 2022})
    
    tx.commit()
    
    # Test Alice's friends
    tx = test_db.begin_transaction()
    alice_neighbors = tx.get_neighbors(alice_id)
    assert len(alice_neighbors) == 2
    
    # Test two-hop traversal
    two_hop = tx.get_neighbors_two_hops(alice_id)
    assert len(two_hop) >= 2
    
    tx.commit()
```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic usage
- Check the [Configuration Guide](configuration.md) for performance tuning
- Review the [Operations Guide](operations.md) for production deployment
- Browse the [examples](../examples/) directory for complete applications