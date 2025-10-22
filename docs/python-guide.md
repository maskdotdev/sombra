# Python Binding Guide

This guide covers using Sombra from Python, including installation, basic operations, advanced patterns, and integration with the Python ecosystem.

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
db = sombra.GraphDB("example.db")

# Start a transaction
tx = db.begin_transaction()

# Create nodes
alice = tx.create_node("Person", {
    "name": "Alice",
    "age": 30,
    "email": "alice@example.com"
})

bob = tx.create_node("Person", {
    "name": "Bob",
    "age": 25,
    "email": "bob@example.com"
})

# Create an edge
tx.create_edge(alice, bob, "KNOWS", {
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
user = tx.create_node("User", {
    "username": "john_doe",
    "email": "john@example.com",
    "active": True,
    "created_at": 1640995200  # Unix timestamp
})

# Get node by ID
node = tx.get_node(user.id)
print(f"Node {node.id}: {node.label}")

# Get node properties
props = tx.get_node_properties(user.id)
print(f"Name: {props.get('name')}")
print(f"Age: {props.get('age')}")

# Update node properties
tx.update_node_properties(user.id, {
    "last_login": 1640995300,
    "login_count": 5
})

# Find nodes by label
users = tx.find_nodes_by_label("User")
print(f"Found {len(users)} users")

# Find nodes by property
john = tx.find_nodes_by_property("User", "username", "john_doe")
if john:
    print(f"Found user: {john[0].id}")
```

### Working with Edges

```python
# Create an edge
follow_edge = tx.create_edge(alice, bob, "FOLLOWS", {
    "since": 2021,
    "strength": 0.8
})

# Get edges from a node
outgoing = tx.get_outgoing_edges(alice.id)
incoming = tx.get_incoming_edges(bob.id)

print(f"Alice follows {len(outgoing)} users")
print(f"Bob has {len(incoming)} followers")

# Get edge properties
edge_props = tx.get_edge_properties(follow_edge.id)
print(f"Follow strength: {edge_props.get('strength')}")

# Update edge properties
tx.update_edge_properties(follow_edge.id, {
    "strength": 0.9,
    "last_interaction": 1640995400
})

# Delete an edge
tx.delete_edge(follow_edge.id)
```

### Graph Traversal

```python
# Basic traversal
friends = tx.traverse() \
    .from_node(alice.id) \
    .outgoing("KNOWS") \
    .collect()

print(f"Alice's friends: {[f.id for f in friends]}")

# Multi-hop traversal
friends_of_friends = tx.traverse() \
    .from_node(alice.id) \
    .outgoing("KNOWS") \
    .outgoing("KNOWS") \
    .collect()

print(f"Friends of friends: {[f.id for f in friends_of_friends]}")

# Traversal with filters
active_friends = tx.traverse() \
    .from_node(alice.id) \
    .outgoing("KNOWS") \
    .filter(lambda node: tx.get_node_properties(node.id).get("active", False)) \
    .collect()

print(f"Active friends: {[f.id for f in active_friends]}")

# Bidirectional traversal
mutual_friends = tx.traverse() \
    .from_node(alice.id) \
    .outgoing("KNOWS") \
    .incoming("KNOWS") \
    .filter(lambda node: node.id == bob.id) \
    .collect()
```

## Advanced Patterns

### Context Managers

Use context managers for automatic transaction management:

```python
# Automatic commit on success, rollback on exception
with db.transaction() as tx:
    user = tx.create_node("User", {"name": "Alice"})
    tx.create_edge(user, user, "SELF", {})
    # Transaction automatically committed here

# Manual rollback
with db.transaction() as tx:
    user = tx.create_node("User", {"name": "Bob"})
    if some_condition:
        tx.rollback()
        # Transaction rolled back
```

### Batch Operations

Optimize performance with batch operations:

```python
# Batch node creation
users = []
with db.transaction() as tx:
    for i in range(1000):
        user = tx.create_node("User", {
            "name": f"User{i}",
            "index": i
        })
        users.append(user)

# Batch edge creation
with db.transaction() as tx:
    for i, user in enumerate(users):
        if i > 0:
            tx.create_edge(users[i-1], user, "FOLLOWS", {"order": i})
```

### Property Queries

Advanced property-based queries:

```python
# Find users by age range
young_users = tx.find_nodes_by_property_range("User", "age", 18, 25)

# Find users with specific property values
active_users = tx.find_nodes_by_property("User", "active", True)

# Complex property queries
engineers = tx.find_nodes_by_properties("User", {
    "department": "Engineering",
    "level": ["Senior", "Principal"],
    "active": True
})
```

### Aggregation Operations

Perform aggregations on graph data:

```python
# Count nodes by label
node_counts = {}
for label in ["User", "Post", "Comment"]:
    count = len(tx.find_nodes_by_label(label))
    node_counts[label] = count

# Calculate average degree
def average_degree(node_ids):
    total_degree = 0
    for node_id in node_ids:
        outgoing = len(tx.get_outgoing_edges(node_id))
        incoming = len(tx.get_incoming_edges(node_id))
        total_degree += outgoing + incoming
    return total_degree / len(node_ids) if node_ids else 0

users = tx.find_nodes_by_label("User")
avg_user_degree = average_degree([u.id for u in users])
print(f"Average user degree: {avg_user_degree:.2f}")
```

## Integration with Python Ecosystem

### Pandas Integration

Convert graph data to pandas DataFrames:

```python
import pandas as pd

# Nodes to DataFrame
nodes = tx.find_nodes_by_label("User")
node_data = []
for node in nodes:
    props = tx.get_node_properties(node.id)
    props["id"] = node.id
    props["label"] = node.label
    node_data.append(props)

df_nodes = pd.DataFrame(node_data)
print(df_nodes.head())

# Edges to DataFrame
edges = tx.get_all_edges()
edge_data = []
for edge in edges:
    props = tx.get_edge_properties(edge.id)
    props.update({
        "id": edge.id,
        "from_node": edge.from_node,
        "to_node": edge.to_node,
        "label": edge.label
    })
    edge_data.append(props)

df_edges = pd.DataFrame(edge_data)
print(df_edges.head())
```

### NetworkX Integration

Use NetworkX for graph analysis:

```python
import networkx as nx

# Create NetworkX graph from Sombra data
G = nx.DiGraph()

# Add nodes
nodes = tx.find_nodes_by_label("User")
for node in nodes:
    props = tx.get_node_properties(node.id)
    G.add_node(node.id, **props, label=node.label)

# Add edges
edges = tx.get_all_edges()
for edge in edges:
    props = tx.get_edge_properties(edge.id)
    G.add_edge(edge.from_node, edge.to_node, **props, label=edge.label)

# NetworkX analysis
print(f"Number of nodes: {G.number_of_nodes()}")
print(f"Number of edges: {G.number_of_edges()}")
print(f"Density: {nx.density(G):.4f}")

# Centrality measures
centrality = nx.betweenness_centrality(G)
top_nodes = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
print("Top 5 nodes by betweenness centrality:")
for node_id, score in top_nodes:
    props = tx.get_node_properties(node_id)
    name = props.get("name", f"Node{node_id}")
    print(f"  {name}: {score:.4f}")
```

### Matplotlib Visualization

Visualize graph data with Matplotlib:

```python
import matplotlib.pyplot as plt
import networkx as nx

# Create a subgraph for visualization
subgraph_nodes = [alice.id, bob.id] + [f.id for f in friends]
H = G.subgraph(subgraph_nodes)

# Draw the graph
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(H)

# Draw nodes
node_labels = {n: tx.get_node_properties(n).get("name", f"Node{n}") for n in H.nodes()}
nx.draw_networkx_nodes(H, pos, node_color='lightblue', node_size=500)
nx.draw_networkx_labels(H, pos, labels=node_labels)

# Draw edges
edge_labels = {(u, v): H[u][v].get("label", "") for u, v in H.edges()}
nx.draw_networkx_edges(H, pos, edge_color='gray', arrows=True)
nx.draw_networkx_edge_labels(H, pos, edge_labels=edge_labels)

plt.title("Social Network Subgraph")
plt.axis('off')
plt.show()
```

### Concurrent Read Operations

Sombra supports multiple concurrent read operations using a multi-reader, single-writer concurrency model:

```python
import sombra
import concurrent.futures

db = sombra.GraphDB("concurrent.db")

def concurrent_reads():
    """Execute multiple read operations concurrently"""
    user_ids = [1, 2, 3, 4, 5]
    
    def get_user_friends(user_id):
        with db.transaction() as tx:
            return tx.get_outgoing_edges(user_id)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(get_user_friends, user_ids))
    
    for idx, friends in enumerate(results):
        print(f"User {user_ids[idx]} has {len(friends)} friends")

def parallel_property_reads():
    """Fetch node properties concurrently"""
    node_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    def get_properties(node_id):
        with db.transaction() as tx:
            return tx.get_node_properties(node_id)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        properties = list(executor.map(get_properties, node_ids))
    
    print(f"Fetched {len(properties)} node properties concurrently")
    return properties

def parallel_traversals():
    """Execute multiple graph traversals concurrently"""
    start_nodes = [1, 2, 3, 4]
    
    def traverse_from_node(node_id):
        with db.transaction() as tx:
            return tx.traverse() \
                .from_node(node_id) \
                .outgoing("KNOWS") \
                .collect()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(traverse_from_node, start_nodes))
    
    for idx, friends in enumerate(results):
        print(f"Node {start_nodes[idx]} friends: {len(friends)}")
    
    return results

concurrent_reads()
parallel_property_reads()
parallel_traversals()
```

### Async Integration

Use with async frameworks like asyncio:

```python
import asyncio
import sombra

async def process_user_data(user_id):
    # Run database operations in thread pool
    loop = asyncio.get_event_loop()
    
    def db_operations():
        with db.transaction() as tx:
            user = tx.get_node(user_id)
            friends = tx.traverse() \
                .from_node(user_id) \
                .outgoing("KNOWS") \
                .collect()
            return user, friends
    
    user, friends = await loop.run_in_executor(None, db_operations)
    
    # Process results asynchronously
    tasks = []
    for friend in friends:
        task = asyncio.create_task(analyze_friend(friend.id))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    return user, friends, results

async def analyze_friend(friend_id):
    # Simulate async analysis
    await asyncio.sleep(0.1)
    return f"Analysis for friend {friend_id}"

# Usage
async def main():
    db = sombra.GraphDB("social.db")
    
    # Process multiple users concurrently
    user_ids = [1, 2, 3, 4, 5]
    tasks = [process_user_data(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        print(f"Processed user: {result[0].id}")

asyncio.run(main())
```

## Error Handling

### Exception Types

```python
import sombra

try:
    db = sombra.GraphDB("/invalid/path/db.db")
except sombra.GraphError as e:
    print(f"Database error: {e}")
    print(f"Error code: {e.code}")

try:
    with db.transaction() as tx:
        node = tx.create_node("User", {"name": "Alice"})
        # Try to create invalid edge
        tx.create_edge(node.id, 999999, "KNOWS", {})  # Invalid target
except sombra.NodeNotFoundError as e:
    print(f"Node not found: {e}")
except sombra.GraphError as e:
    print(f"General graph error: {e}")
```

### Validation

```python
def validate_user_properties(properties):
    """Validate user properties before creation"""
    required_fields = ["name", "email"]
    
    for field in required_fields:
        if field not in properties:
            raise ValueError(f"Missing required field: {field}")
    
    if "@" not in properties["email"]:
        raise ValueError("Invalid email format")
    
    if "age" in properties and not isinstance(properties["age"], int):
        raise ValueError("Age must be an integer")

# Usage
try:
    validate_user_properties({
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    })
    
    with db.transaction() as tx:
        user = tx.create_node("User", properties)
        
except ValueError as e:
    print(f"Validation error: {e}")
except sombra.GraphError as e:
    print(f"Database error: {e}")
```

## Performance Optimization

### Connection Pooling

```python
import threading
from contextlib import contextmanager

class DatabasePool:
    def __init__(self, db_path, pool_size=5):
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool = threading.Semaphore(pool_size)
        self._local = threading.local()
    
    @contextmanager
    def get_connection(self):
        self._pool.acquire()
        try:
            if not hasattr(self._local, 'db'):
                self._local.db = sombra.GraphDB(self.db_path)
            yield self._local.db
        finally:
            self._pool.release()

# Usage
pool = DatabasePool("production.db", pool_size=10)

def worker_function(user_data):
    with pool.get_connection() as db:
        with db.transaction() as tx:
            user = tx.create_node("User", user_data)
            return user.id

# Use with threads
import concurrent.futures

users = [
    {"name": f"User{i}", "email": f"user{i}@example.com"}
    for i in range(100)
]

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(worker_function, users))
```

### Bulk Operations

```python
def bulk_insert_users(db, users_data):
    """Efficiently insert many users"""
    batch_size = 1000
    
    for i in range(0, len(users_data), batch_size):
        batch = users_data[i:i + batch_size]
        
        with db.transaction() as tx:
            for user_data in batch:
                tx.create_node("User", user_data)

# Usage
users_data = [
    {"name": f"User{i}", "email": f"user{i}@example.com"}
    for i in range(10000)
]

bulk_insert_users(db, users_data)
```

### Caching

```python
from functools import lru_cache
import time

class CachedGraphDB:
    def __init__(self, db_path):
        self.db = sombra.GraphDB(db_path)
        self._cache_timeout = 300  # 5 minutes
        self._cache = {}
    
    def get_node_properties_cached(self, node_id):
        cache_key = f"node_props_{node_id}"
        current_time = time.time()
        
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if current_time - timestamp < self._cache_timeout:
                return data
        
        # Fetch from database
        with self.db.transaction() as tx:
            props = tx.get_node_properties(node_id)
        
        # Cache the result
        self._cache[cache_key] = (props, current_time)
        return props
    
    def invalidate_cache(self, node_id=None):
        if node_id:
            cache_key = f"node_props_{node_id}"
            self._cache.pop(cache_key, None)
        else:
            self._cache.clear()

# Usage
cached_db = CachedGraphDB("large_graph.db")

# Fast cached access
props = cached_db.get_node_properties_cached(user_id)

# Invalidate cache after updates
with cached_db.db.transaction() as tx:
    tx.update_node_properties(user_id, {"last_seen": time.time()})
    cached_db.invalidate_cache(user_id)
```

## Testing

### Unit Testing

```python
import unittest
import tempfile
import os

class TestSombraOperations(unittest.TestCase):
    def setUp(self):
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)
        self.temp_db.close()
        self.db = sombra.GraphDB(self.temp_db.name)
    
    def tearDown(self):
        os.unlink(self.temp_db.name)
    
    def test_create_and_retrieve_node(self):
        with self.db.transaction() as tx:
            user = tx.create_node("User", {"name": "Alice"})
            
        with self.db.transaction() as tx:
            retrieved = tx.get_node(user.id)
            self.assertEqual(retrieved.label, "User")
            
            props = tx.get_node_properties(user.id)
            self.assertEqual(props["name"], "Alice")
    
    def test_traversal(self):
        with self.db.transaction() as tx:
            alice = tx.create_node("User", {"name": "Alice"})
            bob = tx.create_node("User", {"name": "Bob"})
            tx.create_edge(alice, bob, "KNOWS", {})
            
        with self.db.transaction() as tx:
            friends = tx.traverse() \
                .from_node(alice.id) \
                .outgoing("KNOWS") \
                .collect()
            
            self.assertEqual(len(friends), 1)
            self.assertEqual(friends[0].id, bob.id)

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing

```python
import pytest
import sombra

@pytest.fixture
def test_db():
    """Fixture providing a temporary database for testing"""
    import tempfile
    import os
    
    temp_db = tempfile.NamedTemporaryFile(delete=False)
    temp_db.close()
    
    db = sombra.GraphDB(temp_db.name)
    yield db
    
    os.unlink(temp_db.name)

def test_social_network_scenario(test_db):
    """Test a complete social network scenario"""
    with test_db.transaction() as tx:
        # Create users
        alice = tx.create_node("User", {"name": "Alice", "age": 30})
        bob = tx.create_node("User", {"name": "Bob", "age": 25})
        charlie = tx.create_node("User", {"name": "Charlie", "age": 35})
        
        # Create friendships
        tx.create_edge(alice, bob, "FRIENDS", {"since": 2020})
        tx.create_edge(bob, charlie, "FRIENDS", {"since": 2021})
        tx.create_edge(alice, charlie, "FRIENDS", {"since": 2022})
    
    with test_db.transaction() as tx:
        # Test Alice's friends
        alice_friends = tx.traverse() \
            .from_node(alice.id) \
            .outgoing("FRIENDS") \
            .collect()
        
        assert len(alice_friends) == 2
        
        # Test friends of friends
        fofs = tx.traverse() \
            .from_node(alice.id) \
            .outgoing("FRIENDS") \
            .outgoing("FRIENDS") \
            .collect()
        
        # Should include Bob and Charlie (Alice's direct friends)
        # and potentially their friends
        assert len(fofs) >= 2
```

## Type Hints and IDE Support

Sombra provides full type hints for better IDE support:

```python
from typing import Dict, List, Optional, Union
import sombra

def create_user_with_validation(
    tx: sombra.Transaction,
    name: str,
    email: str,
    age: Optional[int] = None,
    metadata: Optional[Dict[str, Union[str, int, bool]]] = None
) -> sombra.Node:
    """Create a user with validation and type hints"""
    
    # Validate email
    if "@" not in email:
        raise ValueError("Invalid email format")
    
    # Build properties
    properties: Dict[str, sombra.PropertyValue] = {
        "name": name,
        "email": email
    }
    
    if age is not None:
        properties["age"] = age
    
    if metadata:
        for key, value in metadata.items():
            properties[key] = value
    
    return tx.create_node("User", properties)

# Usage with full IDE support
with db.transaction() as tx:
    user: sombra.Node = create_user_with_validation(
        tx,
        name="Alice",
        email="alice@example.com",
        age=30,
        metadata={"department": "Engineering", "active": True}
    )
```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic usage
- Check the [Configuration Guide](configuration.md) for performance tuning
- Review the [Operations Guide](operations.md) for production deployment
- Browse the [examples](../examples/) directory for complete applications