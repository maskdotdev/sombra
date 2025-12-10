# Edge Type Index Documentation

## Overview

The Edge Type Index provides O(1) lookup of edges by their type name. This is a global index that maintains a mapping from edge type names to the set of edge IDs with that type.

## API Reference

### `get_edges_by_type_global`

Returns all edge IDs that have the specified type name.

```rust
pub fn get_edges_by_type_global(&self, type_name: &str) -> Vec<EdgeId>
```

**Parameters:**
- `type_name`: The edge type to search for (case-sensitive)

**Returns:**
- A vector of all edge IDs matching the type
- Empty vector if no edges match

**Time Complexity:** O(1) lookup + O(k) to collect results, where k is the number of matching edges

**Example:**
```rust
use sombra::{GraphDB, Node, Edge};

let mut db = GraphDB::open_arc("graph.db")?;
let mut tx = db.begin_transaction()?;

let user1 = tx.add_node(Node::new(0))?;
let user2 = tx.add_node(Node::new(0))?;
let user3 = tx.add_node(Node::new(0))?;

tx.add_edge(Edge::new(0, user1, user2, "FOLLOWS"))?;
tx.add_edge(Edge::new(0, user2, user3, "FOLLOWS"))?;
tx.add_edge(Edge::new(0, user1, user3, "BLOCKS"))?;
tx.commit()?;

// Find all FOLLOWS edges
let follows_edges = db.get_edges_by_type_global("FOLLOWS");
assert_eq!(follows_edges.len(), 2);

// Find all BLOCKS edges
let blocks_edges = db.get_edges_by_type_global("BLOCKS");
assert_eq!(blocks_edges.len(), 1);
```

### `count_edges_by_type_global`

Returns the count of edges with the specified type name.

```rust
pub fn count_edges_by_type_global(&self, type_name: &str) -> usize
```

**Parameters:**
- `type_name`: The edge type to count (case-sensitive)

**Returns:**
- The number of edges with the specified type
- 0 if no edges match

**Time Complexity:** O(1)

**Example:**
```rust
use sombra::{GraphDB, Node, Edge};

let mut db = GraphDB::open_arc("graph.db")?;
let mut tx = db.begin_transaction()?;

let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(0))?;

tx.add_edge(Edge::new(0, node1, node2, "KNOWS"))?;
tx.add_edge(Edge::new(0, node2, node1, "KNOWS"))?;
tx.commit()?;

// Count KNOWS edges
assert_eq!(db.count_edges_by_type_global("KNOWS"), 2);

// Count non-existent type
assert_eq!(db.count_edges_by_type_global("DISLIKES"), 0);
```

### `get_all_edge_types`

Returns a sorted list of all unique edge type names in the database.

```rust
pub fn get_all_edge_types(&self) -> Vec<String>
```

**Returns:**
- A sorted vector of all edge type names
- Empty vector if no edges exist

**Time Complexity:** O(n log n) where n is the number of unique edge types

**Example:**
```rust
use sombra::{GraphDB, Node, Edge};

let mut db = GraphDB::open_arc("graph.db")?;
let mut tx = db.begin_transaction()?;

let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(0))?;

tx.add_edge(Edge::new(0, node1, node2, "KNOWS"))?;
tx.add_edge(Edge::new(0, node2, node1, "LIKES"))?;
tx.add_edge(Edge::new(0, node1, node2, "FOLLOWS"))?;
tx.commit()?;

// Get all edge types (sorted)
let types = db.get_all_edge_types();
assert_eq!(types, vec!["FOLLOWS", "KNOWS", "LIKES"]);
```

## Use Cases

### 1. Schema Discovery

Discover what types of relationships exist in your graph:

```rust
let types = db.get_all_edge_types();
for edge_type in types {
    let count = db.count_edges_by_type_global(&edge_type);
    println!("{}: {} edges", edge_type, count);
}
```

### 2. Type-Specific Graph Analysis

Analyze subgraphs based on edge type:

```rust
// Find all social connections
let social_edges = db.get_edges_by_type_global("FOLLOWS");

// Analyze the social subgraph
for edge_id in social_edges {
    let edge = db.get_edge(edge_id)?;
    // Process social connections...
}
```

### 3. Code Graph Analysis

For code analysis graphs, find all relationships of specific types:

```rust
// Find all function calls
let call_edges = db.get_edges_by_type_global("CALLS");
println!("Total function calls: {}", call_edges.len());

// Find all containment relationships
let contains_edges = db.get_edges_by_type_global("CONTAINS");
println!("Total containment relationships: {}", contains_edges.len());

// Find all type references
let ref_edges = db.get_edges_by_type_global("REFERENCES");
println!("Total type references: {}", ref_edges.len());
```

### 4. Filtering Combined with Other Queries

Combine edge type queries with other operations:

```rust
// Get all FOLLOWS edges and load their details
let follows_edges = db.get_edges_by_type_global("FOLLOWS");

for edge_id in follows_edges {
    let edge = db.get_edge(edge_id)?;
    let source_node = db.get_node(edge.source_node_id)?;
    let target_node = db.get_node(edge.target_node_id)?;
    
    // Analyze follower/followee relationships
    println!("{} follows {}", source_node.id, target_node.id);
}
```

## Implementation Details

### Index Structure

The edge type index uses a `HashMap<String, BTreeSet<EdgeId>>`:
- **Key**: Edge type name (String)
- **Value**: Set of edge IDs with that type (BTreeSet for ordered iteration)

### Index Maintenance

The index is automatically maintained during edge operations:

1. **Edge Creation**: When an edge is added, its ID is inserted into the index under its type name
2. **Edge Deletion**: When an edge is deleted, its ID is removed from the index
3. **Database Rebuild**: When indexes are rebuilt (e.g., after recovery), the edge type index is reconstructed by scanning all edges

### Persistence

The edge type index is an in-memory index that is reconstructed on database open:
- Rebuilt during `rebuild_indexes()` or `rebuild_remaining_indexes()`
- Maintained incrementally during normal operations
- No separate on-disk persistence required

### Performance Characteristics

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| `get_edges_by_type_global` | O(1) + O(k) | O(k) |
| `count_edges_by_type_global` | O(1) | O(1) |
| `get_all_edge_types` | O(n log n) | O(n) |
| Edge add (index update) | O(log k) | O(1) |
| Edge delete (index update) | O(log k) | O(1) |

Where:
- k = number of edges with the queried type
- n = number of unique edge types

## Edge Type Naming

Edge types are case-sensitive strings. Consider these conventions:

### Recommended Naming Conventions

1. **All uppercase for relationship types**: `KNOWS`, `FOLLOWS`, `CONTAINS`
2. **Snake_case for compound types**: `HAS_PROPERTY`, `IS_TYPE_OF`
3. **Hyphenated for special cases**: `HAS-VALUE`, `IS-A`

### Case Sensitivity

Edge types are case-sensitive:

```rust
tx.add_edge(Edge::new(0, n1, n2, "knows"))?;
tx.add_edge(Edge::new(0, n1, n2, "KNOWS"))?;
tx.add_edge(Edge::new(0, n1, n2, "Knows"))?;

// These are three different types
assert_eq!(db.count_edges_by_type_global("knows"), 1);
assert_eq!(db.count_edges_by_type_global("KNOWS"), 1);
assert_eq!(db.count_edges_by_type_global("Knows"), 1);
```

## Thread Safety

The edge type index is part of the `GraphDB` struct and follows the same thread safety guarantees:
- Read operations (`get_edges_by_type_global`, `count_edges_by_type_global`, `get_all_edge_types`) require shared access (`&self`)
- Write operations (index updates) happen internally during edge add/delete and require mutable access (`&mut self`)
- Use transactions for concurrent access

## See Also

- [Query API Plan](query_api_plan.md) - Complete query API roadmap
- [Traversal Performance](traversal_performance.md) - Performance characteristics of graph traversals
- [Operations Guide](operations.md) - General database operations
