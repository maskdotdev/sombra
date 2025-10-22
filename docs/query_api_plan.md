# Comprehensive Query API Implementation Plan

## Executive Summary

Based on analysis of the Sombra codebase, the current graph database has:
- ✅ Basic node/edge CRUD operations
- ✅ Property indexes for O(log n) lookups
- ✅ Label-based node queries
- ✅ Basic neighbor traversal (outgoing/incoming)
- ✅ Multi-hop traversal (2-hop, 3-hop, BFS, parallel BFS)
- ✅ Transaction support

**Missing critical APIs for code analysis and general graph querying:**
- ✅ Edge type filtering in traversals (Phase 1 Complete)
- ✅ Hierarchical queries (Phase 2 Complete)
- ✅ Path finding algorithms (Phase 3 Complete)
- ❌ Pattern matching queries
- ❌ Subgraph extraction
- ❌ Aggregation queries
- ❌ Edge property queries

---

## Phase 1: Edge Type Filtering (CRITICAL - Code Analysis Blocker)

### 1.1 Core Edge Type Filtering APIs

**Location:** `src/db/core/traversal.rs`

#### APIs to Implement:

```rust
/// Get neighbors filtered by edge type(s)
pub fn get_neighbors_by_edge_type(
    &mut self,
    node_id: NodeId,
    edge_types: &[&str],      // Multiple types for OR semantics
    direction: EdgeDirection,
) -> Result<Vec<NodeId>>

/// Get edges filtered by type
pub fn get_edges_by_type(
    &mut self,
    node_id: NodeId,
    edge_types: &[&str],
    direction: EdgeDirection,
) -> Result<Vec<Edge>>

/// Get neighbors with their connecting edges filtered by type
pub fn get_neighbors_with_edges_by_type(
    &mut self,
    node_id: NodeId,
    edge_types: &[&str],
    direction: EdgeDirection,
) -> Result<Vec<(NodeId, Edge)>>

/// BFS traversal filtered by edge type
pub fn bfs_traversal_by_edge_type(
    &mut self,
    start_node_id: NodeId,
    max_depth: usize,
    edge_types: &[&str],
    direction: EdgeDirection,
) -> Result<Vec<(NodeId, usize)>>
```

**Add EdgeDirection enum:**
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}
```

**Implementation Strategy:**
1. Reuse existing edge chain traversal logic
2. Add type name filtering during edge iteration
3. Cache edge types to avoid repeated deserialization
4. Optimize for single type filter (common case)

**Performance Optimization:**
- Edge type cache: `HashMap<EdgeId, String>`
- Lazy loading of edge types (only when filtering needed)
- Batch edge loading when multiple types requested

**Testing:**
- Unit tests with mixed edge types
- Benchmark vs unfiltered traversal overhead (<10% slowdown acceptable)
- Test with code graph patterns (CONTAINS, CALLS, REFERENCES)

---

## Phase 2: Pattern Matching Queries

### 2.1 Simple Pattern Matching

**Location:** `src/db/query/` (NEW module)

```rust
/// Match a simple path pattern
/// Example: (a:Person)-[:KNOWS]->(b:Person)
pub fn match_pattern(
    &mut self,
    pattern: &Pattern,
) -> Result<Vec<Match>>

/// Pattern DSL structs
pub struct Pattern {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

pub struct NodePattern {
    pub var_name: String,
    pub labels: Vec<String>,          // OR semantics
    pub properties: PropertyFilters,   // AND semantics
}

pub struct EdgePattern {
    pub from_var: String,
    pub to_var: String,
    pub types: Vec<String>,
    pub properties: PropertyFilters,
    pub direction: EdgeDirection,
}

pub struct PropertyFilters {
    pub equals: BTreeMap<String, PropertyValue>,
    pub not_equals: BTreeMap<String, PropertyValue>,
    pub ranges: Vec<PropertyRangeFilter>,
}
```

**Use Cases:**
1. Find all function calls to a specific function
2. Find all files containing a specific class
3. Find all import chains

---

## Phase 3: Advanced Traversal Queries ✅ COMPLETE

### 3.1 Variable-Length Path Queries

**Status:** ✅ Implemented

**Location:** `src/db/core/traversal.rs`, `src/model.rs`

**Implemented APIs:**

```rust
/// Find all paths between two nodes with constraints
pub fn find_paths(
    &mut self,
    start: NodeId,
    end: NodeId,
    min_hops: usize,
    max_hops: usize,
    edge_types: Option<&[&str]>,
) -> Result<Vec<Path>>

/// Find shortest path between nodes
pub fn shortest_path(
    &mut self,
    start: NodeId,
    end: NodeId,
    edge_types: Option<&[&str]>,
) -> Result<Option<Path>>

pub struct Path {
    pub nodes: Vec<NodeId>,
    pub edges: Vec<EdgeId>,
    pub length: usize,
}
```

**Algorithms Implemented:**
- ✅ BFS for shortest path (unweighted)
- ✅ Limited DFS for all paths (with cycle detection)
- ⏳ Bidirectional BFS for long distances (future optimization)

**Features:**
- ✅ Shortest path finding with BFS
- ✅ All paths enumeration with min/max hop constraints
- ✅ Edge type filtering support
- ✅ Cycle detection in path finding
- ✅ Path struct with nodes, edges, and length

**Use Cases Validated:**
1. ✅ Impact analysis (call chains)
2. ✅ Dependency resolution
3. ✅ Reachability checks

**Testing:**
- ✅ 17 comprehensive tests in `tests/path_finding.rs`
- ✅ Chain graphs, grid graphs, diamond graphs
- ✅ Edge type filtering tests
- ✅ Min/max hop constraint tests
- ✅ Large graph performance tests (100 nodes)
- ✅ Code analysis use case tests

**Benchmarks:**
- ✅ Path finding benchmarks in `benches/path_finding_benchmark.rs`
- ✅ Chain traversal benchmarks
- ✅ Grid shortest path benchmarks
- ✅ Filtered path finding benchmarks
- ✅ Multiple paths enumeration benchmarks

**Performance:**
- Shortest path: O(V + E) time, O(V) space
- Find paths: O(V + E) per path, limited by max_hops

---

## Phase 4: Subgraph Extraction

### 4.1 Subgraph Queries

**Location:** `src/db/query/subgraph.rs` (NEW)

```rust
/// Extract subgraph around nodes
pub fn extract_subgraph(
    &mut self,
    root_nodes: &[NodeId],
    depth: usize,
    edge_filter: Option<EdgeTypeFilter>,
) -> Result<Subgraph>

/// Extract induced subgraph (nodes + all edges between them)
pub fn extract_induced_subgraph(
    &mut self,
    node_ids: &[NodeId],
) -> Result<Subgraph>

pub struct Subgraph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub boundary_nodes: Vec<NodeId>,  // Nodes with edges outside subgraph
}
```

**Use Cases:**
1. Extract entire file AST
2. Extract function and all its dependencies
3. Extract module boundaries

---

## Phase 5: Aggregation & Analytics ✅ COMPLETE

### 5.1 Node/Edge Statistics

**Status:** ✅ Implemented

**Location:** `src/db/query/analytics.rs`

**Implemented APIs:**

```rust
/// Count nodes by label
pub fn count_nodes_by_label(&self) -> HashMap<String, usize>

/// Count edges by type
pub fn count_edges_by_type(&mut self) -> Result<HashMap<String, usize>>

/// Get degree distribution
pub fn degree_distribution(&mut self) -> Result<DegreeDistribution>

pub struct DegreeDistribution {
    pub in_degree: HashMap<NodeId, usize>,
    pub out_degree: HashMap<NodeId, usize>,
    pub total_degree: HashMap<NodeId, usize>,
}

/// Find high-degree nodes (hubs)
pub fn find_hubs(
    &mut self,
    min_degree: usize,
    degree_type: DegreeType,
) -> Result<Vec<(NodeId, usize)>>

pub enum DegreeType {
    In,
    Out,
    Total,
}

/// Additional APIs:
/// - get_total_node_count()
/// - get_total_edge_count()
/// - find_isolated_nodes()
/// - find_leaf_nodes(direction)
/// - get_average_degree()
/// - get_density()
/// - count_nodes_with_label(label)
/// - count_edges_with_type(edge_type)
/// - get_label_statistics()
/// - get_edge_type_statistics()
/// - get_degree_statistics()
```

**Implementation Details:**
- 15 public analytics APIs
- Complete degree distribution tracking (in/out/total)
- Graph density and statistics calculations
- Efficient hub and isolated node detection

**Testing:**
- ✅ 10 unit tests (all passing)
- ✅ 5 integration tests (all passing)
- ✅ Code graph analytics use cases validated

**Use Cases Validated:**
1. ✅ Identify highly-coupled functions (god objects)
2. ✅ Find unused/dead code (isolated nodes)
3. ✅ Calculate graph statistics for query planning
4. ✅ Generate reports on code structure

---

## Phase 6: Ancestor & Descendant Queries

### 6.1 Tree/DAG Navigation

**Location:** `src/db/query/hierarchy.rs` (NEW)

```rust
/// Find first ancestor with matching label
pub fn find_ancestor_by_label(
    &mut self,
    start: NodeId,
    label: &str,
    edge_type: &str,  // e.g., "CONTAINS"
) -> Result<Option<NodeId>>

/// Get all ancestors up to root
pub fn get_ancestors(
    &mut self,
    start: NodeId,
    edge_type: &str,
    max_depth: Option<usize>,
) -> Result<Vec<NodeId>>

/// Get all descendants (breadth-first)
pub fn get_descendants(
    &mut self,
    start: NodeId,
    edge_type: &str,
    max_depth: Option<usize>,
) -> Result<Vec<NodeId>>

/// Get containing file for a code node
pub fn get_containing_file(&mut self, node_id: NodeId) -> Result<NodeId> {
    self.find_ancestor_by_label(node_id, "File", "CONTAINS")
        .and_then(|opt| opt.ok_or(GraphError::NotFound("containing file")))
}
```

**Use Cases (Code Analysis):**
1. Find containing file/class/function
2. Get entire AST subtree
3. Navigate up the AST hierarchy

---

## Phase 7: Index Enhancements

### 7.1 Edge Type Index

**Location:** `src/db/core/graphdb.rs`

```rust
/// Add edge type index for fast edge type queries
pub(crate) edge_type_index: HashMap<String, BTreeSet<EdgeId>>

/// Index maintenance methods
impl GraphDB {
    fn update_edge_type_index_on_add(&mut self, edge_id: EdgeId, type_name: &str) {
        self.edge_type_index
            .entry(type_name.to_string())
            .or_default()
            .insert(edge_id);
    }
    
    fn update_edge_type_index_on_delete(&mut self, edge_id: EdgeId, type_name: &str) {
        if let Some(edges) = self.edge_type_index.get_mut(type_name) {
            edges.remove(&edge_id);
        }
    }
    
    /// Get all edges of a specific type
    pub fn get_edges_by_type_global(&self, type_name: &str) -> Vec<EdgeId> {
        self.edge_type_index
            .get(type_name)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }
}
```

**Benefits:**
- O(1) lookup of all edges by type
- Useful for schema analysis
- Supports type-based queries efficiently

---

## Phase 8: Query Builder API

### 8.1 Fluent Query API

**Location:** `src/db/query/builder.rs` (NEW)

```rust
/// Fluent query builder for complex queries
pub struct QueryBuilder<'db> {
    db: &'db mut GraphDB,
    filters: Vec<Filter>,
    traversal: Option<TraversalSpec>,
}

impl<'db> QueryBuilder<'db> {
    pub fn new(db: &'db mut GraphDB) -> Self { ... }
    
    pub fn start_from(mut self, node_ids: Vec<NodeId>) -> Self { ... }
    pub fn start_from_label(mut self, label: &str) -> Self { ... }
    pub fn start_from_property(mut self, label: &str, key: &str, value: PropertyValue) -> Self { ... }
    
    pub fn traverse(mut self, edge_types: &[&str], direction: EdgeDirection, depth: usize) -> Self { ... }
    
    pub fn filter_nodes(mut self, filter: NodeFilter) -> Self { ... }
    pub fn filter_edges(mut self, filter: EdgeFilter) -> Self { ... }
    
    pub fn limit(mut self, n: usize) -> Self { ... }
    
    pub fn execute(self) -> Result<QueryResult> { ... }
}

// Usage example:
let results = QueryBuilder::new(&mut db)
    .start_from_label("Function")
    .filter_nodes(|n| n.properties.get("name") == Some(&PropertyValue::String("foo".into())))
    .traverse(&["CALLS"], EdgeDirection::Outgoing, 3)
    .limit(100)
    .execute()?;
```

**Benefits:**
- Ergonomic API for complex queries
- Query optimization opportunities
- Type-safe query construction

---

## Implementation Priority for Code Analysis

### High Priority (Phase 1) - Blocking Code Analysis
1. ✅ **Edge type filtering** (`get_neighbors_by_edge_type`, `get_edges_by_type`)
2. ✅ **Ancestor queries** (`find_ancestor_by_label`, `get_containing_file`)
3. ✅ **Filtered BFS** (`bfs_traversal_by_edge_type`)

### Medium Priority (Phase 2-3) - Essential for Full Functionality
4. ⚡ **Path finding** (`shortest_path`, `find_paths`)
5. ⚡ **Subgraph extraction** (`extract_subgraph`)
6. ⚡ **Edge type index** (performance optimization)

### Lower Priority (Phase 4-5) - Nice to Have
7. 🔹 **Pattern matching** (can use edge filtering + manual logic initially)
8. 🔹 **Aggregations** (can compute on-demand initially)
9. 🔹 **Query builder** (syntactic sugar)

---

## Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    // Edge type filtering
    #[test]
    fn test_filter_by_single_edge_type() { ... }
    
    #[test]
    fn test_filter_by_multiple_edge_types() { ... }
    
    #[test]
    fn test_edge_type_with_direction() { ... }
    
    // Ancestor queries
    #[test]
    fn test_find_ancestor_by_label() { ... }
    
    #[test]
    fn test_get_containing_file() { ... }
    
    // Path finding
    #[test]
    fn test_shortest_path() { ... }
    
    #[test]
    fn test_find_all_paths() { ... }
}
```

### Integration Tests (Code Graph Scenarios)
```rust
// tests/code_graph.rs
#[test]
fn test_impact_analysis_call_chain() {
    // Build graph: File -> Function -> CallExpr -> Function
    // Query: Find all functions affected by change to funcA
}

#[test]
fn test_unused_function_detection() {
    // Build graph with entry points
    // Query: Find functions with no incoming CALLS edges
}

#[test]
fn test_import_chain_resolution() {
    // Build graph: File -IMPORTS-> File
    // Query: Find all transitive imports
}
```

### Benchmark Tests
```rust
// benches/query_benchmark.rs
fn bench_edge_type_filtering(c: &mut Criterion) { ... }
fn bench_ancestor_traversal(c: &mut Criterion) { ... }
fn bench_path_finding(c: &mut Criterion) { ... }
```

---

## Module Structure

```
src/
├── db/
│   ├── core/
│   │   ├── traversal.rs          # Enhanced with edge type filtering
│   │   └── ...
│   ├── query/                    # NEW: High-level query APIs
│   │   ├── mod.rs
│   │   ├── builder.rs            # Fluent query builder
│   │   ├── pattern.rs            # Pattern matching
│   │   ├── hierarchy.rs          # Ancestor/descendant queries
│   │   ├── subgraph.rs           # Subgraph extraction
│   │   └── analytics.rs          # Aggregations & statistics
│   └── ...
└── ...
```

---

## Performance Targets

| Query Type | Target Performance | Current Status |
|-----------|-------------------|----------------|
| Edge type filtering | < 5% overhead vs unfiltered | Not implemented |
| Ancestor query (depth=5) | < 1ms | Not implemented |
| Shortest path (100 hops) | < 10ms | Not implemented |
| Subgraph extraction (1000 nodes) | < 50ms | Not implemented |
| Property lookup (indexed) | < 1ms | ✅ Implemented |
| BFS traversal (10K nodes) | < 100ms | ✅ Implemented |

---

## API Documentation Requirements

Each new API must include:
1. Rust doc comments with description
2. Parameter documentation
3. Return value documentation
4. Error conditions
5. Time complexity (Big-O)
6. Space complexity
7. Example usage
8. Related APIs (see also)

Example:
```rust
/// Finds the first ancestor node with the specified label.
///
/// Traverses backwards through edges of the given type until finding
/// a node with the target label, or reaching a node with no incoming
/// edges of that type.
///
/// # Arguments
/// * `start` - The node ID to start from
/// * `label` - The label to search for
/// * `edge_type` - The edge type to traverse (e.g., "CONTAINS")
///
/// # Returns
/// * `Ok(Some(node_id))` - Ancestor node found
/// * `Ok(None)` - No ancestor with that label exists
/// * `Err(...)` - Database error
///
/// # Time Complexity
/// O(depth) where depth is the distance to the ancestor
///
/// # Example
/// ```rust
/// // Find the file containing a function node
/// let file_id = db.find_ancestor_by_label(func_id, "File", "CONTAINS")?;
/// ```
///
/// # See Also
/// * [`get_ancestors`] - Get all ancestors up to root
/// * [`get_containing_file`] - Specialized version for file lookup
pub fn find_ancestor_by_label(
    &mut self,
    start: NodeId,
    label: &str,
    edge_type: &str,
) -> Result<Option<NodeId>>
```

---

## Migration & Backward Compatibility

1. **New APIs are additive** - no breaking changes to existing APIs
2. **Existing traversal APIs remain unchanged** - new APIs are additions
3. **Index migration** - edge type index built on first use, persisted on checkpoint
4. **Feature flag** - `query-api` feature flag for new query module (default enabled)

---

## Success Metrics

1. ✅ All code analysis use cases can be implemented efficiently
2. ✅ < 10% performance overhead for filtered vs unfiltered queries
3. ✅ 100% test coverage for new query APIs
4. ✅ Comprehensive documentation with examples
5. ✅ Benchmarks show O(log n) or better for indexed operations
6. ✅ Zero breaking changes to existing API

---

## Next Steps

1. Review and approve plan
2. Start with Phase 1 (edge type filtering) - highest priority
3. Implement in order: Phase 1 → Phase 6 → Phase 3 → Phase 4 → Phase 5 → Phase 2 → Phase 7 → Phase 8
