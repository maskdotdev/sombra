# Sombra Lookup Performance Optimization Plan

## Executive Summary

Based on comprehensive benchmarking comparing Sombra vs SQLite for graph operations, we identified severe scaling issues in Sombra's lookup mechanisms. While Sombra outperforms SQLite on small datasets (2.3x faster), it degrades dramatically on larger datasets:
- **Medium datasets**: SQLite 8.3x faster
- **Large datasets**: SQLite 390x faster

This plan outlines a systematic approach to fix these performance bottlenecks and make Sombra competitive across all dataset sizes.

## Root Cause Analysis

### Current Implementation Problems

1. **O(n) Linear Scans in `get_nodes_by_label`** (lines 518-531 in db.rs):
   ```rust
   // Current implementation - scans ALL nodes
   for (node_id, pointer) in self.node_index.iter() {
       let node = self.read_node_at(pointer)?;  // Disk I/O for each node
       if node.labels.contains(&label.to_string()) {
           nodes.push(node_id);
       }
   }
   ```

2. **Hash Map Index Limitations**:
   - `node_index: HashMap<NodeId, RecordPointer>` only provides O(1) lookups by ID
   - No secondary indexes for labels, properties, or adjacency patterns
   - Each node lookup requires disk I/O through pager

3. **Adjacency Traversal Inefficiency**:
   - `get_neighbors()` chains through linked lists via `load_edge()` calls
   - Each edge load requires separate disk I/O
   - No bulk loading or caching of adjacency data

### Performance Impact Analysis

| Dataset Size | Sombra Node Read | SQLite Node Read | Performance Gap |
|-------------|------------------|------------------|-----------------|
| Small (1K nodes) | 0.43ms | 1.00ms | Sombra 2.3x faster |
| Medium (10K nodes) | 4.17ms | 0.50ms | SQLite 8.3x faster |
| Large (100K nodes) | 390.00ms | 1.00ms | SQLite 390x faster |

The fundamental issue is that Sombra uses a **record storage model** with minimal indexing, while SQLite uses sophisticated **B-tree indexes**.

## Optimization Strategy

### Phase 1: Critical Index Infrastructure (High Priority)

#### 1.1 Label Secondary Index
**Goal**: Eliminate O(n) scans in `get_nodes_by_label()`

**Implementation**:
```rust
// Add to GraphDB struct
label_index: HashMap<String, BTreeSet<NodeId>>,

// Updated get_nodes_by_label implementation
pub fn get_nodes_by_label(&mut self, label: &str) -> Result<Vec<NodeId>> {
    Ok(self.label_index
        .get(label)
        .map(|nodes| nodes.iter().cloned().collect())
        .unwrap_or_default())
}
```

**Expected Impact**: 100-300x improvement for label-based queries on large datasets

#### 1.2 B-tree Primary Index
**Goal**: Replace HashMap with more cache-efficient B-tree structure

**Implementation**:
- Implement on-disk B-tree for node_id → RecordPointer mapping
- Better cache locality for sequential access patterns
- Reduced memory overhead compared to HashMap

**Expected Impact**: 2-5x improvement for ID-based lookups

#### 1.3 Page-Level Caching Enhancement
**Goal**: Reduce disk I/O for frequently accessed nodes

**Implementation**:
```rust
// Enhanced cache with LRU eviction
struct NodeCache {
    cache: LruCache<NodeId, Node>,
    max_size: usize,
}

// Cache frequently accessed nodes
impl GraphDB {
    fn get_node_cached(&mut self, node_id: NodeId) -> Result<Node> {
        if let Some(node) = self.cache.get(&node_id) {
            return Ok(node.clone());
        }
        // Load from disk and cache
        let node = self.get_node_from_disk(node_id)?;
        self.cache.put(node_id, node.clone());
        Ok(node)
    }
}
```

### Phase 2: Advanced Query Optimization (Medium Priority)

#### 2.1 Adjacency Indexing
**Goal**: Optimize neighbor traversal patterns

**Implementation**:
```rust
// Pre-computed adjacency lists
adjacency_index: HashMap<NodeId, Vec<NodeId>>,

// Bulk neighbor loading
pub fn get_neighbors_batch(&mut self, node_ids: &[NodeId]) -> Result<HashMap<NodeId, Vec<NodeId>>> {
    // Load all adjacency data in minimal I/O operations
}
```

#### 2.2 Property-Based Indexes
**Goal**: Support efficient property-based queries

**Implementation**:
```rust
// Multi-value property indexes
property_indexes: HashMap<String, HashMap<PropertyValue, BTreeSet<NodeId>>>,
```

#### 2.3 Query Planner
**Goal**: Choose optimal access paths based on query patterns

**Implementation**:
- Analyze query predicates
- Select best index (label, property, or scan)
- Optimize multi-hop traversals

### Phase 3: Specialized Graph Structures (Medium Priority)

#### 3.1 Compressed Sparse Row (CSR) Representation
**Goal**: Optimize for dense graphs with many connections

**Implementation**:
```rust
struct CSRGraph {
    // For dense graphs, store adjacency in compressed format
    row_offsets: Vec<usize>,
    col_indices: Vec<NodeId>,
}
```

#### 3.2 Neighbor Caching for High-Degree Nodes
**Goal**: Cache adjacency lists for hub nodes

**Implementation**:
```rust
struct HubCache {
    high_degree_nodes: HashMap<NodeId, Vec<NodeId>>,
    degree_threshold: usize,
}
```

#### 3.3 Path Compression
**Goal**: Optimize common traversal patterns

**Implementation**:
- Cache frequently traversed paths
- Pre-compute shortest paths for common queries
- Materialize view results for complex queries

## Implementation Roadmap

### Sprint 1: Label Index (Week 1)
- [ ] Implement `label_index: HashMap<String, BTreeSet<NodeId>>`
- [ ] Update `add_node()` to maintain label index
- [ ] Update `delete_node()` to maintain label index
- [ ] Rewrite `get_nodes_by_label()` to use index
- [ ] Add comprehensive tests for label index consistency
- [ ] Benchmark label query performance

### Sprint 2: Enhanced Caching (Week 2)
- [ ] Implement LRU node cache
- [ ] Add cache invalidation on mutations
- [ ] Implement cache size limits and eviction
- [ ] Add cache hit/miss metrics
- [ ] Benchmark cache effectiveness

### Sprint 3: B-tree Primary Index (Week 3-4)
- [ ] Design on-disk B-tree format
- [ ] Implement B-tree node structure
- [ ] Implement B-tree operations (insert, search, delete)
- [ ] Replace HashMap with B-tree in GraphDB
- [ ] Add B-tree persistence and recovery
- [ ] Benchmark B-tree vs HashMap performance

### Sprint 4: Adjacency Optimization (Week 5)
- [ ] Implement adjacency index structure
- [ ] Add bulk neighbor loading
- [ ] Optimize edge traversal patterns
- [ ] Add adjacency cache for high-degree nodes
- [ ] Benchmark adjacency operations

### Sprint 5: Query Planner (Week 6)
- [ ] Implement query analysis framework
- [ ] Add cost-based index selection
- [ ] Optimize multi-hop traversals
- [ ] Add query execution statistics
- [ ] Benchmark query planner effectiveness

## Success Metrics

### Performance Targets
- **Label queries**: < 1ms for 100K node datasets (current: 390ms)
- **Node lookups**: < 0.1ms for cached nodes (current: varies)
- **Neighbor traversal**: 10x improvement for high-degree nodes
- **Memory usage**: < 2x current memory footprint

### Quality Metrics
- **Test coverage**: > 95% for new indexing code
- **Benchmark suite**: Automated performance regression testing
- **Documentation**: Complete API documentation with performance characteristics

## Risk Assessment

### Technical Risks
1. **Index Consistency**: Complex mutation patterns may corrupt indexes
   - **Mitigation**: Comprehensive transaction testing and validation
   
2. **Memory Overhead**: Caching may increase memory usage significantly
   - **Mitigation**: Configurable cache sizes and LRU eviction

3. **Complexity**: B-tree implementation is non-trivial
   - **Mitigation**: Use existing crates where possible, extensive testing

### Performance Risks
1. **Write Amplification**: Index maintenance may slow down writes
   - **Mitigation**: Batch index updates, lazy index building

2. **Cache Thrashing**: Poor cache hit ratios on certain workloads
   - **Mitigation**: Adaptive cache sizing, workload-specific tuning

## Testing Strategy

### Unit Tests
- Index consistency under all mutation operations
- Cache behavior under various access patterns
- B-tree correctness for all operations

### Integration Tests
- End-to-end query performance validation
- Crash recovery with indexes
- Concurrency testing with multiple readers/writers

### Benchmark Suite
- Automated performance regression testing
- Comparison with baseline (current implementation)
- Scalability testing across dataset sizes

## Conclusion

This optimization plan addresses the fundamental scalability issues in Sombra's lookup mechanisms. By implementing proper indexing, caching, and query optimization, we can achieve:

1. **100-300x improvement** for label-based queries
2. **2-5x improvement** for ID-based lookups  
3. **10x improvement** for adjacency traversals
4. **Competitive performance** with SQLite across all dataset sizes

The phased approach allows us to deliver incremental value while managing complexity and risk. The most impactful changes (label indexing and caching) are addressed first, providing immediate performance benefits.

---

*Last Updated: October 18, 2025*
*Status: Planning Phase - Ready for Implementation*