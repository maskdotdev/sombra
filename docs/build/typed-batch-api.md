# Typed Batch API Implementation Plan

## Overview

This document outlines the implementation plan for a high-performance typed batch API for the Sombra Node.js bindings. The goal is to eliminate JSON/serde overhead for bulk operations while providing a Drizzle-style developer experience.

## Problem Statement

The current `CreateBuilder` API uses JSON serialization for all operations:
- JS `CreateBuilder.execute()` → `native.databaseCreate()` → `serde_json::from_value()` → `create_script()` → `CreateBuilder.execute()` → `pager.commit()`
- Hot spots: `serde_json` parsing on every call, `value_to_prop_value()` called per-property
- While FFI round-trips are already optimized (1 call per batch), serde cost is the bottleneck

## Target Performance

- **3-5x improvement** over JSON path for large batches (10K+ records)
- Memory-efficient ID returns using `BigUint64Array`
- Support for streaming large imports

## API Design

### Transaction API (Drizzle-style)

```javascript
const { summary, result } = await db.transaction(async (tx) => {
  const alice = tx.createNode('User', { name: 'Alice', $alias: 'alice' })
  const bob = tx.createNode('User', { name: 'Bob' })
  tx.createEdge(alice, 'FOLLOWS', bob)
  tx.createEdge('$alice', 'FOLLOWS', bob)  // alias ref
  tx.rollback()  // explicit rollback (like Drizzle)
  return { alice, bob }
})
// summary.nodes → BigUint64Array, summary.aliases → { alice: 1n }
```

### BatchCreate API (fluent builder)

```javascript
const result = await db.batchCreate()
  .nodes('User', [
    { name: 'Alice', $alias: 'alice' },
    { name: 'Bob', $alias: 'bob' },
  ])
  .edges('FOLLOWS', [
    { src: '$alice', dst: '$bob' },  // alias reference
    { src: 0, dst: 1 },              // handle index
    { src: 12345n, dst: '$bob' },    // existing ID (bigint)
  ])
  .execute()  // or .stream() for large imports
// result.nodes → BigUint64Array, result.aliases → Map
```

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| ID type | `BigUint64Array` | Memory-efficient, Node 10.4+, no fallback needed |
| Alias prefix | `$` | `$alias` in props, `$name` in refs |
| Batch size | Auto-tune | Samples records, targets ~1.5MB/batch, range 100-10000 |
| Rollback | Drizzle-style | Both `throw` and explicit `tx.rollback()` work |
| Error handling | `BatchError` | Includes `batchIndex`, `recordIndex`, `recordOffset`, `committedIds` |
| Single label | Optimized | Most common case; multi-label via separate call |

## Reference Resolution Rules

| Input | Resolution |
|-------|------------|
| `'$name'` string | Alias lookup |
| `number` (0, 1, 2) | Handle index in current batch |
| `bigint` (12345n) | Existing node ID |
| `{ alias: 'x' }` | Explicit alias |
| `{ handle: 0 }` | Explicit handle |
| `{ id: 12345n }` | Explicit ID |

## Implementation Phases

### Phase 1: Rust FFI Layer (Current)

**Files to modify:**
- `bindings/node/src/lib.rs` - Add typed structs and FFI function
- `src/ffi/mod.rs` - Add core typed batch logic

**New Rust types:**

```rust
/// Property value without JSON wrapper
#[derive(Debug)]
pub enum TypedPropValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// Single node specification for typed batch
#[derive(Debug)]
#[napi(object)]
pub struct TypedNodeSpec {
    pub label: String,
    pub props: Vec<TypedPropEntry>,
    pub alias: Option<String>,
}

/// Property entry without JSON
#[derive(Debug)]
#[napi(object)]
pub struct TypedPropEntry {
    pub key: String,
    pub value: TypedPropValue,
}

/// Edge specification with typed references
#[derive(Debug)]
#[napi(object)]
pub struct TypedEdgeSpec {
    pub ty: String,
    pub src: TypedNodeRef,
    pub dst: TypedNodeRef,
    pub props: Vec<TypedPropEntry>,
}

/// Node reference for edges
#[derive(Debug)]
pub enum TypedNodeRef {
    Alias(String),      // '$alice'
    Handle(u32),        // 0, 1, 2
    Id(u64),            // existing node ID
}

/// Batch specification
#[derive(Debug)]
#[napi(object)]
pub struct TypedBatchSpec {
    pub nodes: Vec<TypedNodeSpec>,
    pub edges: Vec<TypedEdgeSpec>,
}

/// Result with BigUint64Array for IDs
#[derive(Debug)]
#[napi(object)]
pub struct TypedBatchResult {
    pub nodes: BigUint64Array,
    pub edges: BigUint64Array,
    pub aliases: HashMap<String, u64>,
}
```

**New FFI function:**

```rust
#[napi]
pub fn databaseCreateTypedBatch(
    handle: &DatabaseHandle,
    spec: TypedBatchSpec,
) -> NapiResult<TypedBatchResult> {
    handle.with_db(|db| {
        db.create_typed_batch(spec).map_err(to_napi_err)
    })
}
```

### Phase 2: Core Implementation in `src/ffi/mod.rs`

Add to `Database` impl:

```rust
/// Creates nodes and edges from typed specifications (bypasses JSON).
pub fn create_typed_batch(&self, spec: TypedBatchSpec) -> Result<TypedBatchResult> {
    let mut write = self.pager.begin_write()?;
    
    // Pre-intern all labels, types, and property names
    let mut label_cache: HashMap<String, LabelId> = HashMap::new();
    let mut type_cache: HashMap<String, TypeId> = HashMap::new();
    let mut prop_cache: HashMap<String, PropId> = HashMap::new();
    
    // Process nodes
    let mut node_ids = Vec::with_capacity(spec.nodes.len());
    let mut aliases: HashMap<String, u64> = HashMap::new();
    
    for node_spec in &spec.nodes {
        let label_id = self.resolve_or_cache_label(&mut write, &node_spec.label, &mut label_cache)?;
        self.ensure_label_index(&mut write, label_id)?;
        
        let props = self.typed_props_to_storage(&mut write, &node_spec.props, &mut prop_cache)?;
        let node_id = self.graph.create_node(&mut write, StorageNodeSpec {
            labels: &[label_id],
            props: &props,
        })?;
        
        if let Some(ref alias) = node_spec.alias {
            aliases.insert(alias.clone(), node_id.0);
        }
        node_ids.push(node_id.0);
    }
    
    // Process edges
    let mut edge_ids = Vec::with_capacity(spec.edges.len());
    
    for edge_spec in &spec.edges {
        let ty_id = self.resolve_or_cache_type(&mut write, &edge_spec.ty, &mut type_cache)?;
        let src = self.resolve_typed_ref(&edge_spec.src, &node_ids, &aliases)?;
        let dst = self.resolve_typed_ref(&edge_spec.dst, &node_ids, &aliases)?;
        let props = self.typed_props_to_storage(&mut write, &edge_spec.props, &mut prop_cache)?;
        
        let edge_id = self.graph.create_edge(&mut write, StorageEdgeSpec {
            src: NodeId(src),
            dst: NodeId(dst),
            ty: ty_id,
            props: &props,
        })?;
        edge_ids.push(edge_id.0);
    }
    
    self.pager.commit(write)?;
    
    Ok(TypedBatchResult {
        nodes: node_ids,
        edges: edge_ids,
        aliases,
    })
}

fn resolve_typed_ref(
    &self,
    ref_: &TypedNodeRef,
    node_ids: &[u64],
    aliases: &HashMap<String, u64>,
) -> Result<u64> {
    match ref_ {
        TypedNodeRef::Alias(name) => aliases
            .get(name)
            .copied()
            .ok_or_else(|| FfiError::Message(format!("unknown alias '{name}'"))),
        TypedNodeRef::Handle(idx) => node_ids
            .get(*idx as usize)
            .copied()
            .ok_or_else(|| FfiError::Message(format!("invalid handle index {idx}"))),
        TypedNodeRef::Id(id) => Ok(*id),
    }
}

fn typed_props_to_storage(
    &self,
    write: &mut WriteGuard<'_>,
    props: &[TypedPropEntry],
    cache: &mut HashMap<String, PropId>,
) -> Result<Vec<PropEntry>> {
    let mut result = Vec::with_capacity(props.len());
    for entry in props {
        let prop_id = self.resolve_or_cache_prop(write, &entry.key, cache)?;
        let value = typed_value_to_prop_value(&entry.value)?;
        result.push(PropEntry::new(prop_id, value));
    }
    Ok(result)
}
```

### Phase 3: JavaScript API Layer

**Files to modify:**
- `bindings/node/main.js` - Add new classes

**New classes:**

```javascript
/**
 * Error thrown when a batch operation fails.
 * Contains detailed information about the failure point.
 */
class BatchError extends SombraError {
  constructor(message, details = {}) {
    super(message, ErrorCode.INVALID_ARG)
    this.name = 'BatchError'
    this.batchIndex = details.batchIndex ?? null
    this.recordIndex = details.recordIndex ?? null
    this.recordOffset = details.recordOffset ?? null
    this.committedIds = details.committedIds ?? null
  }
}

/**
 * Builder for typed batch create operations.
 */
class BatchCreateBuilder {
  constructor(db) {
    this._db = db
    this._nodes = []
    this._edges = []
    this._sealed = false
  }

  /**
   * Add nodes with a single label.
   * @param {string} label - Node label
   * @param {Array<Object>} records - Node property objects (may include $alias)
   */
  nodes(label, records) {
    this._ensureMutable()
    if (typeof label !== 'string' || !label.trim()) {
      throw new TypeError('nodes() requires a non-empty label string')
    }
    if (!Array.isArray(records)) {
      throw new TypeError('nodes() requires an array of records')
    }
    for (const record of records) {
      const { $alias, ...props } = record
      this._nodes.push({
        label,
        props: this._encodeProps(props),
        alias: $alias ?? null,
      })
    }
    return this
  }

  /**
   * Add edges with a single type.
   * @param {string} type - Edge type
   * @param {Array<Object>} records - Edge specs with src/dst and optional props
   */
  edges(type, records) {
    this._ensureMutable()
    if (typeof type !== 'string' || !type.trim()) {
      throw new TypeError('edges() requires a non-empty type string')
    }
    if (!Array.isArray(records)) {
      throw new TypeError('edges() requires an array of records')
    }
    for (const record of records) {
      const { src, dst, ...props } = record
      this._edges.push({
        ty: type,
        src: this._encodeRef(src),
        dst: this._encodeRef(dst),
        props: this._encodeProps(props),
      })
    }
    return this
  }

  /**
   * Execute the batch and return results.
   * @returns {{nodes: BigUint64Array, edges: BigUint64Array, aliases: Map<string, bigint>}}
   */
  execute() {
    this._ensureMutable()
    this._db._assertOpen()
    this._sealed = true
    
    const spec = {
      nodes: this._nodes,
      edges: this._edges,
    }
    
    const result = callNative(native.databaseCreateTypedBatch, this._db._handle, spec)
    return {
      nodes: result.nodes,  // BigUint64Array
      edges: result.edges,  // BigUint64Array
      aliases: new Map(Object.entries(result.aliases).map(([k, v]) => [k, BigInt(v)])),
    }
  }

  _encodeRef(value) {
    if (typeof value === 'string' && value.startsWith('$')) {
      return { kind: 'alias', alias: value.slice(1) }
    }
    if (typeof value === 'number') {
      if (!Number.isInteger(value) || value < 0) {
        throw new TypeError('handle reference must be a non-negative integer')
      }
      return { kind: 'handle', index: value }
    }
    if (typeof value === 'bigint') {
      if (value < 0n) {
        throw new TypeError('ID reference must be non-negative')
      }
      return { kind: 'id', id: value }
    }
    throw new TypeError('edge reference must be $alias string, handle number, or bigint ID')
  }

  _encodeProps(props) {
    const entries = []
    for (const [key, value] of Object.entries(props)) {
      entries.push({ key, value: this._encodeValue(value) })
    }
    return entries
  }

  _encodeValue(value) {
    if (value === null || value === undefined) {
      return { kind: 'null' }
    }
    if (typeof value === 'boolean') {
      return { kind: 'bool', value }
    }
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new TypeError('numeric values must be finite')
      }
      if (Number.isInteger(value)) {
        return { kind: 'int', value }
      }
      return { kind: 'float', value }
    }
    if (typeof value === 'string') {
      return { kind: 'string', value }
    }
    if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
      return { kind: 'bytes', value: Buffer.from(value).toString('base64') }
    }
    throw new TypeError(`unsupported property value type: ${typeof value}`)
  }

  _ensureMutable() {
    if (this._sealed) {
      throw new Error('batch already executed')
    }
  }
}
```

### Phase 4: Auto-tune Batch Size

```javascript
/**
 * Estimates optimal batch size based on payload sampling.
 * @param {Array} records - Sample of records to analyze
 * @param {Object} options - Tuning options
 * @returns {number} Recommended batch size
 */
function autoTuneBatchSize(records, options = {}) {
  const {
    targetBytes = 1.5 * 1024 * 1024,  // 1.5MB target
    minBatch = 100,
    maxBatch = 10000,
    sampleSize = 10,
  } = options

  if (!records || records.length === 0) {
    return minBatch
  }

  // Sample records to estimate average size
  const sampleCount = Math.min(sampleSize, records.length)
  const indices = new Set()
  while (indices.size < sampleCount) {
    indices.add(Math.floor(Math.random() * records.length))
  }

  let totalBytes = 0
  for (const idx of indices) {
    totalBytes += estimateRecordBytes(records[idx])
  }

  const avgBytes = totalBytes / sampleCount
  const estimated = Math.floor(targetBytes / avgBytes)
  
  return Math.max(minBatch, Math.min(maxBatch, estimated))
}

function estimateRecordBytes(record) {
  // Rough estimate: JSON stringify + overhead
  return JSON.stringify(record).length * 1.2
}
```

### Phase 5: Tests

**File:** `bindings/node/__test__/typed-batch.test.mjs`

```javascript
import test from 'ava'
import { Database } from '../main.js'
import { tmpdir } from 'os'
import { join } from 'path'
import { randomUUID } from 'crypto'

function tempDb() {
  return join(tmpdir(), `sombra-test-${randomUUID()}.db`)
}

test('batchCreate creates nodes with aliases', (t) => {
  const db = Database.open(tempDb())
  try {
    const result = db.batchCreate()
      .nodes('User', [
        { name: 'Alice', $alias: 'alice' },
        { name: 'Bob', $alias: 'bob' },
      ])
      .execute()

    t.is(result.nodes.length, 2)
    t.true(result.aliases.has('alice'))
    t.true(result.aliases.has('bob'))
  } finally {
    db.close()
  }
})

test('batchCreate edges with alias references', (t) => {
  const db = Database.open(tempDb())
  try {
    const result = db.batchCreate()
      .nodes('User', [
        { name: 'Alice', $alias: 'alice' },
        { name: 'Bob', $alias: 'bob' },
      ])
      .edges('FOLLOWS', [
        { src: '$alice', dst: '$bob' },
      ])
      .execute()

    t.is(result.edges.length, 1)
  } finally {
    db.close()
  }
})

test('batchCreate edges with handle references', (t) => {
  const db = Database.open(tempDb())
  try {
    const result = db.batchCreate()
      .nodes('User', [
        { name: 'Alice' },
        { name: 'Bob' },
      ])
      .edges('FOLLOWS', [
        { src: 0, dst: 1 },  // handle indices
      ])
      .execute()

    t.is(result.edges.length, 1)
  } finally {
    db.close()
  }
})

test('batchCreate edges with existing ID references', async (t) => {
  const db = Database.open(tempDb())
  try {
    // First create some nodes
    const first = db.batchCreate()
      .nodes('User', [{ name: 'Existing' }])
      .execute()
    
    const existingId = first.nodes[0]

    // Then reference by bigint ID
    const result = db.batchCreate()
      .nodes('User', [{ name: 'New', $alias: 'new' }])
      .edges('FOLLOWS', [
        { src: existingId, dst: '$new' },  // bigint ID reference
      ])
      .execute()

    t.is(result.edges.length, 1)
  } finally {
    db.close()
  }
})

test('batchCreate returns BigUint64Array for IDs', (t) => {
  const db = Database.open(tempDb())
  try {
    const result = db.batchCreate()
      .nodes('User', [{ name: 'Test' }])
      .execute()

    t.true(result.nodes instanceof BigUint64Array)
  } finally {
    db.close()
  }
})
```

### Phase 6: Benchmarks

**File:** `bindings/node/benchmark/typed-bench.mjs`

```javascript
import { Database } from '../main.js'
import { tmpdir } from 'os'
import { join } from 'path'
import { randomUUID } from 'crypto'

function tempDb() {
  return join(tmpdir(), `sombra-bench-${randomUUID()}.db`)
}

function generateUsers(count) {
  return Array.from({ length: count }, (_, i) => ({
    name: `User${i}`,
    email: `user${i}@example.com`,
    age: 20 + (i % 50),
  }))
}

async function benchmarkJsonPath(db, users) {
  const start = performance.now()
  const builder = db.create()
  for (const user of users) {
    builder.node('User', user)
  }
  builder.execute()
  return performance.now() - start
}

async function benchmarkTypedPath(db, users) {
  const start = performance.now()
  db.batchCreate()
    .nodes('User', users)
    .execute()
  return performance.now() - start
}

async function main() {
  const counts = [1000, 10000, 50000]
  
  for (const count of counts) {
    const users = generateUsers(count)
    
    // JSON path
    const dbJson = Database.open(tempDb())
    const jsonMs = await benchmarkJsonPath(dbJson, users)
    dbJson.close()
    
    // Typed path
    const dbTyped = Database.open(tempDb())
    const typedMs = await benchmarkTypedPath(dbTyped, users)
    dbTyped.close()
    
    const speedup = (jsonMs / typedMs).toFixed(2)
    console.log(`${count} users: JSON ${jsonMs.toFixed(1)}ms, Typed ${typedMs.toFixed(1)}ms (${speedup}x faster)`)
  }
}

main().catch(console.error)
```

## File Modifications Summary

| File | Changes |
|------|---------|
| `bindings/node/src/lib.rs` | Add `TypedNodeSpec`, `TypedEdgeSpec`, `TypedNodeRef`, `TypedPropValue`, `TypedBatchSpec`, `TypedBatchResult`, `databaseCreateTypedBatch()` |
| `src/ffi/mod.rs` | Add `create_typed_batch()`, `typed_props_to_storage()`, `resolve_typed_ref()`, caching helpers |
| `bindings/node/main.js` | Add `BatchCreateBuilder`, `BatchError`, `autoTuneBatchSize()`, update `Database.batchCreate` |
| `bindings/node/index.d.ts` | Add TypeScript type definitions |
| `bindings/node/__test__/typed-batch.test.mjs` | New test file |
| `bindings/node/benchmark/typed-bench.mjs` | New benchmark file |

## Constraints

- Keep existing JSON API intact (additive change only)
- Engine is single-writer (no parallel commits, but parallel prep OK)
- Target 3-5x improvement over JSON path for large batches
- Maintain backward compatibility with existing CreateBuilder

## Success Criteria

1. Typed batch API creates nodes/edges successfully
2. Alias resolution works for all reference types
3. BigUint64Array returned for node/edge IDs
4. Performance improvement of 3-5x for 10K+ records
5. All tests pass
6. Documentation complete
