#![deny(clippy::all)]
#![allow(clippy::arc_with_non_send_sync)]

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::BigInt;
use napi::{bindgen_prelude::Result as NapiResult, Error as NapiError, Status};
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sombra::{
  ffi::{BfsVisitInfo, Database, DatabaseOptions, FfiError, NeighborInfo, QueryStream},
  primitives::pager::{PagerOptions, Synchronous},
  storage::Dir,
};

#[derive(Debug, Default, Deserialize)]
#[napi(object)]
pub struct ConnectOptions {
  pub create_if_missing: Option<bool>,
  pub page_size: Option<u32>,
  pub cache_pages: Option<u32>,
  pub distinct_neighbors_default: Option<bool>,
  pub synchronous: Option<String>,
  #[napi(js_name = "commitCoalesceMs")]
  pub commit_coalesce_ms: Option<u32>,
  #[napi(js_name = "commitMaxFrames")]
  pub commit_max_frames: Option<u32>,
  #[napi(js_name = "commitMaxCommits")]
  pub commit_max_commits: Option<u32>,
  #[napi(js_name = "groupCommitMaxWriters")]
  pub group_commit_max_writers: Option<u32>,
  #[napi(js_name = "groupCommitMaxFrames")]
  pub group_commit_max_frames: Option<u32>,
  #[napi(js_name = "groupCommitMaxWaitMs")]
  pub group_commit_max_wait_ms: Option<u32>,
  #[napi(js_name = "asyncFsync")]
  pub async_fsync: Option<bool>,
  #[napi(js_name = "asyncFsyncMaxWaitMs")]
  pub async_fsync_max_wait_ms: Option<u32>,
  #[napi(js_name = "walSegmentBytes")]
  pub wal_segment_bytes: Option<u32>,
  #[napi(js_name = "walPreallocateSegments")]
  pub wal_preallocate_segments: Option<u32>,
  #[napi(js_name = "autocheckpointMs")]
  pub autocheckpoint_ms: Option<u32>,
}

#[napi]
pub struct DatabaseHandle {
  inner: Mutex<Option<Arc<Database>>>,
}

impl DatabaseHandle {
  fn new(db: Database) -> Self {
    Self {
      inner: Mutex::new(Some(Arc::new(db))),
    }
  }

  fn with_db<T, F>(&self, f: F) -> NapiResult<T>
  where
    F: FnOnce(&Database) -> NapiResult<T>,
  {
    let guard = self
      .inner
      .lock()
      .map_err(|_| NapiError::new(Status::GenericFailure, "database handle is poisoned"))?;
    let db = guard
      .as_ref()
      .ok_or_else(|| NapiError::new(Status::GenericFailure, "database is closed"))?;
    f(db)
  }
}

#[napi]
pub struct StreamHandle {
  inner: Mutex<Option<QueryStream>>,
}

#[derive(Debug, Default, Deserialize)]
#[napi(object)]
pub struct NeighborOptions {
  pub direction: Option<String>,
  #[napi(js_name = "edgeType")]
  pub edge_type: Option<String>,
  pub distinct: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[napi(object)]
pub struct BfsTraversalOptions {
  pub direction: Option<String>,
  #[napi(js_name = "edgeTypes")]
  pub edge_types: Option<Vec<String>>,
  #[napi(js_name = "maxResults")]
  pub max_results: Option<u32>,
}

#[derive(Debug, Clone)]
#[napi(object)]
pub struct NeighborRecord {
  #[napi(js_name = "nodeId")]
  pub node_id: i64,
  #[napi(js_name = "edgeId")]
  pub edge_id: i64,
  #[napi(js_name = "typeId")]
  pub type_id: u32,
}

#[derive(Debug, Clone)]
#[napi(object)]
pub struct BfsVisitRecord {
  #[napi(js_name = "nodeId")]
  pub node_id: i64,
  pub depth: u32,
}

fn js_id_from_u64(value: u64, ctx: &str) -> NapiResult<i64> {
  if value > i64::MAX as u64 {
    Err(NapiError::new(
      Status::InvalidArg,
      format!("{ctx} result exceeds supported range"),
    ))
  } else {
    Ok(value as i64)
  }
}

fn u64_from_js_id(value: i64, ctx: &str) -> NapiResult<u64> {
  if value < 0 {
    Err(NapiError::new(
      Status::InvalidArg,
      format!("{ctx} requires a non-negative integer id"),
    ))
  } else {
    Ok(value as u64)
  }
}

#[allow(non_snake_case)]
#[napi]
pub fn openDatabase(path: String, options: Option<ConnectOptions>) -> NapiResult<DatabaseHandle> {
  let opts = options.unwrap_or_default();
  let mut pager_opts = PagerOptions::default();
  if let Some(page_size) = opts.page_size {
    pager_opts.page_size = page_size;
  }
  if let Some(cache_pages) = opts.cache_pages {
    pager_opts.cache_pages = cache_pages as usize;
  }
  if let Some(mode) = opts.synchronous.as_deref() {
    pager_opts.synchronous = parse_synchronous(mode)?;
  }
  if let Some(ms) = opts.group_commit_max_wait_ms.or(opts.commit_coalesce_ms) {
    pager_opts.group_commit_max_wait_ms = ms as u64;
  }
  if let Some(frames) = opts.group_commit_max_frames.or(opts.commit_max_frames) {
    pager_opts.group_commit_max_frames = frames as usize;
  }
  if let Some(commits) = opts.group_commit_max_writers.or(opts.commit_max_commits) {
    pager_opts.group_commit_max_writers = commits as usize;
  }
  if let Some(async_fsync) = opts.async_fsync {
    pager_opts.async_fsync = async_fsync;
  }
  if let Some(wait_ms) = opts.async_fsync_max_wait_ms {
    pager_opts.async_fsync_max_wait_ms = wait_ms as u64;
  }
  if let Some(bytes) = opts.wal_segment_bytes {
    pager_opts.wal_segment_size_bytes = bytes as u64;
  }
  if let Some(preallocate) = opts.wal_preallocate_segments {
    pager_opts.wal_preallocate_segments = preallocate;
  }
  if let Some(auto_ms) = opts.autocheckpoint_ms {
    pager_opts.autocheckpoint_ms = Some(auto_ms as u64);
  }

  let db_opts = DatabaseOptions {
    create_if_missing: opts.create_if_missing.unwrap_or(true),
    pager: pager_opts,
    distinct_neighbors_default: opts.distinct_neighbors_default.unwrap_or(false),
    ..DatabaseOptions::default()
  };

  let db = Database::open(path, db_opts).map_err(to_napi_err)?;
  Ok(DatabaseHandle::new(db))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseExecute(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.with_db(|db| db.execute_json(&spec).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseExplain(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.with_db(|db| db.explain_json(&spec).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseStream(handle: &DatabaseHandle, spec: Value) -> NapiResult<StreamHandle> {
  handle.with_db(|db| {
    let stream = db.stream_json(&spec).map_err(to_napi_err)?;
    Ok(StreamHandle {
      inner: Mutex::new(Some(stream)),
    })
  })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseMutate(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.with_db(|db| db.mutate_json(&spec).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCreate(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.with_db(|db| db.create_json(&spec).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseIntern(handle: &DatabaseHandle, name: String) -> NapiResult<u32> {
  handle.with_db(|db| db.intern(&name).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseSeedDemo(handle: &DatabaseHandle) -> NapiResult<()> {
  handle.with_db(|db| db.seed_demo().map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databasePragmaGet(handle: &DatabaseHandle, name: String) -> NapiResult<Value> {
  handle.with_db(|db| db.pragma(&name, None).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databasePragmaSet(handle: &DatabaseHandle, name: String, value: Value) -> NapiResult<Value> {
  handle.with_db(|db| db.pragma(&name, Some(value)).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCancelRequest(handle: &DatabaseHandle, request_id: String) -> NapiResult<bool> {
  handle.with_db(|db| Ok(db.cancel_request(&request_id)))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseGetNode(handle: &DatabaseHandle, node_id: i64) -> NapiResult<Option<Value>> {
  let id = u64_from_js_id(node_id, "getNodeRecord")?;
  handle.with_db(|db| {
    let record = db.get_node_record(id).map_err(to_napi_err)?;
    record.map(to_json_value).transpose()
  })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseGetEdge(handle: &DatabaseHandle, edge_id: i64) -> NapiResult<Option<Value>> {
  let id = u64_from_js_id(edge_id, "getEdgeRecord")?;
  handle.with_db(|db| {
    let record = db.get_edge_record(id).map_err(to_napi_err)?;
    record.map(to_json_value).transpose()
  })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCountNodesWithLabel(handle: &DatabaseHandle, label: String) -> NapiResult<u64> {
  handle.with_db(|db| db.count_nodes_with_label(&label).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCountEdgesWithType(handle: &DatabaseHandle, ty: String) -> NapiResult<u64> {
  handle.with_db(|db| db.count_edges_with_type(&ty).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseListNodesWithLabel(handle: &DatabaseHandle, label: String) -> NapiResult<Vec<u64>> {
  handle.with_db(|db| db.node_ids_with_label(&label).map_err(to_napi_err))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseNeighbors(
  handle: &DatabaseHandle,
  node_id: i64,
  options: Option<NeighborOptions>,
) -> NapiResult<Vec<NeighborRecord>> {
  let id = u64_from_js_id(node_id, "neighbors")?;
  let opts = options.unwrap_or_default();
  let dir = parse_direction(opts.direction.as_deref())?;
  let distinct = opts.distinct.unwrap_or(true);
  handle.with_db(|db| {
    let neighbors = db
      .neighbors_with_options(id, dir, opts.edge_type.as_deref(), distinct)
      .map_err(to_napi_err)?;
    neighbors
      .into_iter()
      .map(NeighborRecord::try_from)
      .collect::<Result<Vec<_>, _>>()
  })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseBfsTraversal(
  handle: &DatabaseHandle,
  start_id: i64,
  max_depth: u32,
  options: Option<BfsTraversalOptions>,
) -> NapiResult<Vec<BfsVisitRecord>> {
  let start = u64_from_js_id(start_id, "bfsTraversal")?;
  let opts = options.unwrap_or_default();
  let dir = parse_direction(opts.direction.as_deref())?;
  let max_results = opts.max_results.map(|value| value as usize);
  handle.with_db(|db| {
    let visits = db
      .bfs_traversal(
        start,
        dir,
        max_depth,
        opts.edge_types.as_deref(),
        max_results,
      )
      .map_err(to_napi_err)?;
    visits
      .into_iter()
      .map(BfsVisitRecord::try_from)
      .collect::<Result<Vec<_>, _>>()
  })
}

/// Closes the database handle, releasing all resources.
///
/// After calling close(), all subsequent operations on this handle will fail
/// with a "database is closed" error.
#[allow(non_snake_case)]
#[napi]
pub fn databaseClose(handle: &DatabaseHandle) -> NapiResult<()> {
  let mut guard = handle
    .inner
    .lock()
    .map_err(|_| NapiError::new(Status::GenericFailure, "database handle is poisoned"))?;
  if guard.take().is_none() {
    return Err(NapiError::new(
      Status::GenericFailure,
      "database is already closed",
    ));
  }
  Ok(())
}

#[napi]
impl StreamHandle {
  #[napi]
  pub fn next(&self) -> NapiResult<Option<Value>> {
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| NapiError::new(Status::GenericFailure, "[CLOSED] stream handle is poisoned"))?;
    let stream = guard
      .as_mut()
      .ok_or_else(|| NapiError::new(Status::GenericFailure, "[CLOSED] stream is closed"))?;
    stream.next().map_err(to_napi_err)
  }

  #[napi]
  pub fn close(&self) -> NapiResult<()> {
    let mut guard = self
      .inner
      .lock()
      .map_err(|_| NapiError::new(Status::GenericFailure, "[CLOSED] stream handle is poisoned"))?;
    guard.take();
    Ok(())
  }
}

fn to_napi_err(err: FfiError) -> napi::Error {
  // Include error code in the message for programmatic handling
  let code_name = err.code_name();
  let message = err.to_string();
  napi::Error::new(Status::GenericFailure, format!("[{code_name}] {message}"))
}

fn to_json_value<T>(value: T) -> NapiResult<Value>
where
  T: Serialize,
{
  serde_json::to_value(value).map_err(|err| napi::Error::from_reason(err.to_string()))
}

fn parse_direction(value: Option<&str>) -> NapiResult<Dir> {
  match value.unwrap_or("out") {
    "out" => Ok(Dir::Out),
    "in" => Ok(Dir::In),
    "both" => Ok(Dir::Both),
    other => Err(napi::Error::from_reason(format!(
      "invalid direction '{other}', expected 'out', 'in', or 'both'"
    ))),
  }
}

fn parse_synchronous(value: &str) -> NapiResult<Synchronous> {
  Synchronous::from_str(value).ok_or_else(|| {
    napi::Error::from_reason(format!(
      "invalid synchronous mode '{value}', expected 'full', 'normal', or 'off'"
    ))
  })
}

impl TryFrom<NeighborInfo> for NeighborRecord {
  type Error = NapiError;

  fn try_from(value: NeighborInfo) -> Result<Self, Self::Error> {
    Ok(Self {
      node_id: js_id_from_u64(value.node_id, "neighbor node id")?,
      edge_id: js_id_from_u64(value.edge_id, "neighbor edge id")?,
      type_id: value.type_id,
    })
  }
}

impl TryFrom<BfsVisitInfo> for BfsVisitRecord {
  type Error = NapiError;

  fn try_from(value: BfsVisitInfo) -> Result<Self, Self::Error> {
    Ok(Self {
      node_id: js_id_from_u64(value.node_id, "bfs traversal node id")?,
      depth: value.depth,
    })
  }
}

// ============================================================================
// Typed Batch API - Bypasses JSON serialization for high-performance bulk ops
// ============================================================================

/// Property value for typed batch operations (bypasses JSON).
///
/// This enum mirrors the property types supported by the storage layer
/// but avoids JSON serialization overhead by using direct Rust types.
#[derive(Debug, Clone)]
#[napi(object)]
pub struct TypedPropEntry {
  /// Property key name.
  pub key: String,
  /// Property value kind: "null", "bool", "int", "float", "string", "bytes".
  pub kind: String,
  /// Boolean value (when kind is "bool").
  pub bool_value: Option<bool>,
  /// Integer value (when kind is "int").
  pub int_value: Option<i64>,
  /// Float value (when kind is "float").
  pub float_value: Option<f64>,
  /// String value (when kind is "string").
  pub string_value: Option<String>,
  /// Base64-encoded bytes (when kind is "bytes").
  pub bytes_value: Option<String>,
}

/// Node specification for typed batch creation.
#[derive(Debug, Clone)]
#[napi(object)]
pub struct TypedNodeSpec {
  /// Node label (single label for optimization).
  pub label: String,
  /// Node properties.
  pub props: Vec<TypedPropEntry>,
  /// Optional alias for edge references.
  pub alias: Option<String>,
}

/// Node reference for typed edge creation.
#[derive(Debug, Clone)]
#[napi(object)]
pub struct TypedNodeRef {
  /// Reference kind: "alias", "handle", or "id".
  pub kind: String,
  /// Alias name (when kind is "alias").
  pub alias: Option<String>,
  /// Handle index (when kind is "handle").
  pub handle: Option<u32>,
  /// Node ID as BigInt (when kind is "id").
  pub id: Option<BigInt>,
}

/// Edge specification for typed batch creation.
#[derive(Debug, Clone)]
#[napi(object)]
pub struct TypedEdgeSpec {
  /// Edge type name.
  pub ty: String,
  /// Source node reference.
  pub src: TypedNodeRef,
  /// Destination node reference.
  pub dst: TypedNodeRef,
  /// Edge properties.
  pub props: Vec<TypedPropEntry>,
}

/// Batch specification for typed creation.
#[derive(Debug, Clone)]
#[napi(object)]
pub struct TypedBatchSpec {
  /// Nodes to create.
  pub nodes: Vec<TypedNodeSpec>,
  /// Edges to create.
  pub edges: Vec<TypedEdgeSpec>,
}

/// Options controlling typed bulk load behavior.
#[derive(Debug, Default, Deserialize)]
#[napi(object)]
pub struct BulkLoadOptions {
  #[napi(js_name = "nodeChunkSize")]
  pub node_chunk_size: Option<u32>,
  #[napi(js_name = "edgeChunkSize")]
  pub edge_chunk_size: Option<u32>,
}

// ============================================================================
// Conversion from NAPI types to core FFI types
// ============================================================================

impl TypedPropEntry {
  fn to_ffi(&self) -> NapiResult<sombra::ffi::TypedPropEntry> {
    Ok(sombra::ffi::TypedPropEntry {
      key: self.key.clone(),
      kind: self.kind.clone(),
      bool_value: self.bool_value,
      int_value: self.int_value,
      float_value: self.float_value,
      string_value: self.string_value.clone(),
      bytes_value: self.bytes_value.clone(),
    })
  }
}

impl TypedNodeRef {
  fn to_ffi(&self) -> NapiResult<sombra::ffi::TypedNodeRef> {
    let id = match &self.id {
      Some(bigint) => {
        let (signed, value, _lossless) = bigint.get_u64();
        if signed {
          return Err(NapiError::new(
            Status::InvalidArg,
            "node ID cannot be negative",
          ));
        }
        Some(value)
      }
      None => None,
    };
    Ok(sombra::ffi::TypedNodeRef {
      kind: self.kind.clone(),
      alias: self.alias.clone(),
      handle: self.handle,
      id,
    })
  }
}

impl TypedNodeSpec {
  fn to_ffi(&self) -> NapiResult<sombra::ffi::TypedNodeSpec> {
    let props = self
      .props
      .iter()
      .map(|p| p.to_ffi())
      .collect::<NapiResult<Vec<_>>>()?;
    Ok(sombra::ffi::TypedNodeSpec {
      label: self.label.clone(),
      props,
      alias: self.alias.clone(),
    })
  }
}

impl TypedEdgeSpec {
  fn to_ffi(&self) -> NapiResult<sombra::ffi::TypedEdgeSpec> {
    let props = self
      .props
      .iter()
      .map(|p| p.to_ffi())
      .collect::<NapiResult<Vec<_>>>()?;
    Ok(sombra::ffi::TypedEdgeSpec {
      ty: self.ty.clone(),
      src: self.src.to_ffi()?,
      dst: self.dst.to_ffi()?,
      props,
    })
  }
}

impl TypedBatchSpec {
  fn to_ffi(&self) -> NapiResult<sombra::ffi::TypedBatchSpec> {
    let nodes = self
      .nodes
      .iter()
      .map(|n| n.to_ffi())
      .collect::<NapiResult<Vec<_>>>()?;
    let edges = self
      .edges
      .iter()
      .map(|e| e.to_ffi())
      .collect::<NapiResult<Vec<_>>>()?;
    Ok(sombra::ffi::TypedBatchSpec { nodes, edges })
  }
}

/// Result of typed batch creation.
#[derive(Debug, Clone, Serialize)]
#[napi(object)]
pub struct TypedBatchResult {
  /// Created node IDs as array of BigInt-compatible values.
  pub nodes: Vec<i64>,
  /// Created edge IDs as array of BigInt-compatible values.
  pub edges: Vec<i64>,
  /// Alias to node ID mapping.
  pub aliases: HashMap<String, i64>,
}

/// Creates nodes and edges from typed specifications (bypasses JSON).
///
/// This is a high-performance alternative to `databaseCreate` that avoids
/// JSON serialization overhead. Property values are passed directly as
/// typed Rust values.
///
/// # Arguments
///
/// * `handle` - Database handle
/// * `spec` - Typed batch specification with nodes and edges
///
/// # Returns
///
/// Result containing created node IDs, edge IDs, and alias mappings.
#[allow(non_snake_case)]
#[napi]
pub fn databaseCreateTypedBatch(
  handle: &DatabaseHandle,
  spec: TypedBatchSpec,
) -> NapiResult<TypedBatchResult> {
  handle.with_db(|db| {
    // Convert NAPI types to core FFI types
    let ffi_spec = spec.to_ffi()?;
    let result = db.create_typed_batch(&ffi_spec).map_err(to_napi_err)?;

    // Convert u64 to i64 for JavaScript compatibility
    // (BigInt in napi is handled separately, but for arrays we use i64)
    let nodes: Vec<i64> = result
      .node_ids
      .iter()
      .map(|id| {
        if id.0 > i64::MAX as u64 {
          Err(NapiError::new(
            Status::InvalidArg,
            "node ID exceeds JavaScript safe integer range",
          ))
        } else {
          Ok(id.0 as i64)
        }
      })
      .collect::<NapiResult<Vec<_>>>()?;

    let edges: Vec<i64> = result
      .edge_ids
      .iter()
      .map(|id| {
        if id.0 > i64::MAX as u64 {
          Err(NapiError::new(
            Status::InvalidArg,
            "edge ID exceeds JavaScript safe integer range",
          ))
        } else {
          Ok(id.0 as i64)
        }
      })
      .collect::<NapiResult<Vec<_>>>()?;

    let aliases: HashMap<String, i64> = result
      .aliases
      .iter()
      .map(|(k, v)| {
        if v.0 > i64::MAX as u64 {
          Err(NapiError::new(
            Status::InvalidArg,
            "alias ID exceeds JavaScript safe integer range",
          ))
        } else {
          Ok((k.clone(), v.0 as i64))
        }
      })
      .collect::<NapiResult<HashMap<_, _>>>()?;

    Ok(TypedBatchResult {
      nodes,
      edges,
      aliases,
    })
  })
}

/// Bulk loads nodes from typed specifications using chunked transactions.
///
/// This is a non-atomic bulk-ingest API: each chunk is committed
/// independently. Node aliases are not supported in bulk mode.
#[allow(non_snake_case)]
#[napi]
pub fn databaseBulkLoadNodesTyped(
  handle: &DatabaseHandle,
  nodes: Vec<TypedNodeSpec>,
  options: Option<BulkLoadOptions>,
) -> NapiResult<Vec<i64>> {
  handle.with_db(|db| {
    let ffi_nodes: Vec<sombra::ffi::TypedNodeSpec> = nodes
      .iter()
      .map(|n| n.to_ffi())
      .collect::<NapiResult<_>>()?;

    let mut opts = sombra::ffi::BulkLoadOptions::default();
    if let Some(o) = options {
      if let Some(chunk) = o.node_chunk_size {
        if chunk > 0 {
          opts.node_chunk_size = chunk as usize;
        }
      }
      if let Some(chunk) = o.edge_chunk_size {
        if chunk > 0 {
          opts.edge_chunk_size = chunk as usize;
        }
      }
    }

    let mut bulk = db.begin_bulk_load(opts);
    let result = bulk.load_nodes(&ffi_nodes).map_err(to_napi_err)?;
    let _stats = bulk.finish();

    let ids: Vec<i64> = result
      .iter()
      .map(|id| {
        if id.0 > i64::MAX as u64 {
          Err(NapiError::new(
            Status::InvalidArg,
            "node ID exceeds JavaScript safe integer range",
          ))
        } else {
          Ok(id.0 as i64)
        }
      })
      .collect::<NapiResult<Vec<_>>>()?;

    Ok(ids)
  })
}

/// Bulk loads edges from typed specifications using chunked transactions.
///
/// This is a non-atomic bulk-ingest API: each chunk is committed
/// independently. Edge endpoints must use `kind == "id"` and refer to
/// already-existing node IDs.
#[allow(non_snake_case)]
#[napi]
pub fn databaseBulkLoadEdgesTyped(
  handle: &DatabaseHandle,
  edges: Vec<TypedEdgeSpec>,
  options: Option<BulkLoadOptions>,
) -> NapiResult<Vec<i64>> {
  handle.with_db(|db| {
    let ffi_edges: Vec<sombra::ffi::TypedEdgeSpec> = edges
      .iter()
      .map(|e| e.to_ffi())
      .collect::<NapiResult<_>>()?;

    let mut opts = sombra::ffi::BulkLoadOptions::default();
    if let Some(o) = options {
      if let Some(chunk) = o.node_chunk_size {
        if chunk > 0 {
          opts.node_chunk_size = chunk as usize;
        }
      }
      if let Some(chunk) = o.edge_chunk_size {
        if chunk > 0 {
          opts.edge_chunk_size = chunk as usize;
        }
      }
    }

    let mut bulk = db.begin_bulk_load(opts);
    let result = bulk.load_edges(&ffi_edges).map_err(to_napi_err)?;
    let _stats = bulk.finish();

    let ids: Vec<i64> = result
      .iter()
      .map(|id| {
        if id.0 > i64::MAX as u64 {
          Err(NapiError::new(
            Status::InvalidArg,
            "edge ID exceeds JavaScript safe integer range",
          ))
        } else {
          Ok(id.0 as i64)
        }
      })
      .collect::<NapiResult<Vec<_>>>()?;

    Ok(ids)
  })
}
