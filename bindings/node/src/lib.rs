#![deny(clippy::all)]

use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

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
    let guard = self.inner.lock().map_err(|_| {
      NapiError::new(Status::GenericFailure, "database handle is poisoned")
    })?;
    let db = guard.as_ref().ok_or_else(|| {
      NapiError::new(Status::GenericFailure, "database is closed")
    })?;
    f(db)
  }
}

#[napi]
pub struct StreamHandle {
  inner: QueryStream,
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
  if let Some(ms) = opts
    .group_commit_max_wait_ms
    .or(opts.commit_coalesce_ms)
  {
    pager_opts.group_commit_max_wait_ms = ms as u64;
  }
  if let Some(frames) = opts
    .group_commit_max_frames
    .or(opts.commit_max_frames)
  {
    pager_opts.group_commit_max_frames = frames as usize;
  }
  if let Some(commits) = opts
    .group_commit_max_writers
    .or(opts.commit_max_commits)
  {
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
    Ok(StreamHandle { inner: stream })
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
    record.map(|node| to_json_value(node)).transpose()
  })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseGetEdge(handle: &DatabaseHandle, edge_id: i64) -> NapiResult<Option<Value>> {
  let id = u64_from_js_id(edge_id, "getEdgeRecord")?;
  handle.with_db(|db| {
    let record = db.get_edge_record(id).map_err(to_napi_err)?;
    record.map(|edge| to_json_value(edge)).transpose()
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
pub fn databaseListNodesWithLabel(
  handle: &DatabaseHandle,
  label: String,
) -> NapiResult<Vec<u64>> {
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
      .bfs_traversal(start, dir, max_depth, opts.edge_types.as_deref(), max_results)
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
  let mut guard = handle.inner.lock().map_err(|_| {
    NapiError::new(Status::GenericFailure, "database handle is poisoned")
  })?;
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
    self.inner.next().map_err(to_napi_err)
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
