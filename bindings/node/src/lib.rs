#![deny(clippy::all)]

use std::sync::Arc;

use napi::bindgen_prelude::Result as NapiResult;
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
  #[napi(js_name = "autocheckpointMs")]
  pub autocheckpoint_ms: Option<u32>,
}

#[napi]
pub struct DatabaseHandle {
  inner: Arc<Database>,
}

impl DatabaseHandle {
  fn new(db: Database) -> Self {
    Self {
      inner: Arc::new(db),
    }
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
  pub node_id: u64,
  #[napi(js_name = "edgeId")]
  pub edge_id: u64,
  #[napi(js_name = "typeId")]
  pub type_id: u32,
}

#[derive(Debug, Clone)]
#[napi(object)]
pub struct BfsVisitRecord {
  #[napi(js_name = "nodeId")]
  pub node_id: u64,
  pub depth: u32,
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
  if let Some(ms) = opts.commit_coalesce_ms {
    pager_opts.wal_commit_coalesce_ms = ms as u64;
  }
  if let Some(frames) = opts.commit_max_frames {
    pager_opts.wal_commit_max_frames = frames as usize;
  }
  if let Some(commits) = opts.commit_max_commits {
    pager_opts.wal_commit_max_commits = commits as usize;
  }
  if let Some(auto_ms) = opts.autocheckpoint_ms {
    pager_opts.autocheckpoint_ms = Some(auto_ms as u64);
  }

  let db_opts = DatabaseOptions {
    create_if_missing: opts.create_if_missing.unwrap_or(true),
    pager: pager_opts,
    distinct_neighbors_default: opts.distinct_neighbors_default.unwrap_or(false),
  };

  let db = Database::open(path, db_opts).map_err(to_napi_err)?;
  Ok(DatabaseHandle::new(db))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseExecute(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.inner.execute_json(&spec).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseExplain(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.inner.explain_json(&spec).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseStream(handle: &DatabaseHandle, spec: Value) -> NapiResult<StreamHandle> {
  let stream = handle.inner.stream_json(&spec).map_err(to_napi_err)?;
  Ok(StreamHandle { inner: stream })
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseMutate(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.inner.mutate_json(&spec).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCreate(handle: &DatabaseHandle, spec: Value) -> NapiResult<Value> {
  handle.inner.create_json(&spec).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseIntern(handle: &DatabaseHandle, name: String) -> NapiResult<u32> {
  handle.inner.intern(&name).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseSeedDemo(handle: &DatabaseHandle) -> NapiResult<()> {
  handle.inner.seed_demo().map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databasePragmaGet(handle: &DatabaseHandle, name: String) -> NapiResult<Value> {
  handle.inner.pragma(&name, None).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databasePragmaSet(handle: &DatabaseHandle, name: String, value: Value) -> NapiResult<Value> {
  handle.inner.pragma(&name, Some(value)).map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCancelRequest(handle: &DatabaseHandle, request_id: String) -> NapiResult<bool> {
  Ok(handle.inner.cancel_request(&request_id))
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseGetNode(handle: &DatabaseHandle, node_id: u64) -> NapiResult<Option<Value>> {
  let record = handle.inner.get_node_record(node_id).map_err(to_napi_err)?;
  record.map(|node| to_json_value(node)).transpose()
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseGetEdge(handle: &DatabaseHandle, edge_id: u64) -> NapiResult<Option<Value>> {
  let record = handle.inner.get_edge_record(edge_id).map_err(to_napi_err)?;
  record.map(|edge| to_json_value(edge)).transpose()
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCountNodesWithLabel(handle: &DatabaseHandle, label: String) -> NapiResult<u64> {
  handle
    .inner
    .count_nodes_with_label(&label)
    .map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseCountEdgesWithType(handle: &DatabaseHandle, ty: String) -> NapiResult<u64> {
  handle
    .inner
    .count_edges_with_type(&ty)
    .map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseListNodesWithLabel(
  handle: &DatabaseHandle,
  label: String,
) -> NapiResult<Vec<u64>> {
  handle
    .inner
    .node_ids_with_label(&label)
    .map_err(to_napi_err)
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseNeighbors(
  handle: &DatabaseHandle,
  node_id: u64,
  options: Option<NeighborOptions>,
) -> NapiResult<Vec<NeighborRecord>> {
  let opts = options.unwrap_or_default();
  let dir = parse_direction(opts.direction.as_deref())?;
  let distinct = opts.distinct.unwrap_or(true);
  let neighbors = handle
    .inner
    .neighbors_with_options(node_id, dir, opts.edge_type.as_deref(), distinct)
    .map_err(to_napi_err)?;
  Ok(neighbors.into_iter().map(NeighborRecord::from).collect())
}

#[allow(non_snake_case)]
#[napi]
pub fn databaseBfsTraversal(
  handle: &DatabaseHandle,
  start_id: u64,
  max_depth: u32,
  options: Option<BfsTraversalOptions>,
) -> NapiResult<Vec<BfsVisitRecord>> {
  let opts = options.unwrap_or_default();
  let dir = parse_direction(opts.direction.as_deref())?;
  let max_results = opts.max_results.map(|value| value as usize);
  let visits = handle
    .inner
    .bfs_traversal(start_id, dir, max_depth, opts.edge_types.as_deref(), max_results)
    .map_err(to_napi_err)?;
  Ok(visits.into_iter().map(BfsVisitRecord::from).collect())
}

#[napi]
impl StreamHandle {
  #[napi]
  pub fn next(&self) -> NapiResult<Option<Value>> {
    self.inner.next().map_err(to_napi_err)
  }
}

fn to_napi_err(err: FfiError) -> napi::Error {
  napi::Error::from_reason(err.to_string())
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

impl From<NeighborInfo> for NeighborRecord {
  fn from(value: NeighborInfo) -> Self {
    Self {
      node_id: value.node_id,
      edge_id: value.edge_id,
      type_id: value.type_id,
    }
  }
}

impl From<BfsVisitInfo> for BfsVisitRecord {
  fn from(value: BfsVisitInfo) -> Self {
    Self {
      node_id: value.node_id,
      depth: value.depth,
    }
  }
}
