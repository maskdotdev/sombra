#![deny(clippy::all)]

use std::sync::Arc;

use napi::bindgen_prelude::Result as NapiResult;
use napi_derive::napi;
use serde::Deserialize;
use serde_json::Value;
use sombra::{
  ffi::{Database, DatabaseOptions, FfiError, QueryStream},
  primitives::pager::{PagerOptions, Synchronous},
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
pub fn databaseExecute(handle: &DatabaseHandle, spec: Value) -> NapiResult<Vec<Value>> {
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

fn parse_synchronous(value: &str) -> NapiResult<Synchronous> {
  Synchronous::from_str(value).ok_or_else(|| {
    napi::Error::from_reason(format!(
      "invalid synchronous mode '{value}', expected 'full', 'normal', or 'off'"
    ))
  })
}
