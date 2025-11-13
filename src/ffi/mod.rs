#![forbid(unsafe_code)]

//! High-level FFI helpers shared by the language bindings.
//!
//! This module exposes a safe wrapper around the Stage 8 planner/executor so
//! bindings can submit JSON-friendly query specifications without reimplementing
//! the core logic.

use crate::primitives::pager::{PageStore, Pager, PagerOptions, Synchronous, WriteGuard};
use crate::query::{
    analyze::{self, MAX_BYTES_LITERAL, MAX_IN_VALUES},
    ast::{
        BoolExpr, Comparison, EdgeClause, EdgeDirection, MatchClause, Projection, QueryAst, Var,
    },
    errors::{AnalyzerError, SchemaVersionState},
    executor::{Executor, QueryResult, ResultStream, Row, Value as ExecValue},
    metadata::{CatalogMetadata, MetadataProvider},
    planner::{ExplainNode, PlanExplain, Planner, PlannerConfig, PlannerOutput},
    profile::profile_snapshot as query_profile_snapshot,
    Value as QueryValue,
};
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::{
    DeleteNodeOpts, EdgeSpec as StorageEdgeSpec, Graph, GraphOptions, IndexDef, IndexKind,
    NodeSpec as StorageNodeSpec, PropEntry, PropPatch, PropPatchOp, PropValue, PropValueOwned,
    TypeTag,
};
use crate::types::{EdgeId, LabelId, NodeId, PropId, SombraError, StrId, TypeId};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::{
    collections::{HashMap, HashSet},
    fs, mem,
    ops::Bound,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};
use thiserror::Error;

/// Result type for FFI operations, using [`FfiError`] for error handling.
pub type Result<T> = std::result::Result<T, FfiError>;

/// Performance profiling snapshot from FFI layer and query execution.
///
/// Contains timing and count statistics for various phases of query planning,
/// execution, and serialization to help identify performance bottlenecks.
#[derive(Debug, Clone, Copy)]
pub struct ProfileSnapshot {
    /// Total time spent planning queries (nanoseconds).
    pub plan_ns: u64,
    /// Number of query planning operations.
    pub plan_count: u64,
    /// Total time spent executing queries (nanoseconds).
    pub exec_ns: u64,
    /// Number of query execution operations.
    pub exec_count: u64,
    /// Total time spent serializing results (nanoseconds).
    pub serde_ns: u64,
    /// Number of serialization operations.
    pub serde_count: u64,
    /// Time spent acquiring read guards (nanoseconds).
    pub query_read_guard_ns: u64,
    /// Number of read guard acquisitions.
    pub query_read_guard_count: u64,
    /// Time spent building query streams (nanoseconds).
    pub query_stream_build_ns: u64,
    /// Number of query stream builds.
    pub query_stream_build_count: u64,
    /// Time spent iterating query streams (nanoseconds).
    pub query_stream_iter_ns: u64,
    /// Number of query stream iterations.
    pub query_stream_iter_count: u64,
    /// Time spent in property index operations (nanoseconds).
    pub query_prop_index_ns: u64,
    /// Number of property index operations.
    pub query_prop_index_count: u64,
    /// Time spent in property index lookups (nanoseconds).
    pub query_prop_index_lookup_ns: u64,
    /// Number of property index lookups.
    pub query_prop_index_lookup_count: u64,
    /// Time spent encoding property index values (nanoseconds).
    pub query_prop_index_encode_ns: u64,
    /// Number of property index encodings.
    pub query_prop_index_encode_count: u64,
    /// Time spent building property index streams (nanoseconds).
    pub query_prop_index_stream_build_ns: u64,
    /// Number of property index stream builds.
    pub query_prop_index_stream_build_count: u64,
    /// Time spent iterating property index streams (nanoseconds).
    pub query_prop_index_stream_iter_ns: u64,
    /// Number of property index stream iterations.
    pub query_prop_index_stream_iter_count: u64,
    /// Time spent expanding graph edges (nanoseconds).
    pub query_expand_ns: u64,
    /// Number of graph edge expansions.
    pub query_expand_count: u64,
    /// Time spent filtering results (nanoseconds).
    pub query_filter_ns: u64,
    /// Number of filter operations.
    pub query_filter_count: u64,
}

#[derive(Default)]
struct ProfileCounters {
    plan_ns: AtomicU64,
    plan_count: AtomicU64,
    exec_ns: AtomicU64,
    exec_count: AtomicU64,
    serde_ns: AtomicU64,
    serde_count: AtomicU64,
}

static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE_COUNTERS: OnceLock<ProfileCounters> = OnceLock::new();

fn profiling_enabled() -> bool {
    *PROFILE_ENABLED.get_or_init(|| std::env::var_os("SOMBRA_PROFILE").is_some())
}

fn profile_counters() -> Option<&'static ProfileCounters> {
    profiling_enabled().then(|| PROFILE_COUNTERS.get_or_init(ProfileCounters::default))
}

fn profile_timer() -> Option<Instant> {
    profiling_enabled().then(Instant::now)
}

fn record_profile_timer(kind: ProfileKind, start: Option<Instant>) {
    let Some(start) = start else {
        return;
    };
    let Some(counters) = profile_counters() else {
        return;
    };
    let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    match kind {
        ProfileKind::Plan => {
            counters.plan_ns.fetch_add(nanos, AtomicOrdering::Relaxed);
            counters.plan_count.fetch_add(1, AtomicOrdering::Relaxed);
        }
        ProfileKind::Execute => {
            counters.exec_ns.fetch_add(nanos, AtomicOrdering::Relaxed);
            counters.exec_count.fetch_add(1, AtomicOrdering::Relaxed);
        }
        ProfileKind::Serialize => {
            counters.serde_ns.fetch_add(nanos, AtomicOrdering::Relaxed);
            counters.serde_count.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }
}

/// Captures a snapshot of profiling counters.
///
/// Returns `None` if profiling is not enabled (requires `SOMBRA_PROFILE` environment variable).
/// If `reset` is true, the counters are reset to zero after capturing.
pub fn profile_snapshot(reset: bool) -> Option<ProfileSnapshot> {
    let counters = profile_counters()?;
    let load = |counter: &AtomicU64| {
        if reset {
            counter.swap(0, AtomicOrdering::Relaxed)
        } else {
            counter.load(AtomicOrdering::Relaxed)
        }
    };
    let query = query_profile_snapshot(reset);
    let (
        query_read_guard_ns,
        query_read_guard_count,
        query_stream_build_ns,
        query_stream_build_count,
        query_stream_iter_ns,
        query_stream_iter_count,
        query_prop_index_ns,
        query_prop_index_count,
        query_prop_index_lookup_ns,
        query_prop_index_lookup_count,
        query_prop_index_encode_ns,
        query_prop_index_encode_count,
        query_prop_index_stream_build_ns,
        query_prop_index_stream_build_count,
        query_prop_index_stream_iter_ns,
        query_prop_index_stream_iter_count,
        query_expand_ns,
        query_expand_count,
        query_filter_ns,
        query_filter_count,
    ) = match query {
        Some(snapshot) => (
            snapshot.read_guard_ns,
            snapshot.read_guard_count,
            snapshot.stream_build_ns,
            snapshot.stream_build_count,
            snapshot.stream_iter_ns,
            snapshot.stream_iter_count,
            snapshot.prop_index_ns,
            snapshot.prop_index_count,
            snapshot.prop_index_lookup_ns,
            snapshot.prop_index_lookup_count,
            snapshot.prop_index_encode_ns,
            snapshot.prop_index_encode_count,
            snapshot.prop_index_stream_build_ns,
            snapshot.prop_index_stream_build_count,
            snapshot.prop_index_stream_iter_ns,
            snapshot.prop_index_stream_iter_count,
            snapshot.expand_ns,
            snapshot.expand_count,
            snapshot.filter_ns,
            snapshot.filter_count,
        ),
        None => (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    };
    Some(ProfileSnapshot {
        plan_ns: load(&counters.plan_ns),
        plan_count: load(&counters.plan_count),
        exec_ns: load(&counters.exec_ns),
        exec_count: load(&counters.exec_count),
        serde_ns: load(&counters.serde_ns),
        serde_count: load(&counters.serde_count),
        query_read_guard_ns,
        query_read_guard_count,
        query_stream_build_ns,
        query_stream_build_count,
        query_stream_iter_ns,
        query_stream_iter_count,
        query_prop_index_ns,
        query_prop_index_count,
        query_prop_index_lookup_ns,
        query_prop_index_lookup_count,
        query_prop_index_encode_ns,
        query_prop_index_encode_count,
        query_prop_index_stream_build_ns,
        query_prop_index_stream_build_count,
        query_prop_index_stream_iter_ns,
        query_prop_index_stream_iter_count,
        query_expand_ns,
        query_expand_count,
        query_filter_ns,
        query_filter_count,
    })
}

enum ProfileKind {
    Plan,
    Execute,
    Serialize,
}

/// Errors that can occur during FFI operations.
///
/// Encompasses errors from message creation, core database operations, and JSON serialization.
#[derive(Debug, Error)]
pub enum FfiError {
    /// A custom error message.
    #[error("{0}")]
    Message(String),
    /// Analyzer error surfaced with structured information.
    #[error(transparent)]
    Analyzer(#[from] AnalyzerError),
    /// An error from the core Sombra database engine.
    #[error(transparent)]
    Core(#[from] SombraError),
    /// A JSON serialization/deserialization error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Configuration options for opening a Sombra database via FFI.
#[derive(Clone, Debug)]
pub struct DatabaseOptions {
    /// Create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// Pager configuration options.
    pub pager: PagerOptions,
    /// Enable distinct neighbors by default in graph queries.
    pub distinct_neighbors_default: bool,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            pager: PagerOptions::default(),
            distinct_neighbors_default: false,
        }
    }
}

#[derive(Default)]
struct CancellationRegistry {
    tokens: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl CancellationRegistry {
    fn new() -> Self {
        Self {
            tokens: Mutex::new(HashMap::new()),
        }
    }

    fn register(self: &Arc<Self>, request_id: &str) -> Result<CancellationHandle> {
        if request_id.trim().is_empty() {
            return Err(FfiError::Message(
                "request_id must be a non-empty string".into(),
            ));
        }
        let mut guard = self
            .tokens
            .lock()
            .map_err(|_| FfiError::Message("cancellation registry poisoned".into()))?;
        if guard.contains_key(request_id) {
            return Err(FfiError::Message(format!(
                "request '{request_id}' already has a running query"
            )));
        }
        let flag = Arc::new(AtomicBool::new(false));
        guard.insert(request_id.to_string(), Arc::clone(&flag));
        Ok(CancellationHandle {
            inner: Arc::new(CancellationHandleInner {
                id: request_id.to_string(),
                registry: Arc::clone(self),
                flag,
            }),
        })
    }

    fn cancel(&self, request_id: &str) -> bool {
        match self.tokens.lock() {
            Ok(mut tokens) => {
                if let Some(flag) = tokens.remove(request_id) {
                    flag.store(true, AtomicOrdering::SeqCst);
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }

    fn unregister(&self, id: &str, flag: &Arc<AtomicBool>) {
        if let Ok(mut tokens) = self.tokens.lock() {
            if let Some(existing) = tokens.get(id) {
                if Arc::ptr_eq(existing, flag) {
                    tokens.remove(id);
                }
            }
        }
    }
}

#[derive(Clone)]
struct CancellationHandle {
    inner: Arc<CancellationHandleInner>,
}

impl CancellationHandle {
    fn token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.inner.flag)
    }
}

struct CancellationHandleInner {
    id: String,
    registry: Arc<CancellationRegistry>,
    flag: Arc<AtomicBool>,
}

impl Drop for CancellationHandleInner {
    fn drop(&mut self) {
        self.registry.unregister(&self.id, &self.flag);
    }
}

/// Shared database handle used by language bindings (Node.js, Python, etc.).
///
/// This is the main entry point for FFI clients to interact with the Sombra database.
/// It provides methods to execute queries, perform mutations, and manage data creation
/// through a fluent builder pattern.
pub struct Database {
    pager: Arc<Pager>,
    graph: Arc<Graph>,
    dict: Arc<Dict>,
    metadata: Arc<dyn MetadataProvider>,
    planner: Planner,
    executor: Executor,
    cancellations: Arc<CancellationRegistry>,
}

impl Database {
    /// Opens or creates a database at the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database directory
    /// * `opts` - Configuration options (see [`DatabaseOptions`])
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or created.
    pub fn open(path: impl AsRef<Path>, opts: DatabaseOptions) -> Result<Self> {
        let path = path.as_ref();
        let should_create = opts.create_if_missing && !path.exists();
        if should_create {
            ensure_parent_dir(path)?;
        }
        let pager = if should_create {
            Arc::new(Pager::create(path, opts.pager.clone())?)
        } else {
            Arc::new(Pager::open(path, opts.pager.clone())?)
        };

        let store: Arc<dyn PageStore> = pager.clone();
        let mut graph_opts = GraphOptions::new(Arc::clone(&store));
        graph_opts = graph_opts.distinct_neighbors_default(opts.distinct_neighbors_default);
        let graph = Arc::new(Graph::open(graph_opts)?);

        let dict = Arc::new(Dict::open(Arc::clone(&store), DictOptions::default())?);
        let catalog_root = graph.index_catalog_root();
        let metadata: Arc<dyn MetadataProvider> = Arc::new(CatalogMetadata::from_parts(
            Arc::clone(&dict),
            Arc::clone(&store),
            catalog_root,
            Arc::clone(&graph),
        )?);
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata));
        let executor = Executor::new(
            Arc::clone(&graph),
            Arc::clone(&pager),
            Arc::clone(&metadata),
        );
        let cancellations = Arc::new(CancellationRegistry::new());

        Ok(Self {
            pager,
            graph,
            dict,
            metadata,
            planner,
            executor,
            cancellations,
        })
    }

    /// Executes a JSON-serialized query specification and returns all results.
    ///
    /// Deserializes the JSON query specification and executes it against the database.
    pub fn execute_json(&self, spec: &Value) -> Result<Value> {
        enforce_payload_size(spec)?;
        let spec: QuerySpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.execute(spec)
    }

    /// Explains a JSON-serialized query without executing it.
    ///
    /// Returns the query execution plan for inspection and optimization.
    pub fn explain_json(&self, spec: &Value) -> Result<Value> {
        enforce_payload_size(spec)?;
        let spec: ExplainSpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.explain_with_options(spec.query, spec.redact_literals)
    }

    /// Creates a streaming query from a JSON specification.
    ///
    /// Returns an iterator-like [`QueryStream`] for processing large result sets.
    pub fn stream_json(&self, spec: &Value) -> Result<QueryStream> {
        enforce_payload_size(spec)?;
        let spec: QuerySpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.stream(spec)
    }

    /// Samples label IDs from the first `node_limit` nodes and returns the top `max_labels` names.
    pub fn sample_labels(
        &self,
        node_limit: usize,
        max_labels: usize,
    ) -> Result<Vec<(String, u64)>> {
        if node_limit == 0 || max_labels == 0 {
            return Ok(Vec::new());
        }
        let read = self.pager.begin_read()?;
        let samples = self.graph.sample_node_labels(&read, node_limit)?;
        drop(read);

        let mut counts: HashMap<u32, u64> = HashMap::new();
        for label_list in samples {
            for label in label_list {
                *counts.entry(label.0).or_insert(0) += 1;
            }
        }

        let mut entries: Vec<(u32, u64)> = counts.into_iter().collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let mut results = Vec::new();
        for (label_id, count) in entries.into_iter().take(max_labels) {
            let name = match self.dict.resolve_str(StrId(label_id)) {
                Ok(value) => value,
                Err(_) => format!("LABEL#{label_id}"),
            };
            results.push((name, count));
        }
        Ok(results)
    }

    /// Ensures label indexes exist for the provided label names. Returns how many were created.
    pub fn ensure_label_indexes(&self, labels: &[String]) -> Result<usize> {
        if labels.is_empty() {
            return Ok(0);
        }
        let mut unique = HashSet::new();
        for label in labels {
            let trimmed = label.trim();
            if !trimmed.is_empty() {
                unique.insert(trimmed.to_string());
            }
        }
        if unique.is_empty() {
            return Ok(0);
        }

        let mut to_create = Vec::new();
        for name in unique {
            let label_id = self
                .metadata
                .resolve_label(&name)
                .map_err(|_| FfiError::Message(format!("unknown label '{name}'")))?;
            if !self.graph.has_label_index(label_id)? {
                to_create.push(label_id);
            }
        }
        if to_create.is_empty() {
            return Ok(0);
        }

        let mut write = self.pager.begin_write()?;
        for label_id in &to_create {
            self.graph.create_label_index(&mut write, *label_id)?;
        }
        self.pager.commit(write)?;
        Ok(to_create.len())
    }

    /// Applies a JSON mutation specification (create, update, delete operations).
    pub fn mutate_json(&self, spec: &Value) -> Result<Value> {
        let spec: MutationSpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid mutation spec: {err}")))?;
        let summary = self.mutate(spec)?;
        serde_json::to_value(summary)
            .map_err(|err| FfiError::Message(format!("failed to encode mutation result: {err}")))
    }

    /// Applies a JSON create script (nodes and edges with optional aliases).
    pub fn create_json(&self, spec: &Value) -> Result<Value> {
        let script: CreateScript = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid create spec: {err}")))?;
        let result = self.create_script(script)?;
        let summary = CreateSummary::from(result);
        serde_json::to_value(summary)
            .map_err(|err| FfiError::Message(format!("failed to encode create result: {err}")))
    }

    /// Handles database pragmas (configuration settings).
    ///
    /// Supported pragmas:
    /// - `synchronous`: Set write synchronization mode (full, normal, off)
    /// - `wal_coalesce_ms`: Set WAL coalescing interval
    /// - `autocheckpoint_ms`: Set automatic checkpoint interval
    pub fn pragma(&self, name: &str, value: Option<Value>) -> Result<Value> {
        match name.to_ascii_lowercase().as_str() {
            "synchronous" => self.handle_synchronous_pragma(value),
            "wal_coalesce_ms" => self.handle_wal_coalesce_pragma(value),
            "autocheckpoint_ms" => self.handle_autocheckpoint_ms_pragma(value),
            other => Err(FfiError::Message(format!("unknown pragma '{other}'"))),
        }
    }

    /// Executes a query specification and returns all results.
    pub fn execute(&self, spec: QuerySpec) -> Result<Value> {
        let plan_timer = profile_timer();
        let plan = self.plan(spec)?;
        record_profile_timer(ProfileKind::Plan, plan_timer);
        let guard = self.register_cancellation(plan.request_id.as_deref())?;
        let cancel_token = guard.as_ref().map(|handle| handle.token());
        let exec_timer = profile_timer();
        let result = self.executor.execute(&plan.plan, cancel_token)?;
        record_profile_timer(ProfileKind::Execute, exec_timer);
        let serde_timer = profile_timer();
        let rows = rows_to_values(&result)?;
        record_profile_timer(ProfileKind::Serialize, serde_timer);
        Ok(execution_payload(plan.request_id.clone(), rows))
    }

    /// Returns the query execution plan for a specification.
    pub fn explain(&self, spec: QuerySpec) -> Result<Value> {
        self.explain_with_options(spec, false)
    }

    fn explain_with_options(&self, spec: QuerySpec, redact_literals: bool) -> Result<Value> {
        let plan = self.plan(spec)?;
        Ok(explain_payload(
            plan.request_id.clone(),
            plan.plan_hash,
            &plan.explain,
            redact_literals,
        ))
    }

    /// Creates a streaming query result.
    pub fn stream(&self, spec: QuerySpec) -> Result<QueryStream> {
        let plan = self.plan(spec)?;
        let guard = self.register_cancellation(plan.request_id.as_deref())?;
        let token = guard.as_ref().map(|h| h.token());
        let stream = self.executor.stream(&plan.plan, token)?;
        Ok(QueryStream::new(stream, guard))
    }

    /// Issues a best-effort cancellation signal for a running query.
    pub fn cancel_request(&self, request_id: &str) -> bool {
        self.cancellations.cancel(request_id)
    }

    /// Interns a string in the dictionary and returns its ID.
    pub fn intern(&self, name: &str) -> Result<u32> {
        let mut write = self.pager.begin_write()?;
        let id = self.dict.intern(&mut write, name)?;
        self.pager.commit(write)?;
        Ok(id.0)
    }

    /// Seeds the database with demo data (Users and FOLLOWS edges).
    pub fn seed_demo(&self) -> Result<()> {
        let mut write = self.pager.begin_write()?;
        let label_user = LabelId(self.dict.intern(&mut write, "User")?.0);
        let prop_name = PropId(self.dict.intern(&mut write, "name")?.0);
        let type_follows = TypeId(self.dict.intern(&mut write, "FOLLOWS")?.0);

        if !self.graph.has_label_index(label_user)? {
            self.graph.create_label_index(&mut write, label_user)?;
        }
        if !self.graph.has_property_index(label_user, prop_name)? {
            let def = IndexDef {
                label: label_user,
                prop: prop_name,
                kind: IndexKind::Chunked,
                ty: TypeTag::String,
            };
            self.graph.create_property_index(&mut write, def)?;
        }

        let ada = self.graph.create_node(
            &mut write,
            StorageNodeSpec {
                labels: &[label_user],
                props: &[PropEntry::new(prop_name, PropValue::Str("Ada"))],
            },
        )?;
        let grace = self.graph.create_node(
            &mut write,
            StorageNodeSpec {
                labels: &[label_user],
                props: &[PropEntry::new(prop_name, PropValue::Str("Grace"))],
            },
        )?;
        let alan = self.graph.create_node(
            &mut write,
            StorageNodeSpec {
                labels: &[label_user],
                props: &[PropEntry::new(prop_name, PropValue::Str("Alan"))],
            },
        )?;

        self.graph.create_edge(
            &mut write,
            StorageEdgeSpec {
                src: ada,
                dst: grace,
                ty: type_follows,
                props: &[],
            },
        )?;
        self.graph.create_edge(
            &mut write,
            StorageEdgeSpec {
                src: grace,
                dst: ada,
                ty: type_follows,
                props: &[],
            },
        )?;
        self.graph.create_edge(
            &mut write,
            StorageEdgeSpec {
                src: ada,
                dst: alan,
                ty: type_follows,
                props: &[],
            },
        )?;

        self.pager.commit(write)?;
        Ok(())
    }

    /// Starts a fluent builder for creating nodes and edges transactionally.
    pub fn create(&self) -> CreateBuilder<'_> {
        CreateBuilder::new(self)
    }

    /// Applies a JSON-friendly create script by reusing the fluent builder.
    pub fn create_script(&self, script: CreateScript) -> Result<CreateResult> {
        let mut builder = self.create();
        let mut handles = Vec::with_capacity(script.nodes.len());
        for node in script.nodes {
            let handle = if let Some(alias) = node.alias {
                builder.node_with_alias(node.labels, node.props, alias)?
            } else {
                builder.node(node.labels, node.props)
            };
            handles.push(handle);
        }
        for edge in script.edges {
            let CreateEdgeSpec {
                src,
                ty,
                dst,
                props,
            } = edge;
            let src_ref = src.into_node_ref(&handles)?;
            let dst_ref = dst.into_node_ref(&handles)?;
            builder.edge(src_ref, ty, dst_ref, props)?;
        }
        builder.execute()
    }

    fn handle_synchronous_pragma(&self, value: Option<Value>) -> Result<Value> {
        if let Some(val) = value {
            let mode = parse_synchronous_value(&val)?;
            self.pager.set_synchronous(mode);
        }
        let current = self.pager.synchronous();
        Ok(Value::String(current.as_str().to_string()))
    }

    fn handle_wal_coalesce_pragma(&self, value: Option<Value>) -> Result<Value> {
        if let Some(val) = value {
            let ms = parse_u64(&val, "wal_coalesce_ms")?;
            self.pager.set_wal_coalesce_ms(ms);
        }
        let current = self.pager.wal_coalesce_ms();
        Ok(Value::Number(Number::from(current)))
    }

    fn handle_autocheckpoint_ms_pragma(&self, value: Option<Value>) -> Result<Value> {
        if let Some(val) = value {
            let ms = parse_optional_u64(&val, "autocheckpoint_ms")?;
            self.pager.set_autocheckpoint_ms(ms);
        }
        match self.pager.autocheckpoint_ms() {
            Some(ms) => Ok(Value::Number(Number::from(ms))),
            None => Ok(Value::Null),
        }
    }

    /// Applies a mutation specification (create, update, delete operations).
    pub fn mutate(&self, spec: MutationSpec) -> Result<MutationSummary> {
        let mut write = self.pager.begin_write()?;
        let mut summary = MutationSummary::default();
        for op in spec.ops {
            self.apply_mutation_op(&mut write, &mut summary, op)?;
        }
        self.pager.commit(write)?;
        Ok(summary)
    }

    fn apply_mutation_op(
        &self,
        write: &mut WriteGuard<'_>,
        summary: &mut MutationSummary,
        op: MutationOp,
    ) -> Result<()> {
        match op {
            MutationOp::CreateNode { labels, props } => {
                let label_ids = self.resolve_labels(write, &labels)?;
                for label in &label_ids {
                    self.ensure_label_index(write, *label)?;
                }
                let mut prop_storage: Vec<(PropId, PropValueOwned)> =
                    Vec::with_capacity(props.len());
                for (name, value) in props {
                    let prop = self.resolve_prop(write, &name)?;
                    let owned = value_to_prop_value(&value)?;
                    prop_storage.push((prop, owned));
                }
                let mut prop_entries = Vec::with_capacity(prop_storage.len());
                for (prop, owned) in &prop_storage {
                    prop_entries.push(PropEntry::new(*prop, prop_value_ref(owned)));
                }
                let node_id = self.graph.create_node(
                    write,
                    StorageNodeSpec {
                        labels: &label_ids,
                        props: &prop_entries,
                    },
                )?;
                summary.created_nodes.push(node_id.0);
                drop(prop_entries);
                drop(prop_storage);
                Ok(())
            }
            MutationOp::UpdateNode { id, set, unset } => {
                let mut storage: Vec<(PropId, PropValueOwned)> = Vec::with_capacity(set.len());
                for (name, value) in set {
                    let prop = self.resolve_prop(write, &name)?;
                    let owned = value_to_prop_value(&value)?;
                    storage.push((prop, owned));
                }
                let mut ops: Vec<PropPatchOp> = Vec::with_capacity(storage.len() + unset.len());
                for (prop, owned) in &storage {
                    ops.push(PropPatchOp::Set(*prop, prop_value_ref(owned)));
                }
                for name in unset {
                    let prop = self.resolve_prop(write, &name)?;
                    ops.push(PropPatchOp::Delete(prop));
                }
                self.graph
                    .update_node(write, NodeId(id), PropPatch::new(ops))?;
                summary.updated_nodes += 1;
                Ok(())
            }
            MutationOp::DeleteNode { id, cascade } => {
                let opts = if cascade {
                    DeleteNodeOpts::cascade()
                } else {
                    DeleteNodeOpts::restrict()
                };
                self.graph.delete_node(write, NodeId(id), opts)?;
                summary.deleted_nodes += 1;
                Ok(())
            }
            MutationOp::CreateEdge {
                src,
                dst,
                ty,
                props,
            } => {
                let ty_id = self.resolve_type(write, &ty)?;
                let mut prop_storage: Vec<(PropId, PropValueOwned)> =
                    Vec::with_capacity(props.len());
                for (name, value) in props {
                    let prop = self.resolve_prop(write, &name)?;
                    let owned = value_to_prop_value(&value)?;
                    prop_storage.push((prop, owned));
                }
                let mut prop_entries = Vec::with_capacity(prop_storage.len());
                for (prop, owned) in &prop_storage {
                    prop_entries.push(PropEntry::new(*prop, prop_value_ref(owned)));
                }
                let edge_id = self.graph.create_edge(
                    write,
                    StorageEdgeSpec {
                        src: NodeId(src),
                        dst: NodeId(dst),
                        ty: ty_id,
                        props: &prop_entries,
                    },
                )?;
                summary.created_edges.push(edge_id.0);
                drop(prop_entries);
                drop(prop_storage);
                Ok(())
            }
            MutationOp::UpdateEdge { id, set, unset } => {
                let mut storage: Vec<(PropId, PropValueOwned)> = Vec::with_capacity(set.len());
                for (name, value) in set {
                    let prop = self.resolve_prop(write, &name)?;
                    let owned = value_to_prop_value(&value)?;
                    storage.push((prop, owned));
                }
                let mut ops: Vec<PropPatchOp> = Vec::with_capacity(storage.len() + unset.len());
                for (prop, owned) in &storage {
                    ops.push(PropPatchOp::Set(*prop, prop_value_ref(owned)));
                }
                for name in unset {
                    let prop = self.resolve_prop(write, &name)?;
                    ops.push(PropPatchOp::Delete(prop));
                }
                self.graph
                    .update_edge(write, EdgeId(id), PropPatch::new(ops))?;
                summary.updated_edges += 1;
                Ok(())
            }
            MutationOp::DeleteEdge { id } => {
                self.graph.delete_edge(write, EdgeId(id))?;
                summary.deleted_edges += 1;
                Ok(())
            }
        }
    }

    fn resolve_labels(
        &self,
        write: &mut WriteGuard<'_>,
        labels: &[String],
    ) -> Result<Vec<LabelId>> {
        let mut ids = Vec::with_capacity(labels.len());
        for label in labels {
            ids.push(self.resolve_label(write, label)?);
        }
        Ok(ids)
    }

    fn resolve_label(&self, write: &mut WriteGuard<'_>, name: &str) -> Result<LabelId> {
        let id = self.dict.intern(write, name)?;
        Ok(LabelId(id.0))
    }

    fn resolve_prop(&self, write: &mut WriteGuard<'_>, name: &str) -> Result<PropId> {
        let id = self.dict.intern(write, name)?;
        Ok(PropId(id.0))
    }

    fn resolve_type(&self, write: &mut WriteGuard<'_>, name: &str) -> Result<TypeId> {
        let id = self.dict.intern(write, name)?;
        Ok(TypeId(id.0))
    }

    fn ensure_label_index(&self, write: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if self.graph.has_label_index(label)? {
            return Ok(());
        }
        self.graph
            .create_label_index(write, label)
            .map_err(FfiError::from)
    }

    fn plan(&self, spec: QuerySpec) -> Result<PlannerOutput> {
        let ast = spec.into_ast()?;
        let analyzed = analyze::analyze(&ast, self.metadata.as_ref())?;
        self.planner
            .plan_analyzed(&analyzed)
            .map_err(FfiError::from)
    }

    fn register_cancellation(
        &self,
        request_id: Option<&str>,
    ) -> Result<Option<CancellationHandle>> {
        match request_id {
            Some(id) => Ok(Some(self.cancellations.register(id)?)),
            None => Ok(None),
        }
    }
}

struct StreamInner {
    stream: Mutex<ResultStream>,
    _guard: Option<CancellationHandle>,
}

/// A streaming query result that can be consumed incrementally.
///
/// This allows processing large result sets without loading everything into memory.
pub struct QueryStream {
    inner: Arc<StreamInner>,
}

impl QueryStream {
    fn new(stream: ResultStream, guard: Option<CancellationHandle>) -> Self {
        Self {
            inner: Arc::new(StreamInner {
                stream: Mutex::new(stream),
                _guard: guard,
            }),
        }
    }

    /// Fetches the next result row from the stream.
    ///
    /// Returns `Ok(Some(value))` for each row, `Ok(None)` when exhausted,
    /// or an error if something goes wrong.
    pub fn next(&self) -> Result<Option<Value>> {
        let mut guard = self
            .inner
            .stream
            .lock()
            .map_err(|_| FfiError::Message("stream poisoned".into()))?;
        match guard.next() {
            Some(Ok(row)) => Ok(Some(row_to_value(&row)?)),
            Some(Err(err)) => Err(err.into()),
            None => Ok(None),
        }
    }
}

impl Clone for QueryStream {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

fn parse_synchronous_value(value: &Value) -> Result<Synchronous> {
    let Some(raw) = value.as_str() else {
        return Err(FfiError::Message(
            "PRAGMA synchronous expects a string value".into(),
        ));
    };
    Synchronous::from_str(raw).ok_or_else(|| {
        FfiError::Message(format!(
            "invalid synchronous mode '{raw}', expected 'full', 'normal', or 'off'"
        ))
    })
}

fn parse_u64(value: &Value, field: &str) -> Result<u64> {
    match value {
        Value::Number(num) => num.as_u64().ok_or_else(|| {
            FfiError::Message(format!("PRAGMA {field} requires a non-negative integer"))
        }),
        Value::String(s) => s.parse::<u64>().map_err(|_| {
            FfiError::Message(format!("PRAGMA {field} requires a non-negative integer"))
        }),
        _ => Err(FfiError::Message(format!(
            "PRAGMA {field} requires a numeric value"
        ))),
    }
}

fn parse_optional_u64(value: &Value, field: &str) -> Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_u64(value, field)?))
}

fn enforce_payload_size(spec: &Value) -> Result<()> {
    let serialized = serde_json::to_vec(spec)
        .map_err(|err| FfiError::Message(format!("failed to measure payload size: {err}")))?;
    if serialized.len() > MAX_PAYLOAD_BYTES {
        Err(FfiError::Message(format!(
            "payload exceeds {} bytes",
            MAX_PAYLOAD_BYTES
        )))
    } else {
        Ok(())
    }
}

/// JSON-deserializable query specification for FFI clients.
///
/// Defines match clauses, edges, predicates, and projections for graph queries.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySpec {
    /// Schema version for the query payload.
    #[serde(rename = "$schemaVersion")]
    pub schema_version: Option<u32>,
    /// Optional client-supplied request identifier.
    #[serde(default, alias = "request_id")]
    pub request_id: Option<String>,
    /// MATCH clauses for node patterns.
    #[serde(default)]
    pub matches: Vec<MatchSpec>,
    /// Edge traversal specifications.
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    /// Canonical boolean predicate tree.
    #[serde(default)]
    pub predicate: Option<PredicateSpec>,
    /// Column projections for result output.
    #[serde(default)]
    pub projections: Vec<ProjectionSpec>,
    /// Whether to return distinct results only.
    #[serde(default)]
    pub distinct: bool,
}

/// Explain-specific options layered on top of [`QuerySpec`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExplainSpec {
    /// Core query specification.
    #[serde(flatten)]
    pub query: QuerySpec,
    /// Whether to redact literal values in explain output.
    #[serde(default, rename = "redact_literals", alias = "redactLiterals")]
    pub redact_literals: bool,
}

#[allow(dead_code)]
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

impl QuerySpec {
    fn into_ast(self) -> Result<QueryAst> {
        let schema_version = self
            .schema_version
            .map(SchemaVersionState::Value)
            .unwrap_or(SchemaVersionState::Missing);
        let schema_version = match schema_version {
            SchemaVersionState::Value(version) if version == 1 => version,
            state => {
                return Err(AnalyzerError::UnsupportedSchemaVersion {
                    found: state,
                    supported: 1,
                }
                .into())
            }
        };

        if self.matches.is_empty() {
            return Err(FfiError::Message(
                "query requires at least one match".into(),
            ));
        }

        let matches = self
            .matches
            .into_iter()
            .map(MatchSpec::into_clause)
            .collect::<Result<Vec<_>>>()?;
        let edges = self
            .edges
            .into_iter()
            .map(EdgeSpec::into_clause)
            .collect::<Result<Vec<_>>>()?;
        let predicate = self
            .predicate
            .map(|spec| spec.into_expr())
            .transpose()?
            .and_then(normalized_predicate);
        let projections = self
            .projections
            .into_iter()
            .map(ProjectionSpec::into_projection)
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryAst {
            schema_version,
            request_id: self.request_id,
            matches,
            edges,
            predicate,
            distinct: self.distinct,
            projections,
        })
    }
}

/// Specification for a MATCH clause in a query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchSpec {
    /// Variable name to bind matched nodes.
    pub var: String,
    /// Optional label to filter matched nodes.
    #[serde(default)]
    pub label: Option<String>,
}

impl MatchSpec {
    fn into_clause(self) -> Result<MatchClause> {
        if self.var.is_empty() {
            return Err(FfiError::Message("match variable cannot be empty".into()));
        }
        Ok(MatchClause {
            var: Var(self.var),
            label: self.label,
        })
    }
}

/// Specification for an edge traversal in a query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeSpec {
    /// Source variable name.
    pub from: String,
    /// Destination variable name.
    pub to: String,
    /// Optional edge type to filter traversals.
    #[serde(default)]
    pub edge_type: Option<String>,
    /// Direction of edge traversal (defaults to outgoing).
    #[serde(default = "DirectionSpec::default_out")]
    pub direction: DirectionSpec,
}

impl EdgeSpec {
    fn into_clause(self) -> Result<EdgeClause> {
        if self.from.is_empty() || self.to.is_empty() {
            return Err(FfiError::Message(
                "edge requires source and destination vars".into(),
            ));
        }
        Ok(EdgeClause {
            from: Var(self.from),
            to: Var(self.to),
            edge_type: self.edge_type,
            direction: self.direction.into_direction()?,
        })
    }
}

/// Edge traversal direction specification.
#[derive(Debug, Clone)]
pub struct DirectionSpec(String);

impl DirectionSpec {
    fn default_out() -> Self {
        Self("out".into())
    }

    /// Convenience constructor for outgoing traversals.
    pub fn out() -> Self {
        Self("out".into())
    }

    /// Convenience constructor for incoming traversals.
    pub fn r#in() -> Self {
        Self("in".into())
    }

    /// Convenience constructor for bidirectional traversals.
    pub fn both() -> Self {
        Self("both".into())
    }

    fn into_direction(self) -> Result<EdgeDirection> {
        match self.0.as_str() {
            "out" => Ok(EdgeDirection::Out),
            "in" => Ok(EdgeDirection::In),
            "both" => Ok(EdgeDirection::Both),
            other => Err(AnalyzerError::DirectionInvalid {
                direction: other.to_string(),
            }
            .into()),
        }
    }
}

impl<'de> Deserialize<'de> for DirectionSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(DirectionSpec(value))
    }
}

/// Literal value emitted by bindings before semantic validation.
#[derive(Debug, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum PayloadValue {
    /// Null literal.
    Null,
    /// Boolean literal.
    Bool(bool),
    /// Signed 64-bit integer literal.
    Int(i64),
    /// 64-bit floating point literal.
    Float(f64),
    /// UTF-8 string literal.
    String(String),
    /// Base64-encoded bytes literal (decoded later).
    Bytes(String),
    /// Nanoseconds since Unix epoch (UTC).
    DateTime(i128),
}

impl PayloadValue {
    fn into_value(self) -> Result<QueryValue> {
        Ok(match self {
            PayloadValue::Null => QueryValue::Null,
            PayloadValue::Bool(v) => QueryValue::Bool(v),
            PayloadValue::Int(v) => QueryValue::Int(v),
            PayloadValue::Float(v) => QueryValue::Float(v),
            PayloadValue::String(v) => QueryValue::String(v),
            PayloadValue::Bytes(raw) => {
                let decoded = BASE64
                    .decode(raw.as_bytes())
                    .map_err(|_| AnalyzerError::BytesEncoding)?;
                QueryValue::Bytes(decoded)
            }
            PayloadValue::DateTime(v) => QueryValue::DateTime(v),
        })
    }
}

/// Boolean predicate specification emitted by bindings.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum PredicateSpec {
    /// Logical conjunction.
    #[serde(rename = "and")]
    And {
        /// Child expressions.
        args: Vec<PredicateSpec>,
    },
    /// Logical disjunction.
    #[serde(rename = "or")]
    Or {
        /// Child expressions.
        args: Vec<PredicateSpec>,
    },
    /// Logical negation.
    #[serde(rename = "not")]
    Not {
        /// Child expressions (must contain exactly one entry).
        args: Vec<PredicateSpec>,
    },
    /// Equality comparison.
    #[serde(rename = "eq")]
    Eq {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Inequality comparison.
    #[serde(rename = "ne")]
    Ne {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Less-than comparison.
    #[serde(rename = "lt")]
    Lt {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Less-than-or-equal comparison.
    #[serde(rename = "le")]
    Le {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Greater-than comparison.
    #[serde(rename = "gt")]
    Gt {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Greater-than-or-equal comparison.
    #[serde(rename = "ge")]
    Ge {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal value to compare against.
        value: PayloadValue,
    },
    /// Between comparison with optional bound inclusivity.
    #[serde(rename = "between")]
    Between {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Lower bound literal.
        low: PayloadValue,
        /// Upper bound literal.
        high: PayloadValue,
        #[serde(default)]
        /// Inclusive flags for the lower/upper bounds.
        inclusive: Option<[bool; 2]>,
    },
    /// Membership comparison.
    #[serde(rename = "in")]
    In {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being compared.
        prop: String,
        /// Literal set to test membership against.
        values: Vec<PayloadValue>,
    },
    /// Property existence test.
    #[serde(rename = "exists")]
    Exists {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being inspected.
        prop: String,
    },
    /// Property is null or missing.
    #[serde(rename = "isnull")]
    #[serde(alias = "isNull")]
    IsNull {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being inspected.
        prop: String,
    },
    /// Property is non-null.
    #[serde(rename = "isnotnull")]
    #[serde(alias = "isNotNull")]
    IsNotNull {
        /// Variable binding referenced by the predicate.
        var: String,
        /// Property name being inspected.
        prop: String,
    },
}

fn validate_scalar_value(value: &QueryValue) -> Result<()> {
    match value {
        QueryValue::Float(v) if !v.is_finite() => {
            Err(FfiError::Message("float literal must be finite".into()))
        }
        QueryValue::Bytes(bytes) if bytes.len() > MAX_BYTES_LITERAL => Err(FfiError::Message(
            format!("binary literal exceeds {} bytes", MAX_BYTES_LITERAL),
        )),
        QueryValue::DateTime(ts) if *ts < i64::MIN as i128 || *ts > i64::MAX as i128 => Err(
            FfiError::Message("datetime literal must fit within 64-bit range".into()),
        ),
        _ => Ok(()),
    }
}

fn ensure_orderable(value: &QueryValue, ctx: &str) -> Result<()> {
    match value {
        QueryValue::Int(_)
        | QueryValue::Float(_)
        | QueryValue::String(_)
        | QueryValue::DateTime(_) => Ok(()),
        QueryValue::Bytes(_) => Err(FfiError::Message(format!(
            "bytes literals are only supported with eq()/ne(), not {ctx}"
        ))),
        QueryValue::Null => Err(FfiError::Message(format!(
            "{ctx} does not accept null literals"
        ))),
        QueryValue::Bool(_) => Err(FfiError::Message(format!(
            "{ctx} requires a numeric, datetime, or string literal"
        ))),
    }
}

fn validate_in_values(values: &[QueryValue]) -> Result<()> {
    if values.is_empty() {
        return Err(FfiError::Message("in() requires at least one value".into()));
    }
    if values.len() > MAX_IN_VALUES {
        return Err(FfiError::Message(format!(
            "in() may not exceed {} values",
            MAX_IN_VALUES
        )));
    }
    let mut tag = None;
    let mut total_bytes = 0usize;
    for value in values {
        validate_scalar_value(value)?;
        if let QueryValue::Bytes(bytes) = value {
            total_bytes = total_bytes.checked_add(bytes.len()).ok_or_else(|| {
                FfiError::Message(format!(
                    "total bytes in in() literals exceeds {} bytes",
                    MAX_BYTES_LITERAL
                ))
            })?;
            if total_bytes > MAX_BYTES_LITERAL {
                return Err(FfiError::Message(format!(
                    "total bytes in in() literals exceeds {} bytes",
                    MAX_BYTES_LITERAL
                )));
            }
        }
        if matches!(value, QueryValue::Null) {
            continue;
        }
        let disc = mem::discriminant(value);
        if let Some(existing) = tag {
            if existing != disc {
                return Err(FfiError::Message(
                    "in() requires all values to share the same type".into(),
                ));
            }
        } else {
            tag = Some(disc);
        }
    }
    Ok(())
}

fn validate_between_literals(low: &QueryValue, high: &QueryValue) -> Result<()> {
    validate_scalar_value(low)?;
    validate_scalar_value(high)?;
    ensure_orderable(low, "between()")?;
    ensure_orderable(high, "between()")?;
    ensure_bounds_order(low, high)?;
    Ok(())
}

fn ensure_bounds_order(low: &QueryValue, high: &QueryValue) -> Result<()> {
    if mem::discriminant(low) != mem::discriminant(high) {
        return Err(FfiError::Message(
            "between() bounds must share the same literal type".into(),
        ));
    }
    let ordering = match (low, high) {
        (QueryValue::Int(a), QueryValue::Int(b)) => a.cmp(b),
        (QueryValue::Float(a), QueryValue::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| FfiError::Message("between() bounds are not comparable".into()))?,
        (QueryValue::String(a), QueryValue::String(b)) => a.cmp(b),
        (QueryValue::DateTime(a), QueryValue::DateTime(b)) => a.cmp(b),
        _ => Ordering::Equal,
    };
    if ordering == Ordering::Greater {
        return Err(FfiError::Message(
            "between() lower bound must be <= upper bound".into(),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
fn bound_value(bound: &Bound<QueryValue>) -> Option<&QueryValue> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}

#[allow(dead_code)]
fn validate_range_bounds(
    lower: &Bound<QueryValue>,
    upper: &Bound<QueryValue>,
    ctx: &str,
) -> Result<()> {
    if let Some(value) = bound_value(lower) {
        validate_scalar_value(value)?;
        ensure_orderable(value, ctx)?;
    }
    if let Some(value) = bound_value(upper) {
        validate_scalar_value(value)?;
        ensure_orderable(value, ctx)?;
    }
    if let (Some(low), Some(high)) = (bound_value(lower), bound_value(upper)) {
        ensure_bounds_order(low, high)?;
    }
    Ok(())
}

impl PredicateSpec {
    fn into_expr(self) -> Result<BoolExpr> {
        match self {
            PredicateSpec::And { args } => Ok(BoolExpr::And(into_expr_vec(args)?)),
            PredicateSpec::Or { args } => Ok(BoolExpr::Or(into_expr_vec(args)?)),
            PredicateSpec::Not { args } => {
                if args.len() != 1 {
                    return Err(FfiError::Message(
                        "not() requires exactly one argument".into(),
                    ));
                }
                let inner = args.into_iter().next().unwrap().into_expr()?;
                Ok(BoolExpr::Not(Box::new(inner)))
            }
            PredicateSpec::Eq { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                Ok(BoolExpr::Cmp(Comparison::Eq {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Ne { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                Ok(BoolExpr::Cmp(Comparison::Ne {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Lt { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                ensure_orderable(&value, "lt()")?;
                Ok(BoolExpr::Cmp(Comparison::Lt {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Le { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                ensure_orderable(&value, "le()")?;
                Ok(BoolExpr::Cmp(Comparison::Le {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Gt { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                ensure_orderable(&value, "gt()")?;
                Ok(BoolExpr::Cmp(Comparison::Gt {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Ge { var, prop, value } => {
                let value = value.into_value()?;
                validate_scalar_value(&value)?;
                ensure_orderable(&value, "ge()")?;
                Ok(BoolExpr::Cmp(Comparison::Ge {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    value,
                }))
            }
            PredicateSpec::Between {
                var,
                prop,
                low,
                high,
                inclusive,
            } => {
                let low = low.into_value()?;
                let high = high.into_value()?;
                validate_between_literals(&low, &high)?;
                let (low_bound, high_bound) = between_bounds(low, high, inclusive);
                Ok(BoolExpr::Cmp(Comparison::Between {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    low: low_bound,
                    high: high_bound,
                }))
            }
            PredicateSpec::In { var, prop, values } => {
                let mut literals = Vec::with_capacity(values.len());
                for value in values {
                    literals.push(value.into_value()?);
                }
                validate_in_values(&literals)?;
                Ok(BoolExpr::Cmp(Comparison::In {
                    var: into_var(var)?,
                    prop: into_prop(prop)?,
                    values: literals,
                }))
            }
            PredicateSpec::Exists { var, prop } => Ok(BoolExpr::Cmp(Comparison::Exists {
                var: into_var(var)?,
                prop: into_prop(prop)?,
            })),
            PredicateSpec::IsNull { var, prop } => Ok(BoolExpr::Cmp(Comparison::IsNull {
                var: into_var(var)?,
                prop: into_prop(prop)?,
            })),
            PredicateSpec::IsNotNull { var, prop } => Ok(BoolExpr::Cmp(Comparison::IsNotNull {
                var: into_var(var)?,
                prop: into_prop(prop)?,
            })),
        }
    }
}

fn into_expr_vec(args: Vec<PredicateSpec>) -> Result<Vec<BoolExpr>> {
    args.into_iter().map(|arg| arg.into_expr()).collect()
}

fn into_var(name: String) -> Result<Var> {
    if name.trim().is_empty() {
        Err(FfiError::Message("variable name cannot be empty".into()))
    } else {
        Ok(Var(name))
    }
}

fn into_prop(name: String) -> Result<String> {
    if name.trim().is_empty() {
        Err(FfiError::Message("property name cannot be empty".into()))
    } else {
        Ok(name)
    }
}

enum SimplifiedBoolExpr {
    True,
    False,
    Expr(BoolExpr),
}

fn normalized_predicate(expr: BoolExpr) -> Option<BoolExpr> {
    match simplify_bool_expr(expr) {
        SimplifiedBoolExpr::True => None,
        SimplifiedBoolExpr::False => Some(BoolExpr::Or(Vec::new())),
        SimplifiedBoolExpr::Expr(expr) => Some(expr),
    }
}

fn simplify_bool_expr(expr: BoolExpr) -> SimplifiedBoolExpr {
    match expr {
        BoolExpr::Cmp(_) => SimplifiedBoolExpr::Expr(expr),
        BoolExpr::Not(child) => match simplify_bool_expr(*child) {
            SimplifiedBoolExpr::True => SimplifiedBoolExpr::False,
            SimplifiedBoolExpr::False => SimplifiedBoolExpr::True,
            SimplifiedBoolExpr::Expr(expr) => match expr {
                BoolExpr::Not(inner) => simplify_bool_expr(*inner),
                other => SimplifiedBoolExpr::Expr(BoolExpr::Not(Box::new(other))),
            },
        },
        BoolExpr::And(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify_bool_expr(child) {
                    SimplifiedBoolExpr::True => {}
                    SimplifiedBoolExpr::False => return SimplifiedBoolExpr::False,
                    SimplifiedBoolExpr::Expr(expr) => match expr {
                        BoolExpr::And(grand_children) => flattened.extend(grand_children),
                        other => flattened.push(other),
                    },
                }
            }
            match flattened.len() {
                0 => SimplifiedBoolExpr::True,
                1 => SimplifiedBoolExpr::Expr(flattened.into_iter().next().unwrap()),
                _ => SimplifiedBoolExpr::Expr(BoolExpr::And(flattened)),
            }
        }
        BoolExpr::Or(children) => {
            let mut flattened = Vec::new();
            for child in children {
                match simplify_bool_expr(child) {
                    SimplifiedBoolExpr::False => {}
                    SimplifiedBoolExpr::True => return SimplifiedBoolExpr::True,
                    SimplifiedBoolExpr::Expr(expr) => match expr {
                        BoolExpr::Or(grand_children) => flattened.extend(grand_children),
                        other => flattened.push(other),
                    },
                }
            }
            match flattened.len() {
                0 => SimplifiedBoolExpr::False,
                1 => SimplifiedBoolExpr::Expr(flattened.into_iter().next().unwrap()),
                _ => SimplifiedBoolExpr::Expr(BoolExpr::Or(flattened)),
            }
        }
    }
}

fn between_bounds(
    low: QueryValue,
    high: QueryValue,
    inclusive: Option<[bool; 2]>,
) -> (Bound<QueryValue>, Bound<QueryValue>) {
    let flags = inclusive.unwrap_or([true, true]);
    let low_bound = if flags[0] {
        Bound::Included(low)
    } else {
        Bound::Excluded(low)
    };
    let high_bound = if flags[1] {
        Bound::Included(high)
    } else {
        Bound::Excluded(high)
    };
    (low_bound, high_bound)
}

/// Result column projection specification.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ProjectionSpec {
    /// Project a variable with optional alias.
    Var {
        /// Variable name to project.
        var: String,
        /// Optional column alias.
        #[serde(default)]
        alias: Option<String>,
    },
    /// Project a property from a bound variable.
    Prop {
        /// Variable name exposing the property.
        var: String,
        /// Property name to project.
        prop: String,
        /// Optional column alias.
        #[serde(default)]
        alias: Option<String>,
    },
}

impl ProjectionSpec {
    fn into_projection(self) -> Result<Projection> {
        match self {
            ProjectionSpec::Var { var, alias } => {
                if var.trim().is_empty() {
                    return Err(FfiError::Message(
                        "projection variable cannot be empty".into(),
                    ));
                }
                Ok(Projection::Var {
                    var: Var(var),
                    alias,
                })
            }
            ProjectionSpec::Prop { var, prop, alias } => {
                if var.trim().is_empty() {
                    return Err(FfiError::Message(
                        "property projection variable cannot be empty".into(),
                    ));
                }
                if prop.trim().is_empty() {
                    return Err(FfiError::Message(
                        "property projection name cannot be empty".into(),
                    ));
                }
                Ok(Projection::Prop {
                    var: Var(var),
                    prop,
                    alias,
                })
            }
        }
    }
}

/// Specification for mutation operations (create/update/delete).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationSpec {
    /// List of mutation operations to apply.
    #[serde(default)]
    pub ops: Vec<MutationOp>,
}

/// Individual mutation operation (create, update, or delete).
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum MutationOp {
    /// Create a new node with labels and properties.
    CreateNode {
        /// Node labels.
        labels: Vec<String>,
        /// Node properties.
        #[serde(default)]
        props: Map<String, Value>,
    },
    /// Update an existing node's properties.
    UpdateNode {
        /// Node ID to update.
        id: u64,
        /// Properties to set or update.
        #[serde(default)]
        set: Map<String, Value>,
        /// Property names to remove.
        #[serde(default)]
        unset: Vec<String>,
    },
    /// Delete an existing node.
    DeleteNode {
        /// Node ID to delete.
        id: u64,
        /// If true, cascade delete connected edges.
        #[serde(default)]
        cascade: bool,
    },
    /// Create a new edge between two nodes.
    CreateEdge {
        /// Source node ID.
        src: u64,
        /// Destination node ID.
        dst: u64,
        /// Edge type name.
        ty: String,
        /// Edge properties.
        #[serde(default)]
        props: Map<String, Value>,
    },
    /// Update an existing edge's properties.
    UpdateEdge {
        /// Edge ID to update.
        id: u64,
        /// Properties to set or update.
        #[serde(default)]
        set: Map<String, Value>,
        /// Property names to remove.
        #[serde(default)]
        unset: Vec<String>,
    },
    /// Delete an existing edge.
    DeleteEdge {
        /// Edge ID to delete.
        id: u64,
    },
}

/// Specification for batch node and edge creation with aliasing support.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateScript {
    /// Nodes to create.
    #[serde(default)]
    pub nodes: Vec<CreateNodeSpec>,
    /// Edges to create.
    #[serde(default)]
    pub edges: Vec<CreateEdgeSpec>,
}

/// Specification for creating a node in a create script.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateNodeSpec {
    /// Node labels.
    pub labels: Vec<String>,
    /// Node properties.
    #[serde(default)]
    pub props: Map<String, Value>,
    /// Optional alias for referencing in edges.
    #[serde(default)]
    pub alias: Option<String>,
}

/// Specification for creating an edge in a create script.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateEdgeSpec {
    /// Source node reference.
    pub src: CreateRefSpec,
    /// Edge type name.
    pub ty: String,
    /// Destination node reference.
    pub dst: CreateRefSpec,
    /// Edge properties.
    #[serde(default)]
    pub props: Map<String, Value>,
}

/// Reference to a node in a create script (by handle, alias, or existing ID).
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CreateRefSpec {
    /// Reference by node index in the creation array.
    Handle {
        /// Array index of the node.
        index: usize,
    },
    /// Reference by node alias.
    Alias {
        /// Node alias string.
        alias: String,
    },
    /// Reference to an existing node by ID.
    Id {
        /// Existing node ID.
        id: u64,
    },
}

/// Summary of applied mutations.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationSummary {
    /// IDs of created nodes.
    pub created_nodes: Vec<u64>,
    /// IDs of created edges.
    pub created_edges: Vec<u64>,
    /// Number of updated nodes.
    pub updated_nodes: u64,
    /// Number of updated edges.
    pub updated_edges: u64,
    /// Number of deleted nodes.
    pub deleted_nodes: u64,
    /// Number of deleted edges.
    pub deleted_edges: u64,
}

fn rows_to_values(result: &QueryResult) -> Result<Vec<Value>> {
    result
        .rows
        .iter()
        .map(row_to_value)
        .collect::<Result<Vec<_>>>()
}

fn execution_payload(request_id: Option<String>, rows: Vec<Value>) -> Value {
    let mut map = Map::new();
    map.insert(
        "request_id".into(),
        request_id.map(Value::String).unwrap_or(Value::Null),
    );
    map.insert("features".into(), Value::Array(Vec::new()));
    map.insert("rows".into(), Value::Array(rows));
    Value::Object(map)
}

fn row_to_value(row: &Row) -> Result<Value> {
    let mut map = Map::new();
    for (key, value) in row {
        map.insert(key.clone(), exec_value_to_json(value)?);
    }
    Ok(Value::Object(map))
}

fn exec_value_to_json(value: &ExecValue) -> Result<Value> {
    Ok(match value {
        ExecValue::Null => Value::Null,
        ExecValue::Bool(v) => Value::Bool(*v),
        ExecValue::Int(v) => Value::Number((*v).into()),
        ExecValue::Float(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .ok_or_else(|| FfiError::Message("float value not representable in JSON".into()))?,
        ExecValue::String(v) => Value::String(v.clone()),
        ExecValue::Bytes(bytes) => Value::String(BASE64.encode(bytes)),
        ExecValue::Date(v) => Value::Number((*v).into()),
        ExecValue::DateTime(v) => Value::Number((*v).into()),
        ExecValue::Object(map) => {
            let mut obj = Map::new();
            for (key, value) in map {
                obj.insert(key.clone(), exec_value_to_json(value)?);
            }
            Value::Object(obj)
        }
        ExecValue::NodeId(node) => Value::Number(node.0.into()),
    })
}

fn value_to_prop_value(value: &Value) -> Result<PropValueOwned> {
    match value {
        Value::Null => Ok(PropValueOwned::Null),
        Value::Bool(v) => Ok(PropValueOwned::Bool(*v)),
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                return Ok(PropValueOwned::Int(i));
            }
            if let Some(u) = num.as_u64() {
                if u <= i64::MAX as u64 {
                    return Ok(PropValueOwned::Int(u as i64));
                }
            }
            if let Some(f) = num.as_f64() {
                return Ok(PropValueOwned::Float(f));
            }
            Err(FfiError::Message("numeric literal out of range".into()))
        }
        Value::String(s) => Ok(PropValueOwned::Str(s.clone())),
        _ => Err(FfiError::Message(
            "only bool/int/float/string/null property literals are supported".into(),
        )),
    }
}

fn prop_value_ref(value: &PropValueOwned) -> PropValue<'_> {
    match value {
        PropValueOwned::Null => PropValue::Null,
        PropValueOwned::Bool(v) => PropValue::Bool(*v),
        PropValueOwned::Int(v) => PropValue::Int(*v),
        PropValueOwned::Float(v) => PropValue::Float(*v),
        PropValueOwned::Str(v) => PropValue::Str(v.as_str()),
        PropValueOwned::Bytes(v) => PropValue::Bytes(v.as_slice()),
        PropValueOwned::Date(v) => PropValue::Date(*v),
        PropValueOwned::DateTime(v) => PropValue::DateTime(*v),
    }
}

fn explain_payload(
    request_id: Option<String>,
    plan_hash: u64,
    explain: &PlanExplain,
    redact_literals: bool,
) -> Value {
    let mut root = Map::new();
    root.insert(
        "request_id".into(),
        request_id.map(Value::String).unwrap_or(Value::Null),
    );
    root.insert("features".into(), Value::Array(Vec::new()));
    root.insert(
        "plan_hash".into(),
        Value::String(format_plan_hash(plan_hash)),
    );
    root.insert(
        "plan".into(),
        Value::Array(vec![explain_node_to_value(&explain.root, redact_literals)]),
    );
    Value::Object(root)
}

fn explain_node_to_value(node: &ExplainNode, redact_literals: bool) -> Value {
    let mut map = Map::new();
    map.insert("op".into(), Value::String(node.op.clone()));
    if !node.props.is_empty() {
        let mut props = Map::new();
        for prop in &node.props {
            let value = if redact_literals && prop.redactable {
                Value::String("<redacted>".into())
            } else {
                Value::String(prop.value.clone())
            };
            props.insert(prop.key.clone(), value);
        }
        map.insert("props".into(), Value::Object(props));
    }
    let inputs = node
        .inputs
        .iter()
        .map(|child| explain_node_to_value(child, redact_literals))
        .collect::<Vec<_>>();
    map.insert("inputs".into(), Value::Array(inputs));
    Value::Object(map)
}

fn format_plan_hash(hash: u64) -> String {
    format!("0x{hash:016x}")
}

/// Result of a batch creation operation with node IDs, edge IDs, and aliases.
#[derive(Debug, Default, Clone)]
pub struct CreateResult {
    /// IDs of created nodes.
    pub node_ids: Vec<NodeId>,
    /// IDs of created edges.
    pub edge_ids: Vec<EdgeId>,
    /// Mapping of aliases to their corresponding node IDs.
    pub aliases: HashMap<String, NodeId>,
}

/// JSON-serializable summary of creation results for bindings.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSummary {
    /// IDs of created nodes.
    pub nodes: Vec<u64>,
    /// IDs of created edges.
    pub edges: Vec<u64>,
    /// Map of aliases to their node IDs.
    pub aliases: HashMap<String, u64>,
}

impl From<CreateResult> for CreateSummary {
    fn from(result: CreateResult) -> Self {
        Self {
            nodes: result.node_ids.iter().map(|id| id.0).collect(),
            edges: result.edge_ids.iter().map(|id| id.0).collect(),
            aliases: result
                .aliases
                .into_iter()
                .map(|(alias, id)| (alias, id.0))
                .collect(),
        }
    }
}

impl CreateResult {
    /// Returns node IDs as u64 values.
    pub fn node_ids_as_u64(&self) -> Vec<u64> {
        self.node_ids.iter().map(|id| id.0).collect()
    }

    /// Returns edge IDs as u64 values.
    pub fn edge_ids_as_u64(&self) -> Vec<u64> {
        self.edge_ids.iter().map(|id| id.0).collect()
    }
}

/// Fluent builder for staging nodes and edges, executing them transactionally.
///
/// Allows building complex graph structures with node aliasing for cross-references,
/// then executing all operations atomically within a single write transaction.
pub struct CreateBuilder<'db> {
    db: &'db Database,
    nodes: Vec<DraftNode>,
    edges: Vec<DraftEdge>,
    used_aliases: HashSet<String>,
}

impl<'db> CreateBuilder<'db> {
    fn new(db: &'db Database) -> Self {
        Self {
            db,
            nodes: Vec::new(),
            edges: Vec::new(),
            used_aliases: HashSet::new(),
        }
    }

    /// Adds a node without an alias and returns a handle that edges can reuse.
    pub fn node<L, S>(&mut self, labels: L, props: Map<String, Value>) -> NodeHandle
    where
        L: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.push_node(collect_labels(labels), props, None)
            .expect("alias-free node insertion cannot fail")
    }

    /// Adds a node with an alias (Pattern 2) and returns its handle.
    pub fn node_with_alias<L, S>(
        &mut self,
        labels: L,
        props: Map<String, Value>,
        alias: impl Into<String>,
    ) -> Result<NodeHandle>
    where
        L: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.push_node(collect_labels(labels), props, Some(alias.into()))
    }

    /// Adds an edge between two nodes (identified by handles, aliases, or IDs).
    pub fn edge<T>(
        &mut self,
        src: impl Into<NodeRef>,
        ty: T,
        dst: impl Into<NodeRef>,
        props: Map<String, Value>,
    ) -> Result<&mut Self>
    where
        T: Into<String>,
    {
        self.push_edge(src.into(), ty.into(), dst.into(), props)?;
        Ok(self)
    }

    /// Executes all pending nodes + edges within a single write transaction.
    pub fn execute(self) -> Result<CreateResult> {
        let mut write = self.db.pager.begin_write()?;
        let mut handle_ids: Vec<Option<NodeId>> = vec![None; self.nodes.len()];
        let mut alias_ids: HashMap<String, NodeId> = HashMap::new();
        let mut created_nodes = Vec::with_capacity(self.nodes.len());
        let mut created_edges = Vec::with_capacity(self.edges.len());

        for node in &self.nodes {
            let node_id = self.insert_node(&mut write, node)?;
            handle_ids[node.handle.index()] = Some(node_id);
            if let Some(alias) = &node.alias {
                alias_ids.insert(alias.clone(), node_id);
            }
            created_nodes.push(node_id);
        }

        for edge in &self.edges {
            let src_id = self.resolve_node_ref(&edge.src, &handle_ids, &alias_ids)?;
            let dst_id = self.resolve_node_ref(&edge.dst, &handle_ids, &alias_ids)?;
            let edge_id = self.insert_edge(&mut write, src_id, dst_id, edge)?;
            created_edges.push(edge_id);
        }

        self.db.pager.commit(write)?;
        Ok(CreateResult {
            node_ids: created_nodes,
            edge_ids: created_edges,
            aliases: alias_ids,
        })
    }

    fn push_node(
        &mut self,
        labels: Vec<String>,
        props: Map<String, Value>,
        alias: Option<String>,
    ) -> Result<NodeHandle> {
        if labels.is_empty() {
            return Err(FfiError::Message(
                "node requires at least one label".to_string(),
            ));
        }
        let alias = if let Some(alias) = alias {
            if alias.is_empty() {
                return Err(FfiError::Message(
                    "node alias must be a non-empty string".to_string(),
                ));
            }
            if !self.used_aliases.insert(alias.clone()) {
                return Err(FfiError::Message(format!("duplicate node alias '{alias}'")));
            }
            Some(alias)
        } else {
            None
        };
        let handle = NodeHandle(self.nodes.len());
        self.nodes.push(DraftNode {
            labels,
            props,
            alias,
            handle,
        });
        Ok(handle)
    }

    fn push_edge(
        &mut self,
        src: NodeRef,
        ty: String,
        dst: NodeRef,
        props: Map<String, Value>,
    ) -> Result<()> {
        if ty.is_empty() {
            return Err(FfiError::Message(
                "edge type must be a non-empty string".to_string(),
            ));
        }
        self.edges.push(DraftEdge {
            src,
            dst,
            ty,
            props,
        });
        Ok(())
    }

    fn insert_node(&self, write: &mut WriteGuard<'_>, node: &DraftNode) -> Result<NodeId> {
        let label_ids = self.db.resolve_labels(write, &node.labels)?;
        for label in &label_ids {
            self.db.ensure_label_index(write, *label)?;
        }
        let prop_storage = collect_prop_storage(self.db, write, &node.props)?;
        let mut prop_entries = Vec::with_capacity(prop_storage.len());
        for (prop, owned) in &prop_storage {
            prop_entries.push(PropEntry::new(*prop, prop_value_ref(owned)));
        }
        let node_id = self.db.graph.create_node(
            write,
            StorageNodeSpec {
                labels: &label_ids,
                props: &prop_entries,
            },
        )?;
        Ok(node_id)
    }

    fn insert_edge(
        &self,
        write: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        edge: &DraftEdge,
    ) -> Result<EdgeId> {
        let ty = self.db.resolve_type(write, &edge.ty)?;
        let prop_storage = collect_prop_storage(self.db, write, &edge.props)?;
        let mut prop_entries = Vec::with_capacity(prop_storage.len());
        for (prop, owned) in &prop_storage {
            prop_entries.push(PropEntry::new(*prop, prop_value_ref(owned)));
        }
        let edge_id = self.db.graph.create_edge(
            write,
            StorageEdgeSpec {
                src,
                dst,
                ty,
                props: &prop_entries,
            },
        )?;
        Ok(edge_id)
    }

    fn resolve_node_ref(
        &self,
        reference: &NodeRef,
        handles: &[Option<NodeId>],
        aliases: &HashMap<String, NodeId>,
    ) -> Result<NodeId> {
        match reference {
            NodeRef::Handle(handle) => {
                handles
                    .get(handle.index())
                    .and_then(|id| *id)
                    .ok_or_else(|| {
                        FfiError::Message("referenced node handle has not been created yet".into())
                    })
            }
            NodeRef::Alias(alias) => aliases
                .get(alias)
                .copied()
                .ok_or_else(|| FfiError::Message(format!("unknown node alias '{alias}'"))),
            NodeRef::Existing(id) => Ok(*id),
        }
    }
}

/// Handle to a node created within a transaction (for aliasing and edge creation).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeHandle(usize);

impl NodeHandle {
    fn index(self) -> usize {
        self.0
    }
}

/// Reference to a node that can be resolved during execution.
///
/// Can reference a node handle from the current transaction, an alias, or an existing node ID.
#[derive(Clone, Debug)]
pub enum NodeRef {
    /// Reference to a node by its handle in the current transaction.
    Handle(NodeHandle),
    /// Reference to a node by an alias name.
    Alias(String),
    /// Reference to a node by its existing node ID.
    Existing(NodeId),
}

impl NodeRef {
    /// Creates a node reference from an alias.
    pub fn alias(name: impl Into<String>) -> Self {
        NodeRef::Alias(name.into())
    }

    /// Creates a node reference from an existing node ID.
    pub fn existing(id: impl Into<NodeId>) -> Self {
        NodeRef::Existing(id.into())
    }
}

impl From<NodeHandle> for NodeRef {
    fn from(handle: NodeHandle) -> Self {
        NodeRef::Handle(handle)
    }
}

impl From<NodeId> for NodeRef {
    fn from(id: NodeId) -> Self {
        NodeRef::Existing(id)
    }
}

impl From<u64> for NodeRef {
    fn from(id: u64) -> Self {
        NodeRef::Existing(NodeId(id))
    }
}

impl CreateRefSpec {
    fn into_node_ref(self, handles: &[NodeHandle]) -> Result<NodeRef> {
        match self {
            CreateRefSpec::Handle { index } => {
                let handle = handles.get(index).ok_or_else(|| {
                    FfiError::Message(format!("invalid node handle index {index}"))
                })?;
                Ok(NodeRef::from(*handle))
            }
            CreateRefSpec::Alias { alias } => Ok(NodeRef::alias(alias)),
            CreateRefSpec::Id { id } => Ok(NodeRef::existing(NodeId(id))),
        }
    }
}

#[derive(Debug)]
struct DraftNode {
    labels: Vec<String>,
    props: Map<String, Value>,
    alias: Option<String>,
    handle: NodeHandle,
}

#[derive(Debug)]
struct DraftEdge {
    src: NodeRef,
    dst: NodeRef,
    ty: String,
    props: Map<String, Value>,
}

fn collect_labels<L, S>(labels: L) -> Vec<String>
where
    L: IntoIterator<Item = S>,
    S: Into<String>,
{
    labels.into_iter().map(Into::into).collect()
}

fn collect_prop_storage(
    db: &Database,
    write: &mut WriteGuard<'_>,
    props: &Map<String, Value>,
) -> Result<Vec<(PropId, PropValueOwned)>> {
    let mut storage: Vec<(PropId, PropValueOwned)> = Vec::with_capacity(props.len());
    for (name, value) in props {
        let prop = db.resolve_prop(write, name)?;
        let owned = value_to_prop_value(value)?;
        storage.push((prop, owned));
    }
    Ok(storage)
}

/// Ensures the parent directory exists for a given database path.
///
/// Creates all parent directories if they don't exist.
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            FfiError::Message(format!("unable to create parent directory: {err}"))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Value as QueryValue;
    use serde_json::json;
    use std::path::Path;
    use tempfile::tempdir;

    fn props(entries: &[(&str, Value)]) -> Map<String, Value> {
        let mut map = Map::new();
        for (key, value) in entries {
            map.insert((*key).to_string(), value.clone());
        }
        map
    }

    fn find_plan_node_with_prop<'a>(
        value: &'a Value,
        predicate_key: &str,
    ) -> Option<&'a Map<String, Value>> {
        let obj = value.as_object()?;
        let matches = obj
            .get("props")
            .and_then(Value::as_object)
            .and_then(|props| props.get(predicate_key))
            .is_some();
        if matches {
            return Some(obj);
        }
        if let Some(inputs) = obj.get("inputs").and_then(Value::as_array) {
            for child in inputs {
                if let Some(found) = find_plan_node_with_prop(child, predicate_key) {
                    return Some(found);
                }
            }
        }
        None
    }

    #[test]
    fn create_builder_supports_handles_and_aliases() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("builder_handles_aliases.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let mut builder = db.create();
        let alice = builder.node(["User"], props(&[("name", json!("Alice"))]));
        let bob = builder.node_with_alias(["User"], props(&[("name", json!("Bob"))]), "$bob")?;
        builder
            .edge(alice, "KNOWS", NodeRef::alias("$bob"), Map::new())?
            .edge(NodeRef::alias("$bob"), "KNOWS", bob, Map::new())?;
        let result = builder.execute()?;
        assert_eq!(result.node_ids.len(), 2);
        assert_eq!(result.edge_ids.len(), 2);
        assert_eq!(result.aliases.get("$bob"), Some(&result.node_ids[1]));
        Ok(())
    }

    #[test]
    fn predicate_and_empty_normalizes_to_true() -> Result<()> {
        let spec = QuerySpec {
            schema_version: Some(1),
            request_id: None,
            matches: vec![MatchSpec {
                var: "a".into(),
                label: Some("User".into()),
            }],
            edges: Vec::new(),
            predicate: Some(PredicateSpec::And { args: vec![] }),
            projections: Vec::new(),
            distinct: false,
        };
        let ast = spec.into_ast()?;
        assert!(ast.predicate.is_none());
        Ok(())
    }

    #[test]
    fn predicate_or_empty_normalizes_to_false() -> Result<()> {
        let spec = QuerySpec {
            schema_version: Some(1),
            request_id: None,
            matches: vec![MatchSpec {
                var: "a".into(),
                label: Some("User".into()),
            }],
            edges: Vec::new(),
            predicate: Some(PredicateSpec::Or { args: vec![] }),
            projections: Vec::new(),
            distinct: false,
        };
        let ast = spec.into_ast()?;
        match ast.predicate {
            Some(BoolExpr::Or(children)) => assert!(children.is_empty()),
            other => panic!("unexpected predicate shape: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn sample_labels_returns_entries_for_demo_db() -> Result<()> {
        let path = Path::new("tests/fixtures/demo-db/graph-demo.sombra");
        let opts = DatabaseOptions {
            create_if_missing: false,
            ..DatabaseOptions::default()
        };
        let db = Database::open(path, opts)?;
        let labels = db.sample_labels(100, 5)?;
        assert!(!labels.is_empty(), "expected at least one label in demo db");
        Ok(())
    }

    #[test]
    fn json_predicate_spec_parses_into_ast() -> Result<()> {
        let spec = json!({
            "$schemaVersion": 1,
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "edges": [],
            "projections": [],
            "distinct": false,
            "predicate": {
                "op": "eq",
                "var": "a",
                "prop": "name",
                "value": { "t": "String", "v": "Ada" }
            }
        });
        let spec: QuerySpec = serde_json::from_value(spec)?;
        let ast = spec.into_ast()?;
        assert!(matches!(ast.predicate, Some(BoolExpr::Cmp(_))));
        Ok(())
    }

    #[test]
    fn missing_schema_version_rejected() {
        let spec = QuerySpec {
            schema_version: None,
            request_id: None,
            matches: vec![MatchSpec {
                var: "a".into(),
                label: Some("User".into()),
            }],
            edges: Vec::new(),
            predicate: None,
            projections: Vec::new(),
            distinct: false,
        };
        let err = spec.into_ast().expect_err("schema version required");
        match err {
            FfiError::Analyzer(AnalyzerError::UnsupportedSchemaVersion { found, supported }) => {
                assert_eq!(supported, 1);
                assert_eq!(found, SchemaVersionState::Missing);
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn invalid_direction_reports_code() {
        let spec = QuerySpec {
            schema_version: Some(1),
            request_id: None,
            matches: vec![
                MatchSpec {
                    var: "a".into(),
                    label: Some("User".into()),
                },
                MatchSpec {
                    var: "b".into(),
                    label: Some("User".into()),
                },
            ],
            edges: vec![EdgeSpec {
                from: "a".into(),
                to: "b".into(),
                edge_type: None,
                direction: DirectionSpec("sideways".into()),
            }],
            predicate: None,
            projections: Vec::new(),
            distinct: false,
        };
        let err = spec.into_ast().expect_err("direction check");
        match err {
            FfiError::Analyzer(AnalyzerError::DirectionInvalid { direction }) => {
                assert_eq!(direction, "sideways");
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn invalid_bytes_literal_returns_bytes_encoding() {
        let spec = json!({
            "$schemaVersion": 1,
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "edges": [],
            "projections": [],
            "distinct": false,
            "predicate": {
                "op": "eq",
                "var": "a",
                "prop": "blob",
                "value": { "t": "Bytes", "v": "**not-base64**" }
            }
        });
        let spec: QuerySpec = serde_json::from_value(spec).expect("spec parses");
        let err = spec.into_ast().expect_err("bytes literal should fail");
        assert!(matches!(
            err,
            FfiError::Analyzer(AnalyzerError::BytesEncoding)
        ));
    }

    #[test]
    fn execute_json_with_predicate_filters_rows() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("predicate_filter.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        db.seed_demo()?;
        let spec = json!({
            "$schemaVersion": 1,
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "edges": [],
            "projections": [
                { "kind": "var", "var": "a", "alias": null }
            ],
            "predicate": {
                "op": "eq",
                "var": "a",
                "prop": "name",
                "value": { "t": "String", "v": "Ada" }
            }
        });
        let response = db.execute_json(&spec)?;
        let rows = response
            .get("rows")
            .and_then(Value::as_array)
            .expect("rows array");
        assert_eq!(rows.len(), 1);
        Ok(())
    }

    #[test]
    fn validate_in_values_rejects_excessive_bytes() {
        let chunk = QueryValue::Bytes(vec![0u8; MAX_BYTES_LITERAL / 2 + 1]);
        let err = super::validate_in_values(&[chunk.clone(), chunk]).unwrap_err();
        match err {
            FfiError::Message(msg) => {
                assert!(msg.contains("total bytes in in() literals exceeds"))
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_in_values_allows_bytes_within_budget() -> Result<()> {
        let chunk = QueryValue::Bytes(vec![0u8; MAX_BYTES_LITERAL / 2]);
        super::validate_in_values(&[chunk.clone(), chunk])?;
        Ok(())
    }

    #[test]
    fn create_builder_errors_on_unknown_alias() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("builder_unknown_alias.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let mut builder = db.create();
        builder.node(["User"], props(&[("name", json!("Alice"))]));
        builder.edge(
            NodeRef::alias("$missing"),
            "LIKES",
            NodeRef::alias("$also_missing"),
            Map::new(),
        )?;
        let err = builder.execute().unwrap_err();
        match err {
            FfiError::Message(msg) => assert!(msg.contains("unknown node alias")),
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn create_builder_rejects_duplicate_alias() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("builder_duplicate_alias.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let mut builder = db.create();
        builder.node_with_alias(["User"], Map::new(), "$dup")?;
        let err = builder
            .node_with_alias(["User"], Map::new(), "$dup")
            .unwrap_err();
        match err {
            FfiError::Message(msg) => assert!(msg.contains("duplicate node alias")),
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn create_script_supports_structs_and_json() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("builder_script_spec.db");
        let db = Database::open(&path, DatabaseOptions::default())?;

        let script = CreateScript {
            nodes: vec![
                CreateNodeSpec {
                    labels: vec!["User".into()],
                    props: props(&[("name", json!("Alice"))]),
                    alias: Some("$alice".into()),
                },
                CreateNodeSpec {
                    labels: vec!["Company".into()],
                    props: props(&[("name", json!("Acme Inc"))]),
                    alias: None,
                },
            ],
            edges: vec![CreateEdgeSpec {
                src: CreateRefSpec::Alias {
                    alias: "$alice".into(),
                },
                ty: "WORKS_AT".into(),
                dst: CreateRefSpec::Handle { index: 1 },
                props: props(&[("role", json!("Engineer"))]),
            }],
        };
        let result = db.create_script(script)?;
        assert_eq!(result.node_ids.len(), 2);
        assert_eq!(result.edge_ids.len(), 1);
        assert_eq!(result.aliases.get("$alice"), Some(&result.node_ids[0]));

        let json_spec = json!({
            "nodes": [
                { "labels": ["User"], "props": { "name": "Bob" }, "alias": "$bob" },
                { "labels": ["User"], "props": { "name": "Charlie" } }
            ],
            "edges": [
                {
                    "src": { "kind": "alias", "alias": "$bob" },
                    "ty": "KNOWS",
                    "dst": { "kind": "handle", "index": 1 },
                    "props": {}
                }
            ]
        });
        let summary = db.create_json(&json_spec)?;
        assert_eq!(summary["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(summary["edges"].as_array().unwrap().len(), 1);
        assert!(summary["aliases"]["$bob"].as_u64().is_some());
        Ok(())
    }

    #[test]
    fn pragma_synchronous_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pragma_sync.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let set = db.pragma("synchronous", Some(Value::String("normal".into())))?;
        assert_eq!(set, Value::String("normal".into()));
        let current = db.pragma("synchronous", None)?;
        assert_eq!(current, Value::String("normal".into()));
        Ok(())
    }

    #[test]
    fn pragma_wal_coalesce_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pragma_coalesce.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let set = db.pragma("wal_coalesce_ms", Some(Value::Number(Number::from(7))))?;
        assert_eq!(set, Value::Number(Number::from(7)));
        let current = db.pragma("wal_coalesce_ms", None)?;
        assert_eq!(current, Value::Number(Number::from(7)));
        Ok(())
    }

    #[test]
    fn pragma_autocheckpoint_ms_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pragma_auto_ms.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        let set = db.pragma("autocheckpoint_ms", Some(Value::Number(Number::from(10))))?;
        assert_eq!(set, Value::Number(Number::from(10)));
        let current = db.pragma("autocheckpoint_ms", None)?;
        assert_eq!(current, Value::Number(Number::from(10)));
        let cleared = db.pragma("autocheckpoint_ms", Some(Value::Null))?;
        assert_eq!(cleared, Value::Null);
        Ok(())
    }

    #[test]
    fn explain_json_includes_union_dedup_flag() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("explain_union_dedup.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        db.seed_demo()?;
        let spec = json!({
            "$schemaVersion": 1,
            "request_id": "req-42",
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "projections": [
                { "kind": "var", "var": "a" }
            ],
            "distinct": true,
            "predicate": {
                "op": "or",
                "args": [
                    {
                        "op": "eq",
                        "var": "a",
                        "prop": "name",
                        "value": { "t": "String", "v": "Ada" }
                    },
                    {
                        "op": "eq",
                        "var": "a",
                        "prop": "name",
                        "value": { "t": "String", "v": "Grace" }
                    }
                ]
            }
        });
        let explain = db.explain_json(&spec)?;
        assert_eq!(
            explain.get("request_id").and_then(Value::as_str),
            Some("req-42")
        );
        let plan = explain
            .get("plan")
            .and_then(Value::as_array)
            .expect("plan array");
        dbg!(plan);
        assert_eq!(plan.len(), 1);
        let project = plan[0].as_object().expect("plan node");
        assert_eq!(project.get("op").and_then(Value::as_str), Some("Project"));
        let inputs = project
            .get("inputs")
            .and_then(Value::as_array)
            .expect("project inputs");
        assert_eq!(inputs.len(), 1);
        let union = inputs[0].as_object().expect("union node object");
        assert_eq!(union.get("op").and_then(Value::as_str), Some("Union"));
        let dedup = union
            .get("props")
            .and_then(Value::as_object)
            .and_then(|props| props.get("dedup"))
            .and_then(Value::as_str);
        assert_eq!(dedup, Some("true"));
        let features = explain
            .get("features")
            .and_then(Value::as_array)
            .expect("features array");
        assert!(features.is_empty());
        let plan_hash = explain
            .get("plan_hash")
            .and_then(Value::as_str)
            .expect("plan hash");
        assert!(plan_hash.starts_with("0x"));
        Ok(())
    }

    #[test]
    fn explain_json_can_redact_literals() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("explain_redact.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        db.seed_demo()?;
        let spec = json!({
            "$schemaVersion": 1,
            "request_id": "req-redact",
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "predicate": {
                "op": "eq",
                "var": "a",
                "prop": "name",
                "value": { "t": "String", "v": "Ada" }
            },
            "projections": [
                { "kind": "var", "var": "a" }
            ],
            "redact_literals": true
        });
        let explain = db.explain_json(&spec)?;
        let plan = explain
            .get("plan")
            .and_then(Value::as_array)
            .expect("plan array");
        let project = plan[0].as_object().expect("project node");
        assert_eq!(project.get("op").and_then(Value::as_str), Some("Project"));
        let predicate_node = plan
            .iter()
            .find_map(|node| find_plan_node_with_prop(node, "predicate"))
            .expect("predicate node");
        let props = predicate_node
            .get("props")
            .and_then(Value::as_object)
            .expect("predicate props");
        assert_eq!(
            props.get("predicate").and_then(Value::as_str),
            Some("<redacted>")
        );
        Ok(())
    }

    #[test]
    fn cancel_request_interrupts_stream() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cancel_stream.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        db.seed_demo()?;
        let spec = json!({
            "$schemaVersion": 1,
            "request_id": "req-cancel",
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "projections": [
                { "kind": "var", "var": "a" }
            ]
        });
        let stream = db.stream_json(&spec)?;
        assert!(db.cancel_request("req-cancel"));
        match stream.next() {
            Err(FfiError::Core(SombraError::Cancelled)) => {}
            other => panic!("expected cancellation, got {other:?}"),
        }
        assert!(!db.cancel_request("req-cancel"));
        Ok(())
    }

    #[test]
    fn execute_json_includes_metadata() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("execute_with_meta.db");
        let db = Database::open(&path, DatabaseOptions::default())?;
        db.seed_demo()?;
        let spec = json!({
            "$schemaVersion": 1,
            "request_id": "req-777",
            "matches": [
                { "var": "a", "label": "User" }
            ],
            "projections": [
                { "kind": "var", "var": "a" }
            ]
        });
        let response = db.execute_json(&spec)?;
        assert_eq!(
            response.get("request_id").and_then(Value::as_str),
            Some("req-777")
        );
        let features = response
            .get("features")
            .and_then(Value::as_array)
            .expect("features array");
        assert!(features.is_empty());
        let rows = response
            .get("rows")
            .and_then(Value::as_array)
            .expect("rows array");
        assert!(!rows.is_empty());
        Ok(())
    }
}
