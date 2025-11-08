#![forbid(unsafe_code)]

//! High-level FFI helpers shared by the language bindings.
//!
//! This module exposes a safe wrapper around the Stage 8 planner/executor so
//! bindings can submit JSON-friendly query specifications without reimplementing
//! the core logic.

use crate::primitives::pager::{PageStore, Pager, PagerOptions, Synchronous, WriteGuard};
use crate::query::{
    ast::{
        EdgeClause, EdgeDirection, Literal, MatchClause, Projection, PropPredicate, QueryAst, Var,
    },
    executor::{Executor, QueryResult, ResultStream, Row, Value as ExecValue},
    metadata::CatalogMetadata,
    planner::{ExplainNode, PlanExplain, Planner, PlannerConfig, PlannerOutput},
    profile::profile_snapshot as query_profile_snapshot,
};
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::{
    DeleteNodeOpts, EdgeSpec as StorageEdgeSpec, Graph, GraphOptions, IndexDef, IndexKind,
    NodeSpec as StorageNodeSpec, PropEntry, PropPatch, PropPatchOp, PropValue, PropValueOwned,
    TypeTag,
};
use crate::types::{EdgeId, LabelId, NodeId, PropId, SombraError, TypeId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::{HashMap, HashSet},
    fs,
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
            counters.plan_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.plan_count.fetch_add(1, Ordering::Relaxed);
        }
        ProfileKind::Execute => {
            counters.exec_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.exec_count.fetch_add(1, Ordering::Relaxed);
        }
        ProfileKind::Serialize => {
            counters.serde_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.serde_count.fetch_add(1, Ordering::Relaxed);
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
            counter.swap(0, Ordering::Relaxed)
        } else {
            counter.load(Ordering::Relaxed)
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

/// Shared database handle used by language bindings (Node.js, Python, etc.).
///
/// This is the main entry point for FFI clients to interact with the Sombra database.
/// It provides methods to execute queries, perform mutations, and manage data creation
/// through a fluent builder pattern.
pub struct Database {
    pager: Arc<Pager>,
    graph: Arc<Graph>,
    dict: Arc<Dict>,
    planner: Planner,
    executor: Executor,
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
        let metadata = Arc::new(CatalogMetadata::from_dict(
            Arc::clone(&dict),
            Arc::clone(&store),
            catalog_root,
        )?);
        let planner = Planner::new(PlannerConfig::default(), Arc::clone(&metadata) as _);
        let executor = Executor::new(
            Arc::clone(&graph),
            Arc::clone(&pager),
            Arc::clone(&metadata) as _,
        );

        Ok(Self {
            pager,
            graph,
            dict,
            planner,
            executor,
        })
    }

    /// Executes a JSON-serialized query specification and returns all results.
    ///
    /// Deserializes the JSON query specification and executes it against the database.
    pub fn execute_json(&self, spec: &Value) -> Result<Vec<Value>> {
        let spec: QuerySpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.execute(spec)
    }

    /// Explains a JSON-serialized query without executing it.
    ///
    /// Returns the query execution plan for inspection and optimization.
    pub fn explain_json(&self, spec: &Value) -> Result<Value> {
        let spec: QuerySpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.explain(spec)
    }

    /// Creates a streaming query from a JSON specification.
    ///
    /// Returns an iterator-like [`QueryStream`] for processing large result sets.
    pub fn stream_json(&self, spec: &Value) -> Result<QueryStream> {
        let spec: QuerySpec = serde_json::from_value(spec.clone())
            .map_err(|err| FfiError::Message(format!("invalid query spec: {err}")))?;
        self.stream(spec)
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
    pub fn execute(&self, spec: QuerySpec) -> Result<Vec<Value>> {
        let plan_timer = profile_timer();
        let plan = self.plan(spec)?;
        record_profile_timer(ProfileKind::Plan, plan_timer);
        let exec_timer = profile_timer();
        let result = self.executor.execute(&plan.plan)?;
        record_profile_timer(ProfileKind::Execute, exec_timer);
        let serde_timer = profile_timer();
        let rows = rows_to_values(&result);
        record_profile_timer(ProfileKind::Serialize, serde_timer);
        rows
    }

    /// Returns the query execution plan for a specification.
    pub fn explain(&self, spec: QuerySpec) -> Result<Value> {
        let plan = self.plan(spec)?;
        Ok(explain_to_value(&plan.explain))
    }

    /// Creates a streaming query result.
    pub fn stream(&self, spec: QuerySpec) -> Result<QueryStream> {
        let plan = self.plan(spec)?;
        let stream = self.executor.stream(&plan.plan)?;
        Ok(QueryStream::new(stream))
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
        self.planner.plan(&ast).map_err(FfiError::from)
    }
}

/// A streaming query result that can be consumed incrementally.
///
/// This allows processing large result sets without loading everything into memory.
pub struct QueryStream {
    inner: Arc<Mutex<ResultStream>>,
}

impl QueryStream {
    fn new(stream: ResultStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(stream)),
        }
    }

    /// Fetches the next result row from the stream.
    ///
    /// Returns `Ok(Some(value))` for each row, `Ok(None)` when exhausted,
    /// or an error if something goes wrong.
    pub fn next(&self) -> Result<Option<Value>> {
        let mut guard = self
            .inner
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

/// JSON-deserializable query specification for FFI clients.
///
/// Defines match clauses, edges, predicates, and projections for graph queries.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuerySpec {
    /// MATCH clauses for node patterns.
    #[serde(default)]
    pub matches: Vec<MatchSpec>,
    /// Edge traversal specifications.
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    /// Property predicates for filtering.
    #[serde(default)]
    pub predicates: Vec<PredicateSpec>,
    /// Whether to return distinct results only.
    #[serde(default)]
    pub distinct: bool,
    /// Column projections for result output.
    #[serde(default)]
    pub projections: Vec<ProjectionSpec>,
}

impl QuerySpec {
    fn into_ast(self) -> Result<QueryAst> {
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
        let predicates = self
            .predicates
            .into_iter()
            .map(PredicateSpec::into_predicate)
            .collect::<Result<Vec<_>>>()?;
        let projections = self
            .projections
            .into_iter()
            .map(ProjectionSpec::into_projection)
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryAst {
            matches,
            edges,
            predicates,
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
            direction: self.direction.into(),
        })
    }
}

/// Edge traversal direction specification.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DirectionSpec {
    /// Outgoing edges only.
    Out,
    /// Incoming edges only.
    In,
    /// Both incoming and outgoing edges.
    Both,
}

impl DirectionSpec {
    fn default_out() -> Self {
        DirectionSpec::Out
    }
}

impl From<DirectionSpec> for EdgeDirection {
    fn from(value: DirectionSpec) -> Self {
        match value {
            DirectionSpec::Out => EdgeDirection::Out,
            DirectionSpec::In => EdgeDirection::In,
            DirectionSpec::Both => EdgeDirection::Both,
        }
    }
}

/// Property predicate for filtering query results.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum PredicateSpec {
    /// Equality predicate for exact property value matching.
    Eq {
        /// Variable name to test.
        var: String,
        /// Property name to check.
        prop: String,
        /// Expected value.
        value: LiteralSpec,
    },
    /// Range predicate for property values within bounds.
    Range {
        /// Variable name to test.
        var: String,
        /// Property name to check.
        prop: String,
        /// Lower bound (inclusive or exclusive).
        lower: BoundSpec,
        /// Upper bound (inclusive or exclusive).
        upper: BoundSpec,
    },
    /// Custom predicate expression.
    Custom {
        /// Custom filter expression.
        expr: String,
    },
}

impl PredicateSpec {
    fn into_predicate(self) -> Result<PropPredicate> {
        match self {
            PredicateSpec::Eq { var, prop, value } => Ok(PropPredicate::Eq {
                var: Var(var),
                prop,
                value: value.into_literal(),
            }),
            PredicateSpec::Range {
                var,
                prop,
                lower,
                upper,
            } => Ok(PropPredicate::Range {
                var: Var(var),
                prop,
                lower: lower.into_bound()?,
                upper: upper.into_bound()?,
            }),
            PredicateSpec::Custom { expr } => Ok(PropPredicate::Custom { expr }),
        }
    }
}

/// Range bound specification for range predicates.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BoundSpec {
    /// No bound (infinity).
    Unbounded,
    /// Inclusive bound with a value.
    Included {
        /// Bound value.
        value: LiteralSpec,
    },
    /// Exclusive bound with a value.
    Excluded {
        /// Bound value.
        value: LiteralSpec,
    },
}

impl BoundSpec {
    fn into_bound(self) -> Result<Bound<Literal>> {
        match self {
            BoundSpec::Unbounded => Ok(Bound::Unbounded),
            BoundSpec::Included { value } => Ok(Bound::Included(value.into_literal())),
            BoundSpec::Excluded { value } => Ok(Bound::Excluded(value.into_literal())),
        }
    }
}

/// Literal value specification for predicates and properties.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum LiteralSpec {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// Integer value.
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// String value.
    String(String),
}

impl LiteralSpec {
    fn into_literal(self) -> Literal {
        match self {
            LiteralSpec::Null => Literal::Null,
            LiteralSpec::Bool(v) => Literal::Bool(v),
            LiteralSpec::Int(v) => Literal::Int(v),
            LiteralSpec::Float(v) => Literal::Float(v),
            LiteralSpec::String(v) => Literal::String(v),
        }
    }
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
    /// Project a custom expression with required alias.
    Expr {
        /// Expression to evaluate.
        expr: String,
        /// Column alias for the result.
        alias: String,
    },
}

impl ProjectionSpec {
    fn into_projection(self) -> Result<Projection> {
        match self {
            ProjectionSpec::Var { var, alias } => Ok(Projection::Var {
                var: Var(var),
                alias,
            }),
            ProjectionSpec::Expr { expr, alias } => {
                if alias.is_empty() {
                    Err(FfiError::Message(
                        "projection expressions require an alias".into(),
                    ))
                } else {
                    Ok(Projection::Expr { expr, alias })
                }
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

fn explain_to_value(explain: &PlanExplain) -> Value {
    let mut root = Map::new();
    root.insert("plan".into(), explain_node_to_value(&explain.root));
    Value::Object(root)
}

fn explain_node_to_value(node: &ExplainNode) -> Value {
    let mut map = Map::new();
    map.insert("op".into(), Value::String(node.op.clone()));
    if !node.props.is_empty() {
        let mut props = Map::new();
        for (k, v) in &node.props {
            props.insert(k.clone(), Value::String(v.clone()));
        }
        map.insert("props".into(), Value::Object(props));
    }
    let inputs = node
        .inputs
        .iter()
        .map(explain_node_to_value)
        .collect::<Vec<_>>();
    map.insert("inputs".into(), Value::Array(inputs));
    Value::Object(map)
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
    use serde_json::json;
    use tempfile::tempdir;

    fn props(entries: &[(&str, Value)]) -> Map<String, Value> {
        let mut map = Map::new();
        for (key, value) in entries {
            map.insert((*key).to_string(), value.clone());
        }
        map
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
}
