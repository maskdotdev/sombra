//! Dashboard API server implementation (experimental).
#![allow(clippy::field_reassign_with_default)]

use std::{
    fmt::Write as _,
    fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::Instant,
};

#[cfg(not(feature = "bundled-dashboard"))]
use axum::response::Html;
#[cfg(feature = "bundled-dashboard")]
use axum::{body::Body, http::Uri};
use axum::{
    extract::{Query, State},
    http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderValue, Method, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
#[cfg(feature = "bundled-dashboard")]
use bytes::Bytes;
use hyper::Error as HyperError;
#[cfg(feature = "bundled-dashboard")]
use include_dir::{include_dir, Dir, File};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use tempfile::Builder as TempFileBuilder;
use thiserror::Error;
use tokio::{net::TcpListener, task};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    services::ServeDir,
    trace::TraceLayer,
};
use tracing_subscriber::{fmt, EnvFilter};

use crate::admin::{self, AdminOpenOptions, StatsReport};
use crate::ffi::{self, DatabaseOptions};
use crate::primitives::pager::MVCC_READER_WARN_THRESHOLD_MS;


/// Runtime options used to boot the dashboard HTTP server.
#[derive(Clone, Debug)]
pub struct DashboardOptions {
    /// Path to the primary database file.
    pub db_path: PathBuf,
    /// Pager/graph options shared with existing CLI admins.
    pub open_opts: AdminOpenOptions,
    /// Network interface to bind to.
    pub host: IpAddr,
    /// Listening port.
    pub port: u16,
    /// Optional static asset directory for the dashboard UI.
    pub assets_dir: Option<PathBuf>,
    /// Whether to disable mutating/admin endpoints.
    pub read_only: bool,
    /// Whether to open the default browser automatically.
    pub open_browser: bool,
    /// Allowed CORS origins for remote dashboards.
    pub allow_origins: Vec<String>,
}

impl DashboardOptions {
    /// Convenience accessor for `(host, port)` tuples.
    pub fn socket_parts(&self) -> (IpAddr, u16) {
        (self.host, self.port)
    }
}

/// Errors that can occur while running the dashboard server.
#[derive(Debug, Error)]
pub enum DashboardError {
    /// Opening the database or pager failed.
    #[error("failed to open database: {0}")]
    Admin(#[from] admin::AdminError),
    /// Failed to initialize the query engine.
    #[error("failed to initialize query engine: {0}")]
    Ffi(#[from] ffi::FfiError),
    /// Binding the TCP listener failed.
    #[error("failed to bind dashboard listener: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP server error bubbled up from Axum/Hyper.
    #[error("dashboard server error: {0}")]
    Http(#[from] HyperError),
}

type AppState = Arc<ServerState>;

const MAX_STREAMED_ROWS: usize = 1_000;

#[cfg(feature = "bundled-dashboard")]
static EMBEDDED_DASHBOARD_ASSETS: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/packages/dashboard/build/client");

#[cfg(feature = "bundled-dashboard")]
type EmbeddedFile = File<'static>;

#[derive(Clone, Debug)]
enum AssetMode {
    Filesystem(PathBuf),
    #[cfg(feature = "bundled-dashboard")]
    Embedded,
    #[cfg(not(feature = "bundled-dashboard"))]
    Inline,
}

struct ServerState {
    db_path: PathBuf,
    open_opts: AdminOpenOptions,
    read_only: bool,
    asset_mode: AssetMode,
    allow_origins: Vec<String>,
}

impl ServerState {
    fn new(opts: DashboardOptions) -> Result<Self, DashboardError> {
        let asset_mode = resolve_asset_mode(opts.assets_dir);
        Ok(Self {
            db_path: opts.db_path,
            open_opts: opts.open_opts,
            read_only: opts.read_only,
            asset_mode,
            allow_origins: opts.allow_origins,
        })
    }

    fn open_database(&self) -> Result<ffi::Database, DashboardError> {
        let mut db_opts = DatabaseOptions::default();
        db_opts.create_if_missing = false;
        db_opts.pager = self.open_opts.pager.clone();
        db_opts.distinct_neighbors_default = self.open_opts.distinct_neighbors_default;
        let db = ffi::Database::open(&self.db_path, db_opts)?;
        Ok(db)
    }
}

fn resolve_asset_mode(dir: Option<PathBuf>) -> AssetMode {
    match dir {
        Some(path) => {
            if path.is_dir() {
                AssetMode::Filesystem(path)
            } else {
                tracing::warn!(
                    provided = %path.display(),
                    "dashboard assets directory not found; falling back to embedded assets (if available)"
                );
                default_asset_mode()
            }
        }
        None => default_asset_mode(),
    }
}

#[cfg(feature = "bundled-dashboard")]
fn default_asset_mode() -> AssetMode {
    AssetMode::Embedded
}

#[cfg(not(feature = "bundled-dashboard"))]
fn default_asset_mode() -> AssetMode {
    AssetMode::Inline
}

/// Starts the dashboard server and runs until shutdown.
pub async fn serve(options: DashboardOptions) -> Result<(), DashboardError> {
    install_tracing_subscriber();

    if options.open_browser {
        tracing::warn!("--open-browser not implemented yet; continuing without auto-launch");
    }

    let (host, port) = options.socket_parts();
    let state = Arc::new(ServerState::new(options)?);
    let app = build_router(state.clone());
    let addr = SocketAddr::from((host, port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!(
        %addr,
        db_path = %state.db_path.display(),
        read_only = state.read_only,
        asset_mode = ?state.asset_mode,
        allow_origins = ?state.allow_origins,
        "dashboard listening"
    );
    tracing::warn!(
        %addr,
        read_only = state.read_only,
        allow_origins = ?state.allow_origins,
        "dashboard server runs without TLS/auth; intended for local use only"
    );

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
    let cors = build_cors_layer(&state.allow_origins);

    let mut router = Router::new()
        .route("/health", get(health_handler))
        .route("/health/live", get(live_handler))
        .route("/health/ready", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/query", post(query_handler))
        .route("/api/labels", get(labels_handler))
        .route("/api/labels/indexes", post(ensure_label_indexes_handler))
        .route("/api/graph/full", get(full_graph_handler));

    match state.asset_mode.clone() {
        AssetMode::Filesystem(dir) => {
            let service = ServeDir::new(dir).append_index_html_on_directories(true);
            router = router.fallback_service(service);
        }
        #[cfg(feature = "bundled-dashboard")]
        AssetMode::Embedded => {
            router = router.fallback(embedded_dashboard_handler);
        }
        #[cfg(not(feature = "bundled-dashboard"))]
        AssetMode::Inline => {
            router = router.route("/", get(inline_index));
        }
    }

    if let Some(layer) = cors {
        router = router.layer(layer);
    }

    router.with_state(state).layer(TraceLayer::new_for_http())
}

fn build_cors_layer(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }

    let mut allowed = Vec::new();
    for origin in origins {
        let normalized = normalize_origin(origin);
        match normalized
            .as_deref()
            .and_then(|value| HeaderValue::from_str(value).ok())
        {
            Some(value) => allowed.push(value),
            None => {
                tracing::warn!(%origin, ?normalized, "ignoring invalid CORS origin");
            }
        }
    }

    if allowed.is_empty() {
        return None;
    }

    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allowed))
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([ACCEPT, CONTENT_TYPE]),
    )
}

fn normalize_origin(origin: &str) -> Option<String> {
    let trimmed = origin.trim();
    if trimmed.is_empty() {
        return None;
    }
    let without_trailing_slash = trimmed.trim_end_matches('/');
    if without_trailing_slash.is_empty() {
        return None;
    }
    Some(without_trailing_slash.to_string())
}

async fn live_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        read_only: state.read_only,
    })
}

async fn health_handler(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let report = evaluate_readiness(&state);
    let status = if report.ok { "ok" } else { "error" };
    let code = if report.ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(HealthResponse {
            status,
            read_only: state.read_only,
        }),
    )
}

async fn ready_handler(State(state): State<AppState>) -> (StatusCode, Json<ReadyResponse>) {
    let report = evaluate_readiness(&state);
    let status = if report.ok { "ok" } else { "error" };
    let code = if report.ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(ReadyResponse {
            status,
            read_only: state.read_only,
            checks: report.checks,
        }),
    )
}

async fn metrics_handler(State(state): State<AppState>) -> Result<Response, AppError> {
    let collect_start = Instant::now();
    let path = state.db_path.clone();
    let opts = state.open_opts.clone();
    let report = task::spawn_blocking(move || admin::stats(path, &opts)).await??;
    let collect_ms = collect_start.elapsed().as_millis();
    let profile = ffi::profile_snapshot(false);
    let storage_profile = crate::storage::storage_profile_snapshot(false);

    let mut body = String::new();
    // Server-level gauges.
    push_metric(
        &mut body,
        "sombra_read_only",
        "Whether the dashboard server is running in read-only mode (1=yes)",
        "gauge",
        if state.read_only { 1 } else { 0 },
    );
    push_metric(
        &mut body,
        "sombra_metrics_collect_ms",
        "Time to collect metrics from admin::stats",
        "gauge",
        collect_ms,
    );

    // Pager stats.
    push_metric(
        &mut body,
        "sombra_pager_cache_hits_total",
        "Pager cache hits",
        "counter",
        report.pager.hits,
    );
    push_metric(
        &mut body,
        "sombra_pager_cache_misses_total",
        "Pager cache misses",
        "counter",
        report.pager.misses,
    );
    push_metric(
        &mut body,
        "sombra_pager_cache_evictions_total",
        "Pager cache evictions",
        "counter",
        report.pager.evictions,
    );
    push_metric(
        &mut body,
        "sombra_pager_dirty_writebacks_total",
        "Dirty pages written back",
        "counter",
        report.pager.dirty_writebacks,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_reader_begin_total",
        "MVCC reader begin events",
        "counter",
        report.pager.mvcc_reader_begin_total,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_reader_end_total",
        "MVCC reader end events",
        "counter",
        report.pager.mvcc_reader_end_total,
    );
    push_metric(
        &mut body,
        "sombra_pager_last_checkpoint_lsn",
        "Last checkpoint LSN",
        "gauge",
        report.pager.last_checkpoint_lsn,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_page_versions_total",
        "Total MVCC page versions retained",
        "gauge",
        report.pager.mvcc_page_versions_total,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_pages_with_versions",
        "Pages currently tracking historical versions",
        "gauge",
        report.pager.mvcc_pages_with_versions,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_readers_active",
        "Active MVCC readers",
        "gauge",
        report.pager.mvcc_readers_active,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_reader_max_age_ms",
        "Maximum observed MVCC reader age (ms)",
        "gauge",
        report.pager.mvcc_reader_max_age_ms,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_max_chain_len",
        "Maximum MVCC version chain length",
        "gauge",
        report.pager.mvcc_max_chain_len,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_overlay_pages",
        "Pages with uncheckpointed overlays",
        "gauge",
        report.pager.mvcc_overlay_pages,
    );
    push_metric(
        &mut body,
        "sombra_pager_mvcc_overlay_entries",
        "Overlay entries across pages",
        "gauge",
        report.pager.mvcc_overlay_entries,
    );
    push_metric(
        &mut body,
        "sombra_pager_lock_readers",
        "Active reader locks",
        "gauge",
        report.pager.lock_readers,
    );
    push_metric(
        &mut body,
        "sombra_pager_lock_writer",
        "Writer lock held (1=yes)",
        "gauge",
        if report.pager.lock_writer { 1 } else { 0 },
    );
    push_metric(
        &mut body,
        "sombra_pager_lock_checkpoint",
        "Checkpoint lock held (1=yes)",
        "gauge",
        if report.pager.lock_checkpoint { 1 } else { 0 },
    );

    // WAL stats.
    push_metric(
        &mut body,
        "sombra_wal_size_bytes",
        "Total size of WAL files",
        "gauge",
        report.wal.size_bytes,
    );
    push_metric(
        &mut body,
        "sombra_wal_last_checkpoint_lsn",
        "Last checkpoint LSN observed by WAL",
        "gauge",
        report.wal.last_checkpoint_lsn,
    );
    push_metric(
        &mut body,
        "sombra_wal_segment_size_bytes",
        "WAL segment size in bytes",
        "gauge",
        report.wal.segment_size_bytes,
    );
    push_metric(
        &mut body,
        "sombra_wal_preallocate_segments",
        "Target number of preallocated WAL segments",
        "gauge",
        report.wal.preallocate_segments,
    );
    push_metric(
        &mut body,
        "sombra_wal_ready_segments",
        "Segments ready for activation",
        "gauge",
        report.wal.ready_segments,
    );
    push_metric(
        &mut body,
        "sombra_wal_recycle_segments",
        "Segments queued for recycling",
        "gauge",
        report.wal.recycle_segments,
    );
    push_metric(
        &mut body,
        "sombra_wal_allocation_error",
        "Whether the WAL allocator has reported an error (1=yes)",
        "gauge",
        if report.wal.allocation_error.is_some() {
            1
        } else {
            0
        },
    );

    // Query profiling (if enabled via SOMBRA_PROFILE).
    if let Some(p) = profile {
        push_metric(
            &mut body,
            "sombra_query_plan_ns_total",
            "Total nanoseconds spent planning queries",
            "counter",
            p.plan_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_plan_count_total",
            "Query planning operations",
            "counter",
            p.plan_count,
        );
        push_metric(
            &mut body,
            "sombra_query_exec_ns_total",
            "Total nanoseconds spent executing queries",
            "counter",
            p.exec_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_exec_count_total",
            "Query execution operations",
            "counter",
            p.exec_count,
        );
        push_metric(
            &mut body,
            "sombra_query_serde_ns_total",
            "Total nanoseconds spent serializing results",
            "counter",
            p.serde_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_serde_count_total",
            "Result serialization operations",
            "counter",
            p.serde_count,
        );
        push_metric(
            &mut body,
            "sombra_query_read_guard_ns_total",
            "Total nanoseconds spent acquiring read guards",
            "counter",
            p.query_read_guard_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_read_guard_count_total",
            "Read guard acquisitions",
            "counter",
            p.query_read_guard_count,
        );
        push_metric(
            &mut body,
            "sombra_query_stream_build_ns_total",
            "Total nanoseconds spent building query streams",
            "counter",
            p.query_stream_build_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_stream_build_count_total",
            "Query stream builds",
            "counter",
            p.query_stream_build_count,
        );
        push_metric(
            &mut body,
            "sombra_query_stream_iter_ns_total",
            "Total nanoseconds spent iterating query streams",
            "counter",
            p.query_stream_iter_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_stream_iter_count_total",
            "Query stream iterations",
            "counter",
            p.query_stream_iter_count,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_ns_total",
            "Total nanoseconds spent in property index ops",
            "counter",
            p.query_prop_index_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_count_total",
            "Property index operations",
            "counter",
            p.query_prop_index_count,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_lookup_ns_total",
            "Total nanoseconds spent in property index lookups",
            "counter",
            p.query_prop_index_lookup_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_lookup_count_total",
            "Property index lookups",
            "counter",
            p.query_prop_index_lookup_count,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_encode_ns_total",
            "Total nanoseconds spent encoding property index values",
            "counter",
            p.query_prop_index_encode_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_encode_count_total",
            "Property index encodings",
            "counter",
            p.query_prop_index_encode_count,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_stream_build_ns_total",
            "Total nanoseconds spent building property index streams",
            "counter",
            p.query_prop_index_stream_build_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_stream_build_count_total",
            "Property index stream builds",
            "counter",
            p.query_prop_index_stream_build_count,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_stream_iter_ns_total",
            "Total nanoseconds spent iterating property index streams",
            "counter",
            p.query_prop_index_stream_iter_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_prop_index_stream_iter_count_total",
            "Property index stream iterations",
            "counter",
            p.query_prop_index_stream_iter_count,
        );
        push_metric(
            &mut body,
            "sombra_query_expand_ns_total",
            "Total nanoseconds spent expanding graph edges",
            "counter",
            p.query_expand_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_expand_count_total",
            "Graph edge expansions",
            "counter",
            p.query_expand_count,
        );
        push_metric(
            &mut body,
            "sombra_query_filter_ns_total",
            "Total nanoseconds spent filtering results",
            "counter",
            p.query_filter_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_filter_count_total",
            "Filter operations",
            "counter",
            p.query_filter_count,
        );
        push_metric(
            &mut body,
            "sombra_query_exec_p50_ns",
            "Approximate p50 query execution latency (ns)",
            "gauge",
            p.exec_p50_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_exec_p90_ns",
            "Approximate p90 query execution latency (ns)",
            "gauge",
            p.exec_p90_ns,
        );
        push_metric(
            &mut body,
            "sombra_query_exec_p99_ns",
            "Approximate p99 query execution latency (ns)",
            "gauge",
            p.exec_p99_ns,
        );
    }

    // Storage stats.
    push_metric(
        &mut body,
        "sombra_storage_next_node_id",
        "Next available node id",
        "gauge",
        report.storage.next_node_id,
    );
    push_metric(
        &mut body,
        "sombra_storage_next_edge_id",
        "Next available edge id",
        "gauge",
        report.storage.next_edge_id,
    );
    push_metric(
        &mut body,
        "sombra_storage_estimated_node_count",
        "Estimated node count",
        "gauge",
        report.storage.estimated_node_count,
    );
    push_metric(
        &mut body,
        "sombra_storage_estimated_edge_count",
        "Estimated edge count",
        "gauge",
        report.storage.estimated_edge_count,
    );

    // Filesystem stats.
    push_metric(
        &mut body,
        "sombra_filesystem_db_size_bytes",
        "Database file size in bytes",
        "gauge",
        report.filesystem.db_size_bytes,
    );
    push_metric(
        &mut body,
        "sombra_filesystem_wal_size_bytes",
        "WAL size in bytes",
        "gauge",
        report.filesystem.wal_size_bytes,
    );

    // Storage profiling (if enabled via SOMBRA_PROFILE).
    if let Some(s) = storage_profile {
        push_metric(
            &mut body,
            "sombra_storage_pager_commit_ns_total",
            "Total nanoseconds spent committing through the pager",
            "counter",
            s.pager_commit_ns,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_commit_count_total",
            "Pager commit operations",
            "counter",
            s.pager_commit_count,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_commit_p50_ns",
            "Approximate p50 pager commit latency (ns)",
            "gauge",
            s.pager_commit_p50_ns,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_commit_p90_ns",
            "Approximate p90 pager commit latency (ns)",
            "gauge",
            s.pager_commit_p90_ns,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_commit_p99_ns",
            "Approximate p99 pager commit latency (ns)",
            "gauge",
            s.pager_commit_p99_ns,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_fsync_count_total",
            "Fsync calls issued by the pager",
            "counter",
            s.pager_fsync_count,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_wal_frames_total",
            "WAL frames written",
            "counter",
            s.pager_wal_frames,
        );
        push_metric(
            &mut body,
            "sombra_storage_pager_wal_bytes_total",
            "WAL bytes appended",
            "counter",
            s.pager_wal_bytes,
        );
        push_metric(
            &mut body,
            "sombra_storage_wal_coalesced_writes_total",
            "Coalesced WAL write batches (writev)",
            "counter",
            s.wal_coalesced_writes,
        );
        push_metric(
            &mut body,
            "sombra_storage_wal_reused_segments_total",
            "WAL segments reused",
            "counter",
            s.wal_reused_segments,
        );
        push_metric(
            &mut body,
            "sombra_storage_wal_commit_group_p50_frames",
            "Median WAL commit batch size (frames)",
            "gauge",
            s.wal_commit_group_p50,
        );
        push_metric(
            &mut body,
            "sombra_storage_wal_commit_group_p95_frames",
            "95th percentile WAL commit batch size (frames)",
            "gauge",
            s.wal_commit_group_p95,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_read_begin_ns_total",
            "Total nanoseconds spent starting reads (snapshot acquisition + registration)",
            "counter",
            s.mvcc_read_begin_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_read_begin_count",
            "Number of reads measured for start latency",
            "counter",
            s.mvcc_read_begin_count,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_begin_ns_total",
            "Total nanoseconds spent starting writes (writer lock + setup)",
            "counter",
            s.mvcc_write_begin_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_begin_count",
            "Number of writes measured for start latency",
            "counter",
            s.mvcc_write_begin_count,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_commit_ns_total",
            "Total nanoseconds spent committing writes (MVCC + pager)",
            "counter",
            s.mvcc_commit_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_commit_count",
            "Number of commits measured for latency",
            "counter",
            s.mvcc_commit_count,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_read_begin_p50_ns",
            "Approximate p50 read-begin latency (ns)",
            "gauge",
            s.mvcc_read_begin_p50_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_read_begin_p90_ns",
            "Approximate p90 read-begin latency (ns)",
            "gauge",
            s.mvcc_read_begin_p90_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_read_begin_p99_ns",
            "Approximate p99 read-begin latency (ns)",
            "gauge",
            s.mvcc_read_begin_p99_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_begin_p50_ns",
            "Approximate p50 write-begin latency (ns)",
            "gauge",
            s.mvcc_write_begin_p50_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_begin_p90_ns",
            "Approximate p90 write-begin latency (ns)",
            "gauge",
            s.mvcc_write_begin_p90_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_begin_p99_ns",
            "Approximate p99 write-begin latency (ns)",
            "gauge",
            s.mvcc_write_begin_p99_ns,
        );
        push_metric(
            &mut body,
            "sombra_mvcc_write_lock_conflicts_total",
            "Writer lock conflicts (writer already held)",
            "counter",
            s.mvcc_write_lock_conflicts,
        );
        push_metric(
            &mut body,
            "sombra_storage_btree_allocator_failures_total",
            "Leaf allocator failures (any reason)",
            "counter",
            s.btree_leaf_allocator_failures,
        );
        push_metric(
            &mut body,
            "sombra_storage_btree_allocator_compactions_total",
            "Leaf allocator compactions",
            "counter",
            s.btree_leaf_allocator_compactions,
        );
    }

    let mut response = Response::new(body.into());
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    Ok(response)
}

fn push_metric<T: std::fmt::Display>(
    buf: &mut String,
    name: &str,
    help: &str,
    metric_type: &str,
    value: T,
) {
    let _ = writeln!(buf, "# HELP {name} {help}");
    let _ = writeln!(buf, "# TYPE {name} {metric_type}");
    let _ = writeln!(buf, "{name} {value}");
}

#[cfg(not(feature = "bundled-dashboard"))]
async fn inline_index() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Sombra Dashboard</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 3rem; line-height: 1.5; }
      code { background: #f4f4f4; padding: 0.1rem 0.3rem; border-radius: 4px; }
    </style>
  </head>
  <body>
    <main>
      <h1>No dashboard assets available</h1>
      <p>
        This binary was built without embedded dashboard assets and no <code>--assets</code>
        directory was provided.
      </p>
      <p>
        Build the frontend bundle and pass
        <code>--assets /path/to/packages/dashboard/build/client</code>
        to <code>sombra dashboard</code> to serve the compiled UI.
      </p>
    </main>
  </body>
</html>"#,
    )
}

#[cfg(feature = "bundled-dashboard")]
async fn embedded_dashboard_handler(method: Method, uri: Uri) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let request_path = uri.path().to_owned();

    match embedded_file_for_path(request_path) {
        Some(file) => {
            let mut response = build_embedded_response(file);
            if method == Method::HEAD {
                *response.body_mut() = Body::empty();
            }
            response
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[cfg(feature = "bundled-dashboard")]
fn embedded_file_for_path(path: String) -> Option<&'static EmbeddedFile> {
    let sanitized = sanitize_asset_path(path.as_str())?;
    if let Some(file) = EMBEDDED_DASHBOARD_ASSETS.get_file(sanitized.as_str()) {
        return Some(file);
    }

    if sanitized.ends_with('/') {
        let mut nested = sanitized.clone();
        nested.push_str("index.html");
        if let Some(file) = EMBEDDED_DASHBOARD_ASSETS.get_file(&nested) {
            return Some(file);
        }
    }

    if sanitized.starts_with("assets/") || sanitized.contains('.') {
        return None;
    }

    EMBEDDED_DASHBOARD_ASSETS.get_file("index.html")
}

#[cfg(feature = "bundled-dashboard")]
fn sanitize_asset_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.contains("..") {
        return None;
    }
    if trimmed.is_empty() {
        return Some("index.html".to_string());
    }
    Some(trimmed.to_string())
}

#[cfg(feature = "bundled-dashboard")]
fn build_embedded_response(file: &'static EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(file.path()).first_or_octet_stream();
    let body = Body::from(Bytes::from_static(file.contents()));
    let mut response = Response::new(body);

    let value = HeaderValue::from_str(mime.as_ref())
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    response.headers_mut().insert(CONTENT_TYPE, value);

    response
}

async fn stats_handler(State(state): State<AppState>) -> Result<Json<StatsReport>, AppError> {
    let path = state.db_path.clone();
    let opts = state.open_opts.clone();
    let report = task::spawn_blocking(move || admin::stats(path, &opts)).await??;
    Ok(Json(report))
}

async fn labels_handler(
    State(state): State<AppState>,
) -> Result<Json<LabelListResponse>, AppError> {
    let db = state.open_database()?;
    let labels = db.sample_labels(1_000, 32).map_err(|err| {
        tracing::error!(?err, "label sampling failed");
        err
    })?;
    let mapped = labels
        .into_iter()
        .map(|(name, count)| LabelEntry { name, count })
        .collect();
    Ok(Json(LabelListResponse { labels: mapped }))
}

async fn ensure_label_indexes_handler(
    State(state): State<AppState>,
    Json(payload): Json<EnsureLabelIndexesRequest>,
) -> Result<Json<EnsureLabelIndexesResponse>, AppError> {
    if state.read_only {
        return Err(AppError::ReadOnly);
    }
    if payload.labels.is_empty() {
        return Ok(Json(EnsureLabelIndexesResponse { created: 0 }));
    }
    let db = state.open_database()?;
    let created = db.ensure_label_indexes(&payload.labels)?;
    Ok(Json(EnsureLabelIndexesResponse { created }))
}

async fn query_handler(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let db = state.open_database()?;
    if let Some(limit) = params.clamped_max_rows() {
        let stream = db.stream_json(&payload)?;
        let mut rows = Vec::new();
        for _ in 0..limit {
            match stream.next()? {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        let truncated = if rows.len() == limit {
            stream.next()?.is_some()
        } else {
            false
        };
        let response = limited_query_payload(&payload, rows, limit, truncated);
        return Ok(Json(response));
    }
    let result = db.execute_json(&payload)?;
    Ok(Json(result))
}

/// Returns all nodes and edges in the database as a compact JSON payload.
/// Response format: `{ nodes: [...], edges: [...] }`
async fn full_graph_handler(
    State(state): State<AppState>,
) -> Result<Json<FullGraphResponse>, AppError> {
    let db = state.open_database()?;

    // First, get all labels to query by
    let labels = db.sample_labels(10_000, 1000).map_err(|err| {
        tracing::error!(?err, "label sampling for full graph failed");
        err
    })?;

    let mut all_nodes = Vec::new();
    let mut seen_node_ids = std::collections::HashSet::new();

    // Query nodes for each label
    for (label, _count) in &labels {
        let nodes_query = serde_json::json!({
            "$schemaVersion": 1,
            "matches": [{"var": "n", "label": label}],
            "edges": [],
            "projections": [{"kind": "var", "var": "n"}],
            "distinct": false
        });
        let nodes_stream = db.stream_json(&nodes_query)?;
        while let Some(row) = nodes_stream.next()? {
            if let Value::Object(map) = row {
                if let Some(node) = map.get("n") {
                    if let Some(id) = node.get("_id").and_then(|v| v.as_u64()) {
                        if seen_node_ids.insert(id) {
                            all_nodes.push(node.clone());
                        }
                    }
                }
            }
        }
    }

    // Query edges for each pair of labels
    let mut all_edges = Vec::new();
    let mut seen_edges = std::collections::HashSet::new();
    
    for (source_label, _) in &labels {
        for (target_label, _) in &labels {
            let edges_query = serde_json::json!({
                "$schemaVersion": 1,
                "matches": [
                    {"var": "a", "label": source_label},
                    {"var": "b", "label": target_label}
                ],
                "edges": [
                    {"from": "a", "to": "b", "direction": "out"}
                ],
                "projections": [
                    {"kind": "var", "var": "a"},
                    {"kind": "var", "var": "b"}
                ],
                "distinct": false
            });
            let edges_stream = db.stream_json(&edges_query)?;
            while let Some(row) = edges_stream.next()? {
                if let Value::Object(map) = row {
                    if let (Some(Value::Object(a)), Some(Value::Object(b))) =
                        (map.get("a"), map.get("b"))
                    {
                        let source_id = a.get("_id").and_then(|v| v.as_u64()).unwrap_or(0);
                        let target_id = b.get("_id").and_then(|v| v.as_u64()).unwrap_or(0);
                        let edge_key = (source_id, target_id);
                        if seen_edges.insert(edge_key) {
                            let mut edge_obj = serde_json::Map::new();
                            edge_obj.insert("_id".to_string(), Value::Number((seen_edges.len() as u64).into()));
                            edge_obj.insert("_source".to_string(), Value::Number(source_id.into()));
                            edge_obj.insert("_target".to_string(), Value::Number(target_id.into()));
                            all_edges.push(Value::Object(edge_obj));
                        }
                    }
                }
            }
        }
    }

    Ok(Json(FullGraphResponse { nodes: all_nodes, edges: all_edges }))
}

#[derive(Debug, Serialize)]
struct FullGraphResponse {
    nodes: Vec<Value>,
    edges: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    read_only: bool,
}

#[derive(Debug, Serialize)]
struct ReadyResponse {
    status: &'static str,
    read_only: bool,
    checks: Vec<HealthCheck>,
}

#[derive(Debug, Serialize)]
struct HealthCheck {
    name: &'static str,
    ok: bool,
    message: Option<String>,
}

struct ReadyReport {
    ok: bool,
    checks: Vec<HealthCheck>,
}

fn evaluate_readiness(state: &ServerState) -> ReadyReport {
    let mut checks = Vec::new();

    let db_exists = state.db_path.exists();
    checks.push(HealthCheck {
        name: "db_path_exists",
        ok: db_exists,
        message: if db_exists {
            None
        } else {
            Some(format!(
                "database path missing: {}",
                state.db_path.display()
            ))
        },
    });

    let db_open_check = if db_exists {
        match state.open_database() {
            Ok(db) => {
                drop(db);
                HealthCheck {
                    name: "db_open",
                    ok: true,
                    message: None,
                }
            }
            Err(err) => HealthCheck {
                name: "db_open",
                ok: false,
                message: Some(err.to_string()),
            },
        }
    } else {
        HealthCheck {
            name: "db_open",
            ok: false,
            message: Some("database path missing".to_string()),
        }
    };

    checks.push(db_open_check);

    let stats_start = Instant::now();
    match admin::stats(state.db_path.clone(), &state.open_opts) {
        Ok(report) => {
            checks.push(HealthCheck {
                name: "stats",
                ok: true,
                message: None,
            });

            checks.push(HealthCheck {
                name: "stats_latency_ms",
                ok: true,
                message: Some(format!("{}ms", stats_start.elapsed().as_millis())),
            });

            let wal_path = PathBuf::from(report.wal.path.clone());
            let wal_exists = report.wal.exists;
            checks.push(HealthCheck {
                name: "wal_exists",
                ok: wal_exists,
                message: if wal_exists {
                    None
                } else {
                    Some(format!("wal file missing: {}", wal_path.display()))
                },
            });

            let wal_dir_check = wal_path
                .parent()
                .map(|dir| {
                    let meta = fs::metadata(dir);
                    let writable = meta.map(|m| !m.permissions().readonly()).unwrap_or(false);
                    HealthCheck {
                        name: "wal_dir_writable",
                        ok: writable,
                        message: if writable {
                            None
                        } else {
                            Some(format!("wal directory not writable: {}", dir.display()))
                        },
                    }
                })
                .unwrap_or(HealthCheck {
                    name: "wal_dir_writable",
                    ok: false,
                    message: Some("wal directory missing".to_string()),
                });
            checks.push(wal_dir_check);

            let wal_dir_tempfile_check = wal_path
                .parent()
                .map(|dir| {
                    if state.read_only {
                        return HealthCheck {
                            name: "wal_dir_tempfile",
                            ok: true,
                            message: Some("skipped (read_only mode)".to_string()),
                        };
                    }
                    match TempFileBuilder::new()
                        .prefix(".sombra-health")
                        .tempfile_in(dir)
                    {
                        Ok(_) => HealthCheck {
                            name: "wal_dir_tempfile",
                            ok: true,
                            message: None,
                        },
                        Err(err) => HealthCheck {
                            name: "wal_dir_tempfile",
                            ok: false,
                            message: Some(format!("wal dir temp file failed: {err}")),
                        },
                    }
                })
                .unwrap_or(HealthCheck {
                    name: "wal_dir_tempfile",
                    ok: false,
                    message: Some("wal directory missing".to_string()),
                });
            checks.push(wal_dir_tempfile_check);

            if let Some(allocation_error) = report.wal.allocation_error {
                checks.push(HealthCheck {
                    name: "wal_allocation_error",
                    ok: false,
                    message: Some(allocation_error),
                });
            } else {
                checks.push(HealthCheck {
                    name: "wal_allocation_error",
                    ok: true,
                    message: None,
                });
            }

            let reader_age_ms = report.pager.mvcc_reader_max_age_ms;
            let reader_age_ok = reader_age_ms <= MVCC_READER_WARN_THRESHOLD_MS;
            checks.push(HealthCheck {
                name: "mvcc_reader_max_age_ms",
                ok: reader_age_ok,
                message: Some(format!(
                    "max_age_ms={reader_age_ms} threshold_ms={MVCC_READER_WARN_THRESHOLD_MS}"
                )),
            });

            checks.push(HealthCheck {
                name: "last_checkpoint_lsn",
                ok: true,
                message: Some(format!(
                    "last_checkpoint_lsn={}",
                    report.pager.last_checkpoint_lsn
                )),
            });
        }
        Err(err) => {
            checks.push(HealthCheck {
                name: "stats",
                ok: false,
                message: Some(err.to_string()),
            });
        }
    }

    let ok = checks.iter().all(|check| check.ok);

    ReadyReport { ok, checks }
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Admin(#[from] admin::AdminError),
    #[error(transparent)]
    State(#[from] DashboardError),
    #[error("internal task failure: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Query(#[from] ffi::FfiError),
    #[error("mutating endpoint is disabled in read-only mode")]
    ReadOnly,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::ReadOnly => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = axum::Json(ErrorPayload {
            message: self.to_string(),
        });
        (status, body).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorPayload {
    message: String,
}

#[derive(Debug, Serialize)]
struct LabelListResponse {
    labels: Vec<LabelEntry>,
}

#[derive(Debug, Serialize)]
struct LabelEntry {
    name: String,
    count: u64,
}

#[derive(Debug, Deserialize)]
struct EnsureLabelIndexesRequest {
    labels: Vec<String>,
}

#[derive(Debug, Serialize)]
struct EnsureLabelIndexesResponse {
    created: usize,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
struct QueryParams {
    #[serde(default)]
    max_rows: Option<usize>,
}

impl QueryParams {
    fn clamped_max_rows(&self) -> Option<usize> {
        self.max_rows.map(|value| value.clamp(1, MAX_STREAMED_ROWS))
    }
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(err) => tracing::error!(?err, "failed to listen for shutdown signal"),
    }
}

fn install_tracing_subscriber() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = fmt().with_env_filter(filter).try_init();
    });
}

fn limited_query_payload(spec: &Value, rows: Vec<Value>, limit: usize, truncated: bool) -> Value {
    let mut map = Map::new();
    map.insert("request_id".into(), request_id_from_spec(spec));
    map.insert("features".into(), Value::Array(Vec::new()));
    map.insert("rows".into(), Value::Array(rows));
    map.insert(
        "row_limit".into(),
        Value::Number(Number::from(limit as u64)),
    );
    map.insert("truncated".into(), Value::Bool(truncated));
    Value::Object(map)
}

fn request_id_from_spec(spec: &Value) -> Value {
    if let Value::Object(map) = spec {
        if let Some(Value::String(rid)) = map.get("request_id").or_else(|| map.get("requestId")) {
            if !rid.trim().is_empty() {
                return Value::String(rid.clone());
            }
        }
    }
    Value::Null
}
