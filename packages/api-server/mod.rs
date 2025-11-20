//! Dashboard API server implementation (experimental).
#![allow(clippy::field_reassign_with_default)]

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock},
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

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
    let cors = build_cors_layer(&state.allow_origins);

    let mut router = Router::new()
        .route("/health", get(health_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/query", post(query_handler))
        .route("/api/labels", get(labels_handler))
        .route("/api/labels/indexes", post(ensure_label_indexes_handler));

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

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        read_only: state.read_only,
    })
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

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    read_only: bool,
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
