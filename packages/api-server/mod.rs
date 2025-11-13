//! Dashboard API server implementation (experimental).

use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use axum::{
    extract::{Query, State},
    http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderValue, Method, StatusCode,
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use hyper::Error as HyperError;
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

struct ServerState {
    db_path: PathBuf,
    open_opts: AdminOpenOptions,
    read_only: bool,
    assets_dir: Option<PathBuf>,
    allow_origins: Vec<String>,
}

impl ServerState {
    fn new(opts: DashboardOptions) -> Result<Self, DashboardError> {
        Ok(Self {
            db_path: opts.db_path,
            open_opts: opts.open_opts,
            read_only: opts.read_only,
            assets_dir: opts.assets_dir,
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
        assets_dir = ?state.assets_dir,
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

    if let Some(dir) = state.assets_dir.clone() {
        let service = ServeDir::new(dir).append_index_html_on_directories(true);
        router = router.fallback_service(service);
    } else {
        router = router.route("/", get(inline_index));
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
      <h1>Sombra dashboard assets not configured</h1>
      <p>
        Build the frontend bundle and pass
        <code>--assets /path/to/dist</code>
        to <code>sombra dashboard</code> to serve the compiled UI.
      </p>
    </main>
  </body>
</html>"#,
    )
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
