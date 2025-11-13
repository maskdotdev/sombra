Dashboard Delivery Plan
=======================

Repository Layout
-----------------
- `packages/api-server`: Rust HTTP server layer that hosts the dashboard UI and exposes JSON APIs. Empty placeholder for now.
- `packages/dashboard`: Frontend application (e.g., Vite + React/Svelte) that compiles to static assets served by the API server.

Phase 1 – Foundations
---------------------
1. Inventory reusable admin/query functionality in `src/bin/cli.rs`, `src/admin`, and `src/query`.
2. Decide on async/web stack (`tokio`, `axum`, `tower-http`, `serde_json`) and extend `Cargo.toml`.
3. Define security defaults (loopback bind, optional auth token, read-only mode toggle).

Phase 1 Findings
----------------
### Reusable Admin Capabilities
- `src/bin/cli.rs` routes subcommands to `admin::{stats, checkpoint, vacuum_into, verify}` and the CSV import/export helpers, so those reports and summaries are already serializable without additional DTO work.
- `docs`: `StatsReport`, `CheckpointReport`, `VacuumReport`, and `VerifyReport` (all in `src/admin`) derive `Serialize`, meaning HTTP endpoints can return them directly as JSON.
- `AdminOpenOptions` + `build_open_options` encapsulate pager tuning flags; `admin::util::open_graph` and `GraphHandle` provide shared `Arc<Pager/Graph/Dict>` handles that the server can keep alive for the session.
- `src/cli/import_export.rs` exposes `run_import`/`run_export` plus the `NodeImportConfig`/`EdgeImportConfig` types we can deserialize from HTTP payloads instead of reimplementing CSV plumbing.

### Query Execution Entry Points
- `src/ffi/mod.rs` already wraps the planner/executor via `Database` and exposes JSON-friendly `QuerySpec`, `ExplainSpec`, `MutationSpec`, `CreateScript`, etc. It handles request IDs, cancellation tokens, and profile snapshots.
- The FFI layer enforces payload size limits, schema-version negotiation, and `spawn`-friendly streaming (`QueryStream`), so the HTTP API can lean on those structs instead of inventing a new query DSL.
- `CatalogMetadata` + `Planner` + `Executor` are all constructed inside `Database::open`, ensuring a single call wires together metadata and shared `Arc` handles—ideal for the dashboard server lifecycle.

### Notable Gaps / Requirements
- No async runtime or HTTP framework is wired in today; serving traffic means moving the CLI binary to `tokio::main` and adding the `axum`/`tower-http` stack defined in Phase 2.
- There is no long-lived process that keeps the DB open; every CLI command opens/closes handles per invocation. The dashboard server must manage lifetime + graceful shutdown.
- Long operations (vacuum, import, verify) are synchronous and blocking; exposing them over HTTP will require background job tracking to keep the API responsive.
- Query submission currently expects well-formed JSON `QuerySpec`; we will need client-side helpers or validation in the dashboard UI so users can craft specs without hand-writing JSON.

Phase 1 Decisions
-----------------
### Async/Web Stack
- **Runtime:** `tokio` (rt-multi-thread + macros + signal + time + fs). This lets the CLI binary host the HTTP server, handle Ctrl+C gracefully, and offload blocking admin work via `spawn_blocking`.
- **Router:** `axum 0.7` with `macros`, `json`, and `ws` (future live updates) features to keep the surface ergonomic and composable with tower layers.
- **Middleware & static assets:** `tower-http 0.5` with `trace`, `cors`, `compression-full`, `timeout`, `catch-panic`, and `fs` so we can add logging, optional CORS for remote dashboards, gzip/brotli, and `ServeDir` for the frontend bundle.
- **Observability:** Promote `tracing` (already a dependency) plus `tracing-subscriber` as a normal (non-dev) dependency so the server can emit structured HTTP/application logs.
- **Utilities:** Reuse existing `serde`/`serde_json` for payloads; no extra JSON crates needed. Rely on existing `rand` for auth token generation.

### Security Baseline
- Bind to `127.0.0.1` by default with a CLI flag (`--bind`) required for remote access; require explicit `--allow-origin` to enable CORS when running remotely.
- Ship the MVP in read-only mode by default; disable import/vacuum/checkpoint/mutation routes entirely so the dashboard can be run safely against production replicas without extra auth. Token-based auth will be revisited once mutating endpoints ship.
- Enforce CSRF protection by avoiding cookie auth entirely when tokens arrive; for now rely on loopback-only defaults and log any remote access attempts.
- Default to serving static assets from the embedded React bundle (via `include_dir`) with `Content-Security-Policy: default-src 'self'` and `Referrer-Policy: no-referrer`. Allow users to point `--assets` at a custom bundle but keep CSP strict unless `--relax-csp` is provided.

Dashboard MVP Behavior
----------------------
- `sombra dashboard` spins up an Axum server with `/health` and `/api/stats` plus a static-file layer that serves the embedded bundle by default. Supplying `--assets <dir>` overrides the bundle; when neither embedded assets nor an override exist a minimal inline HTML page explains how to build and provide the frontend bundle.
- The server currently exposes read-only APIs only (`/health`, `/api/stats`, `/api/query`); mutations/import/export routes remain disabled until auth and background job infrastructure land.

Phase 2 – CLI Surface & Runtime
-------------------------------
1. Add `Command::Dashboard` with flags such as `--bind`, `--port`, `--assets`, `--read-only`, `--open-browser`.
2. Factor shared helper that converts `OpenArgs` into `AdminOpenOptions`.
3. Switch the CLI binary to a `tokio::main` entrypoint; wrap blocking DB calls with `tokio::task::spawn_blocking`.
4. Create `src/cli/dashboard` module that opens the DB via `open_graph`, constructs shared state (pager/graph/dict), and starts the server.

Phase 3 – HTTP API
------------------
1. Build `axum::Router` with middleware for logging (and CORS if needed).
2. `GET /api/stats`: call `admin::stats` and return `StatsReport`.
3. `POST /api/query`: accept JSON query payload, run planner/executor, stream JSON rows.
4. Maintenance endpoints: `POST /api/checkpoint`, `POST /api/vacuum`, `POST /api/import` (disabled when `--read-only`).
5. Background job manager for long-running tasks plus `/api/jobs` status endpoint.

Phase 4 – Frontend Assets
-------------------------
1. Scaffold SPA in `packages/dashboard` (Vite + React/Svelte) with environment-driven API base URL.
2. Pages: Overview (stats), Query console, Maintenance controls & job list.
3. Simple API client wrapper for `/api/*` endpoints; poll or use WebSockets for job updates.
4. Build pipeline emits static files to `packages/dashboard/build/client`; embed them with `include_dir`.

Phase 5 – UX & Ops Polish
-------------------------
1. CLI flag to auto-open default browser; implement port conflict retries.
2. Configurable auth token or OS-auth integration.
3. Structured logging for HTTP requests and admin events.
4. Graceful shutdown on SIGINT/SIGTERM (stop accepting traffic, wait for jobs, close DB cleanly).

Phase 6 – Testing & Documentation
---------------------------------
1. Integration test spins up dashboard on ephemeral port, exercises `/api/stats` and `/api/query`.
2. Unit tests for job manager and key handlers using `axum::Router::oneshot`.
3. Document setup and usage in `docs/dashboard.md` plus README updates advertising `sombra dashboard`.
4. Extend CI to build the frontend bundle and run dashboard integration tests.

Phase 7 – Release & Follow-Ups
------------------------------
1. Smoke-test binaries on macOS/Linux; verify frontend build artifacts bundled correctly.
2. Add CI artifact or release checklist for shipping `sombra dashboard`.
3. Post-release backlog: role-based access, multi-DB switcher, live charts, telemetry opt-in.
