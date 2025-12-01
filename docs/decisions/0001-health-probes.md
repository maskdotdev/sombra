# 0001 – Health endpoints split (liveness vs readiness)

- **Date:** 2024-11-22
- **Owner:** You

## Context
- The Axum API server previously exposed a single `/health` endpoint used by the dashboard for a simple status/read-only flag.
- There was no readiness signal to distinguish “process is up” from “can serve traffic” (e.g., database reachable, WAL path present).
- Even for a personal setup, having probes makes it easier to script basic checks or plug into a future supervisor.

## Decision
- Keep `/health` but make it reflect readiness status (`ok` vs `error`) while preserving the existing payload shape (`status`, `read_only`) for the UI.
- Add `/health/live` for liveness (always `ok` if the process is up).
- Add `/health/ready` for readiness with detailed checks.
- Readiness checks (current scope):
  - Database path exists.
  - Database can be opened (admin options).
  - WAL file presence.
  - WAL directory not marked read-only.
  - Last checkpoint LSN surfaced as a message (informational).

## Alternatives considered
- Keep a single `/health` with minimal checks: rejected—no granularity for routing/monitoring.
- Add aggressive write probes (temp file creation): rejected for now to avoid touching the filesystem in probes.
- Hook readiness into full stats + query: deferred; the current check already calls `admin::stats` for lightweight validation.

## Impact / follow-ups
- Dashboard continues to call `/health`; you can point liveness probes to `/health/live` and readiness to `/health/ready`.
- Future: add writable/WAL sync checks that don’t create files, surface last stats fetch latency, and (when replication exists) include apply lag in readiness.
- No migration/compat concerns; payload shapes for existing UI remain the same.
