Production Readiness Plan
=========================

Phased, actionable roadmap to take Sombra to production. Each phase lists: goal, concrete tasks (checkboxes), and exit criteria. Owners should mark checkboxes as work completes and link to design docs, PRs, and test runs from this file.

Quick links
- Tracker: docs/production-readiness.md (this file)
- Runbooks index: docs/runbooks/README.md
- Test matrices: docs/test-matrices/README.md
- Decision log: docs/decisions/README.md
- Prod sanity test runner: scripts/run-prod-sanity.sh
- Key references: docs/mvcc-baseline.md, docs/mvcc-durability.md, docs/mvcc-optimization-plan.md, docs/dashboard-plan.md, docs/benchmarks.md

Owner roster (fill in)
| Area | DRI | Backup | Notes |
| --- | --- | --- | --- |
| Isolation / Txn / MVCC | You | — | Solo owner; track backlog in this doc. |
| Storage / IO / WAL / GC | You | — | Same owner; note risks in decision log. |
| Replication / Backup / HA | You | — | Currently deferred; single-node only. |
| Security / Multi-tenancy | You | — | Currently deferred; document gaps. |
| Observability / Operations | You | — | Lightweight metrics/health only; no infra setup. |

Decision cadence
- Quick self-review weekly (15–30 minutes) to update checkboxes and risks.
- Lightweight ADRs: one-page note per decision (why/what/scope/rollback) stored under `docs/decisions/` (link from this file).
- Runbook + test-matrix updates are required with each change that affects behavior or coverage.

Phase 1 (1–2 weeks): Scope, observability, test matrices
--------------------------------------------------------
Goal
- Align on what “prod-ready” means, who owns each area, and gain enough visibility and environments to harden safely.

Workstreams & tasks
- [x] Ownership and docs
  - [x] Assign DRIs for isolation/txn engine, storage/IO/WAL/GC, replication/backup/HA, security/multi-tenancy, observability/operations.
  - [ ] Create a prod-readiness doc index: master doc, per-area design docs, runbooks folder, test matrices folder, decision log (lightweight ADRs). (Index + folders exist; need ADR log stub under `docs/decisions/`.)
  - [x] Set decision cadence: weekly readiness review; rules for when an ADR is required.
- [ ] Observability bootstrap
  - [ ] Define minimal metrics: p50/p90/p99 for read/write/commit; active/blocked txns; deadlock count; WAL bytes/s; WAL coalesce lag; checkpoint duration/backlog; fsync/flush counts/errors; GC debt or queue length; oldest active snapshot age; replication lag/apply delay.
  - [ ] Implement missing metrics in txn/storage hot paths; export via standard stack and Axum API server. Keep scope minimal for single-node: expose counters/histograms, skip external infra.
    - [x] Export snapshot gauges via `/metrics` (pager cache hits/misses/evictions, WAL size/segments, MVCC reader age, overlay counts, storage sizes).
    - [x] Export exec latency percentiles (p50/p90/p99) from profiling (`SOMBRA_PROFILE=1`), still missing txn-level histograms and blocked/deadlock counters.
    - [x] Export storage profiling counters (pager commit/fsync/WAL bytes/frames, WAL batch sizes, allocator failures) via `/metrics` when profiling is enabled.
    - [x] Export pager commit latency percentiles (p50/p90/p99) via `/metrics` when profiling is enabled.
    - [x] Export MVCC read/write/commit latency totals + counts via `/metrics` (profiling on by default).
  - [x] Health endpoints: `/health/live` (process/resources) and `/health/ready` (latency budget, WAL writable, checkpoint not stalled, replication lag under budget). Implemented as `/health`, `/health/live`, `/health/ready` with WAL dir writability, allocator errors, MVCC reader age checks.
  - [ ] Initial dashboards (optional for personal use): “Txn & Latency”, “Storage & WAL”, “Replication & GC” with alerts (WAL stall, checkpoint overrun, GC lag, replication lag, deadlock spike).
- [ ] Test matrices & environments
  - [x] Draft test matrices for isolation/MVCC, crash safety/storage, GC/epochs, replication/HA, backup/restore, security, graph-specific invariants. (See `docs/test-matrices/README.md`.)
  - [ ] Decide automation strategy: which suites run in CI vs nightly/soak (tag rows with cadence; add scheduler for nightly/weekly runs). For personal use, keep manual/nightly optional.
  - [ ] Stand up environments: single-node crash-test (local ok); defer multi-node replication + failure injection until needed; soak box optional.
- [ ] Filesystem baseline
  - [ ] Pick supported OS/filesystem matrix (tiered support).
  - [ ] Document expected fsync semantics and platform footguns.
  - [ ] Plan fio/disk characterization runs (seq/random throughput; fsync latency).

Exit criteria
- [x] DRIs and decision rhythm documented.
- [ ] Prod-readiness doc index and per-area skeletons exist.
- [ ] Minimal metrics + health endpoints live; dashboards created. (Health endpoints + `/metrics` export are live; dashboards and latency histograms remain.)
- [ ] Test matrices drafted; required environments identified/allocated.

Phase 2 (2–4 weeks): Single-node correctness, safety, security, backup
----------------------------------------------------------------------
Goal
- Make a single node trustworthy under concurrency and crashes, with basic security and backup/restore automation.

Workstreams & tasks
- [ ] Isolation & MVCC correctness
  - [ ] Document guarantees: Snapshot Isolation baseline; Serializable via SSI or predicate/range locks; fairness/starvation rules; “snapshot too old” semantics.
  - [ ] Implement/verify: predicate locking or SSI dependency tracking; conflict rules (e.g., first-committer-wins); long-snapshot impact on GC; reader/writer fairness; deadlock detection/timeout/backoff policies.
  - [ ] Tests: txn anomaly suite (Jepsen-style or equivalent) for SI/Serializable; long-lived snapshot under write pressure; unique constraint races; secondary index visibility/undo; no-wait/backoff behavior.
- [ ] Crash safety & storage
  - [ ] WAL discipline: enforce pageLSN rules; write-ahead + group commit ordering; durable directory fsyncs on create/rename; torn-write protection strategy.
  - [ ] Page format: checksums; alignment; versioned page formats; bounded version chains; point-lookup fast path.
  - [ ] Crash tests: crash/recovery loops (kill -9/power-fail); partial WAL segments (truncated/corrupted); torn-page injection; page/log validation tool.
- [ ] GC & epochs
  - [ ] Implement global safe point/epoch advancement rules; consider replication lag/logical decoding.
  - [ ] Configurable “snapshot too old” thresholds with clear surfacing (errors, not stalls).
  - [ ] GC logic: bounded version chains; predictable reclamation; secondary index vacuum/repair.
  - [ ] Tests: GC under long snapshots; safe-point advancement with lagging replicas; snapshot-too-old surfaced correctly.
- [ ] Security & multi-tenancy (basics)
  - [ ] TLS everywhere (client-server and server-server). (Deferred for local-only MVP dashboard; revisit post-MVP.)
  - [ ] Authentication: certs/passwords/tokens or pluggable auth; RBAC for common operations. (Deferred for local-only MVP dashboard; revisit post-MVP.)
  - [ ] Tenant isolation foundations in code (no cross-tenant leakage; cache boundaries).
  - [ ] Tests: authz matrix; basic key/cert rotation drill.
- [ ] Backup & restore automation
  - [ ] Define consistency point (LSN) and PITR plan (WAL + base backup).
  - [ ] Implement online backup at consistent LSN; scripts/operators for full backup, WAL archive, and restore.
  - [ ] Tests: periodic restore-from-backup; PITR to arbitrary LSN; corrupted backup detection (checksums).
- [ ] Filesystem reality checks (implementation)
  - [ ] Validate fsync semantics per supported platform; directory fsync after create/rename; aligned I/O checks.
  - [ ] Run platform crash/FSYNC matrix tests; record recommended defaults.

Exit criteria
- [ ] Declared isolation levels pass anomaly tests.
- [ ] Crash-recovery and fsync correctness proven on supported platforms.
- [ ] GC + “snapshot too old” behavior defined, observable, and tested.
- [ ] TLS + basic authz on by default for prod mode.
- [ ] Backup + restore + PITR rehearsed end-to-end.

Phase 3 (4–6 weeks): Replication, HA, upgrades, graph invariants, fuzzing
-------------------------------------------------------------------------
Goal
- Make clusters solid: replication/HA semantics, on-disk versioning and upgrades, graph invariants under churn, and aggressive fuzzing/Jepsen testing.

Workstreams & tasks
- [ ] Replication & HA
  - [ ] Define roles: leader/follower; sync vs async replication; client retry semantics and idempotency rules.
  - [ ] Lag/rollback budgets; acceptable divergence and rollback after promotion.
  - [ ] Monitoring: WAL shipping rate, apply lag; replica snapshot semantics (served LSN).
  - [ ] Tests: planned/unplanned failover drills; split-brain prevention (partitions); lag-induced snapshot expiration; rollback after promotion and traffic re-routing.
- [ ] On-disk format & upgrades
  - [ ] Versioned page/WAL formats; backward/forward compatibility rules; feature flags for new formats.
  - [ ] Upgrade/downgrade path: mixed-version cluster rules; rolling restart strategy; bounded downgrade plan.
  - [ ] Tools/tests: format validators; mixed-version cluster tests; rolling upgrade rehearsal; clear errors on format mismatch.
- [ ] Graph-specific invariants
  - [ ] Enforce: no dangling edges; atomic adjacency updates across pages; supernode strategy; transactional referential integrity.
  - [ ] Tests: concurrent vertex/edge churn; large-degree node handling; crash during adjacency update (no corruption/dangling pointers).
- [ ] Hardening & fuzzing
  - [ ] Property-based tests and fuzzers for WAL/page parsers and txn state machines.
  - [ ] Protocol-level fuzzing (invalid/partial queries).
  - [ ] Crash/soak loops under mixed workloads; long-read under heavy writes.
  - [ ] Jepsen-style anomaly hunts: partitions, clock skew, leader flaps.
  - [ ] Background scrubbing: periodic corruption scanner; policy for auto-repair vs fail-fast.

Exit criteria
- [ ] Failover/promotion flows rehearsed and documented.
- [ ] Replication lag and rollback budgets enforced and observed; split-brain protections validated.
- [ ] Online upgrade path works in mixed-version cluster tests.
- [ ] Graph invariants validated under churn and crash.
- [ ] Fuzzing + Jepsen suites running and feeding bugs to backlog.

Phase 4 (ongoing): Operations, SLOs, capacity, drills
-----------------------------------------------------
Goal
- Operate the service reliably: mature runbooks, tuned SLOs/alerts, capacity controls, and recurring drills.

Workstreams & tasks
- [ ] Runbooks & operations
  - [ ] Failover/promotion; restore/PITR; snapshot-too-old; GC debt/WAL blow-up; lock storms/deadlock spikes; replication lag/stuck replicas.
  - [ ] Each runbook includes diagnostics (metrics/dashboards), safe mitigations, page-vs-ticket guidance.
  - [ ] Operator walkthroughs/training recorded.
- [ ] SLOs, alerts, tuning
  - [ ] Define SLOs: availability; p50/p90/p99 latency budgets; durability guarantees.
  - [ ] Alerts: WAL stall; GC lag; replication lag; stuck locks/deadlock rate; checkpoint overruns; health endpoint semantics.
  - [ ] Chaos/load tests to validate alerting and tune thresholds; trace sampling in hot paths as needed.
- [ ] Capacity & multi-tenancy
  - [ ] Admission control and load shedding under memory/IO pressure.
  - [ ] Per-tenant quotas (storage, QPS, concurrency); defensive defaults to prevent unbounded WAL/GC debt.
  - [ ] Per-tenant usage dashboards; capacity planning guidelines.
- [ ] Long-term rituals
  - [ ] Recurring drills: restore/failover (monthly), disaster recovery simulation (semiannual), background scrubbing coverage checks, quarterly format/upgrade rehearsal in staging.
  - [ ] Residual risk backlog with owner + mitigation/acceptance date reviewed in readiness meetings.

Exit criteria (rolling)
- [ ] Runbooks complete and exercised in drills.
- [ ] SLOs/alerts tuned against real workloads; chaos/load tests recorded.
- [ ] Admission control and quotas enforced with dashboards.
- [ ] Recurring drill calendar active; residual risks tracked with owners/dates.

Mapping: checklist sections → primary phases
--------------------------------------------
- Scope/ownership → Phase 1; ongoing in Phase 4 (risk backlog upkeep).
- Isolation & MVCC correctness → Phase 2; Phase 3 for partitions/Jepsen.
- Crash safety & storage → Phase 2; Phase 3 for fuzzed parsers/heavier crash.
- GC & epochs → Phase 2; tuning in Phase 4.
- Replication & HA → Phase 3; drills in Phase 4.
- Backup & restore → Phase 2; deeper PITR and rehearsals in Phase 3/4.
- Security & multi-tenancy → Phase 2 basics; Phase 4 policies/quotas/rotations.
- Observability & SLOs → Phase 1 bootstrap; Phase 4 tuning.
- Filesystem reality checks → Phase 1 plan; Phase 2 execution.
- Graph-specific invariants → Phase 2 atomicity; Phase 3 churn/crash tests.
- On-disk format & upgrades → Phase 3; quarterly rehearsals in Phase 4.
- Hardening & fuzzing → Phase 3; continuous runs in Phase 4.
- Runbooks & operations → Skeletons in Phase 1; filled in Phases 2–3; mature/drilled in Phase 4.
- Deliverables/checkpoints → Exit criteria per phase above; link artifacts from here.
