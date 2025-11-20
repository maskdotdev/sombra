Snapshot-Too-Old Events
=======================

Summary
- Long-running readers exceeded GC retention/safe-point window; the system refuses to service the old snapshot instead of stalling.

Pre-checks
- Confirm errors are snapshot-expired, not generic timeouts or deadlocks.
- Identify workload causing long snapshots (client, query type, isolation level).

Signals
- Errors: “snapshot too old” / GC threshold exceeded.
- Metrics: oldest active snapshot age; GC debt; version chain lengths; replication lag if safe point is gated by replicas.
- Logs: messages about safe-point advancement blocked/unblocked; GC eviction decisions.

Diagnosis
1) Locate offending sessions/readers and their start times.
2) Check GC retention settings vs observed snapshot age.
3) Check replication lag (if safe-point depends on replicas) and checkpoint backlog.
4) Inspect version chain stats; ensure GC is running and not stalled by errors.

Mitigations
- Short term:
  - Ask/force offending readers to restart with a fresh snapshot (application-level retry).
  - If business-safe, temporarily increase retention window to clear backlog (may grow disk/WAL).
  - Ensure replicas caught up if they are blocking safe-point advancement.
- Correctness guardrails:
  - Do NOT disable GC entirely; avoid unbounded version growth.
  - Prefer failing old snapshots over pausing writers.

Verification
- Errors stop after retries; new queries succeed.
- Oldest active snapshot age drops below threshold; GC debt trends down.
- Disk/WAL growth stabilizes after GC catches up.

Follow-up
- Tune retention defaults based on workload patterns.
- Add alerts on snapshot age approaching limit and on GC debt growth.
- Consider workload guidance: limit transaction duration, chunk long reads, or use lower isolation if acceptable.
