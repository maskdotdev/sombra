Lock Storms and Deadlocks
=========================

Summary
- High contention leading to many blocked transactions or deadlock errors; throughput collapses.

Pre-checks
- Confirm isolation level and lock strategy in use (no-wait/backoff vs blocking).
- Identify hotspot tables/indexes/keys involved.

Signals
- Metrics: active vs blocked txns; deadlock count/rate; p99 latency spikes; contention per index/key if available.
- Logs: deadlock detection reports; lock wait timeouts; aborted transactions.

Diagnosis
1) Identify top contended resources (tables/indexes/keys) and query shapes causing waits.
2) Determine if deadlocks are systematic (query order inversion) vs random.
3) Check if timeout/backoff settings are too low/high for workload.
4) Verify whether long-running readers are holding locks or pins that block writers.

Mitigations
- Immediate:
  - Enable/raise backoff for no-wait workloads to reduce thrash.
  - Kill or reschedule pathological long transactions if they block high-priority writes.
  - If supported, prioritize writers or enable fair queues to avoid starvation.
- Query/order fixes:
  - Enforce consistent lock ordering in conflicting operations.
  - Batch or serialize hotspot mutations temporarily.
- Config levers:
  - Tune deadlock detection frequency and lock timeout thresholds appropriate to workload latency budget.

Verification
- Blocked txn count returns to normal; deadlock rate drops.
- Latency percentiles recover; throughput back to baseline.
- No surge of aborts beyond expected backoff behavior.

Follow-up
- Add dashboards for per-resource contention if missing.
- Add load-shedding/admission rules for known hotspots.
- Bake consistent lock ordering into application or stored procedures.
