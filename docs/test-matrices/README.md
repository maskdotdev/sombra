Test Matrices
=============

Define coverage per domain: workloads, faults, platforms, and cadence (CI vs nightly/soak). Link concrete test suites or scripts per row.

How to read/fill
- Workload/fault cases: short description and reference to test or script.
- Cadence: `CI`, `Nightly`, `Weekly`, or `Manual`.
- Env: link to environment or harness (local, k8s, bare metal, Jepsen rig).

Isolation & MVCC
- Workloads: OLTP mixed reads/writes; high contention hotspots; long-running snapshots.
- Anomalies: write skew, phantom reads, unique constraint races, secondary index visibility, no-wait/backoff behavior.
- Cadence/env: _fill in with suites and schedule_.

Crash safety & storage
- Faults: power-fail/kill -9 loops; partial/truncated/corrupted WAL segments; torn pages; checkpoint mid-crash.
- Platforms: ext4, xfs, zfs, apfs, NTFS (as supported).
- Cadence/env: _fill in with harness (crash simulator) and schedule_.

GC & epochs
- Cases: long snapshots blocking GC; safe-point advancement with lagging replicas; snapshot-too-old surfacing as errors.
- Cadence/env: _fill in suites and schedule_.

Replication & HA
- Cases: planned/unplanned failover; split-brain prevention under partitions; rollback after promotion; lag-induced snapshot expiration.
- Cadence/env: _fill in suites and schedule_.

Backup & restore
- Cases: full/incremental backup; PITR to arbitrary LSN; corrupted backup detection.
- Cadence/env: _fill in suites and schedule_.

Security
- Cases: authz matrix (roles × actions); TLS/auth rotation drills; multi-tenant isolation checks.
- Cadence/env: _fill in suites and schedule_.

Graph-specific invariants
- Cases: concurrent vertex/edge churn; supernode handling; crash during adjacency update; dangling edge detection.
- Cadence/env: _fill in suites and schedule_.

Filesystem reality checks
- Cases: fsync semantics per platform; directory fsync after create/rename; aligned I/O validation.
- Cadence/env: _fill in suites and schedule_.
