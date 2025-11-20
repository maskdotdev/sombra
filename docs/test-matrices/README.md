Test Matrices
=============

Define coverage per domain: workloads, faults, platforms, and cadence (CI vs nightly/soak). Link concrete test suites or scripts per row.

How to read/fill
- Workload/fault cases: short description and reference to test or script.
- Cadence: `CI`, `Nightly`, `Weekly`, or `Manual`.
- Env: link to environment or harness (local, k8s, bare metal, Jepsen rig).
- Status: `Planned`, `Running`, or `Gap` with TODO link/owner.

Use short rows like: `Case | Suite/Script | Cadence | Env | Status/TODO`.

Isolation & MVCC
- OLTP mixed reads/writes | TODO: link CI suite | CI | local | Planned
- High-contention hotspot workload | TODO: property-based or loadgen script | Nightly | local/soak box | Planned
- Long-running snapshot under write pressure | TODO: soak test | Nightly | soak box | Planned
- Anomaly detection: write skew/phantom | TODO: Jepsen-style harness | Weekly | partition rig | Gap (build harness)
- Unique constraint races | TODO: focused test | CI | local | Planned
- Secondary index visibility/undo | TODO: focused test | CI | local | Planned
- No-wait/backoff fairness | TODO: contention test | Nightly | local | Planned

Crash safety & storage
- Power-fail/kill -9 loops | TODO: crash harness | Nightly | crash box | Gap (build harness)
- Partial/truncated/corrupted WAL segments | TODO: WAL injector | Nightly | crash box | Planned
- Torn-page injection | TODO: page mutator tool | Nightly | crash box | Planned
- Checkpoint mid-crash | TODO: combined crash test | Nightly | crash box | Planned
- Platform matrix: ext4/xfs/zfs/apfs/NTFS | TODO: list hosts & configs | Weekly | bare metal/VMs | Planned

GC & epochs
- Long snapshots blocking GC | TODO: stress test | Nightly | soak box | Planned
- Safe-point advancement with lagging replicas | TODO: replication-aware test | Weekly | multi-node env | Planned
- Snapshot-too-old surfaced as errors (not stalls) | TODO: targeted test | CI | local | Planned

Replication & HA
- Planned failover drill | TODO: automation | Weekly | multi-node env | Planned
- Unplanned failover / crash of leader | TODO: automation | Weekly | multi-node env | Planned
- Split-brain prevention under partitions | TODO: Jepsen-style test | Weekly | partition rig | Gap (build harness)
- Rollback after promotion | TODO: targeted test | Weekly | multi-node env | Planned
- Lag-induced snapshot expiration on replicas | TODO: targeted test | Nightly | multi-node env | Planned

Backup & restore
- Full backup + restore rehearsal | TODO: script | Weekly | staging env | Planned
- Incremental/WAL archive replay | TODO: script | Weekly | staging env | Planned
- PITR to arbitrary LSN | TODO: script | Weekly | staging env | Planned
- Corrupted backup detection (checksum) | TODO: negative test | Weekly | staging env | Planned

Security
- Authz matrix (roles × actions) | TODO: test suite | CI | local | Planned
- TLS/auth rotation drill | TODO: script/manual | Monthly | staging env | Planned
- Tenant isolation (no cross-tenant leakage) | TODO: test | Nightly | local | Planned

Graph-specific invariants
- Concurrent vertex/edge churn | TODO: stress | Nightly | soak box | Planned
- Supernode handling | TODO: targeted load | Weekly | soak box | Planned
- Crash during adjacency update | TODO: crash harness | Weekly | crash box | Planned
- Dangling edge detection/repair | TODO: checker test | CI | local | Planned

Filesystem reality checks
- fsync semantics per platform | TODO: validation script | Weekly | per-OS VM/bare metal | Planned
- Directory fsync after create/rename | TODO: script | CI | local | Planned
- Aligned I/O validation | TODO: script | CI | local | Planned
