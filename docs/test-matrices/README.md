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
- Storage regressions (MVCC/storage basics) | `cargo test --tests` (covers storage_phase1/2/3, storage_stage7, catalog_dict) | CI | local | Running
- Graph stress (mixed create/update/delete) | `cargo test --test storage_stress` | CI | local | Running
- OLTP hotspot workload | TODO: loadgen/proptest suite | Nightly | soak box | Gap (needs harness)
- Long-running snapshot under write pressure | TODO: soak test | Nightly | soak box | Gap
- Anomaly detection (write skew/phantom) | TODO: Jepsen-style harness | Weekly | partition rig | Gap
- Unique constraint races | TODO: focused test | CI | local | Gap
- Secondary index visibility/undo | TODO: focused test | CI | local | Gap
- No-wait/backoff fairness | TODO: contention test | Nightly | local | Gap

Crash safety & storage
- WAL replay after crash | `cargo test --test wal_crash_replay` (scripts/run-prod-sanity.sh) | CI | local | Running
- Vacuum worker durability | `cargo test --test vacuum_worker` | CI | local | Running
- Pager checkpoint E2E | `cargo test --test pager_stage3_end_to_end` | CI | local | Running
- Power-fail/kill -9 loops | TODO: crash harness | Nightly | crash box | Gap
- Partial/truncated/corrupted WAL | TODO: WAL injector | Nightly | crash box | Gap
- Torn-page injection | TODO: page mutator tool | Nightly | crash box | Gap
- Checkpoint mid-crash | TODO: combined crash test | Nightly | crash box | Gap
- Platform matrix: ext4/xfs/zfs/apfs/NTFS | TODO: host list + config | Weekly | bare metal/VMs | Gap

GC & epochs
- Long snapshots blocking GC | TODO: stress test | Nightly | soak box | Gap
- Safe-point advancement with lagging replicas | TODO: replication-aware test | Weekly | multi-node env | Gap
- Snapshot-too-old surfaced as errors (not stalls) | TODO: targeted test | CI | local | Gap

Replication & HA
- Planned failover drill | TODO: automation | Weekly | multi-node env | Gap (HA not wired yet)
- Unplanned failover / crash of leader | TODO: automation | Weekly | multi-node env | Gap
- Split-brain prevention under partitions | TODO: Jepsen-style test | Weekly | partition rig | Gap
- Rollback after promotion | TODO: targeted test | Weekly | multi-node env | Gap
- Lag-induced snapshot expiration on replicas | TODO: targeted test | Nightly | multi-node env | Gap

Backup & restore
- Admin vacuum/backup flows | `cargo test --test admin_phase1` (checkpoint/vacuum/verify) (scripts/run-prod-sanity.sh) | CI | local | Running
- CLI admin commands (import/export/checkpoint/vacuum) | `cargo test --test cli_admin_commands` (scripts/run-prod-sanity.sh) | CI | local | Running
- Full backup + restore rehearsal | `sombra checkpoint <db>; sombra vacuum <db> --into <dst>; sombra verify <dst> --level full` | Weekly | staging env | Planned
- PITR to arbitrary LSN | TODO: PITR harness | Weekly | staging env | Gap
- Corrupted backup detection (checksum) | TODO: negative test | Weekly | staging env | Gap

Security
- Authz matrix (roles × actions) | TODO: test suite | CI | local | Gap
- TLS/auth rotation drill | TODO: script/manual | Monthly | staging env | Gap
- Tenant isolation (no cross-tenant leakage) | TODO: test | Nightly | local | Gap

Graph-specific invariants
- Graph storage regressions | `cargo test --tests` (storage_phase*, storage_stage7, catalog_dict) (scripts/run-prod-sanity.sh) | CI | local | Running
- Graph stress (mixed churn) | `cargo test --test storage_stress` (scripts/run-prod-sanity.sh) | CI | local | Running
- Supernode handling | TODO: targeted load | Weekly | soak box | Gap
- Crash during adjacency update | TODO: crash harness | Weekly | crash box | Gap
- Dangling edge detection/repair | TODO: checker test | CI | local | Gap

Filesystem reality checks
- fsync semantics per platform | TODO: validation script | Weekly | per-OS VM/bare metal | Gap
- Directory fsync after create/rename | TODO: script | CI | local | Gap
- Aligned I/O validation | TODO: script | CI | local | Gap
