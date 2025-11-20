Runbook Index
=============

Use this folder to store operational runbooks referenced by the production readiness plan. Each scenario should have a dedicated file named `NN-scenario-name.md`.

Template
--------
- Summary: what is broken/degraded and impact.
- Signals: metrics/logs/alerts that indicate the issue.
- Diagnosis steps: ordered checks and expected results.
- Mitigations: safe actions with commands; note risks and timeouts.
- Verification: how to confirm recovery.
- Postmortem inputs: links to dashboards/logs, owner for follow-up.

Scenarios to fill
- [x] [Failover and promotion](01-failover-promotion.md).
- [x] [Restore-from-backup / PITR](02-restore-pitr.md).
- [x] [Snapshot-too-old events](03-snapshot-too-old.md).
- [x] [GC debt or WAL blow-up](04-gc-or-wal-blowup.md).
- [x] [Lock storms and deadlock spikes](05-lock-storms-and-deadlocks.md).
- [x] [Replication lag or stuck replicas](06-replication-lag.md).
- [x] [Checkpoint overruns or WAL stall](07-checkpoint-wal-stall.md).
- [ ] Add more as they emerge; link each here once written.
