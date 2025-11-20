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
- Failover and promotion (leader/follower).
- Restore-from-backup / PITR.
- Snapshot-too-old events.
- GC debt or WAL blow-up.
- Lock storms and deadlock spikes.
- Replication lag or stuck replicas.
- Checkpoint overruns or WAL stall.
Add more as they emerge; link each here once written.
