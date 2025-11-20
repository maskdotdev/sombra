Restore-from-Backup / PITR
==========================

Summary
- Recover a node or cluster from backups, optionally to a specific LSN/time (PITR), with validation before returning to service.

Pre-checks
- Identify target recovery point (timestamp/LSN) and scope (single node vs cluster).
- Confirm backup artifacts available (base backup + WAL archive) and checksums/manifest location.
- Allocate clean host/storage with equal or larger capacity.

Signals
- Data loss/corruption alerts; unrecoverable node; operator-triggered PITR (user error).
- Metrics: WAL stall or checksum failures; storage errors.
- Logs: checksum errors, torn page detection, missing WAL complaints.

Restore procedure (single node)
1) Provision target host/storage; ensure matching page size/config.
2) Fetch base backup for the chosen snapshot; verify checksums/manifest before use.
3) Rehydrate data dir from backup; apply correct permissions/ownership.
4) Replay WAL up to target LSN/time:
   - Point restore tool to WAL archive location.
   - Stop at target recovery marker (timestamp/LSN); document exact commands here.
5) Run offline validation:
   - Page/log validation tool.
   - Optional fast consistency check or verify tool.
6) Start node in recovery-safe mode (no external clients) and ensure it reaches desired LSN; check logs for missing WAL.
7) Switch to normal mode/readiness once validation passes.

Cluster considerations
- Keep other nodes fenced while restoring leader to avoid divergent timelines.
- After restore, re-seed followers from the new leader or rebuild replicas to the recovered LSN.

Verification
- Health/readiness endpoint passes; latency normal on smoke queries.
- Metrics stable: WAL apply complete; no pending recovery; replication resumes to followers.
- If PITR for user error, validate affected datasets/graphs manually with provided checks.

Follow-up
- Rotate backup credentials if exposed during incident.
- Schedule a rehearsal based on any gaps discovered (asset integrity, missing WAL, slow restore).
- Update backup retention or frequency if RPO/RTO not met.
