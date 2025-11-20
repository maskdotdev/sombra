GC Debt or WAL Blow-Up
======================

Summary
- WAL or version storage grows without bound because GC/checkpoint is stalled or overwhelmed.

Pre-checks
- Verify whether growth is logical (high write rate) or stuck GC/checkpoint.
- Check available disk and alert thresholds.

Signals
- Metrics: WAL size (`sombra stats --format json` → `wal.size_bytes`), checkpoint duration, GC debt/version chain length (TODO: add GC metrics), oldest active snapshot age (`pager.mvcc_reader_max_age_ms`), replication lag (may gate truncation).
- Logs: checkpoint failures, fsync errors, GC errors, safe-point blocked messages.

Diagnosis
1) Check for long-running snapshots blocking GC.
2) Inspect checkpoint health: recent successes? durations? any errors (`sombra stats` for `wal.last_checkpoint_lsn` movement; logs for errors).
3) Look for replication lag preventing WAL truncation.
4) Verify disk/IO health (fsync errors or stalls).
5) Confirm GC workers are running (threads alive, no panics).

Mitigations
- Free space protection:
  - If near disk full, enable admission control or throttle writers.
  - Consider temporary WAL archive offload if supported (TODO: add command when available).
- Unblock GC:
  - Terminate or restart long snapshots (coordinate with app owners).
  - Increase GC worker pacing temporarily.
- Checkpoint recovery:
  - Retry checkpoint in force mode if safe: `sombra checkpoint <db> --mode force`.
  - If WAL is huge and reclaim needed, run `sombra vacuum <db> --into /tmp/compact.sombra && mv` or `sombra vacuum <db> --replace --backup <db>.bak`.
  - Fix underlying errors (permissions, disk space, IO issues) before retrying.
- Replication gating:
  - If a follower is badly lagged and blocking truncation, decide to fence/bypass it after ensuring data safety policy allows it.

Verification
- WAL size plateaus then shrinks; GC debt trending down.
- Checkpoint intervals return to normal; no new errors.
- Disk free space recovers; alerts clear.

Follow-up
- Adjust GC/checkpoint cadence or thresholds for workload.
- Add/update alerts on WAL stall, checkpoint overruns, GC debt.
- If replication repeatedly gates truncation, revisit lag budgets and replica sizing.
