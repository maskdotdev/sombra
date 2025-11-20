GC Debt or WAL Blow-Up
======================

Summary
- WAL or version storage grows without bound because GC/checkpoint is stalled or overwhelmed.

Pre-checks
- Verify whether growth is logical (high write rate) or stuck GC/checkpoint.
- Check available disk and alert thresholds.

Signals
- Metrics: WAL bytes/s rising; WAL size; checkpoint duration; GC debt/version chain length; oldest active snapshot age; replication lag (may gate truncation).
- Logs: checkpoint failures, fsync errors, GC errors, safe-point blocked messages.

Diagnosis
1) Check for long-running snapshots blocking GC.
2) Inspect checkpoint health: recent successes? durations? any errors.
3) Look for replication lag preventing WAL truncation.
4) Verify disk/IO health (fsync errors or stalls).
5) Confirm GC workers are running (threads alive, no panics).

Mitigations
- Free space protection:
  - If near disk full, enable admission control or throttle writers.
  - Consider temporary WAL archive offload if supported.
- Unblock GC:
  - Terminate or restart long snapshots (coordinate with app owners).
  - Increase GC worker pacing temporarily.
- Checkpoint recovery:
  - Retry checkpoint in force mode if safe.
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
