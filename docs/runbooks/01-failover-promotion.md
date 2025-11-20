Failover and Promotion
======================

Summary
- Restore service when the leader is unhealthy or lost by promoting a follower safely and preventing split-brain.

Note
- Native HA/replication control is still WIP. Use infra-level fencing (stop the old leader VM/pod) and orchestration to repoint clients. Replace TODOs below with cluster commands once replication ships.

Pre-checks
- Confirm client-facing impact (writes failing? reads timing out?).
- Identify current leader and candidate follower(s); ensure at least one is roughly caught up.
- Verify whether the old leader is definitively down vs flapping.

Signals
- Alerts: leader health/readiness failing; elevated write latency; replication apply lag high; client errors for write unavailability.
- Metrics to check: replication lag (LSN distance) per follower; WAL apply delay; leader CPU/IO saturation; checkpoint duration; WAL stall.
- Logs: leadership change attempts, replication errors, disk/IO errors.

Diagnosis
1) Measure lag: pick the healthiest follower with smallest WAL/apply lag.
2) Confirm old leader state:
   - If powered down or isolated, prefer fencing (VM stop/power off) before promotion.
   - If flapping, decide whether to fence or isolate (remove from quorum).
3) Ensure candidate follower is in good health: apply loop advancing, no disk errors, sufficient free space.

Promotion steps
- Fence the old leader if possible to avoid split-brain (power off or remove from network/quorum).
- Promote follower:
  - TODO: promotion command/config once HA is implemented; document it here.
  - Enable write acceptance; ensure it has replayed through latest received WAL.
- Repoint clients:
  - Update service discovery/connection strings to the new leader.
  - If using read replicas, ensure they now follow the new leader.

Post-promotion verification
- Metrics: replication lag from new leader to remaining followers trending down; write latency back to normal; no WAL stall.
- Health: readiness passes on new leader; liveness stable.
- Data check: run `sombra doctor <db> --verify-level full --json` on the new leader when feasible; `sombra stats <db> --format json` for WAL/path sanity.

Follow-up
- Autopsy root cause on original leader (hardware, IO, WAL stall, checkpoint runaway).
- Decide when/how to reinstate the old leader as follower; ensure it does a clean follow with divergence handling.
- Record timeline and actions in incident doc; update this runbook with any command specifics used.
