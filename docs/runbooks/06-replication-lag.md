Replication Lag or Stuck Replicas
=================================

Summary
- Followers fall behind or stop applying WAL, threatening RPO/RTO and potentially blocking WAL truncation.

Pre-checks
- Determine whether lag is receiver (network ingest) or applier (replay) limited.
- Check if lag is intentional (maintenance) or unexpected.

Signals
- Metrics: WAL shipping rate; apply lag (LSN distance/time); replica CPU/IO; checkpoint duration on leader; GC safe point blocked by replicas.
- Logs: replication disconnects; missing WAL complaints; checksum mismatches; apply errors.

Diagnosis
1) Inspect per-replica lag and trend; find the worst offender.
2) Check network path and bandwidth limits to that replica; packet loss?
3) Inspect replica host health: CPU, IO saturation, disk full, checksum errors.
4) Confirm replica configuration matches leader (page size, format version).
5) Check whether replicas are gating WAL truncation or safe-point advancement.

Mitigations
- Unstick replica:
  - Restart replication receiver/applier if wedged; clear any bad WAL segment after ensuring availability of good copy.
  - If disk bound, throttle leader WAL generation temporarily (admission control) or move replica to faster storage.
  - If network bound, relocate traffic or temporarily disable catch-up for least important replica.
- Prevent blockage:
  - If one replica blocks truncation and is far behind, consider fencing it and re-seeding to avoid primary running out of disk—ensure policy permits this.
- For intentional lag (maintenance), adjust alerts/thresholds temporarily but cap maximum allowed lag.

Verification
- Lag decreasing on all replicas; no replication errors in logs.
- WAL truncation resumes; GC safe point no longer blocked by replicas.
- Client-facing latency unaffected (or returns to baseline).

Follow-up
- Tune lag/rollback budgets and alerts; document expected lag under normal load.
- Capacity-plan replicas (IO/network) to match write volume.
- Schedule periodic replica rebuild drills to ensure re-seeding is fast and reliable.
