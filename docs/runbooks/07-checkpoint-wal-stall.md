Checkpoint Overruns / WAL Stall
===============================

Summary
- Checkpoints take too long or stall, causing WAL to grow and risking space exhaustion or latency spikes.

Pre-checks
- Confirm whether checkpoints are running and failing vs simply queued behind workload.
- Check disk free space and IO latency.

Signals
- Metrics: checkpoint duration and throughput; WAL size; WAL stall alerts; fsync/flush latency; GC debt; write latency spikes.
- Logs: checkpoint failures, fsync errors, long sync warnings, throttle messages.

Diagnosis
1) Review recent checkpoints: success/failure timestamps, duration trend.
2) Check IO path: device utilization, read/write latency, any kernel/dmesg errors.
3) Inspect workload: are bursts of dirty pages overwhelming checkpoint? any large bulk loads?
4) Verify WAL/fsync settings (full vs relaxed) align with expectations; detect misconfiguration.
5) If using replicas, check whether replication lag is contributing to WAL backlog.

Mitigations
- Reduce stall pressure:
  - Increase checkpoint frequency or Enable incremental/continuous checkpointing if supported to smooth IO.
  - Throttle/batch writer workload temporarily; enable admission control for heavy writers.
- Fix root causes:
  - Resolve IO errors or move WAL/data to healthier storage.
  - If checksum/torn-write protection causing excessive rewrite, confirm hardware alignment and page size.
- Space safety:
  - If WAL near disk-full, archive WAL to secondary storage if safe and supported.
  - As last resort, fence lagging replicas that block truncation (align with policy).

Verification
- Checkpoint duration returns to normal; WAL size stops growing.
- Write latency improves; no new WAL stall alerts.
- Logs clean of checkpoint/fsync errors.

Follow-up
- Tune checkpoint pacing and triggers; document IO throughput expectations per hardware class.
- Add alerting on checkpoint duration trend and WAL stall earlier than disk-full.
- Consider dedicated IO class or SSD for WAL/checkpoint-heavy workloads.
