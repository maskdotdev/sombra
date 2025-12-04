# Isolation Guarantees

> **Status**: Active  
> **Last Updated**: 2025-12-03

## Summary

Sombra provides **Snapshot Isolation (SI)** for all transactions. This document
explains what SI guarantees, what anomalies it prevents, and what anomalies
remain theoretically possible (though mitigated by the current architecture).

## Transaction Model

Sombra uses a **single-writer, multi-reader** concurrency model:

- **Single-writer**: Only one write transaction can be active at a time. Writers
  acquire an exclusive lock before beginning modifications.
- **Multi-reader**: Multiple read transactions can run concurrently with each
  other and with the active writer.
- **Readers never block writers**: Read transactions see a consistent snapshot
  and never wait for write transactions to complete.
- **Writers never block readers**: Write transactions proceed without waiting
  for read transactions to complete. Readers continue to see their snapshot
  even as new commits occur.

### Transaction Lifecycle

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│  begin_read │──────▶│   read ops  │──────▶│    drop     │
└─────────────┘       └─────────────┘       └─────────────┘
      │                                            │
      │  (pins snapshot)                    (releases pin)
      │                                            │
      ▼                                            ▼
┌──────────────────────────────────────────────────────────┐
│                    CommitTable                           │
│   Tracks: active readers, commit lifecycle, vacuum       │
│           horizon                                        │
└──────────────────────────────────────────────────────────┘
      ▲                                            ▲
      │                                            │
      │  (acquires lock)                   (releases lock)
      │                                            │
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│ begin_write │──────▶│  write ops  │──────▶│   commit    │
└─────────────┘       └─────────────┘       └─────────────┘
```

## Snapshot Isolation Guarantees

### What Snapshot Isolation Provides

When a transaction begins, it receives a **snapshot** — a consistent view of the
database as of that moment. All reads within the transaction see data as it
existed at the snapshot point, regardless of concurrent modifications.

Key properties:
1. **Point-in-time consistency**: All reads see the same consistent state
2. **Repeatable reads**: Re-reading the same data returns identical results
3. **No dirty reads**: Only committed data is visible
4. **No phantom reads**: Scans return consistent sets of rows

### Prevented Anomalies

| Anomaly | Description | Status | Mechanism |
|---------|-------------|--------|-----------|
| **Dirty Read** | Reading uncommitted data from another transaction | **Prevented** | Visibility rules require `CommitStatus::Committed` or `CommitStatus::Durable` |
| **Non-Repeatable Read** | Same query returning different results within a transaction | **Prevented** | Snapshot pinning ensures stable view |
| **Phantom Read** | New rows appearing in repeated range scans | **Prevented** | Snapshot includes index state at transaction start |
| **Lost Update** | Concurrent updates overwriting each other | **Prevented** | Single-writer lock serializes all writes |

### Allowed Anomalies (by SI design)

| Anomaly | Description | Status | Notes |
|---------|-------------|--------|-------|
| **Write Skew** | Two transactions read overlapping data, make disjoint writes that together violate a constraint | **Theoretically allowed by SI** | Currently prevented by single-writer architecture; would require application-level checks if multi-writer is ever implemented |

#### Write Skew Example

Consider an invariant: `sum(account_a, account_b) >= 0`

With multi-writer SI (hypothetical):
1. Both accounts start with balance = 100 (total = 200)
2. Txn A reads both: sees total 200, withdraws 150 from account_a
3. Txn B reads both: sees total 200, withdraws 150 from account_b  
4. Both commit successfully
5. Result: total = -100 (constraint violated!)

**In Sombra today**: This cannot occur because the single-writer lock forces
Txn B to wait until Txn A commits. Txn B would then see Txn A's changes and
reject the withdrawal.

## Comparison to SQL Isolation Levels

| SQL Level | SI Equivalent? | Notes |
|-----------|---------------|-------|
| READ UNCOMMITTED | No (SI is stronger) | SI never exposes uncommitted data |
| READ COMMITTED | No (SI is stronger) | SI provides repeatable reads |
| REPEATABLE READ | Approximately | SI prevents phantoms in addition to non-repeatable reads |
| SERIALIZABLE | No (SI is weaker) | SI allows write skew; serializable prevents it |

### Relationship to PostgreSQL

PostgreSQL's "REPEATABLE READ" is actually Snapshot Isolation. Sombra's
guarantees are equivalent to PostgreSQL's REPEATABLE READ level.

For true serializability, PostgreSQL implements Serializable Snapshot Isolation
(SSI), which tracks read-write dependencies and aborts transactions that could
cause anomalies. Sombra does not currently implement SSI.

## Implementation Details

### Visibility Rules

A record version is visible to a transaction if:

```rust
fn visible_at(&self, snapshot: CommitId) -> bool {
    // Version must have begun before or at snapshot
    if snapshot < self.begin {
        return false;
    }
    // If version has no end (COMMIT_MAX), it's still current
    if self.end == COMMIT_MAX {
        return true;
    }
    // Otherwise, version must not have ended before snapshot
    snapshot < self.end
}
```

Each record carries a `VersionHeader` with:
- `begin`: CommitId when this version became visible
- `end`: CommitId when this version was superseded (0 = still current)
- `flags`: Metadata (tombstone, pending, etc.)

### Vacuum and Reader Interaction

The vacuum process reclaims old record versions that are no longer needed:

1. **Vacuum Horizon**: The oldest commit that must remain visible, computed as:
   ```rust
   min(oldest_active_reader_snapshot, retention_window_cutoff)
   ```

2. **Active Reader Protection**: Readers "pin" their snapshot commit in the
   `CommitTable`. Vacuum cannot reclaim versions newer than the oldest pin.

3. **Retention Window**: A configurable duration (default: 24 hours) that
   guarantees versions remain available even without active readers.

```
Time ──────────────────────────────────────────────────▶
                                                        
Commits: ───[1]───[2]───[3]───[4]───[5]───[6]───[7]───
                                                        
Reader A pins [3] ─────────────────────▶ drop           
Reader B pins [5] ────────────────────────────▶ drop    
                                                        
Vacuum horizon: [3] (oldest active reader)              
Can reclaim: versions superseded before [3]             
```

### Long-Running Reader Impact

Long-running readers prevent vacuum from reclaiming old versions:

- **Risk**: Unbounded version chain growth, increased storage, slower scans
- **Mitigation**: Monitor `mvcc_reader_max_age_ms` metric; future reader timeout
  feature will allow configurable limits
- **Runbook**: See `docs/runbooks/03-snapshot-too-old.md` for operational guidance

## Configuration

### Vacuum Configuration (`VacuumCfg`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enabled` | `true` | Enable background vacuum worker |
| `interval` | 5 seconds | Time between vacuum passes |
| `retention_window` | 24 hours | Minimum version retention duration |
| `log_high_water_bytes` | 512 MB | Version log size triggering eager cleanup |
| `max_pages_per_pass` | 128 | Pages to process per vacuum pass |
| `max_millis_per_pass` | 50 ms | Soft runtime budget per pass |

### Tuning Recommendations

1. **High-throughput writes with short reads**: Reduce `retention_window` to
   allow faster cleanup; consider aggressive `vacuum.interval`.

2. **Long-running analytical queries**: Increase `retention_window`; monitor
   version log growth; consider read replicas for heavy analytics.

3. **Mixed workloads**: Default settings are reasonable; tune based on observed
   `vacuum_horizon_commit` vs `latest_commit` gap.

## Future Roadmap

### Planned Features

1. **Reader Timeout** (P2): Configurable maximum reader age with automatic
   invalidation to prevent vacuum stalls.

2. **Conflict Detection** (P2): First-committer-wins semantics as defense in
   depth, preparing for potential future multi-writer support.

3. **Serializable Snapshot Isolation** (Future): SSI implementation for
   applications requiring true serializability.

### Multi-Writer Considerations

If multi-writer support is added in the future:

- Write skew prevention would require either:
  - SSI with dependency tracking and abort
  - Application-level constraint checking
  - Explicit locking (SELECT FOR UPDATE equivalent)

- Conflict detection would become mandatory rather than defense-in-depth

## Observability

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `mvcc_reader_active` | Active read transactions | Warning if consistently > 100 |
| `mvcc_reader_max_age_ms` | Oldest reader age | Warning > 5 minutes |
| `vacuum_horizon_commit` | Oldest required commit | Monitor gap vs latest |
| `version_log_bytes` | Version chain storage | Warning > `log_high_water_bytes` |
| `mvcc_write_lock_conflicts` | Writer lock contention | Should be 0 (single-writer) |

### Diagnostic Commands

```sql
-- Check active readers (via admin API)
GET /admin/mvcc/readers

-- Check vacuum status
GET /admin/mvcc/vacuum

-- Force vacuum pass
POST /admin/mvcc/vacuum/trigger
```

## References

- `docs/mvcc-baseline.md` — Core MVCC design specification
- `docs/mvcc-durability.md` — Durability and WAL integration
- `docs/production-readiness.md` — Phase 2 checklist items
- `docs/test-matrices/README.md` — Test coverage tracking
- `docs/runbooks/03-snapshot-too-old.md` — Operational guidance for long readers
- `src/storage/mvcc.rs` — Core MVCC implementation
