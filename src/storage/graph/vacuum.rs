use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use crate::primitives::pager::WriteGuard;
use crate::storage::index::IndexVacuumStats;
use crate::storage::mvcc::{CommitId, VersionLogEntry, COMMIT_MAX};
use crate::types::Result;

use super::graph_types::{
    AdjacencyVacuumStats, GraphVacuumStats, VacuumBudget, VacuumMode, VacuumTrigger,
    VersionVacuumStats,
};
use super::helpers::now_millis;
use super::{Graph, RootKind};

const MICRO_GC_MAX_BUDGET: usize = 64;

#[derive(Copy, Clone)]
pub(crate) enum MicroGcTrigger {
    ReadPath,
    CacheMiss,
    PostCommit,
}

enum MicroGcOutcome {
    Done,
    Retry,
    Skip,
}

impl MicroGcTrigger {
    fn budget(self) -> usize {
        match self {
            MicroGcTrigger::ReadPath => 8,
            MicroGcTrigger::CacheMiss => 16,
            MicroGcTrigger::PostCommit => 64,
        }
    }

    fn cooldown_ms(self) -> u64 {
        match self {
            MicroGcTrigger::ReadPath => 25,
            MicroGcTrigger::CacheMiss => 10,
            MicroGcTrigger::PostCommit => 5,
        }
    }
}

impl Graph {
    pub(crate) fn request_micro_gc(&self, trigger: MicroGcTrigger) {
        if !self.vacuum_cfg.enabled {
            return;
        }
        let budget = trigger.budget().min(MICRO_GC_MAX_BUDGET);
        if budget == 0 {
            return;
        }
        self.micro_gc_budget_hint
            .fetch_max(budget, Ordering::Relaxed);
    }

    pub(crate) fn drive_micro_gc(&self, trigger: MicroGcTrigger) {
        if !self.vacuum_cfg.enabled {
            return;
        }
        self.request_micro_gc(trigger);
        let pending_budget = self.micro_gc_budget_hint.swap(0, Ordering::Relaxed);
        if pending_budget == 0 {
            return;
        }
        let budget = pending_budget.min(MICRO_GC_MAX_BUDGET);
        let now_ms = now_millis();
        let now_ms_u64 = now_ms.min(u64::MAX as u128) as u64;
        let last = self.micro_gc_last_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(u128::from(last)) < u128::from(trigger.cooldown_ms()) {
            self.micro_gc_budget_hint
                .fetch_max(budget, Ordering::Relaxed);
            return;
        }
        if self
            .micro_gc_running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            self.micro_gc_budget_hint
                .fetch_max(budget, Ordering::Relaxed);
            return;
        }
        let outcome = match self.compute_vacuum_horizon() {
            Some(horizon) if horizon != COMMIT_MAX => match self.micro_gc(horizon, budget) {
                Ok(Some(_)) => MicroGcOutcome::Done,
                Ok(None) => MicroGcOutcome::Retry,
                Err(err) => {
                    debug!(error = %err, "graph.micro_gc.error");
                    MicroGcOutcome::Retry
                }
            },
            _ => MicroGcOutcome::Skip,
        };
        self.micro_gc_last_ms.store(now_ms_u64, Ordering::Relaxed);
        self.micro_gc_running.store(false, Ordering::Release);
        if matches!(outcome, MicroGcOutcome::Retry) {
            self.micro_gc_budget_hint
                .fetch_max(budget, Ordering::Relaxed);
        }
    }

    /// Removes historical versions whose visibility ended at or before the given horizon.
    /// Optionally limits the number of pruned entries per call.
    pub fn vacuum_version_log(
        &self,
        horizon: CommitId,
        limit: Option<usize>,
    ) -> Result<VersionVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let stats = self.vacuum_version_log_with_write(&mut write, horizon, limit)?;
        self.commit_with_metrics(write)?;
        Ok(stats)
    }

    /// Removes adjacency entries whose visibility ended before the given horizon.
    pub fn vacuum_adjacency(&self, horizon: CommitId) -> Result<AdjacencyVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let fwd = Self::prune_versioned_vec_tree(&self.adj_fwd, &mut write, horizon)?;
        let rev = Self::prune_versioned_vec_tree(&self.adj_rev, &mut write, horizon)?;
        self.commit_with_metrics(write)?;
        Ok(AdjacencyVacuumStats {
            fwd_entries_pruned: fwd,
            rev_entries_pruned: rev,
        })
    }

    /// Removes index entries whose visibility ended before the given horizon.
    pub fn vacuum_indexes(&self, horizon: CommitId) -> Result<IndexVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let stats = self.indexes.vacuum(&mut write, horizon)?;
        self.commit_with_metrics(write)?;
        Ok(stats)
    }

    /// Returns the most recent recorded vacuum statistics.
    pub fn last_vacuum_stats(&self) -> Option<GraphVacuumStats> {
        self.vacuum_sched.last_stats.borrow().clone()
    }

    /// Manually triggers a vacuum pass immediately.
    pub fn trigger_vacuum(&self) {
        self.vacuum_sched
            .pending_trigger
            .set(Some(VacuumTrigger::Manual));
        self.vacuum_sched.next_deadline_ms.set(0);
        self.maybe_background_vacuum(VacuumTrigger::Manual);
    }

    /// Runs a lightweight micro-GC pass up to `max_entries` past the given horizon.
    pub fn micro_gc(
        &self,
        horizon: CommitId,
        max_entries: usize,
    ) -> Result<Option<VersionVacuumStats>> {
        if max_entries == 0 {
            return Ok(None);
        }
        if self.vacuum_sched.running.get() {
            return Ok(None);
        }
        let mut write = self.begin_write_guard()?;
        let stats = self.vacuum_version_log_with_write(&mut write, horizon, Some(max_entries))?;
        if stats.entries_pruned > 0 {
            self.metrics.mvcc_micro_gc_trim(stats.entries_pruned, 0);
        }
        self.commit_with_metrics(write)?;
        Ok(Some(stats))
    }

    pub(crate) fn maybe_background_vacuum(&self, default_trigger: VacuumTrigger) {
        debug_assert_eq!(
            thread::current().id(),
            self.vacuum_sched.owner_tid,
            "vacuum invoked from unexpected thread"
        );
        if !self.vacuum_cfg.enabled {
            return;
        }
        if self.vacuum_sched.running.get() {
            return;
        }
        let now_ms = now_millis();
        let next_deadline = self.vacuum_sched.next_deadline_ms.get();
        let pending = self.vacuum_sched.pending_trigger.get();
        let mode = self.select_vacuum_mode();
        self.vacuum_sched.mode.set(mode);
        let high_water_triggered = self.vacuum_cfg.log_high_water_bytes > 0
            && self.version_log_bytes() >= self.vacuum_cfg.log_high_water_bytes;
        let opportunistic_trigger = self.vacuum_cfg.log_high_water_bytes > 0
            && self.vacuum_sched.pending_trigger.get().is_none()
            && self.version_log_bytes() >= (self.vacuum_cfg.log_high_water_bytes / 2).max(1);
        if opportunistic_trigger {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::Opportunistic));
        }
        if high_water_triggered && pending.is_none() {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::HighWater));
        }
        let pending_trigger = self.vacuum_sched.pending_trigger.get();
        if pending_trigger.is_none()
            && !high_water_triggered
            && default_trigger != VacuumTrigger::Manual
        {
            let interval = mode.tick_interval(self.vacuum_cfg.interval);
            let interval_ms = interval.as_millis().max(1);
            if next_deadline != 0 && now_ms < next_deadline {
                return;
            }
            if next_deadline == 0 {
                self.vacuum_sched
                    .next_deadline_ms
                    .set(now_ms.saturating_add(interval_ms));
                return;
            }
        }
        if self.vacuum_sched.running.replace(true) {
            return;
        }
        let trigger =
            self.vacuum_sched
                .pending_trigger
                .replace(None)
                .unwrap_or(if high_water_triggered {
                    VacuumTrigger::HighWater
                } else {
                    default_trigger
                });
        let Some(horizon) = self.compute_vacuum_horizon() else {
            self.vacuum_sched.running.set(false);
            return;
        };
        let budget = VacuumBudget {
            max_versions: if self.vacuum_cfg.max_pages_per_pass == 0 {
                None
            } else if matches!(trigger, VacuumTrigger::Opportunistic) {
                Some(self.vacuum_cfg.max_pages_per_pass.min(16))
            } else {
                Some(self.vacuum_cfg.max_pages_per_pass)
            },
            max_duration: if matches!(trigger, VacuumTrigger::Opportunistic) {
                Duration::from_millis(self.vacuum_cfg.max_millis_per_pass.max(1) / 2 + 1)
            } else {
                Duration::from_millis(self.vacuum_cfg.max_millis_per_pass.max(1))
            },
            index_cleanup: self.vacuum_cfg.index_cleanup
                && !matches!(trigger, VacuumTrigger::Opportunistic),
        };
        let result = self.vacuum_mvcc(horizon, None, trigger, Some(&budget));
        match result {
            Ok(stats) => self.record_vacuum_stats(stats),
            Err(err) => warn!(error = %err, "graph.vacuum.failed"),
        }
        let interval_ms = mode
            .tick_interval(self.vacuum_cfg.interval)
            .as_millis()
            .max(1);
        self.vacuum_sched
            .next_deadline_ms
            .set(now_ms.saturating_add(interval_ms));
        self.vacuum_sched.running.set(false);
    }

    /// Runs MVCC cleanup across versions, adjacency, and indexes with optional limits.
    pub fn vacuum_mvcc(
        &self,
        horizon: CommitId,
        limit: Option<usize>,
        trigger: VacuumTrigger,
        budget: Option<&VacuumBudget>,
    ) -> Result<GraphVacuumStats> {
        let started_at = SystemTime::now();
        let bytes_before = self.version_log_bytes();
        let version_limit = limit.or_else(|| budget.and_then(|b| b.max_versions));
        let version_stats = self.vacuum_version_log(horizon, version_limit)?;
        let mut adjacency_stats = AdjacencyVacuumStats::default();
        let mut index_stats = IndexVacuumStats::default();
        if budget.map(|b| b.index_cleanup).unwrap_or(true) {
            adjacency_stats = self.vacuum_adjacency(horizon)?;
            index_stats = self.vacuum_indexes(horizon)?;
        }
        let finished_at = SystemTime::now();
        let bytes_after = self.version_log_bytes();
        let run_millis = finished_at
            .duration_since(started_at)
            .map(|dur| dur.as_millis() as u64)
            .unwrap_or(0);
        Ok(GraphVacuumStats {
            started_at,
            finished_at,
            horizon_commit: horizon,
            trigger,
            run_millis,
            log_versions_examined: version_stats.entries_pruned,
            log_versions_pruned: version_stats.entries_pruned,
            orphan_log_versions_pruned: version_stats.entries_pruned,
            heads_purged: 0,
            adjacency_fwd_pruned: adjacency_stats.fwd_entries_pruned,
            adjacency_rev_pruned: adjacency_stats.rev_entries_pruned,
            index_label_pruned: index_stats.label_entries_pruned,
            index_chunked_pruned: index_stats.chunked_segments_pruned,
            index_btree_pruned: index_stats.btree_entries_pruned,
            pages_read: 0,
            pages_written: 0,
            bytes_reclaimed: bytes_before.saturating_sub(bytes_after),
        })
    }

    pub(crate) fn vacuum_version_log_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        horizon: CommitId,
        limit: Option<usize>,
    ) -> Result<VersionVacuumStats> {
        let max_prune = limit.unwrap_or(usize::MAX);
        if max_prune == 0 {
            return Ok(VersionVacuumStats::default());
        }
        let mut to_delete = Vec::new();
        let mut retired = Vec::new();
        let mut retired_sizes = Vec::new();
        self.version_log.for_each_with_write(tx, |key, bytes| {
            if to_delete.len() >= max_prune {
                return Ok(());
            }
            let entry = VersionLogEntry::decode(&bytes)?;
            if entry.header.end != COMMIT_MAX && entry.header.end <= horizon {
                to_delete.push(key);
                retired.push(entry);
                retired_sizes.push(bytes.len() as u64);
            }
            Ok(())
        })?;
        let mut bytes_removed = 0u64;
        for entry in &retired {
            self.retire_version_resources(tx, entry)?;
        }
        for encoded_len in retired_sizes {
            bytes_removed = bytes_removed.saturating_add(encoded_len);
        }
        for key in &to_delete {
            let _ = self.version_log.delete(tx, key)?;
        }
        if !to_delete.is_empty() {
            self.persist_tree_root(tx, RootKind::VersionLog)?;
        }
        if bytes_removed > 0 {
            self.version_log_bytes
                .fetch_sub(bytes_removed, Ordering::Relaxed);
        }
        if !to_delete.is_empty() {
            self.version_log_entries
                .fetch_sub(to_delete.len() as u64, Ordering::Relaxed);
        }
        if bytes_removed > 0 || !to_delete.is_empty() {
            self.publish_version_log_usage_metrics();
        }
        self.vstore.flush_deferred(tx)?;
        if !to_delete.is_empty() {
            self.metrics.mvcc_micro_gc_trim(retired.len() as u64, 0);
        }
        Ok(VersionVacuumStats {
            entries_pruned: to_delete.len() as u64,
        })
    }

    pub(crate) fn maybe_signal_high_water(&self) {
        let threshold = self.vacuum_cfg.log_high_water_bytes;
        if threshold == 0 {
            return;
        }
        if self.version_log_bytes() < threshold {
            return;
        }
        if !matches!(
            self.vacuum_sched.pending_trigger.get(),
            Some(VacuumTrigger::Manual)
        ) {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::HighWater));
        }
        self.vacuum_sched.next_deadline_ms.set(0);
    }

    pub(crate) fn compute_vacuum_horizon(&self) -> Option<CommitId> {
        if let Some(table) = &self.commit_table {
            let guard = table.lock();
            Some(guard.vacuum_horizon(self.vacuum_cfg.retention_window))
        } else {
            Some(COMMIT_MAX)
        }
    }

    pub(crate) fn select_vacuum_mode(&self) -> VacuumMode {
        let bytes = self.version_log_bytes();
        let high_water = self.vacuum_cfg.log_high_water_bytes;
        let stats = self.store.stats();
        let retention_ms = self.vacuum_cfg.retention_window.as_millis().max(1) as u64;
        let reader_lag_ms = stats.mvcc_reader_max_age_ms;
        let reader_lag_ratio = (reader_lag_ms as f64 / retention_ms as f64).min(1.0);
        let fast_due_to_lag = reader_lag_ratio >= 0.8;
        let slow_due_to_lag = reader_lag_ratio <= 0.25;
        let fast_due_to_bytes = high_water > 0 && bytes >= high_water.saturating_mul(3) / 2;
        let slow_due_to_bytes = high_water > 0 && bytes <= high_water / 4;
        let mode = if fast_due_to_bytes || fast_due_to_lag {
            VacuumMode::Fast
        } else if slow_due_to_bytes && slow_due_to_lag {
            VacuumMode::Slow
        } else {
            VacuumMode::Normal
        };
        let mode_label = match mode {
            VacuumMode::Fast => "fast",
            VacuumMode::Normal => "normal",
            VacuumMode::Slow => "slow",
        };
        self.metrics.mvcc_vacuum_mode(mode_label);
        mode
    }

    fn record_vacuum_stats(&self, stats: GraphVacuumStats) {
        self.publish_vacuum_metrics(&stats);
        *self.vacuum_sched.last_stats.borrow_mut() = Some(stats.clone());
        self.log_vacuum_stats(&stats);
    }

    fn publish_vacuum_metrics(&self, stats: &GraphVacuumStats) {
        self.metrics
            .vacuum_versions_pruned(stats.log_versions_pruned);
        self.metrics
            .vacuum_orphan_versions_pruned(stats.orphan_log_versions_pruned);
        self.metrics
            .vacuum_tombstone_heads_purged(stats.heads_purged);
        self.metrics
            .vacuum_adjacency_pruned(stats.adjacency_fwd_pruned, stats.adjacency_rev_pruned);
        self.metrics.vacuum_index_entries_pruned(
            stats.index_label_pruned,
            stats.index_chunked_pruned,
            stats.index_btree_pruned,
        );
        self.metrics.vacuum_bytes_reclaimed(stats.bytes_reclaimed);
        self.metrics.vacuum_run_millis(stats.run_millis);
        self.metrics.vacuum_horizon_commit(stats.horizon_commit);
        self.publish_version_log_usage_metrics();
    }

    fn log_vacuum_stats(&self, stats: &GraphVacuumStats) {
        let made_progress = stats.log_versions_pruned > 0
            || stats.orphan_log_versions_pruned > 0
            || stats.heads_purged > 0
            || stats.adjacency_fwd_pruned > 0
            || stats.adjacency_rev_pruned > 0
            || stats.index_label_pruned > 0
            || stats.index_chunked_pruned > 0
            || stats.index_btree_pruned > 0
            || stats.bytes_reclaimed > 0;
        if made_progress {
            info!(
                horizon = stats.horizon_commit,
                trigger = ?stats.trigger,
                run_millis = stats.run_millis,
                versions = stats.log_versions_pruned,
                orphan_versions = stats.orphan_log_versions_pruned,
                tombstone_heads = stats.heads_purged,
                adj_fwd = stats.adjacency_fwd_pruned,
                adj_rev = stats.adjacency_rev_pruned,
                index_label = stats.index_label_pruned,
                index_chunked = stats.index_chunked_pruned,
                index_btree = stats.index_btree_pruned,
                bytes_reclaimed = stats.bytes_reclaimed,
                "graph.vacuum.completed"
            );
            return;
        }
        debug!(
            horizon = stats.horizon_commit,
            trigger = ?stats.trigger,
            run_millis = stats.run_millis,
            "graph.vacuum.noop"
        );
    }
}
