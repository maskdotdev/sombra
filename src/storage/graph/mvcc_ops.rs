use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::primitives::pager::{AsyncFsyncBacklog, ReadGuard, WriteGuard};
use crate::primitives::wal::{WalAllocatorStats, WalCommitBacklog};
use crate::storage::btree::{BTree, ValCodec};
use crate::storage::mvcc::{
    CommitId, CommitTable, VersionHeader, VersionLogEntry, VersionPtr, VersionSpace,
    VersionedValue, COMMIT_MAX,
};
use crate::storage::mvcc_flags;
use crate::storage::{
    edge, node, record_mvcc_commit, record_mvcc_read_begin, record_mvcc_write_begin,
};
use crate::types::{EdgeId, Lsn, NodeId, Result, SombraError};

use super::graph_types::{GraphMvccStatus, MVCC_METRICS_PUBLISH_INTERVAL};
use super::version_cache::VersionChainRecord;
use super::{Graph, MicroGcTrigger, RootKind, UnitValue};

const WAL_BACKLOG_COMMITS_ALERT: usize = 8;
const WAL_BACKLOG_FRAMES_ALERT: usize = 2_048;
pub(crate) const ASYNC_FSYNC_LAG_ALERT: u64 = 4_096;
pub(crate) const WAL_HORIZON_LAG_ALERT: u64 = 32_768;

pub(crate) fn wal_health(
    page_size: u32,
    wal_backlog: Option<&WalCommitBacklog>,
    wal_allocator: Option<&WalAllocatorStats>,
    async_fsync: Option<&AsyncFsyncBacklog>,
    vacuum_horizon: Option<CommitId>,
    durable_lsn: Option<Lsn>,
) -> (Vec<String>, Option<u64>) {
    let mut alerts = Vec::new();
    let mut recommended_reuse: Option<u64> = None;
    if let Some(backlog) = wal_backlog {
        if backlog.pending_commits > WAL_BACKLOG_COMMITS_ALERT
            || backlog.pending_frames > WAL_BACKLOG_FRAMES_ALERT
        {
            alerts.push(format!(
                "wal backlog high ({} commits, {} frames)",
                backlog.pending_commits, backlog.pending_frames
            ));
        }
    }
    if let Some(async_status) = async_fsync {
        if async_status.pending_lag > ASYNC_FSYNC_LAG_ALERT {
            alerts.push(format!(
                "async fsync lagging by {} LSN from durable cookie",
                async_status.pending_lag
            ));
        }
        if let Some(err) = async_status.last_error.as_ref() {
            alerts.push(format!("async fsync error: {err}"));
        }
    }
    if let Some(allocator) = wal_allocator {
        if let Some(err) = allocator.allocation_error.as_ref() {
            alerts.push(format!("wal allocation error: {err}"));
        }
        if allocator.segment_size_bytes > 0 && page_size > 0 {
            let frames_per_segment = allocator.segment_size_bytes / page_size as u64;
            if frames_per_segment > 0 {
                if let Some(backlog) = wal_backlog {
                    let needed_segments =
                        (backlog.pending_frames as u64).div_ceil(frames_per_segment);
                    let readyish =
                        allocator.ready_segments as u64 + allocator.recycle_segments as u64;
                    if needed_segments > readyish {
                        alerts.push(format!(
                            "wal reuse queue short by {} segments (need ~{}, ready {}, recycle {}, target {})",
                            needed_segments.saturating_sub(readyish),
                            needed_segments,
                            allocator.ready_segments,
                            allocator.recycle_segments,
                            allocator.preallocate_segments
                        ));
                        recommended_reuse =
                            Some(needed_segments.max(allocator.preallocate_segments as u64));
                    } else if allocator.preallocate_segments as u64 > readyish {
                        recommended_reuse = Some(allocator.preallocate_segments as u64);
                    }
                }
            }
        }
    }
    if let (Some(horizon), Some(durable)) = (vacuum_horizon, durable_lsn) {
        if horizon != COMMIT_MAX {
            let lag = durable.0.saturating_sub(horizon);
            if lag > WAL_HORIZON_LAG_ALERT {
                alerts.push(format!(
                    "vacuum horizon lags durable LSN by {lag} commits; WAL reuse may be pinned by readers"
                ));
            }
        }
    }
    (alerts, recommended_reuse)
}

impl Graph {
    pub(crate) fn log_version_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        space: VersionSpace,
        id: u64,
        header: VersionHeader,
        prev_ptr: VersionPtr,
        bytes: Vec<u8>,
    ) -> Result<VersionPtr> {
        let ptr_value = self.next_version_ptr.fetch_add(1, Ordering::SeqCst);
        if ptr_value == 0 {
            return Err(SombraError::Corruption("version log pointer overflowed"));
        }
        let codec_outcome = self.version_codec_cfg.apply_owned(bytes)?;
        let raw_len = u32::try_from(codec_outcome.raw_len)
            .map_err(|_| SombraError::Invalid("version log payload too large"))?;
        let entry = VersionLogEntry {
            space,
            id,
            header,
            prev_ptr,
            codec: codec_outcome.codec,
            raw_len,
            bytes: codec_outcome.encoded,
        };
        let encoded = entry.encode()?;
        self.metrics.version_codec_bytes(
            entry.codec.as_str(),
            u64::from(raw_len),
            encoded.len() as u64,
        );
        self.version_codec_raw_bytes
            .fetch_add(u64::from(raw_len), Ordering::Relaxed);
        self.version_codec_encoded_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        if let Some(cache) = &self.version_cache {
            cache.insert(VersionPtr::from_raw(ptr_value), Arc::new(entry.clone()));
        }
        self.version_log.put(tx, &ptr_value, &encoded)?;
        self.version_log_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        self.version_log_entries.fetch_add(1, Ordering::Relaxed);
        self.publish_version_log_usage_metrics();
        self.maybe_signal_high_water();
        self.persist_tree_root(tx, RootKind::VersionLog)?;
        let next_ptr = ptr_value
            .checked_add(1)
            .ok_or(SombraError::Corruption("version log pointer overflowed"))?;
        tx.update_meta(|meta| {
            if meta.storage_next_version_ptr <= ptr_value {
                meta.storage_next_version_ptr = next_ptr;
            }
        })?;
        Ok(VersionPtr::from_raw(ptr_value))
    }

    pub(crate) fn load_version_entry(
        &self,
        tx: &ReadGuard,
        ptr: VersionPtr,
    ) -> Result<Option<VersionLogEntry>> {
        if ptr.is_null() {
            return Ok(None);
        }
        if let Some(cache) = &self.version_cache {
            if let Some(hit) = cache.get(ptr) {
                self.metrics.version_cache_hit();
                self.version_cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some((*hit).clone()));
            }
        }
        if self.version_cache.is_some() {
            self.request_micro_gc(MicroGcTrigger::CacheMiss);
        }
        let Some(bytes) = self.version_log.get(tx, &ptr.raw())? else {
            return Ok(None);
        };
        let decoded = VersionLogEntry::decode(&bytes)?;
        if let Some(cache) = &self.version_cache {
            cache.insert(ptr, Arc::new(decoded.clone()));
            self.metrics.version_cache_miss();
            self.version_cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(decoded))
    }

    /// Returns the commit table when the underlying pager provides one.
    pub fn commit_table(&self) -> Option<Arc<Mutex<CommitTable>>> {
        self.commit_table.as_ref().map(Arc::clone)
    }

    #[inline]
    pub(crate) fn version_log_bytes(&self) -> u64 {
        self.version_log_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn version_log_entry_count(&self) -> u64 {
        self.version_log_entries.load(Ordering::Relaxed)
    }

    pub(crate) fn publish_version_log_usage_metrics(&self) {
        self.metrics
            .version_log_usage(self.version_log_bytes(), self.version_log_entry_count());
    }

    /// Returns the oldest reader commit currently pinned by any snapshot.
    pub fn oldest_reader_commit(&self) -> Option<CommitId> {
        let table = self.commit_table.as_ref()?;
        let snapshot = table.lock().reader_snapshot(Instant::now());
        snapshot.oldest_snapshot
    }

    #[inline]
    pub(crate) fn begin_read_guard(&self) -> Result<ReadGuard> {
        let start = Instant::now();
        let guard = self.store.begin_read()?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_read_latency_ns(nanos);
        record_mvcc_read_begin(nanos);
        Ok(guard)
    }

    #[inline]
    pub(crate) fn begin_write_guard(&self) -> Result<WriteGuard<'_>> {
        let start = Instant::now();
        let guard = self.store.begin_write()?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_write_latency_ns(nanos);
        record_mvcc_write_begin(nanos);
        Ok(guard)
    }

    #[inline]
    pub(crate) fn commit_with_metrics(&self, write: WriteGuard<'_>) -> Result<Lsn> {
        let start = Instant::now();
        let lsn = self.store.commit(write)?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_commit_latency_ns(nanos);
        record_mvcc_commit(nanos);
        Ok(lsn)
    }

    /// Returns the configured retention window for MVCC vacuum.
    pub fn vacuum_retention_window(&self) -> std::time::Duration {
        self.vacuum_cfg.retention_window
    }

    /// Returns MVCC-related diagnostics for the graph.
    pub fn mvcc_status(&self) -> GraphMvccStatus {
        let commit_table = self
            .commit_table
            .as_ref()
            .map(|table| table.lock().snapshot(Instant::now()));
        let latest_committed_lsn = self.store.latest_committed_lsn();
        let durable_lsn = self.store.durable_lsn();
        let wal_backlog = self.store.wal_commit_backlog();
        let wal_allocator = self.store.wal_allocator_stats();
        let async_fsync_backlog = self.store.async_fsync_backlog();
        let snapshot_pool = self.snapshot_pool.as_ref().map(|pool| pool.status());
        let vacuum_horizon = self.compute_vacuum_horizon();
        let vacuum_mode = self.select_vacuum_mode();
        let version_cache_hits = self.version_cache_hits.load(Ordering::Relaxed);
        let version_cache_misses = self.version_cache_misses.load(Ordering::Relaxed);
        let version_codec_raw_bytes = self.version_codec_raw_bytes.load(Ordering::Relaxed);
        let version_codec_encoded_bytes = self.version_codec_encoded_bytes.load(Ordering::Relaxed);
        let acked_not_durable_commits = commit_table
            .as_ref()
            .map(|snapshot| snapshot.acked_not_durable)
            .or_else(|| match (latest_committed_lsn, durable_lsn) {
                (Some(latest), Some(durable)) => Some(latest.0.saturating_sub(durable.0)),
                _ => None,
            });
        if let Some(backlog) = acked_not_durable_commits {
            self.metrics.mvcc_commit_backlog(backlog);
        }
        let (wal_alerts, wal_reuse_recommended) = wal_health(
            self.store.page_size(),
            wal_backlog.as_ref(),
            wal_allocator.as_ref(),
            async_fsync_backlog.as_ref(),
            vacuum_horizon,
            durable_lsn,
        );
        GraphMvccStatus {
            version_log_bytes: self.version_log_bytes(),
            version_log_entries: self.version_log_entry_count(),
            version_cache_hits,
            version_cache_misses,
            version_codec_raw_bytes,
            version_codec_encoded_bytes,
            retention_window: self.vacuum_cfg.retention_window,
            commit_table,
            latest_committed_lsn,
            durable_lsn,
            acked_not_durable_commits,
            wal_backlog,
            wal_allocator,
            async_fsync_backlog,
            snapshot_pool,
            vacuum_mode,
            vacuum_horizon,
            wal_alerts,
            wal_reuse_recommended,
        }
    }

    pub(crate) fn maybe_publish_mvcc_metrics(&self) {
        if self.commit_table.is_none() {
            return;
        }
        let mut last = match self.mvcc_metrics_last.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let now = Instant::now();
        if let Some(prev) = *last {
            if now.duration_since(prev) < MVCC_METRICS_PUBLISH_INTERVAL {
                return;
            }
        }
        *last = Some(now);
        drop(last);
        let stats = self.store.stats();
        let oldest_reader_commit = stats.mvcc_reader_oldest_snapshot;
        self.metrics.mvcc_page_versions(
            stats.mvcc_page_versions_total,
            stats.mvcc_pages_with_versions,
        );
        self.metrics.mvcc_reader_gauges(
            stats.mvcc_readers_active,
            stats.mvcc_reader_oldest_snapshot,
            stats.mvcc_reader_newest_snapshot,
            stats.mvcc_reader_max_age_ms,
        );
        self.metrics
            .mvcc_reader_totals(stats.mvcc_reader_begin_total, stats.mvcc_reader_end_total);
        self.indexes.set_oldest_reader_commit(oldest_reader_commit);
        self.vstore.set_oldest_reader_commit(oldest_reader_commit);
    }

    #[inline]
    pub(crate) fn tx_version_header(&self, tx: &mut WriteGuard<'_>) -> (CommitId, VersionHeader) {
        self.maybe_publish_mvcc_metrics();
        let commit_lsn = tx.reserve_commit_id();
        let commit_id = commit_lsn.0;
        (commit_id, VersionHeader::new(commit_id, COMMIT_MAX, 0, 0))
    }

    #[inline]
    pub(crate) fn tx_pending_version_header(
        &self,
        tx: &mut WriteGuard<'_>,
    ) -> (CommitId, VersionHeader) {
        let (commit_id, mut header) = self.tx_version_header(tx);
        header.set_pending();
        (commit_id, header)
    }

    pub(crate) fn adjacency_value_for_commit(
        commit: CommitId,
        tombstone: bool,
    ) -> VersionedValue<UnitValue> {
        let mut header = VersionHeader::new(commit, COMMIT_MAX, 0, 0);
        if tombstone {
            header.flags |= mvcc_flags::TOMBSTONE;
        }
        VersionedValue::new(header, UnitValue)
    }

    pub(crate) fn finalize_version_header(&self, header: &mut VersionHeader) -> bool {
        if !header.is_pending() {
            return false;
        }
        header.clear_pending();
        true
    }

    #[inline]
    pub(crate) fn reader_snapshot_commit(tx: &ReadGuard) -> CommitId {
        tx.snapshot_lsn().0
    }

    #[inline]
    pub(crate) fn version_visible(header: &VersionHeader, snapshot: CommitId) -> bool {
        header.visible_at(snapshot) && !header.is_tombstone() && !header.is_pending()
    }

    pub(crate) fn visible_version<T, Decode>(
        &self,
        tx: &ReadGuard,
        space: VersionSpace,
        id: u64,
        bytes: &[u8],
        decode: Decode,
    ) -> Result<Option<T>>
    where
        T: VersionChainRecord,
        Decode: Fn(&[u8]) -> Result<T>,
    {
        let snapshot = Self::reader_snapshot_commit(tx);
        let current = decode(bytes)?;
        if Self::version_visible(<T as VersionChainRecord>::header(&current), snapshot) {
            return Ok(Some(current));
        }
        if let Some(inline) = <T as VersionChainRecord>::inline_history(&current) {
            let decoded_inline = decode(inline)?;
            if Self::version_visible(<T as VersionChainRecord>::header(&decoded_inline), snapshot) {
                return Ok(Some(decoded_inline));
            }
            let mut ptr = <T as VersionChainRecord>::prev_ptr(&decoded_inline);
            while let Some(entry) = self.load_version_entry(tx, ptr)? {
                if entry.space != space || entry.id != id {
                    ptr = entry.prev_ptr;
                    continue;
                }
                let decoded = decode(&entry.bytes)?;
                if Self::version_visible(<T as VersionChainRecord>::header(&decoded), snapshot) {
                    return Ok(Some(decoded));
                }
                ptr = <T as VersionChainRecord>::prev_ptr(&decoded);
            }
            return Ok(None);
        }
        let mut ptr = <T as VersionChainRecord>::prev_ptr(&current);
        if ptr.is_null() {
            return Ok(None);
        }
        while let Some(entry) = self.load_version_entry(tx, ptr)? {
            if entry.space != space || entry.id != id {
                ptr = entry.prev_ptr;
                continue;
            }
            let decoded = decode(&entry.bytes)?;
            if Self::version_visible(<T as VersionChainRecord>::header(&decoded), snapshot) {
                return Ok(Some(decoded));
            }
            ptr = <T as VersionChainRecord>::prev_ptr(&decoded);
        }
        Ok(None)
    }

    pub(crate) fn visible_node_from_bytes(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        bytes: &[u8],
    ) -> Result<Option<node::VersionedNodeRow>> {
        self.visible_version(tx, VersionSpace::Node, id.0, bytes, node::decode)
    }

    pub(crate) fn visible_edge_from_bytes(
        &self,
        tx: &ReadGuard,
        id: EdgeId,
        bytes: &[u8],
    ) -> Result<Option<edge::VersionedEdgeRow>> {
        self.visible_version(tx, VersionSpace::Edge, id.0, bytes, edge::decode)
    }

    pub(crate) fn visible_node(
        &self,
        tx: &ReadGuard,
        id: NodeId,
    ) -> Result<Option<node::VersionedNodeRow>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        self.visible_node_from_bytes(tx, id, &bytes)
    }

    pub(crate) fn visible_edge(
        &self,
        tx: &ReadGuard,
        id: EdgeId,
    ) -> Result<Option<edge::VersionedEdgeRow>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        self.visible_edge_from_bytes(tx, id, &bytes)
    }

    pub(crate) fn recompute_version_log_bytes(&self) -> Result<()> {
        let read = self.begin_read_guard()?;
        let mut cursor = self.version_log.range(
            &read,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
        )?;
        let mut bytes = 0u64;
        while let Some((_, value)) = cursor.next()? {
            bytes = bytes.saturating_add(value.len() as u64);
        }
        self.version_log_bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn retire_version_resources(
        &self,
        tx: &mut WriteGuard<'_>,
        entry: &VersionLogEntry,
    ) -> Result<()> {
        match entry.space {
            VersionSpace::Node => {
                let versioned = node::decode(&entry.bytes)?;
                self.free_node_props(tx, versioned.row.props)
            }
            VersionSpace::Edge => {
                let versioned = edge::decode(&entry.bytes)?;
                self.free_edge_props(tx, versioned.row.props)
            }
        }
    }

    pub(crate) fn prune_versioned_vec_tree<V: ValCodec>(
        tree: &BTree<Vec<u8>, VersionedValue<V>>,
        tx: &mut WriteGuard<'_>,
        horizon: CommitId,
    ) -> Result<u64> {
        let mut keys = Vec::new();
        tree.for_each_with_write(tx, |key, value| {
            if value.header.end != COMMIT_MAX && value.header.end <= horizon {
                keys.push(key);
            }
            Ok(())
        })?;
        let mut pruned = 0u64;
        for key in keys {
            if tree.delete(tx, &key)? {
                pruned = pruned.saturating_add(1);
            }
        }
        Ok(pruned)
    }
}
