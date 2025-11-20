use std::path::Path;

use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::open_graph;
use crate::admin::Result;
use crate::primitives::pager::AsyncFsyncBacklog;
use crate::primitives::wal::{WalAllocatorStats, WalCommitBacklog};
use crate::storage::{
    CommitEntrySnapshot, CommitStatus, CommitTableSnapshot, ReaderSnapshot, ReaderSnapshotEntry,
};

/// MVCC diagnostic report returned by `sombra admin mvcc-status`.
#[derive(Debug, Clone, Serialize)]
pub struct MvccStatusReport {
    /// Bytes retained inside the version log B-tree.
    pub version_log_bytes: u64,
    /// Number of entries stored in the version log.
    pub version_log_entries: u64,
    /// Version cache hits since startup.
    pub version_cache_hits: u64,
    /// Version cache misses since startup.
    pub version_cache_misses: u64,
    /// Raw bytes passed through the version codec.
    pub version_codec_raw_bytes: u64,
    /// Encoded bytes produced by the version codec.
    pub version_codec_encoded_bytes: u64,
    /// Retention window (milliseconds) used when computing the vacuum horizon.
    pub retention_window_ms: u64,
    /// Latest committed LSN when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_committed_lsn: Option<u64>,
    /// Durable watermark LSN when async fsync is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durable_lsn: Option<u64>,
    /// Number of commits acknowledged but not durable yet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acked_not_durable_commits: Option<u64>,
    /// Pending WAL commit backlog when group commit is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_backlog: Option<MvccWalBacklog>,
    /// WAL allocator/preallocation queues.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_allocator: Option<MvccWalAllocator>,
    /// Alerts derived from WAL/async-fsync state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wal_alerts: Vec<String>,
    /// Recommended WAL reuse/preallocation depth when backlog is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_reuse_recommended_segments: Option<u64>,
    /// Async fsync backlog vs persisted durable watermark.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_fsync: Option<MvccAsyncFsync>,
    /// Commit table snapshot (present when MVCC is enabled for the database).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_table: Option<CommitTableReport>,
    /// Current vacuum mode chosen by the scheduler.
    pub vacuum_mode: String,
    /// Current vacuum horizon if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vacuum_horizon: Option<u64>,
    /// Snapshot pool occupancy when enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_pool: Option<MvccSnapshotPool>,
}

/// Snapshot of pending WAL commit work.
#[derive(Debug, Clone, Serialize)]
pub struct MvccWalBacklog {
    pub pending_commits: u64,
    pub pending_frames: u64,
    pub worker_running: bool,
}

/// WAL allocator/preallocation queues for segmented WAL.
#[derive(Debug, Clone, Serialize)]
pub struct MvccWalAllocator {
    pub segment_size_bytes: u64,
    pub preallocate_segments: u32,
    pub ready_segments: u64,
    pub recycle_segments: u64,
    pub reused_segments_total: u64,
    pub created_segments_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_error: Option<String>,
}

/// Async fsync backlog relative to the persisted durable cookie.
#[derive(Debug, Clone, Serialize)]
pub struct MvccAsyncFsync {
    pub pending_lsn: u64,
    pub durable_lsn: u64,
    pub pending_lag: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Snapshot pool occupancy status.
#[derive(Debug, Clone, Serialize)]
pub struct MvccSnapshotPool {
    pub capacity: usize,
    pub available: usize,
}

/// Serializable snapshot of the commit table state.
#[derive(Debug, Clone, Serialize)]
pub struct CommitTableReport {
    pub released_up_to: u64,
    pub oldest_visible: u64,
    pub acked_not_durable: u64,
    pub entries: Vec<CommitEntryReport>,
    pub reader_snapshot: ReaderSnapshotReport,
}

/// Human-friendly representation of an individual commit entry.
#[derive(Debug, Clone, Serialize)]
pub struct CommitEntryReport {
    /// Commit identifier.
    pub id: u64,
    /// Lifecycle state for the commit.
    pub status: CommitStatusKind,
    /// Number of readers currently referencing this commit entry.
    pub reader_refs: u32,
    /// Age of the commit in milliseconds (if committed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committed_ms_ago: Option<u64>,
}

/// Serializable copy of [`ReaderSnapshot`].
#[derive(Debug, Clone, Serialize)]
pub struct ReaderSnapshotReport {
    pub active: u64,
    pub oldest_snapshot: Option<u64>,
    pub newest_snapshot: Option<u64>,
    pub max_age_ms: u64,
    pub slow_readers: Vec<SlowReaderReport>,
}

/// Slow reader details with thread identifier converted to a string.
#[derive(Debug, Clone, Serialize)]
pub struct SlowReaderReport {
    pub reader_id: u32,
    pub snapshot_commit: u64,
    pub age_ms: u64,
    pub thread: String,
}

/// Enumerates commit lifecycle states for serialization.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommitStatusKind {
    /// Commit has been reserved but not finalized.
    Pending,
    /// Commit finished and is visible to readers.
    Committed,
    /// Commit flushed to durable storage.
    Durable,
}

impl From<CommitStatus> for CommitStatusKind {
    fn from(status: CommitStatus) -> Self {
        match status {
            CommitStatus::Pending => CommitStatusKind::Pending,
            CommitStatus::Committed => CommitStatusKind::Committed,
            CommitStatus::Durable => CommitStatusKind::Durable,
        }
    }
}

/// Collects MVCC diagnostics for the database located at `path`.
pub fn mvcc_status(path: impl AsRef<Path>, opts: &AdminOpenOptions) -> Result<MvccStatusReport> {
    let handle = open_graph(path.as_ref(), opts)?;
    let snapshot = handle.graph.mvcc_status();
    Ok(MvccStatusReport {
        version_log_bytes: snapshot.version_log_bytes,
        version_log_entries: snapshot.version_log_entries,
        version_cache_hits: snapshot.version_cache_hits,
        version_cache_misses: snapshot.version_cache_misses,
        version_codec_raw_bytes: snapshot.version_codec_raw_bytes,
        version_codec_encoded_bytes: snapshot.version_codec_encoded_bytes,
        retention_window_ms: snapshot.retention_window.as_millis().min(u64::MAX as u128) as u64,
        latest_committed_lsn: snapshot.latest_committed_lsn.map(|lsn| lsn.0),
        durable_lsn: snapshot.durable_lsn.map(|lsn| lsn.0),
        acked_not_durable_commits: snapshot.acked_not_durable_commits,
        wal_backlog: snapshot.wal_backlog.map(wal_backlog_report),
        wal_allocator: snapshot.wal_allocator.map(wal_allocator_report),
        wal_alerts: snapshot.wal_alerts,
        wal_reuse_recommended_segments: snapshot.wal_reuse_recommended,
        async_fsync: snapshot.async_fsync_backlog.map(async_fsync_report),
        commit_table: snapshot.commit_table.map(commit_table_report),
        vacuum_mode: format!("{:?}", snapshot.vacuum_mode),
        vacuum_horizon: snapshot.vacuum_horizon,
        snapshot_pool: snapshot.snapshot_pool.map(|pool| MvccSnapshotPool {
            capacity: pool.capacity,
            available: pool.available,
        }),
    })
}

fn wal_backlog_report(snapshot: WalCommitBacklog) -> MvccWalBacklog {
    MvccWalBacklog {
        pending_commits: snapshot.pending_commits as u64,
        pending_frames: snapshot.pending_frames as u64,
        worker_running: snapshot.worker_running,
    }
}

fn wal_allocator_report(snapshot: WalAllocatorStats) -> MvccWalAllocator {
    MvccWalAllocator {
        segment_size_bytes: snapshot.segment_size_bytes,
        preallocate_segments: snapshot.preallocate_segments,
        ready_segments: snapshot.ready_segments as u64,
        recycle_segments: snapshot.recycle_segments as u64,
        reused_segments_total: snapshot.reused_segments_total,
        created_segments_total: snapshot.created_segments_total,
        allocation_error: snapshot.allocation_error,
    }
}

fn async_fsync_report(snapshot: AsyncFsyncBacklog) -> MvccAsyncFsync {
    MvccAsyncFsync {
        pending_lsn: snapshot.pending_lsn.0,
        durable_lsn: snapshot.durable_lsn.0,
        pending_lag: snapshot.pending_lag,
        last_error: snapshot.last_error,
    }
}

fn commit_table_report(snapshot: CommitTableSnapshot) -> CommitTableReport {
    CommitTableReport {
        released_up_to: snapshot.released_up_to,
        oldest_visible: snapshot.oldest_visible,
        acked_not_durable: snapshot.acked_not_durable,
        entries: snapshot
            .entries
            .into_iter()
            .map(commit_entry_report)
            .collect(),
        reader_snapshot: reader_snapshot_report(snapshot.reader_snapshot),
    }
}

fn commit_entry_report(entry: CommitEntrySnapshot) -> CommitEntryReport {
    CommitEntryReport {
        id: entry.id,
        status: entry.status.into(),
        reader_refs: entry.reader_refs,
        committed_ms_ago: entry.committed_ms_ago,
    }
}

fn reader_snapshot_report(snapshot: ReaderSnapshot) -> ReaderSnapshotReport {
    let slow_readers = snapshot
        .slow_readers
        .into_iter()
        .map(slow_reader_report)
        .collect();
    ReaderSnapshotReport {
        active: snapshot.active,
        oldest_snapshot: snapshot.oldest_snapshot,
        newest_snapshot: snapshot.newest_snapshot,
        max_age_ms: snapshot.max_age_ms,
        slow_readers,
    }
}

fn slow_reader_report(entry: ReaderSnapshotEntry) -> SlowReaderReport {
    SlowReaderReport {
        reader_id: entry.reader_id,
        snapshot_commit: entry.snapshot_commit,
        age_ms: entry.age_ms,
        thread: format!("{:?}", entry.thread_id),
    }
}
