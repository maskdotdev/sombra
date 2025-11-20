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
    /// Async fsync backlog vs persisted durable watermark.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub async_fsync: Option<MvccAsyncFsync>,
    /// Commit table snapshot (present when MVCC is enabled for the database).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_table: Option<CommitTableReport>,
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

/// Serializable snapshot of the commit table state.
#[derive(Debug, Clone, Serialize)]
pub struct CommitTableReport {
    pub released_up_to: u64,
    pub oldest_visible: u64,
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
}

impl From<CommitStatus> for CommitStatusKind {
    fn from(status: CommitStatus) -> Self {
        match status {
            CommitStatus::Pending => CommitStatusKind::Pending,
            CommitStatus::Committed => CommitStatusKind::Committed,
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
        retention_window_ms: snapshot.retention_window.as_millis().min(u64::MAX as u128) as u64,
        latest_committed_lsn: snapshot.latest_committed_lsn.map(|lsn| lsn.0),
        durable_lsn: snapshot.durable_lsn.map(|lsn| lsn.0),
        acked_not_durable_commits: snapshot.acked_not_durable_commits,
        wal_backlog: snapshot.wal_backlog.map(wal_backlog_report),
        wal_allocator: snapshot.wal_allocator.map(wal_allocator_report),
        async_fsync: snapshot.async_fsync_backlog.map(async_fsync_report),
        commit_table: snapshot.commit_table.map(commit_table_report),
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
