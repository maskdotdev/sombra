use std::path::Path;

use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::open_graph;
use crate::admin::Result;
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
    /// Commit table snapshot (present when MVCC is enabled for the database).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_table: Option<CommitTableReport>,
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
        commit_table: snapshot.commit_table.map(commit_table_report),
    })
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
