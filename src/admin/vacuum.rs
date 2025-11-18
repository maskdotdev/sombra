use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::time::Instant;

use crate::primitives::pager::{
    CheckpointMode, Meta, PageStore, Pager, MVCC_READER_WARN_THRESHOLD_MS,
};
use crate::storage::catalog::Dict;
use crate::storage::{Graph, VacuumTrigger, COMMIT_MAX};
use crate::types::{NodeId, StrId};
use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::{ensure_parent_dir, lock_path, open_graph, wal_path};
use crate::admin::{AdminError, Result};
use tracing::{info, warn};

/// Options for configuring the vacuum operation.
#[derive(Clone, Debug, Default)]
pub struct VacuumOptions {
    /// Whether to analyze the database during vacuum.
    pub analyze: bool,
}

/// Report generated after a vacuum operation completes.
#[derive(Debug, Clone, Serialize)]
pub struct VacuumReport {
    /// Duration of the vacuum operation in milliseconds.
    pub duration_ms: f64,
    /// Number of bytes copied to the new database file.
    pub copied_bytes: u64,
    /// LSN of the last checkpoint.
    pub checkpoint_lsn: u64,
    /// Whether analysis was performed during vacuum.
    pub analyze_performed: bool,
    /// Optional summary of database analysis results.
    pub analyze_summary: Option<AnalyzeSummary>,
    /// Number of historical versions pruned from the version log.
    pub version_log_pruned: u64,
    /// Forward adjacency entries pruned.
    pub adjacency_fwd_pruned: u64,
    /// Reverse adjacency entries pruned.
    pub adjacency_rev_pruned: u64,
    /// Label index entries pruned.
    pub index_label_pruned: u64,
    /// Chunked property segments pruned.
    pub index_chunked_pruned: u64,
    /// B-tree property postings pruned.
    pub index_btree_pruned: u64,
    /// Number of active MVCC readers when vacuum ran.
    pub mvcc_readers_active: Option<u64>,
    /// Oldest reader snapshot commit.
    pub mvcc_reader_oldest_snapshot: Option<u64>,
    /// Maximum reader age observed in milliseconds.
    pub mvcc_reader_max_age_ms: Option<u64>,
    /// Optional warning when readers block MVCC cleanup.
    pub mvcc_warning: Option<String>,
}

/// Summary of database analysis results from a vacuum operation.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AnalyzeSummary {
    /// Statistics for each label in the database.
    pub label_counts: Vec<LabelStat>,
}

/// Statistics for a single label.
#[derive(Debug, Clone, Serialize)]
pub struct LabelStat {
    /// Numeric identifier of the label.
    pub label_id: u32,
    /// Human-readable name of the label, if available.
    pub label_name: Option<String>,
    /// Number of nodes with this label.
    pub nodes: u64,
}

/// Vacuums a database by copying it to a new location and optionally analyzing it.
///
/// This operation checkpoints the source database, copies it to the destination,
/// and optionally collects statistics about the database contents.
///
/// # Errors
///
/// Returns an error if:
/// - The source and destination paths are the same
/// - The destination path already exists
/// - Opening the source database fails
/// - The checkpoint or copy operation fails
pub fn vacuum_into(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    open_opts: &AdminOpenOptions,
    opts: &VacuumOptions,
) -> Result<VacuumReport> {
    let src_path = src.as_ref();
    let dst_path = dst.as_ref();
    if src_path == dst_path {
        return Err(AdminError::Message(
            "vacuum destination must differ from source".to_string(),
        ));
    }
    if dst_path.exists() {
        return Err(AdminError::Message(format!(
            "vacuum destination already exists: {}",
            dst_path.display()
        )));
    }

    let start = Instant::now();
    let handle = open_graph(src_path, open_opts)?;
    let pager = handle.pager.clone();
    let graph = handle.graph.clone();
    let dict = handle.dict.clone();
    pager.checkpoint(CheckpointMode::Force)?;
    let meta = pager.meta()?;

    let analyze_summary = if opts.analyze {
        Some(run_analyze(&graph, &dict, pager.as_ref(), &meta)?)
    } else {
        None
    };

    let (horizon, reader_snapshot) = match graph.commit_table() {
        Some(table) => {
            let guard = table.lock();
            let reader_snapshot = guard.reader_snapshot(Instant::now());
            let horizon = guard.oldest_visible();
            drop(guard);
            (horizon, Some(reader_snapshot))
        }
        None => (COMMIT_MAX, None),
    };
    let vacuum_stats = graph.vacuum_mvcc(horizon, None, VacuumTrigger::Manual, None)?;

    drop(graph);
    drop(dict);
    drop(pager);

    ensure_parent_dir(dst_path)?;
    let copied = fs::copy(src_path, dst_path)?;

    let total_pruned = vacuum_stats.versions_pruned
        + vacuum_stats.adjacency_fwd_pruned
        + vacuum_stats.adjacency_rev_pruned
        + vacuum_stats.index_label_pruned
        + vacuum_stats.index_chunked_pruned
        + vacuum_stats.index_btree_pruned;

    let (mvcc_readers_active, mvcc_reader_oldest_snapshot, mvcc_reader_max_age_ms, mvcc_warning) =
        match &reader_snapshot {
            Some(snapshot) => {
                let oldest = snapshot
                    .oldest_snapshot
                    .unwrap_or(meta.last_checkpoint_lsn.0);
                let warning = if snapshot.active > 0
                    && snapshot.max_age_ms >= MVCC_READER_WARN_THRESHOLD_MS
                    && total_pruned == 0
                {
                    Some(format!(
                        "unable to reclaim MVCC versions: {} readers active (oldest commit {}, max_age {} ms)",
                        snapshot.active, oldest, snapshot.max_age_ms
                    ))
                } else {
                    None
                };
                (
                    Some(snapshot.active),
                    Some(oldest),
                    Some(snapshot.max_age_ms),
                    warning,
                )
            }
            None => (None, None, None, None),
        };
    if let Some(warning) = &mvcc_warning {
        warn!(%warning, "admin.vacuum.reader_lag");
    }
    let report = VacuumReport {
        duration_ms: start.elapsed().as_secs_f64() * 1_000.0,
        copied_bytes: copied,
        checkpoint_lsn: meta.last_checkpoint_lsn.0,
        analyze_performed: opts.analyze,
        analyze_summary,
        version_log_pruned: vacuum_stats.versions_pruned,
        adjacency_fwd_pruned: vacuum_stats.adjacency_fwd_pruned,
        adjacency_rev_pruned: vacuum_stats.adjacency_rev_pruned,
        index_label_pruned: vacuum_stats.index_label_pruned,
        index_chunked_pruned: vacuum_stats.index_chunked_pruned,
        index_btree_pruned: vacuum_stats.index_btree_pruned,
        mvcc_readers_active,
        mvcc_reader_oldest_snapshot,
        mvcc_reader_max_age_ms,
        mvcc_warning,
    };
    if let Some(snapshot) = reader_snapshot {
        info!(
            version_log_pruned = report.version_log_pruned,
            adjacency_fwd_pruned = report.adjacency_fwd_pruned,
            adjacency_rev_pruned = report.adjacency_rev_pruned,
            index_label_pruned = report.index_label_pruned,
            index_chunked_pruned = report.index_chunked_pruned,
            index_btree_pruned = report.index_btree_pruned,
            reader_count = snapshot.active,
            reader_oldest_commit = snapshot
                .oldest_snapshot
                .unwrap_or(meta.last_checkpoint_lsn.0),
            reader_max_age_ms = snapshot.max_age_ms,
            duration_ms = report.duration_ms,
            copied_bytes = report.copied_bytes,
            "admin.vacuum.completed"
        );
    } else {
        info!(
            version_log_pruned = report.version_log_pruned,
            adjacency_fwd_pruned = report.adjacency_fwd_pruned,
            adjacency_rev_pruned = report.adjacency_rev_pruned,
            index_label_pruned = report.index_label_pruned,
            index_chunked_pruned = report.index_chunked_pruned,
            index_btree_pruned = report.index_btree_pruned,
            duration_ms = report.duration_ms,
            copied_bytes = report.copied_bytes,
            "admin.vacuum.completed"
        );
    }
    Ok(report)
}

/// Promotes a vacuumed database copy by swapping it into place.
///
/// # Arguments
/// * `src` - Path to the original database file.
/// * `staging` - Path to the compacted copy produced by [`vacuum_into`].
/// * `backup` - Optional path to store the previous database contents. When
///   provided, the original database (and its WAL/lock files) are renamed to
///   the backup path. Otherwise they are removed.
pub fn promote_vacuumed_copy(
    src: impl AsRef<Path>,
    staging: impl AsRef<Path>,
    backup: Option<impl AsRef<Path>>,
) -> Result<()> {
    let src = src.as_ref();
    let staging = staging.as_ref();
    let backup = backup.map(|value| value.as_ref().to_path_buf());
    if src == staging {
        return Err(AdminError::Message(
            "vacuum staging path must differ from the source database".into(),
        ));
    }
    if !staging.exists() {
        return Err(AdminError::Message(format!(
            "vacuum staging file not found: {}",
            staging.display()
        )));
    }
    if !src.exists() {
        return Err(AdminError::missing_database(src));
    }

    info!(
        db_path = %src.display(),
        staging_path = %staging.display(),
        backup_path = backup.as_ref().map(|p| p.display().to_string()),
        "admin.vacuum.promote.begin"
    );

    let wal_src = wal_path(src);
    let lock_src = lock_path(src);
    if let Some(backup) = backup.as_ref() {
        if backup == src || backup == staging {
            return Err(AdminError::Message(
                "backup path must differ from source and staging paths".into(),
            ));
        }
        if backup.exists() {
            return Err(AdminError::Message(format!(
                "backup path already exists: {}",
                backup.display()
            )));
        }
        ensure_parent_dir(backup)?;
        info!(
            db_path = %src.display(),
            backup_path = %backup.display(),
            "admin.vacuum.promote.backup_move"
        );
        fs::rename(src, backup)?;
        let wal_backup = wal_path(backup);
        let lock_backup = lock_path(backup);
        rename_if_exists(&wal_src, &wal_backup)?;
        rename_if_exists(&lock_src, &lock_backup)?;
        info!(
            db_path = %src.display(),
            wal_backup = %wal_backup.display(),
            lock_backup = %lock_backup.display(),
            "admin.vacuum.promote.backup_aux"
        );
    } else {
        info!(
            db_path = %src.display(),
            "admin.vacuum.promote.remove_old"
        );
        fs::remove_file(src)?;
        remove_if_exists(&wal_src)?;
        remove_if_exists(&lock_src)?;
    }

    fs::rename(staging, src)?;
    info!(
        db_path = %src.display(),
        staging_path = %staging.display(),
        backup_path = backup.as_ref().map(|p| p.display().to_string()),
        "admin.vacuum.promote.complete"
    );
    Ok(())
}

fn run_analyze(graph: &Graph, dict: &Dict, pager: &Pager, meta: &Meta) -> Result<AnalyzeSummary> {
    let read = pager.begin_latest_committed_read()?;
    let max_node = meta.storage_next_node_id.saturating_sub(1);
    let mut labels: HashMap<u32, u64> = HashMap::new();
    for raw_id in 1..=max_node {
        if let Some(node) = graph.get_node(&read, NodeId(raw_id))? {
            for label in node.labels {
                *labels.entry(label.0).or_insert(0) += 1;
            }
        }
    }

    let mut label_counts = Vec::with_capacity(labels.len());
    for (label_id, count) in labels {
        let name = dict.resolve(&read, StrId(label_id)).ok();
        label_counts.push(LabelStat {
            label_id,
            label_name: name,
            nodes: count,
        });
    }
    drop(read);

    label_counts.sort_by(|a, b| a.label_id.cmp(&b.label_id));
    Ok(AnalyzeSummary { label_counts })
}

fn rename_if_exists(src: &Path, dst: &Path) -> std::io::Result<()> {
    match fs::rename(src, dst) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}
