use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

use crate::primitives::pager::{CheckpointMode, Meta, PageStore, Pager};
use crate::storage::catalog::Dict;
use crate::storage::Graph;
use crate::types::{NodeId, StrId};
use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::{ensure_parent_dir, open_graph};
use crate::admin::{AdminError, Result};

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
    drop(graph);
    drop(dict);
    drop(pager);

    ensure_parent_dir(dst_path)?;
    let copied = fs::copy(src_path, dst_path)?;

    Ok(VacuumReport {
        duration_ms: start.elapsed().as_secs_f64() * 1_000.0,
        copied_bytes: copied,
        checkpoint_lsn: meta.last_checkpoint_lsn.0,
        analyze_performed: opts.analyze,
        analyze_summary,
    })
}

fn run_analyze(graph: &Graph, dict: &Dict, pager: &Pager, meta: &Meta) -> Result<AnalyzeSummary> {
    let read = pager.begin_read()?;
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
