use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use sombra_catalog::Dict;
use sombra_pager::{CheckpointMode, Meta, PageStore, Pager};
use sombra_storage::Graph;
use sombra_types::{NodeId, StrId};

use crate::options::AdminOpenOptions;
use crate::util::{ensure_parent_dir, open_graph};
use crate::{AdminError, Result};

#[derive(Clone, Debug, Default)]
pub struct VacuumOptions {
    pub analyze: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct VacuumReport {
    pub duration_ms: f64,
    pub copied_bytes: u64,
    pub checkpoint_lsn: u64,
    pub analyze_performed: bool,
    pub analyze_summary: Option<AnalyzeSummary>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct AnalyzeSummary {
    pub label_counts: Vec<LabelStat>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelStat {
    pub label_id: u32,
    pub label_name: Option<String>,
    pub nodes: u64,
}

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
