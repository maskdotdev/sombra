use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::Path;

use serde::Serialize;
use sombra_pager::{PageStore, ReadGuard};
use sombra_storage::Graph;
use sombra_types::{EdgeId, NodeId, TypeId};

use crate::options::AdminOpenOptions;
use crate::util::open_graph;
use crate::Result;

const MAX_FINDINGS: usize = 32;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyLevel {
    Fast,
    Full,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifySeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct VerifyFinding {
    pub severity: VerifySeverity,
    pub message: String,
}

impl VerifyFinding {
    fn error(message: impl Into<String>) -> Self {
        Self {
            severity: VerifySeverity::Error,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct VerifyCounts {
    pub nodes_found: u64,
    pub edges_found: u64,
    pub adjacency_entries: u64,
    pub adjacency_nodes_touched: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct VerifyReport {
    pub level: VerifyLevel,
    pub success: bool,
    pub findings: Vec<VerifyFinding>,
    pub counts: VerifyCounts,
}

pub fn verify(
    path: impl AsRef<Path>,
    opts: &AdminOpenOptions,
    level: VerifyLevel,
) -> Result<VerifyReport> {
    let handle = open_graph(path.as_ref(), opts)?;
    let pager = handle.pager.clone();
    let graph = handle.graph;
    let mut findings = Vec::new();
    let mut counts = VerifyCounts::default();

    let meta = pager.meta()?;
    if meta.page_size == 0 {
        push_error(&mut findings, "meta page reports zero page size");
    }

    if matches!(level, VerifyLevel::Full) {
        let read = pager.begin_read()?;
        let nodes = collect_nodes(
            &graph,
            &read,
            meta.storage_next_node_id,
            &mut findings,
            &mut counts,
        )?;
        let edges = collect_edges(
            &graph,
            &read,
            meta.storage_next_edge_id,
            &nodes,
            &mut findings,
            &mut counts,
        )?;
        run_adjacency_checks(&graph, &read, &nodes, &edges, &mut findings, &mut counts)?;
    }

    Ok(VerifyReport {
        level,
        success: findings.is_empty(),
        findings,
        counts,
    })
}

fn run_adjacency_checks(
    graph: &Graph,
    read: &ReadGuard,
    nodes: &HashSet<u64>,
    edges: &HashSet<u64>,
    findings: &mut Vec<VerifyFinding>,
    counts: &mut VerifyCounts,
) -> Result<()> {
    let fwd_entries = match graph.debug_collect_adj_fwd(read) {
        Ok(entries) => entries,
        Err(err) => {
            push_error(findings, format!("failed to scan forward adjacency: {err}"));
            Vec::new()
        }
    };
    let rev_entries = match graph.debug_collect_adj_rev(read) {
        Ok(entries) => entries,
        Err(err) => {
            push_error(findings, format!("failed to scan reverse adjacency: {err}"));
            Vec::new()
        }
    };

    counts.adjacency_entries = (fwd_entries.len() + rev_entries.len()) as u64;
    if fwd_entries.is_empty() && rev_entries.is_empty() {
        return Ok(());
    }

    let mut rev_map: HashSet<EdgeRef> = rev_entries
        .into_iter()
        .map(|(dst, ty, src, edge)| EdgeRef::new(src, ty, dst, edge))
        .collect();
    let mut adjacency_edge_ids: HashSet<u64> = HashSet::new();
    let mut sampled_nodes: HashSet<u64> = HashSet::new();

    for (src, ty, dst, edge) in &fwd_entries {
        let ref_key = EdgeRef::new(*src, *ty, *dst, *edge);
        if !rev_map.remove(&ref_key) {
            push_error(
                findings,
                format!(
                    "reverse adjacency missing for edge {} ({} -> {} type {})",
                    edge.0, src.0, dst.0, ty.0
                ),
            );
        }

        if nodes.contains(&src.0) {
            sampled_nodes.insert(src.0);
        } else {
            push_error(
                findings,
                format!("adjacency references missing node {}", src.0),
            );
        }
        if nodes.contains(&dst.0) {
            sampled_nodes.insert(dst.0);
        } else {
            push_error(
                findings,
                format!("adjacency references missing node {}", dst.0),
            );
        }

        match graph.get_edge(read, *edge) {
            Ok(Some(data)) => {
                if data.src != *src || data.dst != *dst || data.ty != *ty {
                    push_error(
                        findings,
                        format!(
                            "edge {} payload mismatch (expected {}-{} type {}, found {}-{} type {})",
                            edge.0,
                            src.0,
                            dst.0,
                            ty.0,
                            data.src.0,
                            data.dst.0,
                            data.ty.0
                        ),
                    );
                }
            }
            Ok(None) => {
                push_error(
                    findings,
                    format!("adjacency references missing edge {}", edge.0),
                );
            }
            Err(err) => {
                push_error(findings, format!("failed to load edge {}: {err}", edge.0));
            }
        }

        if !adjacency_edge_ids.insert(edge.0) {
            push_error(
                findings,
                format!("duplicate adjacency entry for edge {}", edge.0),
            );
        }

        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }

    if !rev_map.is_empty() && findings.len() < MAX_FINDINGS {
        let sample = rev_map.iter().next().copied();
        if let Some(orphan) = sample {
            push_error(
                findings,
                format!(
                    "reverse adjacency entry without forward counterpart (edge {} between {} and {})",
                    orphan.edge, orphan.src, orphan.dst
                ),
            );
        } else {
            push_error(
                findings,
                "reverse adjacency entries remain without matching forward entries",
            );
        }
    }

    for edge_id in edges {
        if !adjacency_edge_ids.contains(edge_id) {
            push_error(
                findings,
                format!("edge {} missing adjacency entries", edge_id),
            );
        }
        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }

    counts.adjacency_nodes_touched = sampled_nodes.len() as u64;
    Ok(())
}

fn collect_nodes(
    graph: &Graph,
    read: &ReadGuard,
    next_node_id: u64,
    findings: &mut Vec<VerifyFinding>,
    counts: &mut VerifyCounts,
) -> Result<HashSet<u64>> {
    let mut nodes = HashSet::new();
    let max_id = next_node_id.saturating_sub(1);
    if max_id == 0 {
        return Ok(nodes);
    }
    for raw_id in 1..=max_id {
        match graph.get_node(read, NodeId(raw_id))? {
            Some(_data) => {
                nodes.insert(raw_id);
                counts.nodes_found += 1;
            }
            None => {}
        }
        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }
    Ok(nodes)
}

fn collect_edges(
    graph: &Graph,
    read: &ReadGuard,
    next_edge_id: u64,
    nodes: &HashSet<u64>,
    findings: &mut Vec<VerifyFinding>,
    counts: &mut VerifyCounts,
) -> Result<HashSet<u64>> {
    let mut edges_set = HashSet::new();
    let max_id = next_edge_id.saturating_sub(1);
    if max_id == 0 {
        return Ok(edges_set);
    }
    for raw_id in 1..=max_id {
        match graph.get_edge(read, EdgeId(raw_id))? {
            Some(data) => {
                edges_set.insert(raw_id);
                counts.edges_found += 1;
                if !nodes.contains(&data.src.0) {
                    push_error(
                        findings,
                        format!("edge {} references missing src node {}", raw_id, data.src.0),
                    );
                }
                if !nodes.contains(&data.dst.0) {
                    push_error(
                        findings,
                        format!("edge {} references missing dst node {}", raw_id, data.dst.0),
                    );
                }
            }
            None => {}
        }
        if findings.len() >= MAX_FINDINGS {
            break;
        }
    }
    Ok(edges_set)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EdgeRef {
    src: u64,
    ty: u32,
    dst: u64,
    edge: u64,
}

impl EdgeRef {
    fn new(src: NodeId, ty: TypeId, dst: NodeId, edge: EdgeId) -> Self {
        Self {
            src: src.0,
            ty: ty.0,
            dst: dst.0,
            edge: edge.0,
        }
    }
}

impl Hash for EdgeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.src.hash(state);
        self.ty.hash(state);
        self.dst.hash(state);
        self.edge.hash(state);
    }
}

fn push_error(findings: &mut Vec<VerifyFinding>, message: impl Into<String>) {
    if findings.len() < MAX_FINDINGS {
        findings.push(VerifyFinding::error(message.into()));
    }
}
