use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::primitives::pager::{PageStore, Pager};
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::{Graph, GraphOptions};

use crate::admin::error::{AdminError, Result};
use crate::admin::options::AdminOpenOptions;

/// Handle containing pager, graph, and dictionary for administrative operations.
pub struct GraphHandle {
    /// The pager instance managing page-level storage.
    pub pager: Arc<Pager>,
    /// The graph database instance.
    pub graph: Arc<Graph>,
    /// The dictionary for string interning and lookups.
    pub dict: Arc<Dict>,
}

/// Opens or creates a pager at the specified path.
///
/// # Errors
///
/// Returns an error if the database doesn't exist and `create_if_missing` is false,
/// or if opening/creating the pager fails.
pub fn open_pager(path: &Path, opts: &AdminOpenOptions) -> Result<Arc<Pager>> {
    if !path.exists() {
        if opts.create_if_missing {
            ensure_parent_dir(path)?;
            let pager = Pager::create(path, opts.pager.clone())?;
            return Ok(Arc::new(pager));
        } else {
            return Err(AdminError::missing_database(path));
        }
    }
    let pager = Pager::open(path, opts.pager.clone())?;
    Ok(Arc::new(pager))
}

/// Opens a graph database with pager, graph, and dictionary components.
///
/// # Errors
///
/// Returns an error if opening the pager or initializing graph/dictionary fails.
pub fn open_graph(path: &Path, opts: &AdminOpenOptions) -> Result<GraphHandle> {
    let pager = open_pager(path, opts)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let graph_opts = GraphOptions::new(Arc::clone(&store))
        .distinct_neighbors_default(opts.distinct_neighbors_default);
    let graph = Arc::new(Graph::open(graph_opts)?);
    let dict = Arc::new(Dict::open(store, DictOptions::default())?);
    Ok(GraphHandle { pager, graph, dict })
}

pub(crate) fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

pub(crate) fn wal_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("sombra"));
    name.push("-wal");
    let mut output = path.to_path_buf();
    output.set_file_name(name);
    output
}
