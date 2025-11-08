use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::primitives::pager::{PageStore, Pager};
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::{Graph, GraphOptions};

use crate::admin::error::{AdminError, Result};
use crate::admin::options::AdminOpenOptions;

pub struct GraphHandle {
    pub pager: Arc<Pager>,
    pub graph: Arc<Graph>,
    pub dict: Arc<Dict>,
}

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
