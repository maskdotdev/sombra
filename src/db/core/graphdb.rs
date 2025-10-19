use lru::LruCache;
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{GraphError, Result};
use crate::index::BTreeIndex;
use crate::model::{EdgeId, NodeId};
use crate::pager::{PageId, Pager};
use crate::storage::header::Header;
use crate::storage::RecordPointer;

use super::header::HeaderState;
use crate::db::config::{Config, SyncMode};
use crate::db::group_commit::{GroupCommitState, TxId};
use crate::db::metrics::PerformanceMetrics;
use crate::db::transaction::Transaction;

pub struct GraphDB {
    pub(crate) path: PathBuf,
    pub(crate) pager: Pager,
    pub header: HeaderState,
    pub(crate) node_index: BTreeIndex,
    pub(crate) edge_index: HashMap<EdgeId, RecordPointer>,
    pub(crate) label_index: HashMap<String, BTreeSet<NodeId>>,
    pub(crate) node_cache: LruCache<NodeId, Node>,
    pub(crate) edge_cache: LruCache<EdgeId, Edge>,
    pub(crate) outgoing_adjacency: HashMap<NodeId, Vec<EdgeId>>,
    pub(crate) incoming_adjacency: HashMap<NodeId, Vec<EdgeId>>,
    pub(crate) outgoing_neighbors_cache: HashMap<NodeId, Vec<NodeId>>,
    pub(crate) incoming_neighbors_cache: HashMap<NodeId, Vec<NodeId>>,
    pub(crate) next_tx_id: TxId,
    pub(crate) tracking_enabled: bool,
    pub(crate) recent_dirty_pages: Vec<PageId>,
    pub active_transaction: Option<TxId>,
    pub(crate) config: Config,
    pub(crate) transactions_since_sync: usize,
    pub(crate) transactions_since_checkpoint: usize,
    pub(crate) group_commit_state: Option<Arc<Mutex<GroupCommitState>>>,
    pub metrics: PerformanceMetrics,
}

impl std::fmt::Debug for GraphDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphDB")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("next_tx_id", &self.next_tx_id)
            .field("tracking_enabled", &self.tracking_enabled)
            .field("active_transaction", &self.active_transaction)
            .field("config", &self.config)
            .field("transactions_since_sync", &self.transactions_since_sync)
            .field(
                "transactions_since_checkpoint",
                &self.transactions_since_checkpoint,
            )
            .finish()
    }
}

impl GraphDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path_ref = path.as_ref();
        let wal_sync_enabled = config.wal_sync_mode != SyncMode::Off;
        let use_mmap = config.use_mmap;
        let cache_size = config.page_cache_size;
        let mut pager =
            Pager::open_with_full_config(path_ref, wal_sync_enabled, use_mmap, cache_size)?;
        let page_size = pager.page_size();

        if pager.page_count() == 0 {
            pager.allocate_page()?;
        }

        let header = {
            let page = pager.fetch_page(0)?;
            match Header::read(&page.data)? {
                Some(header) => {
                    if header.page_size as usize != page_size {
                        return Err(GraphError::Corruption(
                            "page size mismatch between header and pager".into(),
                        ));
                    }
                    header
                }
                None => {
                    let header = Header::new(page_size)?;
                    Header::write(&header, &mut page.data)?;
                    page.dirty = true;
                    header
                }
            }
        };

        let next_tx_id = header.last_committed_tx_id + 1;

        let group_commit_state = if config.wal_sync_mode == SyncMode::GroupCommit {
            Some(GroupCommitState::spawn(
                path_ref.to_path_buf(),
                config.group_commit_timeout_ms,
            )?)
        } else {
            None
        };

        let cache_size =
            NonZeroUsize::new(config.page_cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap());
        let edge_cache_size = NonZeroUsize::new(config.page_cache_size * 10)
            .unwrap_or(NonZeroUsize::new(10000).unwrap());

        let mut db = Self {
            path: path_ref.to_path_buf(),
            pager,
            header: HeaderState::from(header),
            node_index: BTreeIndex::new(),
            edge_index: HashMap::new(),
            label_index: HashMap::new(),
            node_cache: LruCache::new(cache_size),
            edge_cache: LruCache::new(edge_cache_size),
            outgoing_adjacency: HashMap::new(),
            incoming_adjacency: HashMap::new(),
            outgoing_neighbors_cache: HashMap::new(),
            incoming_neighbors_cache: HashMap::new(),
            next_tx_id,
            tracking_enabled: false,
            recent_dirty_pages: Vec::new(),
            active_transaction: None,
            config,
            transactions_since_sync: 0,
            transactions_since_checkpoint: 0,
            group_commit_state,
            metrics: PerformanceMetrics::new(),
        };

        if db.load_btree_index()? {
        } else {
            db.rebuild_indexes()?;
        }
        Ok(db)
    }

    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        let tx_id = self.allocate_tx_id()?;
        Transaction::new(self, tx_id)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.write_header()?;
        self.pager.checkpoint()
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.persist_btree_index()?;
        self.write_header()?;
        self.pager.checkpoint()?;
        if !self.load_btree_index()? {
            return Err(GraphError::Corruption(
                "failed to reload btree index after checkpoint".into(),
            ));
        }
        Ok(())
    }

    pub fn page_size(&self) -> usize {
        self.pager.page_size()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

use crate::model::{Edge, Node};
