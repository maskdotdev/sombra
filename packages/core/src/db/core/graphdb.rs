use crc32fast::hash;
use lru::LruCache;
use rayon::ThreadPoolBuilder;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{info, warn};

use crate::error::{GraphError, Result};
use crate::index::BTreeIndex;
use crate::model::{Edge, EdgeId, Node, NodeId, PropertyValue};
use crate::pager::{PageId, Pager, PAGE_CHECKSUM_SIZE};
use crate::storage::header::Header;
use crate::storage::page::RecordPage;
use crate::storage::record::{RecordHeader, RecordKind, RECORD_HEADER_SIZE};
use crate::storage::RecordPointer;
use crate::storage::{deserialize_edge, deserialize_node};
use crate::storage::heap::RecordStore;

use super::header::HeaderState;
use crate::db::config::{Config, SyncMode};
use crate::db::gc::{BackgroundGcState, GarbageCollector, GcConfig, GcStats};
use crate::db::group_commit::{GroupCommitState, TxId};
use crate::db::metrics::{ConcurrencyMetrics, PerformanceMetrics};
use crate::db::timestamp_oracle::TimestampOracle;
use crate::db::transaction::Transaction;

/// Values that can be indexed for fast property-based lookups.
///
/// Only certain property types are indexable. Float and Bytes values
/// cannot be indexed due to their nature.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexableValue {
    /// Boolean value
    Bool(bool),
    /// 64-bit integer value
    Int(i64),
    /// String value
    String(String),
}

/// Options for database integrity verification.
///
/// Controls the scope and depth of integrity checks performed by
/// [`GraphDB::verify_integrity`].
#[derive(Debug, Clone)]
pub struct IntegrityOptions {
    /// Only verify page checksums, skip record validation
    pub checksum_only: bool,
    /// Maximum number of errors to collect before stopping
    pub max_errors: usize,
    /// Verify that indexes match the actual data
    pub verify_indexes: bool,
    /// Verify that edge references point to valid nodes
    pub verify_adjacency: bool,
}

impl Default for IntegrityOptions {
    fn default() -> Self {
        Self {
            checksum_only: false,
            max_errors: 16,
            verify_indexes: true,
            verify_adjacency: true,
        }
    }
}

/// Report from database integrity verification.
///
/// Contains counts of different types of integrity issues found
/// during verification, plus detailed error messages.
#[derive(Debug)]
pub struct IntegrityReport {
    /// Number of pages that were checked
    pub checked_pages: usize,
    /// Number of pages with checksum mismatches
    pub checksum_failures: usize,
    /// Number of malformed records found
    pub record_errors: usize,
    /// Number of index inconsistencies found
    pub index_errors: usize,
    /// Number of broken adjacency references found
    pub adjacency_errors: usize,
    /// Detailed error messages (up to max_errors)
    pub errors: Vec<String>,
    max_errors: usize,
}

impl IntegrityReport {
    fn new(max_errors: usize) -> Self {
        Self {
            checked_pages: 0,
            checksum_failures: 0,
            record_errors: 0,
            index_errors: 0,
            adjacency_errors: 0,
            errors: Vec::new(),
            max_errors,
        }
    }

    fn push_error(&mut self, message: String) {
        if self.errors.len() < self.max_errors {
            self.errors.push(message);
        }
    }

    pub fn is_clean(&self) -> bool {
        self.checksum_failures == 0
            && self.record_errors == 0
            && self.index_errors == 0
            && self.adjacency_errors == 0
            && self.errors.is_empty()
    }
}

static RAYON_THREAD_POOL: OnceLock<()> = OnceLock::new();

fn configure_rayon_thread_pool(config: &Config) {
    let desired = config.rayon_thread_pool_size;
    let _ = RAYON_THREAD_POOL.get_or_init(|| {
        let mut builder = ThreadPoolBuilder::new();
        if let Some(num_threads) = desired {
            builder = builder.num_threads(num_threads);
        }
        if let Err(err) = builder.build_global() {
            if let Some(num_threads) = desired {
                warn!(
                    threads = num_threads,
                    error = %err,
                    "Failed to configure Rayon global thread pool"
                );
            } else {
                warn!(error = %err, "Failed to configure Rayon global thread pool");
            }
        }
    });
}

impl From<&PropertyValue> for Option<IndexableValue> {
    fn from(value: &PropertyValue) -> Self {
        match value {
            PropertyValue::Bool(b) => Some(IndexableValue::Bool(*b)),
            PropertyValue::Int(i) => Some(IndexableValue::Int(*i)),
            PropertyValue::String(s) => Some(IndexableValue::String(s.clone())),
            PropertyValue::Float(_) | PropertyValue::Bytes(_) => None,
        }
    }
}

/// Main graph database interface.
///
/// GraphDB provides ACID transactions, WAL-based durability, and
/// comprehensive indexing for graph data operations.
///
/// # Thread Safety
///
/// GraphDB is not thread-safe. Use external synchronization (`Arc<Mutex>`)
/// for concurrent access from multiple threads.
///
/// # Examples
///
/// ```rust
/// use sombra::{GraphDB, Node, Edge};
///
/// let mut db = GraphDB::open("example.db")?;
/// let mut tx = db.begin_transaction()?;
/// let node = tx.add_node(Node::new(1))?;
/// tx.commit()?;
/// # Ok::<(), sombra::GraphError>(())
/// ```
pub struct GraphDB {
    pub(crate) path: PathBuf,
    pub(crate) pager: Pager,
    pub header: HeaderState,
    pub(crate) epoch: AtomicU64,
    pub(crate) node_index: BTreeIndex,
    pub(crate) edge_index: HashMap<EdgeId, RecordPointer>,
    pub(crate) label_index: HashMap<String, BTreeSet<NodeId>>,
    pub(crate) property_indexes:
        HashMap<(String, String), BTreeMap<IndexableValue, BTreeSet<NodeId>>>,
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
    pub concurrency_metrics: Arc<ConcurrencyMetrics>,
    pub(crate) pages_with_free_slots: BTreeSet<PageId>,
    // MVCC support
    pub(crate) timestamp_oracle: Option<Arc<TimestampOracle>>,
    pub(crate) gc: Option<GarbageCollector>,
    pub(crate) bg_gc_state: Option<Arc<Mutex<BackgroundGcState>>>,
}

impl std::fmt::Debug for GraphDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphDB")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("epoch", &self.epoch.load(Ordering::Relaxed))
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
    /// Opens a graph database at the specified path with default configuration.
    ///
    /// Creates a new database if it doesn't exist. Performs WAL recovery
    /// if the database was not cleanly closed.
    ///
    /// # Arguments
    /// * `path` - Filesystem path to the database file
    ///
    /// # Returns
    /// A new `GraphDB` instance with default configuration.
    ///
    /// # Errors
    /// * `GraphError::Io` - Cannot create/open file
    /// * `GraphError::Corruption` - Database file is corrupted
    ///
    /// # Example
    /// ```rust
    /// use sombra::GraphDB;
    ///
    /// let db = GraphDB::open("my_graph.db")?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    ///
    /// # Safety
    /// Only one process should access the database at a time.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    /// Opens a graph database with custom configuration.
    ///
    /// # Arguments
    /// * `path` - Filesystem path to the database file
    /// * `config` - Database configuration options
    ///
    /// # Returns
    /// A new `GraphDB` instance with the specified configuration.
    ///
    /// # Errors
    /// * `GraphError::Io` - Cannot create/open file
    /// * `GraphError::Corruption` - Database file is corrupted
    /// * `GraphError::InvalidArgument` - Invalid configuration
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Config};
    ///
    /// let config = Config::production();
    /// let db = GraphDB::open_with_config("my_graph.db", config)?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let path_ref = path.as_ref();
        info!(
            path = ?path_ref,
            sync_mode = ?config.wal_sync_mode,
            cache_size = config.page_cache_size,
            checksum_enabled = config.checksum_enabled,
            "Opening database"
        );
        configure_rayon_thread_pool(&config);
        let wal_sync_enabled = config.wal_sync_mode != SyncMode::Off;
        let use_mmap = config.use_mmap;
        let cache_size = config.page_cache_size;
        let max_size_bytes = config.max_database_size_mb.map(|mb| mb * 1024 * 1024);
        let mut pager = Pager::open_with_all_config(
            path_ref,
            wal_sync_enabled,
            use_mmap,
            cache_size,
            config.checksum_enabled,
            max_size_bytes,
        )?;
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

        let cache_size = NonZeroUsize::new(config.page_cache_size)
            .or_else(|| NonZeroUsize::new(1000))
            .ok_or_else(|| {
                GraphError::InvalidArgument("page_cache_size must be greater than zero".into())
            })?;
        let edge_cache_input = config.page_cache_size.saturating_mul(10);
        let edge_cache_size = NonZeroUsize::new(edge_cache_input)
            .or_else(|| NonZeroUsize::new(10000))
            .ok_or_else(|| {
                GraphError::InvalidArgument("edge cache size must be greater than zero".into())
            })?;

        // Initialize MVCC components if enabled
        let (timestamp_oracle, gc, bg_gc_state) = if config.mvcc_enabled {
            // Restore timestamp oracle from persisted state if available
            let oracle = if header.max_timestamp > 0 {
                Arc::new(TimestampOracle::with_timestamp(header.max_timestamp)?)
            } else {
                Arc::new(TimestampOracle::new())
            };
            let collector = GarbageCollector::new();
            
            // Start background GC if configured
            let bg_gc = if let Some(_interval_secs) = config.gc_interval_secs {
                // Note: Background GC will be started via start_background_gc() after construction
                None
            } else {
                None
            };
            
            (Some(oracle), Some(collector), bg_gc)
        } else {
            (None, None, None)
        };

        let mut db = Self {
            path: path_ref.to_path_buf(),
            pager,
            header: HeaderState::from(header),
            epoch: AtomicU64::new(0),
            node_index: BTreeIndex::new(),
            edge_index: HashMap::new(),
            label_index: HashMap::new(),
            property_indexes: HashMap::new(),
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
            concurrency_metrics: Arc::new(ConcurrencyMetrics::new()),
            pages_with_free_slots: BTreeSet::new(),
            timestamp_oracle,
            gc,
            bg_gc_state,
        };

        // Update header to reflect MVCC state
        if db.config.mvcc_enabled {
            db.header.mvcc_enabled = true;
        }

        if db.load_btree_index()? {
            info!("Loaded existing BTree index");
            if db.load_property_indexes()? {
                info!("Loaded existing property indexes");
            } else {
                warn!("Property indexes not found, will rebuild");
            }
        } else {
            warn!("Rebuilding indexes from scratch");
            db.rebuild_indexes()?;
        }
        info!("Database opened successfully");
        Ok(db)
    }

    /// Begins a new transaction for atomic operations.
    ///
    /// Only one transaction can be active at a time. The transaction
    /// must be either committed or rolled back before another can begin.
    ///
    /// # Returns
    /// A new `Transaction` instance for performing atomic operations.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - A transaction is already active
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let node_id = tx.add_node(Node::new(1))?;
    /// tx.commit()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        let tx_id = self.allocate_tx_id()?;
        Transaction::new(self, tx_id)
    }

    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn increment_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Flushes dirty pages to disk without checkpointing the WAL.
    ///
    /// This is a lightweight operation that writes dirty pages but
    /// keeps the WAL intact for crash recovery. For full durability,
    /// use `checkpoint()` instead.
    ///
    /// # Returns
    /// Ok(()) on success, error on failure.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error
    pub fn flush(&mut self) -> Result<()> {
        self.write_header()?;
        self.pager.checkpoint()
    }

    /// Checkpoints the database by flushing dirty pages and truncating the WAL.
    ///
    /// This is a full durability operation that:
    /// 1. Persists the B-tree index
    /// 2. Writes the database header
    /// 3. Flushes all dirty pages to disk
    /// 4. Truncates the WAL file
    ///
    /// After a successful checkpoint, the database can recover without
    /// replaying any WAL frames.
    ///
    /// # Returns
    /// Ok(()) on success, error on failure.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error
    /// * `GraphError::Corruption` - Index corruption detected
    pub fn checkpoint(&mut self) -> Result<()> {
        let start = std::time::Instant::now();
        let pages_flushed = self.pager.dirty_page_count();
        info!("Starting checkpoint");

        self.start_tracking();

        self.persist_btree_index()?;
        self.persist_property_indexes()?;
        
        // Update timestamp in header if MVCC is enabled
        if let Some(ref oracle) = self.timestamp_oracle {
            self.header.max_timestamp = oracle.current_timestamp();
        }
        
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.stop_tracking();

        for &page_id in &dirty_pages {
            self.pager.append_page_to_wal(page_id, 0)?;
        }

        self.pager.checkpoint()?;

        if !self.load_btree_index()? {
            return Err(GraphError::Corruption(
                "failed to reload btree index after checkpoint".into(),
            ));
        }

        let duration = start.elapsed();
        info!(
            pages_flushed,
            duration_ms = duration.as_millis(),
            "Checkpoint completed"
        );
        Ok(())
    }

    /// Returns the database page size in bytes.
    ///
    /// The page size is determined when the database is created and
    /// cannot be changed afterwards. Typical values are 4096 or 8192.
    ///
    /// # Returns
    /// The page size in bytes.
    pub fn page_size(&self) -> usize {
        self.pager.page_size()
    }

    /// Returns the filesystem path of the database.
    ///
    /// # Returns
    /// A reference to the database file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Verifies the integrity of the database.
    ///
    /// Performs comprehensive checks including:
    /// - Page checksum validation
    /// - Record format validation
    /// - Index consistency checks
    /// - Adjacency reference validation
    ///
    /// # Arguments
    /// * `options` - Configuration for what to verify and error limits
    ///
    /// # Returns
    /// An `IntegrityReport` detailing any issues found.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error during verification
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, IntegrityOptions};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let options = IntegrityOptions::default();
    /// let report = db.verify_integrity(options)?;
    /// if !report.is_clean() {
    ///     println!("Database has integrity issues: {:?}", report.errors);
    /// }
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn verify_integrity(&mut self, mut options: IntegrityOptions) -> Result<IntegrityReport> {
        if options.max_errors == 0 {
            options.max_errors = usize::MAX;
        }

        let verify_records = !options.checksum_only;
        let verify_indexes = verify_records && options.verify_indexes;
        let verify_adjacency = verify_records && options.verify_adjacency;

        let mut report = IntegrityReport::new(options.max_errors);

        let mut node_slots: HashMap<(PageId, u16), NodeId> = HashMap::new();
        let mut edge_slots: HashMap<(PageId, u16), EdgeId> = HashMap::new();
        let mut nodes_seen: HashSet<NodeId> = HashSet::new();
        let mut edges_seen: HashMap<EdgeId, (NodeId, NodeId)> = HashMap::new();

        let page_count = self.pager.page_count();
        for page_index in 0..page_count {
            let page_id = page_index as PageId;
            let mut page_bytes = match self.pager.read_page_image(page_id) {
                Ok(data) => data,
                Err(GraphError::Corruption(message)) => {
                    if message.contains("checksum") {
                        report.checksum_failures += 1;
                    } else {
                        report.record_errors += 1;
                    }
                    report.push_error(format!("failed to read page {page_id}: {message}"));
                    continue;
                }
                Err(err) => {
                    report.record_errors += 1;
                    report.push_error(format!("failed to read page {page_id}: {err:?}"));
                    continue;
                }
            };

            report.checked_pages += 1;
            if page_bytes.len() < PAGE_CHECKSUM_SIZE {
                report.checksum_failures += 1;
                report.push_error(format!(
                    "page {page_id} shorter than checksum trailer ({} bytes)",
                    page_bytes.len()
                ));
                continue;
            }

            let payload_len = page_bytes.len() - PAGE_CHECKSUM_SIZE;
            let (payload, checksum_tail) = page_bytes.split_at(payload_len);
            if checksum_tail.len() != PAGE_CHECKSUM_SIZE {
                report.checksum_failures += 1;
                report.push_error(format!(
                    "page {page_id} checksum trailer has unexpected length {}",
                    checksum_tail.len()
                ));
                continue;
            }

            let stored = u32::from_le_bytes([
                checksum_tail[0],
                checksum_tail[1],
                checksum_tail[2],
                checksum_tail[3],
            ]);
            let computed = hash(payload);
            if stored != computed {
                report.checksum_failures += 1;
                report.push_error(format!(
                    "page {page_id} checksum mismatch (stored=0x{stored:08X}, computed=0x{computed:08X})"
                ));
                continue;
            }

            if !verify_records {
                continue;
            }

            if page_id == 0 {
                match Header::read(payload) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        report.record_errors += 1;
                        report.push_error("header page missing magic".to_string());
                    }
                    Err(err) => {
                        report.record_errors += 1;
                        report.push_error(format!("header page corrupt: {err:?}"));
                    }
                }
                continue;
            }

            let record_page = match RecordPage::from_bytes(page_bytes.as_mut_slice()) {
                Ok(page) => page,
                Err(err) => {
                    report.record_errors += 1;
                    report.push_error(format!("page {page_id} malformed: {err:?}"));
                    continue;
                }
            };

            let record_count = match record_page.record_count() {
                Ok(count) => count as usize,
                Err(err) => {
                    report.record_errors += 1;
                    report.push_error(format!("page {page_id} record count unreadable: {err:?}"));
                    continue;
                }
            };

            for slot_index in 0..record_count {
                let record_bytes = match record_page.record_slice(slot_index) {
                    Ok(slice) => slice,
                    Err(err) => {
                        report.record_errors += 1;
                        report.push_error(format!(
                            "page {page_id} slot {slot_index} unreadable: {err:?}"
                        ));
                        continue;
                    }
                };

                if record_bytes.len() < RECORD_HEADER_SIZE {
                    report.record_errors += 1;
                    report.push_error(format!(
                        "page {page_id} slot {slot_index} shorter than record header"
                    ));
                    continue;
                }

                let header = match RecordHeader::from_bytes(&record_bytes[..RECORD_HEADER_SIZE]) {
                    Ok(header) => header,
                    Err(err) => {
                        report.record_errors += 1;
                        report.push_error(format!(
                            "page {page_id} slot {slot_index} header corrupt: {err:?}"
                        ));
                        continue;
                    }
                };

                if header.kind == RecordKind::Free {
                    continue;
                }

                let payload_len = header.payload_length as usize;
                if payload_len > record_bytes.len().saturating_sub(RECORD_HEADER_SIZE) {
                    report.record_errors += 1;
                    report.push_error(format!(
                        "page {page_id} slot {slot_index} payload exceeds slot bounds"
                    ));
                    continue;
                }
                
                // Check if this is a versioned record by looking at the actual kind byte
                let kind_byte = record_bytes[0];
                let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge
                
                let payload_len = header.payload_length as usize;
                let payload = if is_versioned {
                    // For versioned records, skip the 25-byte metadata header
                    const VERSION_METADATA_SIZE: usize = 25;
                    if payload_len < VERSION_METADATA_SIZE {
                        report.record_errors += 1;
                        report.push_error(format!(
                            "page {page_id} slot {slot_index} versioned record too small"
                        ));
                        continue;
                    }
                    let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                    let data_end = RECORD_HEADER_SIZE + payload_len;
                    if data_end > record_bytes.len() {
                        report.record_errors += 1;
                        report.push_error(format!(
                            "page {page_id} slot {slot_index} payload exceeds slot bounds"
                        ));
                        continue;
                    }
                    &record_bytes[data_start..data_end]
                } else {
                    // Legacy non-versioned record
                    &record_bytes[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len]
                };
                
                let slot_u16 = slot_index as u16;

                match header.kind {
                    RecordKind::Node => match deserialize_node(payload) {
                        Ok(node) => {
                            if !nodes_seen.insert(node.id) {
                                report.record_errors += 1;
                                report.push_error(format!(
                                    "duplicate node id {} detected on page {} slot {}",
                                    node.id, page_id, slot_index
                                ));
                            }
                            if verify_indexes {
                                node_slots.insert((page_id, slot_u16), node.id);
                            }
                        }
                        Err(err) => {
                            report.record_errors += 1;
                            report.push_error(format!(
                                "page {page_id} slot {slot_index} node payload corrupt: {err:?}"
                            ));
                        }
                    },
                    RecordKind::Edge => match deserialize_edge(payload) {
                        Ok(edge) => {
                            if let std::collections::hash_map::Entry::Vacant(entry) =
                                edges_seen.entry(edge.id)
                            {
                                entry.insert((edge.source_node_id, edge.target_node_id));
                            } else {
                                report.record_errors += 1;
                                report.push_error(format!(
                                    "duplicate edge id {} detected on page {} slot {}",
                                    edge.id, page_id, slot_index
                                ));
                            }
                            if verify_indexes {
                                edge_slots.insert((page_id, slot_u16), edge.id);
                            }
                        }
                        Err(err) => {
                            report.record_errors += 1;
                            report.push_error(format!(
                                "page {page_id} slot {slot_index} edge payload corrupt: {err:?}"
                            ));
                        }
                    },
                    RecordKind::Free => {}
                }
            }
        }

        if !verify_records {
            return Ok(report);
        }

        if verify_indexes {
            for (node_id, pointer) in self.node_index.iter() {
                let key = (pointer.page_id, pointer.slot_index);
                match node_slots.get(&key) {
                    Some(found_id) if found_id == &node_id => {}
                    Some(found_id) => {
                        report.index_errors += 1;
                        report.push_error(format!(
                            "node index entry {node_id} points to page {} slot {} which holds node {}",
                            pointer.page_id, pointer.slot_index, found_id
                        ));
                    }
                    None => {
                        report.index_errors += 1;
                        report.push_error(format!(
                            "node index entry {node_id} points to missing page {} slot {}",
                            pointer.page_id, pointer.slot_index
                        ));
                    }
                }
            }

            for ((page_id, slot), node_id) in &node_slots {
                if self.node_index.get(node_id).is_none() {
                    report.index_errors += 1;
                    report.push_error(format!(
                        "node {node_id} stored at page {page_id} slot {slot} missing from node index"
                    ));
                }
            }

            for (edge_id, pointer) in &self.edge_index {
                let key = (pointer.page_id, pointer.slot_index);
                match edge_slots.get(&key) {
                    Some(found_id) if found_id == edge_id => {}
                    Some(found_id) => {
                        report.index_errors += 1;
                        report.push_error(format!(
                            "edge index entry {edge_id} points to page {} slot {} which holds edge {}",
                            pointer.page_id, pointer.slot_index, found_id
                        ));
                    }
                    None => {
                        report.index_errors += 1;
                        report.push_error(format!(
                            "edge index entry {edge_id} points to missing page {} slot {}",
                            pointer.page_id, pointer.slot_index
                        ));
                    }
                }
            }

            for ((page_id, slot), edge_id) in &edge_slots {
                if !self.edge_index.contains_key(edge_id) {
                    report.index_errors += 1;
                    report.push_error(format!(
                        "edge {edge_id} stored at page {page_id} slot {slot} missing from edge index"
                    ));
                }
            }
        }

        if verify_adjacency {
            for (edge_id, (source, target)) in &edges_seen {
                if !nodes_seen.contains(source) {
                    report.adjacency_errors += 1;
                    report.push_error(format!(
                        "edge {edge_id} references missing source node {source}"
                    ));
                }
                if !nodes_seen.contains(target) {
                    report.adjacency_errors += 1;
                    report.push_error(format!(
                        "edge {edge_id} references missing target node {target}"
                    ));
                }
            }
        }

        Ok(report)
    }

    /// Performs a health check on the database.
    ///
    /// Checks various operational metrics to determine if the database
    /// is operating within normal parameters:
    /// - Cache hit rate
    /// - WAL size
    /// - Corruption error count
    /// - Time since last checkpoint
    ///
    /// # Returns
    /// A `HealthCheck` result with status and individual check results.
    ///
    /// # Errors
    /// * `GraphError::Io` - Error checking WAL size
    pub fn health_check(&self) -> Result<crate::db::health::HealthCheck> {
        use crate::db::health::{Check, HealthCheck};

        let mut health = HealthCheck::new();

        let cache_hit_rate = self.metrics.cache_hit_rate();
        let cache_threshold = 0.7;
        health.add_check(Check::CacheHitRate {
            current: cache_hit_rate,
            threshold: cache_threshold,
            healthy: cache_hit_rate >= cache_threshold,
        });

        let wal_size = self.pager.wal_size()?;
        let wal_threshold = 100 * 1024 * 1024;
        health.add_check(Check::WalSize {
            bytes: wal_size,
            threshold: wal_threshold,
            healthy: wal_size < wal_threshold,
        });

        let corruption_count = self.metrics.corruption_errors;
        health.add_check(Check::CorruptionErrors {
            count: corruption_count,
            healthy: corruption_count == 0,
        });

        let checkpoints_performed = self.metrics.checkpoints_performed;
        let seconds_since_checkpoint = if checkpoints_performed == 0 {
            u64::MAX
        } else {
            0
        };
        let checkpoint_threshold = 3600;
        health.add_check(Check::LastCheckpoint {
            seconds_ago: seconds_since_checkpoint,
            threshold: checkpoint_threshold,
            healthy: seconds_since_checkpoint < checkpoint_threshold || checkpoints_performed == 0,
        });

        Ok(health)
    }

    /// Runs garbage collection on MVCC version chains.
    ///
    /// This method scans the database for old versions that are no longer
    /// visible to any active transaction and reclaims them.
    ///
    /// # Returns
    /// Statistics about the GC run including versions examined and reclaimed.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - MVCC is not enabled
    /// * `GraphError::Corruption` - Version chain corruption detected
    ///
    /// # Example
    /// ```rust,ignore
    /// use sombra::GraphDB;
    ///
    /// let mut db = GraphDB::open_with_config("test.db", config)?;
    /// let stats = db.run_gc()?;
    /// println!("Reclaimed {} versions", stats.versions_reclaimed);
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn run_gc(&mut self) -> Result<GcStats> {
        // Ensure MVCC is enabled
        let gc = self.gc.as_ref().ok_or_else(|| {
            GraphError::InvalidArgument("MVCC is not enabled, cannot run GC".into())
        })?;
        
        let oracle = self.timestamp_oracle.as_ref().ok_or_else(|| {
            GraphError::InvalidArgument("MVCC is not enabled, timestamp oracle not found".into())
        })?;

        // Get all node IDs from the index to scan their version chains
        let record_ids = self.node_index.iter().into_iter();

        // Access RecordStore through pager
        let mut record_store = RecordStore::new(&mut self.pager);
        
        // Run GC
        let result = gc.run_gc(&mut record_store, record_ids, oracle)?;
        
        // Register dirty pages from GC operations
        let dirty_pages = record_store.take_dirty_pages();
        for page_id in dirty_pages {
            self.record_page_write(page_id);
        }
        
        Ok(result)
    }

    /// Starts background garbage collection.
    ///
    /// Spawns a background thread that periodically runs GC based on the
    /// configured `gc_interval_secs`.
    ///
    /// # Returns
    /// Ok(()) if background GC was started successfully.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - MVCC or background GC not configured
    ///
    /// # Example
    /// ```rust,ignore
    /// use sombra::{GraphDB, Config};
    ///
    /// let mut config = Config::default();
    /// config.mvcc_enabled = true;
    /// config.gc_interval_secs = Some(60); // Run GC every minute
    /// let mut db = GraphDB::open_with_config("test.db", config)?;
    /// db.start_background_gc()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn start_background_gc(&mut self) -> Result<()> {
        if !self.config.mvcc_enabled {
            return Err(GraphError::InvalidArgument(
                "MVCC is not enabled, cannot start background GC".into(),
            ));
        }

        if self.config.gc_interval_secs.is_none() {
            return Err(GraphError::InvalidArgument(
                "gc_interval_secs not configured, cannot start background GC".into(),
            ));
        }

        if self.bg_gc_state.is_some() {
            return Ok(()); // Already running
        }

        let oracle = self.timestamp_oracle.as_ref().ok_or_else(|| {
            GraphError::InvalidArgument("timestamp oracle not found".into())
        })?;

        // Create GC configuration
        let gc_config = GcConfig {
            enabled: true,
            interval_secs: self.config.gc_interval_secs,
            min_versions_per_record: 1,
            scan_batch_size: 1000,
        };

        // Create background GC state
        let bg_gc = BackgroundGcState::spawn(
            self.path.clone(),
            gc_config,
            oracle.clone(),
        )?;

        // Store bg_gc_state for later shutdown
        self.bg_gc_state = Some(bg_gc);

        Ok(())
    }

    /// Stops background garbage collection.
    ///
    /// Signals the background GC thread to shut down and waits for it to complete.
    ///
    /// # Returns
    /// Ok(()) if background GC was stopped successfully, or was not running.
    ///
    /// # Example
    /// ```rust,ignore
    /// use sombra::GraphDB;
    ///
    /// let mut db = GraphDB::open_with_config("test.db", config)?;
    /// db.start_background_gc()?;
    /// // ... later ...
    /// db.stop_background_gc()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn stop_background_gc(&mut self) -> Result<()> {
        if let Some(bg_gc) = self.bg_gc_state.take() {
            let state = bg_gc.lock().map_err(|e| {
                GraphError::InvalidArgument(format!("failed to lock background GC state: {}", e))
            })?;
            state.shutdown()?;
        }
        Ok(())
    }

    /// Closes the database gracefully.
    ///
    /// Performs a clean shutdown by:
    /// 1. Rolling back any active transaction
    /// 2. Persisting the B-tree index
    /// 3. Writing the database header
    /// 4. Checkpointing the WAL
    ///
    /// After successful close, the database can be reopened without
    /// requiring WAL recovery.
    ///
    /// # Returns
    /// Ok(()) on successful close.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error during close
    ///
    /// # Example
    /// ```rust
    /// use sombra::GraphDB;
    ///
    /// let db = GraphDB::open("test.db")?;
    /// // ... use database ...
    /// db.close()?; // Clean shutdown
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn close(mut self) -> Result<()> {
        info!("Closing database gracefully");

        if self.is_in_transaction() {
            warn!("Active transaction detected during close, rolling back");
            self.exit_transaction();
        }

        self.persist_btree_index()?;
        
        // Update timestamp in header if MVCC is enabled
        if let Some(ref oracle) = self.timestamp_oracle {
            self.header.max_timestamp = oracle.current_timestamp();
        }
        
        self.write_header()?;

        self.pager.checkpoint()?;

        info!("Database closed successfully");
        Ok(())
    }
}
