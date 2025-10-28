//! Concurrent database access wrapper
//!
//! This module provides a thread-safe wrapper around `GraphDB` that enables
//! concurrent transactions with MVCC snapshot isolation.
//!
//! # Example
//!
//! ```rust
//! use sombra::{ConcurrentGraphDB, Node, Config};
//!
//! let mut config = Config::default();
//! config.mvcc_enabled = true;
//! let db = ConcurrentGraphDB::open_with_config("my.db", config)?;
//!
//! // Multiple threads can create transactions concurrently
//! std::thread::scope(|s| {
//!     s.spawn(|| {
//!         let mut tx = db.begin_transaction()?;
//!         tx.add_node(Node::new(1))?;
//!         tx.commit()
//!     });
//!     
//!     s.spawn(|| {
//!         let mut tx = db.begin_transaction()?;
//!         tx.add_node(Node::new(2))?;
//!         tx.commit()
//!     });
//! });
//! # Ok::<(), sombra::GraphError>(())
//! ```

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::db::config::Config;
use crate::db::core::GraphDB;
use crate::db::group_commit::TxId;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NodeId};
use crate::storage::RecordPointer;

/// A thread-safe, concurrent graph database.
///
/// This wrapper around `GraphDB` enables multiple concurrent transactions
/// with MVCC snapshot isolation. Each transaction sees a consistent snapshot
/// of the database state.
///
/// # Thread Safety
///
/// `ConcurrentGraphDB` implements `Clone` and can be safely shared across
/// threads. Internal synchronization ensures data consistency.
///
/// # MVCC Requirement
///
/// This wrapper requires MVCC to be enabled in the database configuration.
/// Attempting to use it without MVCC will result in an error.
///
/// # Performance Note
///
/// Currently uses a coarse-grained `Mutex<GraphDB>` which serializes all operations.
/// This provides correctness but limited scalability (see MVCC_PRODUCTION_GUIDE.md).
///
/// **Optimization Roadmap**:
/// 1. Add interior mutability to `Pager` (wrap in `Mutex<Pager>`)
/// 2. Change `Arc<Mutex<GraphDB>>` to `Arc<RwLock<GraphDB>>`  
/// 3. Update read methods to use `RwLock::read()` (non-blocking concurrent reads)
/// 4. Expected improvement: 5-10x read throughput
///
/// # Example
///
/// ```rust
/// use sombra::{ConcurrentGraphDB, Config, Node};
///
/// let mut config = Config::default();
/// config.mvcc_enabled = true;
/// 
/// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
/// let db2 = db.clone(); // Can be cloned and shared across threads
///
/// let mut tx = db.begin_transaction()?;
/// tx.add_node(Node::new(1))?;
/// tx.commit()?;
/// # Ok::<(), sombra::GraphError>(())
/// ```
#[derive(Clone)]
pub struct ConcurrentGraphDB {
    inner: Arc<Mutex<GraphDB>>,
}

impl ConcurrentGraphDB {
    /// Opens a concurrent database with default configuration.
    ///
    /// This will enable MVCC automatically.
    ///
    /// # Arguments
    /// * `path` - Filesystem path to the database file
    ///
    /// # Returns
    /// A new `ConcurrentGraphDB` instance.
    ///
    /// # Errors
    /// * `GraphError::Io` - Cannot create/open file
    /// * `GraphError::Corruption` - Database file is corrupted
    /// * `GraphError::InvalidArgument` - Database already open in another process
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut config = Config::default();
        config.mvcc_enabled = true;
        Self::open_with_config(path, config)
    }

    /// Opens a concurrent database with custom configuration.
    ///
    /// # Arguments
    /// * `path` - Filesystem path to the database file
    /// * `config` - Database configuration (must have `mvcc_enabled = true`)
    ///
    /// # Returns
    /// A new `ConcurrentGraphDB` instance.
    ///
    /// # Errors
    /// * `GraphError::Io` - Cannot create/open file
    /// * `GraphError::Corruption` - Database file is corrupted
    /// * `GraphError::InvalidArgument` - MVCC not enabled or database already open
    pub fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        if !config.mvcc_enabled {
            return Err(GraphError::InvalidArgument(
                "MVCC must be enabled for concurrent database access".into(),
            ));
        }

        let db = GraphDB::open_with_config(path, config)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }

    /// Begins a new concurrent transaction.
    ///
    /// Multiple transactions can be active simultaneously. Each transaction
    /// sees a consistent snapshot of the database state.
    ///
    /// # Returns
    /// A new `ConcurrentTransaction` instance.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Maximum concurrent transactions exceeded
    ///
    /// # Example
    /// ```rust
    /// use sombra::{ConcurrentGraphDB, Config};
    ///
    /// let mut config = Config::default();
    /// config.mvcc_enabled = true;
    /// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
    ///
    /// // Multiple concurrent transactions
    /// let tx1 = db.begin_transaction()?;
    /// let tx2 = db.begin_transaction()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn begin_transaction(&self) -> Result<ConcurrentTransaction> {
        let mut db = self.inner.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        // Allocate transaction ID
        let tx_id = db.allocate_tx_id()?;

        // Get MVCC transaction manager
        let mvcc_tx_manager = db.mvcc_tx_manager.as_mut().ok_or_else(|| {
            GraphError::InvalidArgument("MVCC not enabled".into())
        })?;

        // Begin MVCC transaction to get snapshot timestamp
        let context = mvcc_tx_manager.begin_transaction(tx_id)?;

        Ok(ConcurrentTransaction {
            db: Arc::clone(&self.inner),
            tx_id,
            snapshot_ts: context.snapshot_ts,
            state: TxState::Active,
            dirty_pages: Vec::new(),
            created_versions: Vec::new(),
            _written_nodes: HashSet::new(),
            _written_edges: HashSet::new(),
        })
    }

    /// Closes the database gracefully.
    ///
    /// Performs a clean shutdown by rolling back any active transactions,
    /// persisting indexes, and checkpointing the WAL.
    ///
    /// # Returns
    /// Ok(()) on successful close.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error during close
    pub fn close(self) -> Result<()> {
        let db = Arc::try_unwrap(self.inner)
            .map_err(|_| {
                GraphError::InvalidArgument(
                    "cannot close database: active references exist".into(),
                )
            })?
            .into_inner()
            .map_err(|e| {
                GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
            })?;

        db.close()
    }
}

/// State of a concurrent transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Transaction is active and can accept operations
    Active,
    /// Transaction has been successfully committed
    Committed,
    /// Transaction has been rolled back
    _RolledBack,
}

/// A concurrent database transaction.
///
/// Provides snapshot isolation - each transaction sees a consistent view
/// of the database as it existed when the transaction started.
///
/// # Lifecycle
///
/// 1. Create with `ConcurrentGraphDB::begin_transaction()`
/// 2. Perform operations (add nodes, edges, queries)
/// 3. Call `commit()` to make changes permanent
///
/// # Important
///
/// Transactions must be explicitly committed. Dropping a transaction
/// without committing will panic (unless the thread is already panicking).
///
/// # Example
///
/// ```rust
/// use sombra::{ConcurrentGraphDB, Config, Node};
///
/// let mut config = Config::default();
/// config.mvcc_enabled = true;
/// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
///
/// let mut tx = db.begin_transaction()?;
/// let node_id = tx.add_node(Node::new(1))?;
/// let retrieved = tx.get_node(node_id)?;
/// tx.commit()?;
/// # Ok::<(), sombra::GraphError>(())
/// ```
pub struct ConcurrentTransaction {
    db: Arc<Mutex<GraphDB>>,
    tx_id: TxId,
    snapshot_ts: u64,
    state: TxState,
    dirty_pages: Vec<crate::pager::PageId>,
    created_versions: Vec<RecordPointer>,
    _written_nodes: HashSet<NodeId>,
    _written_edges: HashSet<EdgeId>,
}

impl ConcurrentTransaction {
    /// Returns the transaction ID.
    pub fn id(&self) -> TxId {
        self.tx_id
    }

    /// Returns the snapshot timestamp.
    ///
    /// This timestamp determines which versions of records are visible
    /// to this transaction.
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Returns the current state of the transaction.
    pub fn state(&self) -> TxState {
        self.state
    }

    /// Adds a new node to the graph.
    ///
    /// The node is visible only to this transaction until committed.
    ///
    /// # Arguments
    /// * `node` - The node to add
    ///
    /// # Returns
    /// The ID assigned to the new node.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::Io` - Disk I/O error
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        let mut db = self.db.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        // Add node with this transaction's ID and commit_ts = 0 (uncommitted)
        let (node_id, version_ptr) = db.add_node_internal(node, self.tx_id, 0)?;

        // Track created version for commit timestamp update
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }

        // Track dirty pages
        let dirty = db.take_recent_dirty_pages();
        self.dirty_pages.extend(dirty);

        Ok(node_id)
    }

    /// Adds a new edge to the graph.
    ///
    /// The edge is visible only to this transaction until committed.
    ///
    /// # Arguments
    /// * `edge` - The edge to add
    ///
    /// # Returns
    /// The ID of the new edge.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active or nodes don't exist
    /// * `GraphError::Io` - Disk I/O error
    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        let mut db = self.db.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        // Add edge with this transaction's ID and commit_ts = 0 (uncommitted)
        let (edge_id, version_ptr) = db.add_edge_internal(edge, self.tx_id, 0)?;

        // Track created version for commit timestamp update
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }

        // Track dirty pages
        let dirty = db.take_recent_dirty_pages();
        self.dirty_pages.extend(dirty);

        Ok(edge_id)
    }

    /// Retrieves a node by ID.
    ///
    /// Returns the version of the node visible to this transaction's snapshot.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to retrieve
    ///
    /// # Returns
    /// The node if it exists and is visible to this transaction, `None` otherwise.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::Io` - Disk I/O error
    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        let mut db = self.db.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))
    }

    /// Retrieves an edge by ID.
    ///
    /// Returns the version of the edge visible to this transaction's snapshot.
    ///
    /// # Arguments
    /// * `edge_id` - The ID of the edge to retrieve
    ///
    /// # Returns
    /// The edge if it exists and is visible to this transaction.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active or edge not found
    /// * `GraphError::Io` - Disk I/O error
    pub fn get_edge(&self, edge_id: EdgeId) -> Result<Edge> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        let mut db = self.db.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        db.load_edge_with_snapshot(edge_id, self.snapshot_ts, Some(self.tx_id))
    }

    /// Commits the transaction.
    ///
    /// Makes all changes permanent and visible to future transactions.
    ///
    /// # Returns
    /// Ok(()) on success.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::Io` - Disk I/O error
    pub fn commit(mut self) -> Result<()> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        let mut db = self.db.lock().map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

        // Allocate commit timestamp
        let commit_ts = db
            .timestamp_oracle
            .as_ref()
            .ok_or_else(|| GraphError::InvalidArgument("timestamp oracle not found".into()))?
            .allocate_commit_timestamp();

        // Update all created versions with commit timestamp
        {
            let mut pager_guard = db.pager.write().unwrap();
            let mut record_store = crate::storage::heap::RecordStore::new(&mut *pager_guard);
            for version_ptr in &self.created_versions {
                use crate::storage::version_chain::update_version_commit_timestamp;
                update_version_commit_timestamp(&mut record_store, *version_ptr, commit_ts)?;
            }

            // Register dirty pages from version updates
            let version_dirty_pages = record_store.take_dirty_pages();
            drop(record_store);
            drop(pager_guard);
            self.dirty_pages.extend(version_dirty_pages);
        }

        // Complete commit in MVCC manager
        let mvcc_tx_manager = db.mvcc_tx_manager.as_mut().ok_or_else(|| {
            GraphError::InvalidArgument("MVCC transaction manager not found".into())
        })?;
        mvcc_tx_manager.complete_commit(self.tx_id, commit_ts)?;
        mvcc_tx_manager.end_transaction(self.tx_id)?;

        // Update header
        db.header.last_committed_tx_id = self.tx_id;
        db.write_header()?;

        // Write to WAL
        db.commit_to_wal(self.tx_id, &self.dirty_pages)?;

        // Mark as committed
        self.state = TxState::Committed;

        Ok(())
    }
}

impl Drop for ConcurrentTransaction {
    fn drop(&mut self) {
        if self.state == TxState::Active {
            if !std::thread::panicking() {
                panic!(
                    "transaction {} was dropped without being committed or rolled back",
                    self.tx_id
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::config::Config;

    #[test]
    fn test_concurrent_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut config = Config::default();
        config.mvcc_enabled = true;

        let db = ConcurrentGraphDB::open_with_config(&path, config).unwrap();

        // Create multiple transactions
        let mut tx1 = db.begin_transaction().unwrap();
        let mut tx2 = db.begin_transaction().unwrap();

        // Each can operate independently
        let mut node1 = Node::new(1);
        node1.labels.push("A".to_string());
        let id1 = tx1.add_node(node1).unwrap();
        
        let mut node2 = Node::new(2);
        node2.labels.push("B".to_string());
        let id2 = tx2.add_node(node2).unwrap();

        // Read-your-own-writes
        assert!(tx1.get_node(id1).unwrap().is_some());
        assert!(tx2.get_node(id2).unwrap().is_some());

        // Commit both
        tx1.commit().unwrap();
        tx2.commit().unwrap();
    }

    #[test]
    fn test_snapshot_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut config = Config::default();
        config.mvcc_enabled = true;

        let db = ConcurrentGraphDB::open_with_config(&path, config).unwrap();

        // Transaction 1: Create a node and commit
        let mut tx1 = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.labels.push("Test".to_string());
        let node_id = tx1.add_node(node).unwrap();
        tx1.commit().unwrap();

        // Transaction 2: Start after tx1 commits - should see the node
        let tx2 = db.begin_transaction().unwrap();
        assert!(tx2.get_node(node_id).unwrap().is_some());
        tx2.commit().unwrap();
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Mutex;
        
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        let mut config = Config::default();
        config.mvcc_enabled = true;

        let db = ConcurrentGraphDB::open_with_config(&path, config).unwrap();

        // Track created node IDs across threads
        let created_ids = Arc::new(Mutex::new(Vec::new()));

        // Spawn multiple threads
        std::thread::scope(|s| {
            for i in 0..5 {
                let db = db.clone();
                let ids = Arc::clone(&created_ids);
                s.spawn(move || {
                    let mut tx = db.begin_transaction().unwrap();
                    let mut node = Node::new(0); // ID will be assigned by DB
                    node.labels.push(format!("Node{}", i));
                    let node_id = tx.add_node(node).unwrap();
                    tx.commit().unwrap();
                    
                    // Track the created ID
                    ids.lock().unwrap().push(node_id);
                });
            }
        });

        // Verify all nodes were created
        let tx = db.begin_transaction().unwrap();
        let ids = created_ids.lock().unwrap();
        assert_eq!(ids.len(), 5);
        for node_id in ids.iter() {
            let node = tx.get_node(*node_id).unwrap();
            assert!(node.is_some(), "Node {} should exist", node_id);
        }
        tx.commit().unwrap();
    }
}
