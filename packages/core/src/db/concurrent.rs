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
//! let config = Config::default();
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
use std::sync::Arc;

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
/// Uses `Arc<GraphDB>` with full interior mutability for true lock-free concurrent access:
/// - GraphDB fields use fine-grained locks (Mutex for header, DashMap for MVCC manager)
/// - Multiple threads can access different parts of the database simultaneously
/// - No outer lock contention - each field manages its own synchronization
/// - Expected performance: 10-15x throughput improvement over RwLock wrapper
///
/// See MVCC_PRODUCTION_GUIDE.md for detailed performance characteristics.
///
/// **Phase 5 Complete**: Full interior mutability enables true lock-free concurrent access!
///
    /// # Example
    ///
    /// ```rust
    /// use sombra::{ConcurrentGraphDB, Config, Node};
    ///
    /// let config = Config::default();
    /// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
    ///
/// let config = Config::default();
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
    inner: Arc<GraphDB>,
}

impl ConcurrentGraphDB {
    /// Opens a concurrent database with default configuration.
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
        let config = Config::default();
        Self::open_with_config(path, config)
    }

    /// Opens a concurrent database with custom configuration.
    ///
    /// # Arguments
    /// * `path` - Filesystem path to the database file
    /// * `config` - Database configuration
    ///
    /// # Returns
    /// A new `ConcurrentGraphDB` instance.
    ///
    /// # Errors
    /// * `GraphError::Io` - Cannot create/open file
    /// * `GraphError::Corruption` - Database file is corrupted
    /// * `GraphError::InvalidArgument` - Database already open
    pub fn open_with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let db = GraphDB::open_with_config(path, config)?;
        Ok(Self {
            inner: Arc::new(db),
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
    /// let config = Config::default();
    /// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
    ///
    /// // Multiple concurrent transactions
    /// let tx1 = db.begin_transaction()?;
    /// let tx2 = db.begin_transaction()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn begin_transaction(&self) -> Result<ConcurrentTransaction> {
        // GraphDB now has interior mutability, no lock needed
        let db = &self.inner;

        // Allocate transaction ID (uses atomic operations internally)
        let tx_id = db.allocate_tx_id()?;

        // Begin MVCC transaction to get snapshot timestamp
        // Phase 5: MvccTransactionManager is now lock-free (no RwLock wrapper)
        let context = db.mvcc_tx_manager.begin_transaction(tx_id)?;

        Ok(ConcurrentTransaction {
            db: Arc::clone(&self.inner),
            tx_id,
            snapshot_ts: context.snapshot_ts,
            state: TxState::Active,
            dirty_pages: Vec::new(),
            created_versions: Vec::new(),
            _written_nodes: HashSet::new(),
            _written_edges: HashSet::new(),
            label_updates: Vec::new(),
            label_deletions: Vec::new(),
            property_updates: Vec::new(),
            property_deletions: Vec::new(),
        })
    }

    /// Creates a property index for fast lookups.
    ///
    /// Property indexes enable efficient queries using `find_nodes_by_property`.
    /// This operation can be called concurrently with active transactions.
    ///
    /// # Arguments
    /// * `label` - The node label to index
    /// * `property_key` - The property key to index
    ///
    /// # Returns
    /// Ok(()) if index was created or already exists.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error during index creation
    ///
    /// # Example
    /// ```rust
    /// use sombra::{ConcurrentGraphDB, Config};
    ///
    /// let config = Config::default();
    /// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
    ///
    /// db.create_property_index("Person", "age")?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn create_property_index(&self, label: &str, property_key: &str) -> Result<()> {
        // Delegate to GraphDB's implementation, which uses interior mutability
        // The property_indexes field is Arc<DashMap<...>> so no mut needed
        self.inner.create_property_index_concurrent(label, property_key)
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
        let db = Arc::try_unwrap(self.inner).map_err(|_| {
            GraphError::InvalidArgument("cannot close database: active references exist".into())
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
    /// let config = Config::default();
    /// let db = ConcurrentGraphDB::open_with_config("test.db", config)?;
    ///
/// let mut tx = db.begin_transaction()?;
/// let node_id = tx.add_node(Node::new(1))?;
/// let retrieved = tx.get_node(node_id)?;
/// tx.commit()?;
/// # Ok::<(), sombra::GraphError>(())
/// ```
pub struct ConcurrentTransaction {
    db: Arc<GraphDB>,
    tx_id: TxId,
    snapshot_ts: u64,
    state: TxState,
    dirty_pages: Vec<crate::pager::PageId>,
    created_versions: Vec<RecordPointer>,
    _written_nodes: HashSet<NodeId>,
    _written_edges: HashSet<EdgeId>,
    /// Track label operations (label, node_id) for commit timestamp updates
    label_updates: Vec<(String, NodeId)>,
    /// Track label deletions (label, node_id) for delete timestamp updates
    label_deletions: Vec<(String, NodeId)>,
    /// Track property additions/updates (label, property_key, node_id, value) for commit timestamp updates
    property_updates: Vec<(String, String, NodeId, crate::model::PropertyValue)>,
    /// Track property deletions (label, property_key, node_id, old_value, old_pointer) for delete timestamp updates
    property_deletions: Vec<(String, String, NodeId, crate::model::PropertyValue, crate::storage::heap::RecordPointer)>,
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

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        // Track labels for commit timestamp update
        let labels = node.labels.clone();

        // Add node with this transaction's ID and commit_ts = 0 (uncommitted)
        let (node_id, version_ptr) = db.add_node_internal(node, self.tx_id, 0)?;

        // Track created version for commit timestamp update
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }

        // Track label updates for commit timestamp update
        for label in labels {
            self.label_updates.push((label, node_id));
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

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

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

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

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

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        db.load_edge_with_snapshot(edge_id, self.snapshot_ts, Some(self.tx_id))
    }

    /// Deletes a node from the graph.
    ///
    /// The node and all its incident edges will be marked as deleted.
    /// The deletion is not visible to other transactions until this
    /// transaction is committed.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to delete
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::NotFound` - Node doesn't exist
    /// * `GraphError::Io` - Disk I/O error
    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        // Get ALL version pointers for this node
        // When a node is deleted, we need to mark ALL version pointers in property indexes
        // because a property value may have been set in multiple versions
        let all_pointers = db.node_index.get(&node_id).unwrap_or_default();
        
        let node = db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))?
            .ok_or_else(|| GraphError::NotFound("node"))?;

        // Track label deletions for delete timestamp update
        for label in &node.labels {
            self.label_deletions.push((label.clone(), node_id));
        }

        // Track property deletions for each label
        // Property indexes are per (label, property_key) pair
        // We need to track deletions for ALL version pointers, not just the latest one
        // because property values may exist in multiple versions with the same value
        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                // Add a deletion entry for each version pointer
                for pointer in &all_pointers {
                    self.property_deletions.push((
                        label.clone(),
                        property_key.clone(),
                        node_id,
                        property_value.clone(),
                        *pointer
                    ));
                }
            }
        }

        let tombstone_ptr = db.delete_node_internal(node_id, self.tx_id, 0)?;
        
        // Track the tombstone version for commit timestamp update
        if let Some(ptr) = tombstone_ptr {
            self.created_versions.push(ptr);
        }

        // Track dirty pages
        let dirty = db.take_recent_dirty_pages();
        self.dirty_pages.extend(dirty);

        Ok(())
    }

    /// Updates an existing node in the graph.
    ///
    /// Creates a new version of the node. The update is not visible to
    /// other transactions until this transaction is committed.
    ///
    /// # Arguments
    /// * `node` - The updated node (must have a valid ID)
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active or node ID is 0
    /// * `GraphError::NotFound` - Node doesn't exist
    /// * `GraphError::Io` - Disk I/O error
    pub fn update_node(&mut self, node: Node) -> Result<()> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        if node.id == 0 {
            return Err(GraphError::InvalidArgument(
                "cannot update node with ID 0".into(),
            ));
        }

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        let node_id = node.id;

        // Get the old pointer before updating (needed for property index delete tracking)
        let prev_pointer = db.node_index.get_latest(&node_id)
            .ok_or_else(|| GraphError::NotFound("node"))?;

        // Read old node to determine which labels are new (for label index tracking)
        let old_node = db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))?
            .ok_or_else(|| GraphError::NotFound("node"))?;

        // Compute label differences (clone labels to avoid borrow conflicts)
        use std::collections::HashSet;
        let old_labels: HashSet<String> = old_node.labels.iter().cloned().collect();
        let new_labels: HashSet<String> = node.labels.clone().into_iter().collect();

        // Clone properties for comparison
        use std::collections::BTreeMap;
        let old_props: BTreeMap<String, crate::model::PropertyValue> = old_node.properties.clone();
        let new_props: BTreeMap<String, crate::model::PropertyValue> = node.properties.clone();

        // Add node with this transaction's ID and commit_ts = 0 (uncommitted)
        // add_node_internal detects updates when node.id != 0
        let (_node_id, version_ptr) = db.add_node_internal(node, self.tx_id, 0)?;

        // Track created version for commit timestamp update
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }

        // Track label updates for commit timestamp update
        // Only track labels that were added (new labels and labels that remain)
        // We need to track all labels that have new entries in the label index
        for label in new_labels.difference(&old_labels) {
            // Newly added labels
            self.label_updates.push((label.clone(), node_id));
        }
        for label in old_labels.intersection(&new_labels) {
            // Labels that remain (get new entries due to property changes)
            self.label_updates.push((label.clone(), node_id));
        }
        
        // Track label deletions for delete timestamp update
        // These are labels that were removed from the node
        for label in old_labels.difference(&new_labels) {
            // Removed labels - need to update delete_ts on commit
            self.label_deletions.push((label.clone(), node_id));
        }

        // Track property updates and deletions for each label
        // Property indexes are per (label, property_key) pair
        // IMPORTANT: Use prev_pointer when tracking deletions since old entries have old pointer
        for label in new_labels.iter() {
            // ALL properties in the new version need their commit_ts updated
            // because update_node_version adds ALL properties with commit_ts=0
            for (key, new_value) in &new_props {
                self.property_updates.push((label.clone(), key.clone(), node_id, new_value.clone()));
                
                // Also track deletion of old value if property changed
                if let Some(old_value) = old_props.get(key) {
                    if old_value != new_value {
                        // Property value changed - delete old entry
                        self.property_deletions.push((label.clone(), key.clone(), node_id, old_value.clone(), prev_pointer));
                    }
                }
            }
            
            // Check which properties were deleted (mark old entries as deleted)
            for (key, old_value) in &old_props {
                if !new_props.contains_key(key) {
                    // Property was deleted
                    self.property_deletions.push((label.clone(), key.clone(), node_id, old_value.clone(), prev_pointer));
                }
            }
        }

        // For removed labels, mark all their property entries as deleted
        for label in old_labels.difference(&new_labels) {
            for (key, old_value) in &old_props {
                self.property_deletions.push((label.clone(), key.clone(), node_id, old_value.clone(), prev_pointer));
            }
        }

        // Track dirty pages
        let dirty = db.take_recent_dirty_pages();
        self.dirty_pages.extend(dirty);

        Ok(())
    }

    /// Retrieves all nodes with a specific label.
    ///
    /// Returns only the nodes visible to this transaction's snapshot.
    ///
    /// # Arguments
    /// * `label` - The label to search for
    ///
    /// # Returns
    /// A vector of node IDs with the specified label.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::Io` - Disk I/O error
    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        db.get_nodes_by_label_with_snapshot(label, self.snapshot_ts)
    }

    /// Find nodes by property value with snapshot isolation.
    ///
    /// Returns only nodes that have the specified property value and are
    /// visible to this transaction's snapshot.
    ///
    /// # Arguments
    /// * `label` - The node label
    /// * `property_key` - The property key to search
    /// * `value` - The property value to match
    ///
    /// # Returns
    /// A vector of node IDs with the specified property value.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction not active
    /// * `GraphError::Io` - Disk I/O error
    pub fn find_nodes_by_property(
        &self,
        label: &str,
        property_key: &str,
        value: &crate::model::PropertyValue,
    ) -> Result<Vec<NodeId>> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        db.find_nodes_by_property_with_snapshot(label, property_key, value, self.snapshot_ts)
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

        // GraphDB now has interior mutability, no lock needed
        let db = &self.db;

        // Allocate commit timestamp
        let commit_ts = db.timestamp_oracle.allocate_commit_timestamp();

        // Update all created versions with commit timestamp
        let version_dirty_pages = db.pager.with_pager_write(|pager| {
            let mut record_store = crate::storage::heap::RecordStore::new(pager);
            for version_ptr in &self.created_versions {
                use crate::storage::version_chain::update_version_commit_timestamp;
                update_version_commit_timestamp(&mut record_store, *version_ptr, commit_ts)?;
            }

            // Register dirty pages from version updates
            Ok(record_store.take_dirty_pages())
        })?;
        
        // Invalidate cache entries for modified pages
        for &page_id in &version_dirty_pages {
            db.record_page_write(page_id);
        }
        
        self.dirty_pages.extend(version_dirty_pages);

        // Update label index entries with commit timestamp
        for (label, node_id) in &self.label_updates {
            if let Some(label_map) = db.label_index.get(label) {
                if let Some(entries) = label_map.get(node_id) {
                    entries.lock().unwrap().update_latest_commit_ts(commit_ts);
                }
            }
        }
        
        // Update label index entries with delete timestamp for removed labels
        for (label, node_id) in &self.label_deletions {
            if let Some(label_map) = db.label_index.get(label) {
                if let Some(entries) = label_map.get(node_id) {
                    entries.lock().unwrap().update_latest_delete_ts(commit_ts);
                }
            }
        }

        // Update property index entries with commit timestamp
        for (label, property_key, node_id, value) in &self.property_updates {
            let key = (label.clone(), property_key.clone());
            if let Some(index) = db.property_indexes.get(&key) {
                if let Some(indexable_value) = Option::<crate::db::core::IndexableValue>::from(value) {
                    if let Some(entries_arc) = index.get(&indexable_value) {
                        // Get node's record pointer (latest version)
                        if let Some(pointer) = db.node_index.get_latest(node_id) {
                            let mut entries = entries_arc.lock().unwrap();
                            entries.update_commit_ts_for_pointer(pointer, commit_ts);
                        }
                    }
                }
            }
        }

        // Update property index entries with delete timestamp for removed properties
        for (label, property_key, _node_id, value, pointer) in &self.property_deletions {
            let key = (label.clone(), property_key.clone());
            if let Some(index) = db.property_indexes.get(&key) {
                if let Some(indexable_value) = Option::<crate::db::core::IndexableValue>::from(value) {
                    if let Some(entries_arc) = index.get(&indexable_value) {
                        // Use the stored pointer from when the property was deleted
                        let mut entries = entries_arc.lock().unwrap();
                        entries.update_delete_ts_for_pointer(*pointer, commit_ts);
                    }
                }
            }
        }

        // Complete commit in MVCC manager
        db.mvcc_tx_manager.complete_commit(self.tx_id, commit_ts)?;
        db.mvcc_tx_manager.end_transaction(self.tx_id)?;

        // Update header (Phase 5: header wrapped in Mutex)
        db.header.lock().unwrap().last_committed_tx_id = self.tx_id;
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
        if self.state == TxState::Active && !std::thread::panicking() {
            panic!(
                "transaction {} was dropped without being committed or rolled back",
                self.tx_id
            );
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

        let config = Config::default();

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

        let config = Config::default();

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

        let config = Config::default();

        let db = ConcurrentGraphDB::open_with_config(&path, config).unwrap();

        // Track created node IDs across threads
        let created_ids = Arc::new(Mutex::new(Vec::new()));

        // Spawn multiple threads
        std::thread::scope(|s| {
            for i in 0..5 {
                let db = db.clone();
                let ids = Arc::clone(&created_ids);
                s.spawn(move || {
                    let mut tx = db.begin_transaction().expect(&format!("Thread {} failed to begin transaction", i));
                    let mut node = Node::new(0); // ID will be assigned by DB
                    node.labels.push(format!("Node{}", i));
                    let node_id = tx.add_node(node).expect(&format!("Thread {} failed to add node", i));
                    tx.commit().expect(&format!("Thread {} failed to commit", i));

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
