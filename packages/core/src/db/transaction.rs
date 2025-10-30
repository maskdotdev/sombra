use super::core::GraphDB;
use super::group_commit::TxId;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NodeId};
use crate::pager::PageId;
use crate::storage::heap::RecordPointer;
use std::collections::HashSet;
use tracing::{debug, info, warn};

/// The state of a transaction.
///
/// Transactions progress through these states during their lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    /// Transaction is active and can accept operations
    Active,
    /// Transaction has been successfully committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
}

/// A database transaction providing ACID guarantees.
///
/// Transactions allow you to group multiple operations into a single
/// atomic unit. All operations within a transaction are either all
/// committed or all rolled back.
///
/// # Lifecycle
///
/// 1. Create transaction with `GraphDB::begin_transaction()`
/// 2. Perform operations (add nodes, edges, etc.)
/// 3. Either `commit()` to make changes permanent or `rollback()` to discard
///
/// # Important
///
/// Transactions must be explicitly committed or rolled back. If a
/// transaction is dropped without doing so, it will panic (unless
/// the thread is already panicking).
///
/// # Example
///
/// ```rust
/// use sombra::{GraphDB, Node, Edge};
///
/// let mut db = GraphDB::open("test.db")?;
/// {
///     let mut tx = db.begin_transaction()?;
///     let alice = tx.add_node(Node::new(1))?;
///     let bob = tx.add_node(Node::new(2))?;
///     let edge = Edge::new(1, alice, bob, "KNOWS");
///     tx.add_edge(edge)?;
///     tx.commit()?; // Make changes permanent
/// }
/// # Ok::<(), sombra::GraphError>(())
/// ```
#[derive(Debug)]
pub struct Transaction<'db> {
    db: &'db GraphDB,
    id: TxId,
    epoch: u64,
    state: TxState,
    pub dirty_pages: Vec<PageId>,
    start_time: std::time::Instant,
    snapshot_ts: u64,
    /// Set of node IDs read by this transaction (for conflict detection)
    read_nodes: HashSet<NodeId>,
    /// Set of node IDs written by this transaction (for conflict detection)
    write_nodes: HashSet<NodeId>,
    /// Set of edge IDs written by this transaction (for conflict detection)
    write_edges: HashSet<EdgeId>,
    /// Version record pointers created by this transaction (for efficient commit timestamp updates)
    created_versions: Vec<RecordPointer>,
    /// Deleted node pointers (for property index delete timestamp updates)
    deleted_node_pointers: Vec<RecordPointer>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db GraphDB, id: TxId) -> Result<Self> {
        db.enter_transaction(id)?;
        db.start_tracking();
        let epoch = db.increment_epoch();

        // Allocate snapshot timestamp from oracle
        let snapshot_ts = db.timestamp_oracle.allocate_read_timestamp();
        // Register snapshot for GC watermark tracking
        db.timestamp_oracle.register_snapshot(snapshot_ts, id)?;

        debug!(
            tx_id = id,
            epoch = epoch,
            snapshot_ts = snapshot_ts,
            "Transaction started"
        );
        Ok(Self {
            db,
            id,
            epoch,
            state: TxState::Active,
            dirty_pages: Vec::new(),
            start_time: std::time::Instant::now(),
            snapshot_ts,
            read_nodes: HashSet::new(),
            write_nodes: HashSet::new(),
            write_edges: HashSet::new(),
            created_versions: Vec::new(),
            deleted_node_pointers: Vec::new(),
        })
    }

    /// Returns the unique identifier for this transaction.
    ///
    /// # Returns
    /// The transaction ID.
    pub fn id(&self) -> TxId {
        self.id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the snapshot timestamp for this transaction.
    ///
    /// This timestamp determines which versions of records are visible to this transaction
    /// in MVCC mode. If MVCC is not enabled, returns 0.
    ///
    /// # Returns
    /// The snapshot timestamp.
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Returns the current state of the transaction.
    ///
    /// # Returns
    /// The current `TxState`.
    pub fn state(&self) -> TxState {
        self.state
    }

    fn capture_dirty_pages(&mut self) -> Result<()> {
        let mut pages = self.db.take_recent_dirty_pages();
        if pages.is_empty() {
            return Ok(());
        }
        self.dirty_pages.append(&mut pages);
        self.dirty_pages.sort_unstable();
        self.dirty_pages.dedup();

        let max_tx_pages = self.db.config.max_transaction_pages;
        if self.dirty_pages.len() > max_tx_pages {
            warn!(
                tx_id = self.id,
                dirty_pages = self.dirty_pages.len(),
                max_pages = max_tx_pages,
                "Transaction exceeded page limit"
            );
            return Err(GraphError::InvalidArgument(format!(
                "Transaction exceeded maximum page limit of {max_tx_pages}"
            )));
        }

        if let Some(timeout_ms) = self.db.config.transaction_timeout_ms {
            let elapsed = self.start_time.elapsed().as_millis() as u64;
            if elapsed > timeout_ms {
                warn!(
                    tx_id = self.id,
                    elapsed_ms = elapsed,
                    timeout_ms,
                    "Transaction timeout exceeded"
                );
                return Err(GraphError::InvalidArgument(format!(
                    "Transaction timeout exceeded: {elapsed}ms > {timeout_ms}ms"
                )));
            }
        }

        Ok(())
    }

    /// Adds a node to the graph within this transaction.
    ///
    /// The node is not visible to other transactions until this transaction
    /// is committed. If the transaction is rolled back, the node will not
    /// be added to the database.
    ///
    /// # Arguments
    /// * `node` - The node to add
    ///
    /// # Returns
    /// The ID assigned to the new node.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction limits exceeded
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let node = Node::new(1);
    /// let node_id = tx.add_node(node)?;
    /// tx.commit()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        // Pass transaction ID and commit_ts=0 (will be set at commit time)
        let (node_id, version_ptr) = self.db.add_node_internal(node, self.id, 0)?;
        self.capture_dirty_pages()?;
        // Track write for conflict detection
        self.write_nodes.insert(node_id);
        // Track version pointer for efficient commit timestamp updates
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }
        Ok(node_id)
    }

    /// Adds an edge to the graph within this transaction.
    ///
    /// The edge is not visible to other transactions until this transaction
    /// is committed. Both source and target nodes must exist.
    ///
    /// # Arguments
    /// * `edge` - The edge to add
    ///
    /// # Returns
    /// The ID assigned to the new edge.
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction limits exceeded
    /// * `GraphError::NotFound` - Source or target node doesn't exist
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node, Edge};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let alice = tx.add_node(Node::new(1))?;
    /// let bob = tx.add_node(Node::new(2))?;
    /// let edge = Edge::new(1, alice, bob, "KNOWS");
    /// let edge_id = tx.add_edge(edge)?;
    /// tx.commit()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId> {
        // Pass transaction ID and commit_ts=0 (will be set at commit time)
        let (edge_id, version_ptr) = self.db.add_edge_internal(edge, self.id, 0)?;
        self.capture_dirty_pages()?;
        // Track write for conflict detection
        self.write_edges.insert(edge_id);
        // Track version pointer for efficient commit timestamp updates
        if let Some(ptr) = version_ptr {
            self.created_versions.push(ptr);
        }
        Ok(edge_id)
    }

    /// Deletes a node from the graph within this transaction.
    ///
    /// The node and all its incident edges will be marked as deleted.
    /// The deletion is not visible to other transactions until this
    /// transaction is committed.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to delete
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction limits exceeded
    /// * `GraphError::NotFound` - Node doesn't exist
    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        // Capture ALL version pointers before deletion (needed for property index updates)
        // When a node is deleted, we need to mark ALL version pointers in property indexes
        // because a property value may have been set in multiple versions
        let all_pointers = self.db.node_index.get(&node_id).unwrap_or_default();
        self.deleted_node_pointers.extend(all_pointers);
        
        self.db.delete_node_internal(node_id, self.id, 0)?;
        self.capture_dirty_pages()?;
        // Track write for conflict detection
        self.write_nodes.insert(node_id);
        Ok(())
    }

    /// Deletes an edge from the graph within this transaction.
    ///
    /// The edge will be marked as deleted. The deletion is not visible
    /// to other transactions until this transaction is committed.
    ///
    /// # Arguments
    /// * `edge_id` - The ID of the edge to delete
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction limits exceeded
    /// * `GraphError::NotFound` - Edge doesn't exist
    pub fn delete_edge(&mut self, edge_id: EdgeId) -> Result<()> {
        self.db.delete_edge_internal(edge_id)?;
        self.capture_dirty_pages()?;
        // Track write for conflict detection
        self.write_edges.insert(edge_id);
        Ok(())
    }

    /// Retrieves a node by ID within this transaction.
    ///
    /// Can see nodes that were added in this transaction as well as
    /// committed nodes from other transactions.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to retrieve
    ///
    /// # Returns
    /// The node with the specified ID.
    ///
    /// # Returns
    /// * `Ok(Some(Node))` - Node found
    /// * `Ok(None)` - Node doesn't exist
    pub fn get_node(&mut self, node_id: NodeId) -> Result<Option<Node>> {
        let result = self
            .db
            .get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.id))?;
        // Track read for conflict detection
        if result.is_some() {
            self.read_nodes.insert(node_id);
        }
        Ok(result)
    }

    pub fn get_edge(&mut self, edge_id: EdgeId) -> Result<Edge> {
        self.db
            .load_edge_with_snapshot(edge_id, self.snapshot_ts, Some(self.id))
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        self.db.get_nodes_by_label(label)
    }

    /// Retrieves all neighboring nodes for a given node.
    ///
    /// Returns both incoming and outgoing neighbors. The result includes
    /// neighbors from this transaction's uncommitted changes as well as
    /// committed data.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node
    ///
    /// # Returns
    /// A vector of neighboring node IDs.
    ///
    /// # Errors
    /// * `GraphError::NotFound` - Node doesn't exist
    pub fn get_neighbors(&self, node_id: NodeId) -> Result<Vec<NodeId>> {
        self.db
            .get_neighbors_with_snapshot(node_id, self.snapshot_ts, Some(self.id))
    }

    /// Creates a property index (not supported within transactions).
    ///
    /// Property indexes must be created outside of transactions as they
    /// affect the global database state.
    ///
    /// # Arguments
    /// * `label` - The node label to index
    /// * `property_key` - The property key to index
    ///
    /// # Errors
    /// Always returns `GraphError::InvalidArgument` as this operation
    /// cannot be performed within a transaction.
    pub fn create_property_index(&mut self, _label: &str, _property_key: &str) -> Result<()> {
        Err(GraphError::InvalidArgument(
            "create_property_index cannot be called within a transaction".into(),
        ))
    }

    /// Finds nodes by property value using an index.
    ///
    /// Requires that a property index has been created for the specified
    /// label and property key. Only indexable property types (bool, int,
    /// string) can be searched.
    ///
    /// # Arguments
    /// * `label` - The node label to search
    /// * `property_key` - The property key to search
    /// * `value` - The property value to match
    ///
    /// # Returns
    /// A vector of node IDs matching the criteria.
    ///
    /// # Errors
    /// * `GraphError::NotFound` - No index exists for the label/property
    /// * `GraphError::InvalidArgument` - Property type is not indexable
    pub fn find_nodes_by_property(
        &self,
        label: &str,
        property_key: &str,
        value: &crate::model::PropertyValue,
    ) -> Result<Vec<NodeId>> {
        self.db.find_nodes_by_property_with_snapshot(label, property_key, value, self.snapshot_ts)
    }

    /// Removes a property from a node within this transaction.
    ///
    /// If the property doesn't exist, this operation succeeds without error.
    /// The change is not visible to other transactions until this transaction
    /// is committed.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node
    /// * `property_key` - The property key to remove
    ///
    /// # Errors
    /// * `GraphError::InvalidArgument` - Transaction limits exceeded
    /// * `GraphError::NotFound` - Node doesn't exist
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node, PropertyValue};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let mut node = Node::new(1);
    /// node.properties.insert("age".to_string(), PropertyValue::Int(30));
    /// let node_id = tx.add_node(node)?;
    /// tx.remove_node_property(node_id, "age")?;
    /// tx.commit()?;
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn remove_node_property(&mut self, node_id: NodeId, property_key: &str) -> Result<()> {
        self.db.remove_node_property_internal(node_id, property_key)?;
        self.capture_dirty_pages()?;
        // Track write for conflict detection
        self.write_nodes.insert(node_id);
        Ok(())
    }

    /// Commits the transaction, making all changes permanent.
    ///
    /// This is an atomic operation - either all changes are committed
    /// or none are. The transaction cannot be used after committing.
    ///
    /// # Returns
    /// Ok(()) on successful commit.
    ///
    /// # Errors
    /// * `GraphError::Io` - Disk I/O error during commit
    /// * `GraphError::Corruption` - Data corruption detected
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let node_id = tx.add_node(Node::new(1))?;
    /// tx.commit()?; // Changes are now permanent
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn commit(mut self) -> Result<()> {
        self.ensure_active()?;
        self.capture_dirty_pages()?;
        let start = std::time::Instant::now();
        let dirty_page_count = self.dirty_pages.len();

        // Phase 5: header wrapped in Mutex
        self.db.header.lock().unwrap().last_committed_tx_id = self.id;

        // Get commit timestamp from oracle
        let commit_ts = self.db.timestamp_oracle.allocate_commit_timestamp();
        self.db.header.lock().unwrap().max_timestamp = commit_ts;

        let write_header_result = self.db.write_header();
        if let Err(err) = write_header_result {
            let _ = self.db.rollback_transaction(&self.dirty_pages);
            self.unregister_snapshot();
            self.db.stop_tracking();
            self.db.exit_transaction(self.id);
            self.state = TxState::RolledBack;
            return Err(err);
        }

        self.capture_dirty_pages()?;

        // Optimization: Skip WAL writes for empty transactions (no modifications)
        // This is common for read-only transactions or rolled-back operations
        if self.dirty_pages.is_empty() {
            self.unregister_snapshot();
            self.db.stop_tracking();
            self.db.exit_transaction(self.id);
            self.state = TxState::Committed;
            let duration = start.elapsed();
            info!(
                tx_id = self.id,
                dirty_pages = 0,
                duration_ms = duration.as_millis(),
                read_nodes = self.read_nodes.len(),
                write_nodes = 0,
                write_edges = 0,
                "Empty transaction committed (no WAL write)"
            );
            return Ok(());
        }

        // Update commit_ts in all version metadata created by this transaction
        // Pass the tracked version pointers for direct update (optimization)
        if commit_ts > 0 {
            if let Err(err) = self.db.update_versions_commit_ts(
                self.id,
                commit_ts,
                &self.dirty_pages,
                &self.created_versions,
            ) {
                let _ = self.db.rollback_transaction(&self.dirty_pages);
                self.unregister_snapshot();
                self.db.stop_tracking();
                self.db.exit_transaction(self.id);
                self.state = TxState::RolledBack;
                return Err(err);
            }

            // Update property index commit timestamps for nodes added in this transaction
            // AND update delete timestamps for deleted nodes
            
            // Update commit timestamps for added/updated nodes
            for node_id in &self.write_nodes {
                let _ = self.db.update_property_index_commit_ts(*node_id, commit_ts);
            }
            
            // Update delete timestamps for deleted nodes
            for pointer in &self.deleted_node_pointers {
                if let Err(err) = self.db.update_property_index_delete_ts_by_pointer(*pointer, commit_ts) {
                    warn!("Failed to update property index delete timestamps for pointer {:?}: {}", pointer, err);
                    // Continue - this is an index update, not critical for consistency
                }
            }
        }

        let pages = self.dirty_pages.clone();
        let result = self.db.commit_to_wal(self.id, &pages);
        match result {
            Ok(()) => {
                self.unregister_snapshot();
                self.db.stop_tracking();
                self.db.exit_transaction(self.id);
                self.state = TxState::Committed;
                let duration = start.elapsed();
                info!(
                    tx_id = self.id,
                    dirty_pages = dirty_page_count,
                    duration_ms = duration.as_millis(),
                    read_nodes = self.read_nodes.len(),
                    write_nodes = self.write_nodes.len(),
                    write_edges = self.write_edges.len(),
                    "Transaction committed"
                );
                Ok(())
            }
            Err(err) => {
                let _ = self.db.rollback_transaction(&pages);
                self.unregister_snapshot();
                self.db.stop_tracking();
                self.db.exit_transaction(self.id);
                self.state = TxState::RolledBack;
                Err(err)
            }
        }
    }

    /// Rolls back the transaction, discarding all changes.
    ///
    /// All operations performed in this transaction are undone and
    /// will not be visible to other transactions. The transaction
    /// cannot be used after rolling back.
    ///
    /// # Returns
    /// Ok(()) on successful rollback.
    ///
    /// # Example
    /// ```rust
    /// use sombra::{GraphDB, Node};
    ///
    /// let mut db = GraphDB::open("test.db")?;
    /// let mut tx = db.begin_transaction()?;
    /// let _node_id = tx.add_node(Node::new(1))?; // Will be discarded
    /// tx.rollback()?; // Changes are discarded
    /// # Ok::<(), sombra::GraphError>(())
    /// ```
    pub fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        self.capture_dirty_pages()?;
        let pages = self.dirty_pages.clone();
        let result = self.db.rollback_transaction(&pages);
        self.unregister_snapshot();
        self.db.stop_tracking();
        self.db.exit_transaction(self.id);
        self.state = TxState::RolledBack;
        warn!(tx_id = self.id, "Transaction rolled back");
        result
    }

    fn ensure_active(&self) -> Result<()> {
        if self.state != TxState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is no longer active".into(),
            ));
        }
        Ok(())
    }

    /// Unregisters the snapshot from the timestamp oracle
    ///
    /// Should be called when the transaction finishes (commit or rollback)
    fn unregister_snapshot(&self) {
        if self.snapshot_ts > 0 {
            let _ = self.db.timestamp_oracle.unregister_snapshot(self.snapshot_ts);
        }
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        if self.state == TxState::Active {
            self.unregister_snapshot();
            self.db.stop_tracking();
            let _ = self.db.rollback_transaction(&self.dirty_pages);
            self.db.exit_transaction(self.id);
            if !std::thread::panicking() {
                panic!("transaction {} dropped without commit or rollback", self.id);
            }
        }
    }
}
