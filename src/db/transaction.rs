use super::core::GraphDB;
use super::group_commit::TxId;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NodeId};
use crate::pager::PageId;
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
    db: &'db mut GraphDB,
    id: TxId,
    epoch: u64,
    state: TxState,
    pub dirty_pages: Vec<PageId>,
    start_time: std::time::Instant,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db mut GraphDB, id: TxId) -> Result<Self> {
        db.enter_transaction(id)?;
        db.start_tracking();
        let epoch = db.increment_epoch();
        debug!(tx_id = id, epoch = epoch, "Transaction started");
        Ok(Self {
            db,
            id,
            epoch,
            state: TxState::Active,
            dirty_pages: Vec::new(),
            start_time: std::time::Instant::now(),
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
        let node_id = self.db.add_node_internal(node)?;
        self.capture_dirty_pages()?;
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
        let edge_id = self.db.add_edge_internal(edge)?;
        self.capture_dirty_pages()?;
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
        self.db.delete_node_internal(node_id)?;
        self.capture_dirty_pages()?;
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
    /// # Errors
    /// * `GraphError::NotFound` - Node doesn't exist
    pub fn get_node(&mut self, node_id: NodeId) -> Result<Node> {
        self.db.get_node(node_id)
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
    pub fn get_neighbors(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        self.db.get_neighbors(node_id)
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
        &mut self,
        label: &str,
        property_key: &str,
        value: &crate::model::PropertyValue,
    ) -> Result<Vec<NodeId>> {
        self.db.find_nodes_by_property(label, property_key, value)
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

        self.db.header.last_committed_tx_id = self.id;
        let write_header_result = self.db.write_header();
        if let Err(err) = write_header_result {
            let _ = self.db.rollback_transaction(&self.dirty_pages);
            self.db.stop_tracking();
            self.db.exit_transaction();
            self.state = TxState::RolledBack;
            return Err(err);
        }

        self.capture_dirty_pages()?;
        let pages = self.dirty_pages.clone();
        let result = self.db.commit_to_wal(self.id, &pages);
        match result {
            Ok(()) => {
                self.db.stop_tracking();
                self.db.exit_transaction();
                self.state = TxState::Committed;
                let duration = start.elapsed();
                info!(
                    tx_id = self.id,
                    dirty_pages = dirty_page_count,
                    duration_ms = duration.as_millis(),
                    "Transaction committed"
                );
                Ok(())
            }
            Err(err) => {
                let _ = self.db.rollback_transaction(&pages);
                self.db.stop_tracking();
                self.db.exit_transaction();
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
        self.db.stop_tracking();
        self.db.exit_transaction();
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
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        self.db.stop_tracking();
        if self.state == TxState::Active {
            let _ = self.db.rollback_transaction(&self.dirty_pages);
            self.db.exit_transaction();
            if !std::thread::panicking() {
                panic!("transaction {} dropped without commit or rollback", self.id);
            }
        }
    }
}
