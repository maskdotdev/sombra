use super::core::GraphDB;
use super::group_commit::TxId;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NodeId};
use crate::pager::PageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug)]
pub struct Transaction<'db> {
    db: &'db mut GraphDB,
    id: TxId,
    state: TxState,
    pub dirty_pages: Vec<PageId>,
}

impl<'db> Transaction<'db> {
    pub(crate) fn new(db: &'db mut GraphDB, id: TxId) -> Result<Self> {
        db.enter_transaction(id)?;
        db.start_tracking();
        Ok(Self {
            db,
            id,
            state: TxState::Active,
            dirty_pages: Vec::new(),
        })
    }

    pub fn id(&self) -> TxId {
        self.id
    }

    pub fn state(&self) -> TxState {
        self.state
    }

    fn capture_dirty_pages(&mut self) {
        let mut pages = self.db.take_recent_dirty_pages();
        if pages.is_empty() {
            return;
        }
        self.dirty_pages.append(&mut pages);
        self.dirty_pages.sort_unstable();
        self.dirty_pages.dedup();
    }

    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        let node_id = self.db.add_node_internal(node)?;
        self.capture_dirty_pages();
        Ok(node_id)
    }

    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId> {
        let edge_id = self.db.add_edge_internal(edge)?;
        self.capture_dirty_pages();
        Ok(edge_id)
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        self.db.delete_node_internal(node_id)?;
        self.capture_dirty_pages();
        Ok(())
    }

    pub fn delete_edge(&mut self, edge_id: EdgeId) -> Result<()> {
        self.db.delete_edge_internal(edge_id)?;
        self.capture_dirty_pages();
        Ok(())
    }

    pub fn get_node(&mut self, node_id: NodeId) -> Result<Node> {
        self.db.get_node(node_id)
    }

    pub fn get_neighbors(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        self.db.get_neighbors(node_id)
    }

    pub fn commit(mut self) -> Result<()> {
        self.ensure_active()?;
        self.capture_dirty_pages();

        self.db.header.last_committed_tx_id = self.id;
        let write_header_result = self.db.write_header();
        if let Err(err) = write_header_result {
            let _ = self.db.rollback_transaction(&self.dirty_pages);
            self.db.stop_tracking();
            self.db.exit_transaction();
            self.state = TxState::RolledBack;
            return Err(err);
        }

        self.capture_dirty_pages();
        let pages = self.dirty_pages.clone();
        let result = self.db.commit_to_wal(self.id, &pages);
        match result {
            Ok(()) => {
                self.db.stop_tracking();
                self.db.exit_transaction();
                self.state = TxState::Committed;
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

    pub fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        self.capture_dirty_pages();
        let pages = self.dirty_pages.clone();
        let result = self.db.rollback_transaction(&pages);
        self.db.stop_tracking();
        self.db.exit_transaction();
        self.state = TxState::RolledBack;
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
