use std::collections::BTreeMap;

use crate::primitives::pager::WriteGuard;
use crate::storage::index::{IndexDef, IndexKind, IndexStore};
use crate::storage::mvcc::CommitId;
use crate::storage::{profile_timer, record_flush_deferred, record_flush_deferred_indexes};
use crate::types::{EdgeId, LabelId, NodeId, Result, TypeId};

use super::{AdjacencyBuffer, Graph, IndexBuffer};

impl Graph {
    pub(crate) fn stage_adjacency_inserts(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_adjacency_flush {
            return self.insert_adjacencies(tx, entries, commit);
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_adj
            .get_or_insert_with(AdjacencyBuffer::default);
        for (src, dst, ty, edge) in entries {
            buffer.inserts.push((*src, *dst, *ty, *edge, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    pub(crate) fn stage_adjacency_removals(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_adjacency_flush {
            for (src, dst, ty, edge) in entries {
                self.remove_adjacency(tx, *src, *dst, *ty, *edge, commit)?;
            }
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_adj
            .get_or_insert_with(AdjacencyBuffer::default);
        for (src, dst, ty, edge) in entries {
            buffer.removals.push((*src, *dst, *ty, *edge, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    /// Flushes buffered adjacency and index updates for the current transaction.
    pub fn flush_deferred_writes(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let start = profile_timer();
        self.flush_deferred_adjacency(tx)?;
        self.flush_deferred_indexes(tx)?;
        if let Some(start) = start {
            record_flush_deferred(start.elapsed().as_nanos() as u64);
        }
        Ok(())
    }

    fn flush_deferred_adjacency(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        if !self.defer_adjacency_flush {
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let Some(mut buffer) = state.deferred_adj.take() else {
            self.store_txn_state(tx, state);
            return Ok(());
        };
        let mut total_inserts = 0usize;
        if !buffer.inserts.is_empty() {
            let mut grouped: BTreeMap<CommitId, Vec<(NodeId, NodeId, TypeId, EdgeId)>> =
                BTreeMap::new();
            for (src, dst, ty, edge, commit) in buffer.inserts.drain(..) {
                grouped
                    .entry(commit)
                    .or_default()
                    .push((src, dst, ty, edge));
            }
            for (commit, batch) in grouped {
                total_inserts = total_inserts.saturating_add(batch.len());
                self.insert_adjacencies(tx, &batch, commit)?;
            }
        }
        let total_removals = buffer.removals.len();
        for (src, dst, ty, edge, commit) in buffer.removals.drain(..) {
            self.remove_adjacency(tx, src, dst, ty, edge, commit)?;
        }
        self.metrics
            .adjacency_bulk_flush(total_inserts, total_removals);
        self.store_txn_state(tx, state);
        Ok(())
    }

    pub(crate) fn stage_label_inserts(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_index_flush {
            return self
                .indexes
                .insert_node_labels_with_commit(tx, node, labels, Some(commit));
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        for label in labels {
            buffer.label_inserts.push((*label, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    pub(crate) fn stage_label_removals(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_index_flush {
            return self
                .indexes
                .remove_node_labels_with_commit(tx, node, labels, Some(commit));
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        for label in labels {
            buffer.label_removes.push((*label, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    pub(crate) fn stage_prop_index_op(
        &self,
        tx: &mut WriteGuard<'_>,
        def: IndexDef,
        key: Vec<u8>,
        node: NodeId,
        commit: CommitId,
        insert: bool,
    ) -> Result<()> {
        if !self.defer_index_flush {
            if insert {
                self.indexes.insert_property_value_with_commit(
                    tx,
                    &def,
                    &key,
                    node,
                    Some(commit),
                )?;
            } else {
                self.indexes.remove_property_value_with_commit(
                    tx,
                    &def,
                    &key,
                    node,
                    Some(commit),
                )?;
            }
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        if insert {
            buffer.prop_inserts.push((def, key, node, commit));
        } else {
            buffer.prop_removes.push((def, key, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn flush_deferred_indexes(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let flush_idx_start = profile_timer();
        if !self.defer_index_flush {
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let Some(mut buffer) = state.deferred_index.take() else {
            self.store_txn_state(tx, state);
            return Ok(());
        };

        // === OPTIMIZED: Batch label inserts ===
        if !buffer.label_inserts.is_empty() {
            let entries: Vec<_> = buffer
                .label_inserts
                .drain(..)
                .collect();
            self.indexes.insert_node_labels_batch(tx, entries)?;
        }

        // Process label removes (unchanged - typically less frequent)
        for (label, node, commit) in buffer.label_removes.drain(..) {
            if self.indexes.has_label_index_with_write(tx, label)? {
                self.indexes
                    .remove_node_labels_with_commit(tx, node, &[label], Some(commit))?;
            }
        }

        // === OPTIMIZED: Batch property inserts ===
        if !buffer.prop_inserts.is_empty() {
            // Separate by index kind
            let mut btree_items: Vec<(Vec<u8>, NodeId, Option<CommitId>)> = Vec::new();
            let mut chunked_groups: BTreeMap<Vec<u8>, (Vec<NodeId>, Option<CommitId>)> =
                BTreeMap::new();

            for (def, key, node, commit) in buffer.prop_inserts.drain(..) {
                match def.kind {
                    IndexKind::BTree => {
                        let prefix = IndexStore::btree_prefix(def.label, def.prop, &key);
                        btree_items.push((prefix, node, Some(commit)));
                    }
                    IndexKind::Chunked => {
                        let prefix = IndexStore::chunked_prefix(def.label, def.prop, &key);
                        chunked_groups
                            .entry(prefix)
                            .or_insert_with(|| (Vec::new(), Some(commit)))
                            .0
                            .push(node);
                    }
                }
            }

            // Batch insert BTree items
            if !btree_items.is_empty() {
                self.indexes
                    .insert_property_values_batch_btree(tx, btree_items)?;
            }

            // Batch insert Chunked items
            if !chunked_groups.is_empty() {
                let chunked_items: Vec<_> = chunked_groups
                    .into_iter()
                    .map(|(prefix, (nodes, commit))| (prefix, nodes, commit))
                    .collect();
                self.indexes
                    .insert_property_values_batch_chunked(tx, chunked_items)?;
            }
        }

        // Property removes (keep individual for now - typically less frequent)
        for (def, key, node, commit) in buffer.prop_removes.drain(..) {
            self.indexes
                .remove_property_value_with_commit(tx, &def, &key, node, Some(commit))?;
        }

        self.store_txn_state(tx, state);
        if let Some(start) = flush_idx_start {
            record_flush_deferred_indexes(start.elapsed().as_nanos() as u64);
        }
        Ok(())
    }
}
