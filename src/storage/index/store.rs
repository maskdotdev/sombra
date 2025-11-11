use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::PutItem;
use crate::types::{LabelId, NodeId, PageId, PropId, Result, SombraError};

use super::btree_postings::{BTreePostings, Unit};
use super::catalog::IndexCatalog;
use super::chunked::ChunkedIndex;
use super::label::{LabelIndex, LabelScan};
use super::types::{IndexDef, IndexKind, PostingStream};

/// Root page IDs for all index structures.
pub struct IndexRoots {
    /// Root page of the index catalog
    pub catalog: PageId,
    /// Root page of the label index
    pub label: PageId,
    /// Root page of the chunked property index
    pub prop_chunk: PageId,
    /// Root page of the B-tree property index
    pub prop_btree: PageId,
}

/// Manages all indexing structures for the graph database.
pub struct IndexStore {
    #[allow(dead_code)]
    store: Arc<dyn PageStore>,
    catalog: IndexCatalog,
    label_index: LabelIndex,
    chunked: ChunkedIndex,
    btree: BTreePostings,
}

impl IndexStore {
    /// Opens an existing index store with the given root pages.
    pub fn open(store: Arc<dyn PageStore>, roots: IndexRoots) -> Result<(Self, IndexRoots)> {
        let (catalog, catalog_root) = IndexCatalog::open(&store, roots.catalog)?;
        let (label_index, label_root) = LabelIndex::open(&store, roots.label)?;
        let (chunked, _chunk_root) = ChunkedIndex::open(&store, roots.prop_chunk)?;
        let (btree, _btree_root) = BTreePostings::open(&store, roots.prop_btree)?;
        let index_store = Self {
            store: Arc::clone(&store),
            catalog,
            label_index,
            chunked,
            btree,
        };
        let roots = IndexRoots {
            catalog: catalog_root,
            label: label_root,
            prop_chunk: index_store.chunked.root_page(),
            prop_btree: index_store.btree.root_page(),
        };
        Ok((index_store, roots))
    }

    /// Returns the current root pages for all index structures.
    pub fn roots(&self) -> IndexRoots {
        IndexRoots {
            catalog: self.catalog.tree().root_page(),
            label: self.label_index.root_page(),
            prop_chunk: self.chunked.root_page(),
            prop_btree: self.btree.root_page(),
        }
    }

    /// Returns a reference to the index catalog.
    pub fn catalog(&self) -> &IndexCatalog {
        &self.catalog
    }

    /// Checks if a label index exists for the given label.
    pub fn has_label_index(&self, label: LabelId) -> Result<bool> {
        self.label_index.is_indexed_read(label)
    }

    /// Checks if a label index exists using a write transaction.
    pub fn has_label_index_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
    ) -> Result<bool> {
        self.label_index.is_indexed_with_write(tx, label)
    }

    /// Creates a new label index and populates it with existing nodes.
    pub fn create_label_index(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        existing_nodes: Vec<NodeId>,
    ) -> Result<()> {
        self.label_index.create_index(tx, label, existing_nodes)
    }

    /// Drops an existing label index.
    pub fn drop_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        self.label_index.drop_index(tx, label)
    }

    /// Inserts a node into all relevant label indexes.
    pub fn insert_node_labels(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
    ) -> Result<()> {
        for label in labels {
            if self.label_index.is_indexed_with_write(tx, *label)? {
                self.label_index.insert_node(tx, *label, node)?;
            }
        }
        Ok(())
    }

    /// Removes a node from all relevant label indexes.
    pub fn remove_node_labels(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
    ) -> Result<()> {
        for label in labels {
            if self.label_index.is_indexed_with_write(tx, *label)? {
                self.label_index.remove_node(tx, *label, node)?;
            }
        }
        Ok(())
    }

    /// Returns an iterator over all nodes with the given label.
    pub fn label_scan<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Option<LabelScan<'a>>> {
        if !self.label_index.is_indexed_read(label)? {
            return Ok(None);
        }
        let scan = self.label_index.scan(tx, label)?;
        Ok(Some(scan))
    }

    /// Retrieves the property index definition for a label and property.
    pub fn get_property_index(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
    ) -> Result<Option<IndexDef>> {
        self.catalog.get(tx, label, prop)
    }

    /// Returns all property indexes for a given label.
    pub fn property_indexes_for_label(
        &self,
        tx: &ReadGuard,
        label: LabelId,
    ) -> Result<Vec<IndexDef>> {
        self.catalog.iter_label(tx, label)
    }

    /// Returns all property indexes for a label using a write transaction.
    pub fn property_indexes_for_label_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
    ) -> Result<Vec<IndexDef>> {
        self.catalog.iter_label_with_write(tx, label)
    }

    /// Creates a new property index and populates it with existing values.
    pub fn create_property_index(
        &self,
        tx: &mut WriteGuard<'_>,
        def: IndexDef,
        existing: &[(Vec<u8>, NodeId)],
    ) -> Result<()> {
        self.catalog.insert(tx, def)?;
        match def.kind {
            IndexKind::Chunked => {
                for (value_key, node) in existing {
                    self.insert_property_value(tx, &def, value_key, *node)?;
                }
            }
            IndexKind::BTree => {
                self.insert_property_values_btree(tx, &def, existing)?;
            }
        }
        Ok(())
    }

    fn insert_property_values_btree(
        &self,
        tx: &mut WriteGuard<'_>,
        def: &IndexDef,
        existing: &[(Vec<u8>, NodeId)],
    ) -> Result<()> {
        if existing.is_empty() {
            return Ok(());
        }
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(existing.len());
        for (value_key, node) in existing {
            let prefix = BTreePostings::make_prefix(def.label, def.prop, value_key);
            keys.push(BTreePostings::make_key(&prefix, *node));
        }
        keys.sort();
        let unit = Unit;
        let iter = keys.iter().map(|key| PutItem { key, value: &unit });
        self.btree.put_many(tx, iter)
    }

    /// Drops an existing property index and removes all entries.
    pub fn drop_property_index(&self, tx: &mut WriteGuard<'_>, def: IndexDef) -> Result<()> {
        self.drop_property_entries(tx, &def)?;
        let removed = self.catalog.remove(tx, def.label, def.prop)?;
        if !removed {
            return Err(SombraError::Invalid("property index not found"));
        }
        Ok(())
    }

    /// Inserts a property value into the appropriate index.
    pub fn insert_property_value(
        &self,
        tx: &mut WriteGuard<'_>,
        def: &IndexDef,
        value_key: &[u8],
        node: NodeId,
    ) -> Result<()> {
        match def.kind {
            IndexKind::Chunked => {
                let prefix = ChunkedIndex::make_prefix(def.label, def.prop, value_key);
                self.chunked.put(tx, &prefix, node)
            }
            IndexKind::BTree => {
                let prefix = BTreePostings::make_prefix(def.label, def.prop, value_key);
                self.btree.put(tx, &prefix, node)
            }
        }
    }

    /// Removes a property value from the appropriate index.
    pub fn remove_property_value(
        &self,
        tx: &mut WriteGuard<'_>,
        def: &IndexDef,
        value_key: &[u8],
        node: NodeId,
    ) -> Result<()> {
        match def.kind {
            IndexKind::Chunked => {
                let prefix = ChunkedIndex::make_prefix(def.label, def.prop, value_key);
                self.chunked.remove(tx, &prefix, node)
            }
            IndexKind::BTree => {
                let prefix = BTreePostings::make_prefix(def.label, def.prop, value_key);
                self.btree.remove(tx, &prefix, node)
            }
        }
    }

    /// Scans for all nodes with a specific property value (equality).
    pub fn scan_property_eq(
        &self,
        tx: &ReadGuard,
        def: &IndexDef,
        value_key: &[u8],
    ) -> Result<Vec<NodeId>> {
        match def.kind {
            IndexKind::Chunked => {
                let prefix = ChunkedIndex::make_prefix(def.label, def.prop, value_key);
                self.chunked.scan(tx, &prefix)
            }
            IndexKind::BTree => {
                let prefix = BTreePostings::make_prefix(def.label, def.prop, value_key);
                self.btree.scan_eq(tx, &prefix)
            }
        }
    }

    /// Scans for all nodes with property values in a given range.
    pub fn scan_property_range(
        &self,
        tx: &ReadGuard,
        def: &IndexDef,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, NodeId)>> {
        match def.kind {
            IndexKind::Chunked => self
                .chunked
                .scan_range_bounds(tx, def.label, def.prop, start, end),
            IndexKind::BTree => self
                .btree
                .scan_range_bounds(tx, def.label, def.prop, start, end),
        }
    }

    /// Returns a streaming iterator over nodes with a specific property value.
    pub fn scan_property_eq_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        def: &IndexDef,
        value_key: &[u8],
    ) -> Result<Box<dyn PostingStream + 'a>> {
        match def.kind {
            IndexKind::Chunked => self.chunked.stream_eq(tx, def.label, def.prop, value_key),
            IndexKind::BTree => self.btree.stream_eq(tx, def.label, def.prop, value_key),
        }
    }

    /// Returns a streaming iterator over nodes with property values in a range.
    pub fn scan_property_range_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        def: &IndexDef,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        match def.kind {
            IndexKind::Chunked => self
                .chunked
                .stream_range_bounds(tx, def.label, def.prop, start, end),
            IndexKind::BTree => self
                .btree
                .stream_range_bounds(tx, def.label, def.prop, start, end),
        }
    }

    fn drop_property_entries(&self, tx: &mut WriteGuard<'_>, def: &IndexDef) -> Result<()> {
        match def.kind {
            IndexKind::Chunked => self.drop_chunked_entries(tx, def.label, def.prop),
            IndexKind::BTree => self.drop_btree_entries(tx, def.label, def.prop),
        }
    }

    fn drop_chunked_entries(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        prop: PropId,
    ) -> Result<()> {
        self.chunked.drop_entries(tx, label, prop)
    }

    fn drop_btree_entries(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        prop: PropId,
    ) -> Result<()> {
        self.btree.drop_entries(tx, label, prop)
    }
}
