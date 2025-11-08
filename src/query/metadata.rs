//! Metadata resolution helpers bridging human-readable names to catalog identifiers.
//!
//! Stage 8 surfaces a fluent API that allows referencing labels, property keys, and edge
//! types by name. The planner requires deterministic identifiers when selecting indexes
//! or adjacency operators, so these helpers provide the translation layer.

use std::collections::HashMap;
use std::sync::Arc;

use crate::primitives::pager::PageStore;
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::index::{IndexCatalog, IndexDef, IndexKind, TypeTag};
use crate::types::{LabelId, PageId, PropId, Result, SombraError, TypeId};

/// Provides name-to-identifier resolution for planner consumers.
pub trait MetadataProvider: Send + Sync {
    fn resolve_label(&self, name: &str) -> Result<LabelId>;
    fn resolve_property(&self, name: &str) -> Result<PropId>;
    fn resolve_edge_type(&self, name: &str) -> Result<TypeId>;
    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>>;
}

/// Metadata provider backed by the Stage 5 string dictionary.
pub struct CatalogMetadata {
    dict: Arc<Dict>,
    catalog: Arc<IndexCatalog>,
}

impl CatalogMetadata {
    /// Opens a dictionary using the supplied pager handle and index catalog root page.
    pub fn open(
        store: Arc<dyn PageStore>,
        opts: DictOptions,
        catalog_root: PageId,
    ) -> Result<Self> {
        let dict = Dict::open(Arc::clone(&store), opts)?;
        Self::from_dict(Arc::new(dict), store, catalog_root)
    }

    /// Wraps an existing dictionary handle and catalog.
    pub fn from_dict(
        dict: Arc<Dict>,
        store: Arc<dyn PageStore>,
        catalog_root: PageId,
    ) -> Result<Self> {
        let (catalog, _) = IndexCatalog::open(&store, catalog_root)?;
        Ok(Self {
            dict,
            catalog: Arc::new(catalog),
        })
    }

    fn lookup(&self, name: &str) -> Result<u32> {
        if let Ok(id) = name.parse::<u32>() {
            return Ok(id);
        }
        match self.dict.lookup(name)? {
            Some(id) => Ok(id.0),
            None => Err(SombraError::NotFound),
        }
    }
}

impl MetadataProvider for CatalogMetadata {
    fn resolve_label(&self, name: &str) -> Result<LabelId> {
        self.lookup(name).map(LabelId)
    }

    fn resolve_property(&self, name: &str) -> Result<PropId> {
        self.lookup(name).map(PropId)
    }

    fn resolve_edge_type(&self, name: &str) -> Result<TypeId> {
        self.lookup(name).map(TypeId)
    }

    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        let read = self.catalog.store().begin_read()?;
        self.catalog.get(&read, label, prop)
    }
}

/// Simple in-memory metadata provider used for tests or prototyping.
pub struct InMemoryMetadata {
    labels: HashMap<String, LabelId>,
    props: HashMap<String, PropId>,
    edge_types: HashMap<String, TypeId>,
    prop_indexes: HashMap<(LabelId, PropId), IndexDef>,
}

impl InMemoryMetadata {
    pub fn new() -> Self {
        Self {
            labels: HashMap::new(),
            props: HashMap::new(),
            edge_types: HashMap::new(),
            prop_indexes: HashMap::new(),
        }
    }

    pub fn with_label(mut self, name: impl Into<String>, id: LabelId) -> Self {
        self.labels.insert(name.into(), id);
        self
    }

    pub fn with_property(mut self, name: impl Into<String>, id: PropId) -> Self {
        self.props.insert(name.into(), id);
        self
    }

    pub fn with_edge_type(mut self, name: impl Into<String>, id: TypeId) -> Self {
        self.edge_types.insert(name.into(), id);
        self
    }

    pub fn with_property_index(mut self, label: LabelId, prop: PropId) -> Self {
        self.prop_indexes.insert(
            (label, prop),
            IndexDef {
                label,
                prop,
                kind: IndexKind::Chunked,
                ty: TypeTag::Null,
            },
        );
        self
    }

    pub fn with_property_index_def(mut self, def: IndexDef) -> Self {
        self.prop_indexes.insert((def.label, def.prop), def);
        self
    }

    fn label_from_str(&self, name: &str) -> Option<LabelId> {
        self.labels
            .get(name)
            .copied()
            .or_else(|| name.parse::<u32>().ok().map(LabelId))
    }

    fn prop_from_str(&self, name: &str) -> Option<PropId> {
        self.props
            .get(name)
            .copied()
            .or_else(|| name.parse::<u32>().ok().map(PropId))
    }

    fn edge_from_str(&self, name: &str) -> Option<TypeId> {
        self.edge_types
            .get(name)
            .copied()
            .or_else(|| name.parse::<u32>().ok().map(TypeId))
    }
}

impl MetadataProvider for InMemoryMetadata {
    fn resolve_label(&self, name: &str) -> Result<LabelId> {
        self.label_from_str(name).ok_or(SombraError::NotFound)
    }

    fn resolve_property(&self, name: &str) -> Result<PropId> {
        self.prop_from_str(name).ok_or(SombraError::NotFound)
    }

    fn resolve_edge_type(&self, name: &str) -> Result<TypeId> {
        self.edge_from_str(name).ok_or(SombraError::NotFound)
    }

    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        Ok(self.prop_indexes.get(&(label, prop)).copied())
    }
}
