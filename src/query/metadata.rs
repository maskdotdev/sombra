//! Metadata resolution helpers bridging human-readable names to catalog identifiers.
//!
//! Stage 8 surfaces a fluent API that allows referencing labels, property keys, and edge
//! types by name. The planner requires deterministic identifiers when selecting indexes
//! or adjacency operators, so these helpers provide the translation layer.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::primitives::pager::PageStore;
use crate::storage::catalog::{Dict, DictOptions};
use crate::storage::index::{IndexCatalog, IndexDef, IndexKind, TypeTag};
use crate::storage::{Graph, PropStats};
use crate::types::{LabelId, PageId, PropId, Result, SombraError, StrId, TypeId};

/// Provides name-to-identifier resolution for planner consumers.
pub trait MetadataProvider {
    /// Resolves a label name to its numeric identifier.
    fn resolve_label(&self, name: &str) -> Result<LabelId>;
    /// Resolves a property name to its numeric identifier.
    fn resolve_property(&self, name: &str) -> Result<PropId>;
    /// Resolves an edge type name to its numeric identifier.
    fn resolve_edge_type(&self, name: &str) -> Result<TypeId>;
    /// Looks up an index definition for the given label and property.
    fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>>;
    /// Resolves a property identifier back to its canonical name.
    fn property_name(&self, id: PropId) -> Result<String>;
    /// Returns statistics for the given (label, property) pair when available.
    fn property_stats(&self, label: LabelId, prop: PropId) -> Result<Option<PropStats>>;
    /// Returns the type hint for the given property, when known.
    fn property_type_hint(&self, _prop: PropId) -> Result<Option<TypeTag>> {
        Ok(None)
    }
    /// Returns whether a label exposes the requested property (best effort).
    fn label_has_property(&self, _label: LabelId, _prop: PropId) -> Result<bool> {
        Ok(true)
    }
    /// Returns the catalog epoch for plan hashing (0 when unknown).
    fn catalog_epoch(&self) -> u64 {
        0
    }
}

/// Metadata provider backed by the Stage 5 string dictionary.
pub struct CatalogMetadata {
    dict: Arc<Dict>,
    catalog: Arc<IndexCatalog>,
    graph: Arc<Graph>,
    prop_stats: Mutex<HashMap<(LabelId, PropId), Arc<PropStats>>>,
}

impl CatalogMetadata {
    /// Opens a dictionary using the supplied pager handle and index catalog root page.
    pub fn open(
        store: Arc<dyn PageStore>,
        opts: DictOptions,
        catalog_root: PageId,
        graph: Arc<Graph>,
    ) -> Result<Self> {
        let dict = Dict::open(Arc::clone(&store), opts)?;
        Self::from_parts(Arc::new(dict), store, catalog_root, graph)
    }

    /// Wraps an existing dictionary handle and catalog.
    pub fn from_parts(
        dict: Arc<Dict>,
        store: Arc<dyn PageStore>,
        catalog_root: PageId,
        graph: Arc<Graph>,
    ) -> Result<Self> {
        let (catalog, _) = IndexCatalog::open(&store, catalog_root)?;
        Ok(Self {
            dict,
            catalog: Arc::new(catalog),
            graph,
            prop_stats: Mutex::new(HashMap::new()),
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
        let read = self.catalog.store().begin_latest_committed_read()?;
        self.catalog.get(&read, label, prop)
    }

    fn property_name(&self, id: PropId) -> Result<String> {
        self.dict.resolve_str(StrId(id.0))
    }

    fn property_stats(&self, label: LabelId, prop: PropId) -> Result<Option<PropStats>> {
        let mut guard = self
            .prop_stats
            .lock()
            .map_err(|_| SombraError::Invalid("prop stats cache lock poisoned"))?;
        if let Some(stats) = guard.get(&(label, prop)).cloned() {
            return Ok(Some((*stats).clone()));
        }
        let stats = self.graph.property_stats(label, prop)?;
        if let Some(stats) = stats {
            let arc = Arc::new(stats);
            guard.insert((label, prop), arc.clone());
            Ok(Some((*arc).clone()))
        } else {
            Ok(None)
        }
    }

    fn label_has_property(&self, _label: LabelId, _prop: PropId) -> Result<bool> {
        Ok(true)
    }

    fn catalog_epoch(&self) -> u64 {
        self.graph.catalog_epoch()
    }
}

/// Simple in-memory metadata provider used for tests or prototyping.
pub struct InMemoryMetadata {
    labels: HashMap<String, LabelId>,
    props: HashMap<String, PropId>,
    prop_names: HashMap<PropId, String>,
    edge_types: HashMap<String, TypeId>,
    prop_indexes: HashMap<(LabelId, PropId), IndexDef>,
    label_props: HashMap<LabelId, HashSet<PropId>>,
}

impl InMemoryMetadata {
    /// Creates a new empty in-memory metadata provider.
    pub fn new() -> Self {
        Self {
            labels: HashMap::new(),
            props: HashMap::new(),
            prop_names: HashMap::new(),
            edge_types: HashMap::new(),
            prop_indexes: HashMap::new(),
            label_props: HashMap::new(),
        }
    }

    /// Registers a label name with its identifier.
    pub fn with_label(mut self, name: impl Into<String>, id: LabelId) -> Self {
        self.labels.insert(name.into(), id);
        self
    }

    /// Registers a property name with its identifier.
    pub fn with_property(mut self, name: impl Into<String>, id: PropId) -> Self {
        let name = name.into();
        self.props.insert(name.clone(), id);
        self.prop_names.insert(id, name);
        self
    }

    /// Registers an edge type name with its identifier.
    pub fn with_edge_type(mut self, name: impl Into<String>, id: TypeId) -> Self {
        self.edge_types.insert(name.into(), id);
        self
    }

    /// Registers a property index for the given label and property.
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

    /// Registers a custom index definition for the given label and property.
    pub fn with_property_index_def(mut self, def: IndexDef) -> Self {
        self.prop_indexes.insert((def.label, def.prop), def);
        self
    }

    /// Restricts the set of properties allowed on a label (empty set denies all).
    pub fn with_label_props<I>(mut self, label: LabelId, props: I) -> Self
    where
        I: IntoIterator<Item = PropId>,
    {
        self.label_props
            .insert(label, props.into_iter().collect::<HashSet<_>>());
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

    fn property_name(&self, id: PropId) -> Result<String> {
        self.prop_names
            .get(&id)
            .cloned()
            .ok_or(SombraError::NotFound)
    }

    fn property_stats(&self, _label: LabelId, _prop: PropId) -> Result<Option<PropStats>> {
        Ok(None)
    }

    fn label_has_property(&self, label: LabelId, prop: PropId) -> Result<bool> {
        Ok(self
            .label_props
            .get(&label)
            .map(|set| set.contains(&prop))
            .unwrap_or(true))
    }
}
