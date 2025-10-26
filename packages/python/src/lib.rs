#![allow(clippy::uninlined_format_args)]

use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyModule};
use pyo3::Bound;

use ::sombra::db::{GraphDB, TxId};
use ::sombra::error::GraphError;
use ::sombra::model::{Edge, Node, PropertyValue, NULL_EDGE_ID};

#[pyclass(module = "sombra", name = "SombraNode")]
pub struct PySombraNode {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub labels: Vec<String>,
    properties: Vec<(String, Py<PyAny>)>,
}

#[pymethods]
impl PySombraNode {
    #[getter]
    fn properties(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in &self.properties {
            dict.set_item(key, value.clone_ref(py))?;
        }
        Ok(dict.into())
    }
}

impl PySombraNode {
    fn from_node(py: Python<'_>, node: Node) -> PyResult<Self> {
        let properties = node
            .properties
            .into_iter()
            .map(|(key, value)| property_value_to_py(py, &value).map(|py_value| (key, py_value)))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            id: node.id,
            labels: node.labels,
            properties,
        })
    }
}

#[pyclass(module = "sombra", name = "SombraEdge")]
pub struct PySombraEdge {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub source_node_id: u64,
    #[pyo3(get)]
    pub target_node_id: u64,
    #[pyo3(get)]
    pub type_name: String,
    properties: Vec<(String, Py<PyAny>)>,
}

#[pymethods]
impl PySombraEdge {
    #[getter]
    fn properties(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in &self.properties {
            dict.set_item(key, value.clone_ref(py))?;
        }
        Ok(dict.into())
    }
}

impl PySombraEdge {
    fn from_edge(py: Python<'_>, edge: Edge) -> PyResult<Self> {
        let properties = edge
            .properties
            .into_iter()
            .map(|(key, value)| property_value_to_py(py, &value).map(|py_value| (key, py_value)))
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            id: edge.id,
            source_node_id: edge.source_node_id,
            target_node_id: edge.target_node_id,
            type_name: edge.type_name,
            properties,
        })
    }
}

#[pyclass(module = "sombra", name = "BfsResult")]
pub struct PyBfsResult {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub depth: usize,
}

impl PyBfsResult {
    fn new(node_id: u64, depth: usize) -> Self {
        Self { node_id, depth }
    }
}

#[pyclass(module = "sombra", name = "SombraDB", unsendable)]
pub struct PySombraDB {
    inner: Arc<RwLock<GraphDB>>,
}

#[pymethods]
impl PySombraDB {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let db = GraphDB::open(path).map_err(graph_error_to_py)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(db)),
        })
    }

    fn begin_transaction(&self) -> PyResult<PySombraTransaction> {
        let tx_id = {
            let mut db = self.inner.write();
            let tx_id = db.allocate_tx_id().map_err(graph_error_to_py)?;
            db.enter_transaction(tx_id).map_err(graph_error_to_py)?;
            db.start_tracking();
            tx_id
        };

        Ok(PySombraTransaction::new(self.inner.clone(), tx_id))
    }

    fn add_node(
        &self,
        labels: Vec<String>,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = extract_properties(properties)?;
        let node_id = {
            let mut db = self.inner.write();
            let mut node = Node::new(0);
            node.labels = labels;
            node.properties = props;
            db.add_node(node).map_err(graph_error_to_py)?
        };
        Ok(node_id)
    }

    fn add_edge(
        &self,
        source_node_id: u64,
        target_node_id: u64,
        label: String,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = extract_properties(properties)?;
        let edge_id = {
            let mut db = self.inner.write();
            let mut edge = Edge::new(0, source_node_id, target_node_id, &label);
            edge.properties = props;
            db.add_edge(edge).map_err(graph_error_to_py)?
        };
        Ok(edge_id)
    }

    fn get_edge(&self, py: Python<'_>, edge_id: u64) -> PyResult<PySombraEdge> {
        let edge = {
            let mut db = self.inner.write();
            db.load_edge(edge_id).map_err(graph_error_to_py)?
        };
        PySombraEdge::from_edge(py, edge)
    }

    fn get_outgoing_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.inner.write();
        let node = db
            .get_node(node_id)
            .map_err(graph_error_to_py)?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?;
        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;
        while edge_id != NULL_EDGE_ID {
            edges.push(edge_id);
            let edge = db.load_edge(edge_id).map_err(graph_error_to_py)?;
            edge_id = edge.next_outgoing_edge_id;
        }
        Ok(edges)
    }

    fn get_incoming_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.inner.write();
        let node = db
            .get_node(node_id)
            .map_err(graph_error_to_py)?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?;
        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;
        while edge_id != NULL_EDGE_ID {
            edges.push(edge_id);
            let edge = db.load_edge(edge_id).map_err(graph_error_to_py)?;
            edge_id = edge.next_incoming_edge_id;
        }
        Ok(edges)
    }

    fn get_node(&self, py: Python<'_>, node_id: u64) -> PyResult<PySombraNode> {
        let node = {
            let mut db = self.inner.write();
            db.get_node(node_id)
                .map_err(graph_error_to_py)?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?
        };
        PySombraNode::from_node(py, node)
    }

    fn get_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.write();
            db.get_neighbors(node_id).map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn delete_node(&self, node_id: u64) -> PyResult<()> {
        let mut db = self.inner.write();
        db.delete_node(node_id).map_err(graph_error_to_py)
    }

    fn delete_edge(&self, edge_id: u64) -> PyResult<()> {
        let mut db = self.inner.write();
        db.delete_edge(edge_id).map_err(graph_error_to_py)
    }

    fn set_node_property(
        &self,
        _py: Python<'_>,
        node_id: u64,
        key: String,
        value: Bound<PyAny>,
    ) -> PyResult<()> {
        let prop_value = py_any_to_property_value(&value)?;
        let mut db = self.inner.write();
        db.set_node_property(node_id, key, prop_value)
            .map_err(graph_error_to_py)
    }

    fn remove_node_property(&self, node_id: u64, key: String) -> PyResult<()> {
        let mut db = self.inner.write();
        db.remove_node_property(node_id, &key)
            .map_err(graph_error_to_py)
    }

    fn flush(&self) -> PyResult<()> {
        let mut db = self.inner.write();
        db.flush().map_err(graph_error_to_py)
    }

    fn checkpoint(&self) -> PyResult<()> {
        let mut db = self.inner.write();
        db.checkpoint().map_err(graph_error_to_py)
    }

    fn get_incoming_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.write();
            db.get_incoming_neighbors(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_two_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.write();
            db.get_neighbors_two_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_three_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.write();
            db.get_neighbors_three_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn bfs_traversal(&self, start_node_id: u64, max_depth: usize) -> PyResult<Vec<PyBfsResult>> {
        let results = {
            let mut db = self.inner.write();
            db.bfs_traversal(start_node_id, max_depth)
                .map_err(graph_error_to_py)?
        };
        Ok(results
            .into_iter()
            .map(|(node_id, depth)| PyBfsResult::new(node_id, depth))
            .collect())
    }

    fn get_nodes_by_label(&self, label: &str) -> PyResult<Vec<u64>> {
        let node_ids = {
            let mut db = self.inner.write();
            db.get_nodes_by_label(label).map_err(graph_error_to_py)?
        };
        Ok(node_ids)
    }

    fn get_nodes_in_range(&self, start: u64, end: u64) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_nodes_in_range(start, end))
    }

    fn get_nodes_from(&self, start: u64) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_nodes_from(start))
    }

    fn get_nodes_to(&self, end: u64) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_nodes_to(end))
    }

    fn get_first_node(&self) -> PyResult<Option<u64>> {
        let db = self.inner.read();
        Ok(db.get_first_node())
    }

    fn get_last_node(&self) -> PyResult<Option<u64>> {
        let db = self.inner.read();
        Ok(db.get_last_node())
    }

    fn get_first_n_nodes(&self, n: usize) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_first_n_nodes(n))
    }

    fn get_last_n_nodes(&self, n: usize) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_last_n_nodes(n))
    }

    fn get_all_node_ids_ordered(&self) -> PyResult<Vec<u64>> {
        let db = self.inner.read();
        Ok(db.get_all_node_ids_ordered())
    }

    fn count_outgoing_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.inner.write();
        db.count_outgoing_edges(node_id).map_err(graph_error_to_py)
    }

    fn count_incoming_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.inner.write();
        db.count_incoming_edges(node_id).map_err(graph_error_to_py)
    }

    fn query(&self) -> PyResult<PyQueryBuilder> {
        Ok(PyQueryBuilder {
            db: self.inner.clone(),
            start_spec: None,
            edge_types: Vec::new(),
            direction: None,
            depth: None,
            limit_val: None,
        })
    }
}

#[pyclass(module = "sombra", name = "SombraTransaction", unsendable)]
pub struct PySombraTransaction {
    db: Arc<RwLock<GraphDB>>,
    tx_id: TxId,
    committed: bool,
}

#[pymethods]
impl PySombraTransaction {
    fn id(&self) -> u64 {
        self.tx_id
    }

    fn add_node(
        &self,
        labels: Vec<String>,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = extract_properties(properties)?;
        let node_id = {
            let mut db = self.db.write();
            let mut node = Node::new(0);
            node.labels = labels;
            node.properties = props;
            db.add_node_internal(node).map_err(graph_error_to_py)?
        };
        Ok(node_id)
    }

    fn add_edge(
        &self,
        source_node_id: u64,
        target_node_id: u64,
        label: String,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = extract_properties(properties)?;
        let edge_id = {
            let mut db = self.db.write();
            let mut edge = Edge::new(0, source_node_id, target_node_id, &label);
            edge.properties = props;
            db.add_edge_internal(edge).map_err(graph_error_to_py)?
        };
        Ok(edge_id)
    }

    fn get_edge(&self, py: Python<'_>, edge_id: u64) -> PyResult<PySombraEdge> {
        let edge = {
            let mut db = self.db.write();
            db.load_edge(edge_id).map_err(graph_error_to_py)?
        };
        PySombraEdge::from_edge(py, edge)
    }

    fn get_outgoing_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.db.write();
        let node = db
            .get_node(node_id)
            .map_err(graph_error_to_py)?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?;
        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;
        while edge_id != NULL_EDGE_ID {
            edges.push(edge_id);
            let edge = db.load_edge(edge_id).map_err(graph_error_to_py)?;
            edge_id = edge.next_outgoing_edge_id;
        }
        Ok(edges)
    }

    fn get_incoming_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.db.write();
        let node = db
            .get_node(node_id)
            .map_err(graph_error_to_py)?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?;
        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;
        while edge_id != NULL_EDGE_ID {
            edges.push(edge_id);
            let edge = db.load_edge(edge_id).map_err(graph_error_to_py)?;
            edge_id = edge.next_incoming_edge_id;
        }
        Ok(edges)
    }

    fn get_node(&self, py: Python<'_>, node_id: u64) -> PyResult<PySombraNode> {
        let node = {
            let mut db = self.db.write();
            db.get_node(node_id)
                .map_err(graph_error_to_py)?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Node not found"))?
        };
        PySombraNode::from_node(py, node)
    }

    fn get_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.write();
            db.get_neighbors(node_id).map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn delete_node(&self, node_id: u64) -> PyResult<()> {
        let mut db = self.db.write();
        db.delete_node_internal(node_id).map_err(graph_error_to_py)
    }

    fn delete_edge(&self, edge_id: u64) -> PyResult<()> {
        let mut db = self.db.write();
        db.delete_edge_internal(edge_id).map_err(graph_error_to_py)
    }

    fn set_node_property(
        &self,
        _py: Python<'_>,
        node_id: u64,
        key: String,
        value: Bound<PyAny>,
    ) -> PyResult<()> {
        let prop_value = py_any_to_property_value(&value)?;
        let mut db = self.db.write();
        db.set_node_property_internal(node_id, key, prop_value)
            .map_err(graph_error_to_py)
    }

    fn remove_node_property(&self, node_id: u64, key: String) -> PyResult<()> {
        let mut db = self.db.write();
        db.remove_node_property_internal(node_id, &key)
            .map_err(graph_error_to_py)
    }

    fn commit(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyValueError::new_err(
                "Transaction already committed or rolled back",
            ));
        }

        let mut db = self.db.write();
        let dirty_pages = db.take_recent_dirty_pages();
        db.header.last_committed_tx_id = self.tx_id;
        db.write_header().map_err(graph_error_to_py)?;

        let header_dirty = db.take_recent_dirty_pages();
        let mut all_dirty: Vec<_> = dirty_pages.into_iter().chain(header_dirty).collect();
        all_dirty.sort_unstable();
        all_dirty.dedup();

        db.commit_to_wal(self.tx_id, &all_dirty)
            .map_err(graph_error_to_py)?;

        db.stop_tracking();
        db.exit_transaction();

        self.committed = true;
        Ok(())
    }

    fn rollback(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyValueError::new_err(
                "Transaction already committed or rolled back",
            ));
        }

        let mut db = self.db.write();
        let dirty_pages = db.take_recent_dirty_pages();
        db.rollback_transaction(&dirty_pages)
            .map_err(graph_error_to_py)?;
        db.stop_tracking();
        db.exit_transaction();

        self.committed = true;
        Ok(())
    }

    fn get_incoming_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.write();
            db.get_incoming_neighbors(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_two_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.write();
            db.get_neighbors_two_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_three_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.write();
            db.get_neighbors_three_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn bfs_traversal(&self, start_node_id: u64, max_depth: usize) -> PyResult<Vec<PyBfsResult>> {
        let results = {
            let mut db = self.db.write();
            db.bfs_traversal(start_node_id, max_depth)
                .map_err(graph_error_to_py)?
        };
        Ok(results
            .into_iter()
            .map(|(node_id, depth)| PyBfsResult::new(node_id, depth))
            .collect())
    }

    fn get_nodes_by_label(&self, label: &str) -> PyResult<Vec<u64>> {
        let node_ids = {
            let mut db = self.db.write();
            db.get_nodes_by_label(label).map_err(graph_error_to_py)?
        };
        Ok(node_ids)
    }

    fn get_nodes_in_range(&self, start: u64, end: u64) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_nodes_in_range(start, end))
    }

    fn get_nodes_from(&self, start: u64) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_nodes_from(start))
    }

    fn get_nodes_to(&self, end: u64) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_nodes_to(end))
    }

    fn get_first_node(&self) -> PyResult<Option<u64>> {
        let db = self.db.read();
        Ok(db.get_first_node())
    }

    fn get_last_node(&self) -> PyResult<Option<u64>> {
        let db = self.db.read();
        Ok(db.get_last_node())
    }

    fn get_first_n_nodes(&self, n: usize) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_first_n_nodes(n))
    }

    fn get_last_n_nodes(&self, n: usize) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_last_n_nodes(n))
    }

    fn get_all_node_ids_ordered(&self) -> PyResult<Vec<u64>> {
        let db = self.db.read();
        Ok(db.get_all_node_ids_ordered())
    }

    fn count_outgoing_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.db.write();
        db.count_outgoing_edges(node_id).map_err(graph_error_to_py)
    }

    fn count_incoming_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.db.write();
        db.count_incoming_edges(node_id).map_err(graph_error_to_py)
    }

    fn query(&self) -> PyResult<PyQueryBuilder> {
        Ok(PyQueryBuilder {
            db: self.db.clone(),
            start_spec: None,
            edge_types: Vec::new(),
            direction: None,
            depth: None,
            limit_val: None,
        })
    }
}

impl PySombraTransaction {
    fn new(db: Arc<RwLock<GraphDB>>, tx_id: TxId) -> Self {
        Self {
            db,
            tx_id,
            committed: false,
        }
    }
}

#[pyclass(module = "sombra", name = "QueryResult")]
pub struct PyQueryResult {
    #[pyo3(get)]
    pub start_nodes: Vec<u64>,
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub limited: bool,
    nodes: Vec<::sombra::model::Node>,
    edges: Vec<::sombra::model::Edge>,
}

#[pymethods]
impl PyQueryResult {
    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<Vec<PySombraNode>> {
        self.nodes
            .iter()
            .map(|node| PySombraNode::from_node(py, node.clone()))
            .collect()
    }

    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<Vec<PySombraEdge>> {
        self.edges
            .iter()
            .map(|edge| PySombraEdge::from_edge(py, edge.clone()))
            .collect()
    }
}

impl PyQueryResult {
    fn from_result(result: ::sombra::db::query::builder::QueryResult) -> Self {
        Self {
            start_nodes: result.start_nodes,
            node_ids: result.node_ids,
            limited: result.limited,
            nodes: result.nodes,
            edges: result.edges,
        }
    }
}

enum StartSpec {
    FromNodes(Vec<u64>),
    FromLabel(String),
    FromProperty(String, String, PropertyValue),
}

#[pyclass(module = "sombra", name = "QueryBuilder")]
pub struct PyQueryBuilder {
    db: Arc<RwLock<GraphDB>>,
    start_spec: Option<StartSpec>,
    edge_types: Vec<String>,
    direction: Option<String>,
    depth: Option<usize>,
    limit_val: Option<usize>,
}

#[pymethods]
impl PyQueryBuilder {
    fn start_from<'py>(slf: PyRefMut<'py, Self>, node_ids: Vec<u64>) -> PyRefMut<'py, Self> {
        let mut builder = slf;
        builder.start_spec = Some(StartSpec::FromNodes(node_ids));
        builder
    }

    fn start_from_label<'py>(slf: PyRefMut<'py, Self>, label: String) -> PyRefMut<'py, Self> {
        let mut builder = slf;
        builder.start_spec = Some(StartSpec::FromLabel(label));
        builder
    }

    fn start_from_property<'py>(
        slf: PyRefMut<'py, Self>,
        label: String,
        key: String,
        value: Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let mut builder = slf;
        let prop_value = py_any_to_property_value(&value)?;
        builder.start_spec = Some(StartSpec::FromProperty(label, key, prop_value));
        Ok(builder)
    }

    fn traverse<'py>(
        slf: PyRefMut<'py, Self>,
        edge_types: Vec<String>,
        direction: String,
        depth: usize,
    ) -> PyRefMut<'py, Self> {
        let mut builder = slf;
        builder.edge_types = edge_types;
        builder.direction = Some(direction);
        builder.depth = Some(depth);
        builder
    }

    fn limit<'py>(slf: PyRefMut<'py, Self>, n: usize) -> PyRefMut<'py, Self> {
        let mut builder = slf;
        builder.limit_val = Some(n);
        builder
    }

    fn get_ids(&self) -> PyResult<PyQueryResult> {
        let mut db = self.db.write();
        let mut builder = db.query();

        match &self.start_spec {
            Some(StartSpec::FromNodes(ids)) => {
                builder = builder.start_from(ids.clone());
            }
            Some(StartSpec::FromLabel(label)) => {
                builder = builder.start_from_label(label);
            }
            Some(StartSpec::FromProperty(label, key, value)) => {
                builder = builder.start_from_property(label, key, value.clone());
            }
            None => {
                return Err(PyValueError::new_err("No start specification provided"));
            }
        }

        if let (Some(depth), Some(direction)) = (self.depth, &self.direction) {
            let edge_type_refs: Vec<&str> = self.edge_types.iter().map(|s| s.as_str()).collect();
            let dir = match direction.as_str() {
                "incoming" => ::sombra::model::EdgeDirection::Incoming,
                "outgoing" => ::sombra::model::EdgeDirection::Outgoing,
                "both" => ::sombra::model::EdgeDirection::Both,
                _ => ::sombra::model::EdgeDirection::Outgoing,
            };
            builder = builder.traverse(&edge_type_refs, dir, depth);
        }

        if let Some(limit) = self.limit_val {
            builder = builder.limit(limit);
        }

        let result = builder.get_ids().map_err(graph_error_to_py)?;
        Ok(PyQueryResult::from_result(result))
    }

    fn get_nodes(&self, py: Python<'_>) -> PyResult<Vec<PySombraNode>> {
        let mut db = self.db.write();
        let mut builder = db.query();

        match &self.start_spec {
            Some(StartSpec::FromNodes(ids)) => {
                builder = builder.start_from(ids.clone());
            }
            Some(StartSpec::FromLabel(label)) => {
                builder = builder.start_from_label(label);
            }
            Some(StartSpec::FromProperty(label, key, value)) => {
                builder = builder.start_from_property(label, key, value.clone());
            }
            None => {
                return Err(PyValueError::new_err("No start specification provided"));
            }
        }

        if let (Some(depth), Some(direction)) = (self.depth, &self.direction) {
            let edge_type_refs: Vec<&str> = self.edge_types.iter().map(|s| s.as_str()).collect();
            let dir = match direction.as_str() {
                "incoming" => ::sombra::model::EdgeDirection::Incoming,
                "outgoing" => ::sombra::model::EdgeDirection::Outgoing,
                "both" => ::sombra::model::EdgeDirection::Both,
                _ => ::sombra::model::EdgeDirection::Outgoing,
            };
            builder = builder.traverse(&edge_type_refs, dir, depth);
        }

        if let Some(limit) = self.limit_val {
            builder = builder.limit(limit);
        }

        let nodes = builder.get_nodes().map_err(graph_error_to_py)?;
        nodes
            .into_iter()
            .map(|node| PySombraNode::from_node(py, node))
            .collect()
    }

    fn execute(&self) -> PyResult<PyQueryResult> {
        self.get_ids()
    }
}

#[pymodule]
fn sombra(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PySombraDB>()?;
    m.add_class::<PySombraTransaction>()?;
    m.add_class::<PySombraNode>()?;
    m.add_class::<PySombraEdge>()?;
    m.add_class::<PyBfsResult>()?;
    m.add_class::<PyQueryResult>()?;
    m.add_class::<PyQueryBuilder>()?;
    Ok(())
}

fn extract_properties(
    properties: Option<&Bound<'_, PyDict>>,
) -> PyResult<BTreeMap<String, PropertyValue>> {
    let mut map = BTreeMap::new();

    if let Some(dict) = properties {
        for (key, value) in dict.iter() {
            let key: String = key
                .extract()
                .map_err(|_| PyTypeError::new_err("Property names must be strings"))?;
            let property = py_any_to_property_value(&value)?;
            map.insert(key, property);
        }
    }

    Ok(map)
}

fn py_any_to_property_value(value: &Bound<'_, PyAny>) -> PyResult<PropertyValue> {
    if value.is_instance_of::<pyo3::types::PyBool>() {
        let bool_val = value.extract::<bool>()?;
        return Ok(PropertyValue::Bool(bool_val));
    }

    if let Ok(py_bytes) = value.cast::<PyBytes>() {
        return Ok(PropertyValue::Bytes(py_bytes.as_bytes().to_vec()));
    }

    if let Ok(py_byte_array) = value.cast::<PyByteArray>() {
        return Ok(PropertyValue::Bytes(
            unsafe { py_byte_array.as_bytes() }.to_vec(),
        ));
    }

    if let Ok(string_val) = value.extract::<String>() {
        return Ok(PropertyValue::String(string_val));
    }

    if let Ok(int_val) = value.extract::<i64>() {
        return Ok(PropertyValue::Int(int_val));
    }

    if let Ok(float_val) = value.extract::<f64>() {
        return Ok(PropertyValue::Float(float_val));
    }

    Err(PyTypeError::new_err(
        "Unsupported property type. Use bool, int, float, str, bytes, or bytearray.",
    ))
}

fn property_value_to_py(py: Python<'_>, value: &PropertyValue) -> PyResult<Py<PyAny>> {
    let any = match value {
        PropertyValue::Bool(b) => b.into_pyobject(py)?.as_any().clone().unbind(),
        PropertyValue::Int(i) => i.into_pyobject(py)?.as_any().clone().unbind(),
        PropertyValue::Float(f) => f.into_pyobject(py)?.as_any().clone().unbind(),
        PropertyValue::String(s) => s.into_pyobject(py)?.as_any().clone().unbind(),
        PropertyValue::Bytes(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
    };
    Ok(any)
}

fn graph_error_to_py(err: GraphError) -> PyErr {
    match err {
        GraphError::Io(io_err) => PyIOError::new_err(io_err.to_string()),
        GraphError::Serialization(msg) => {
            PyRuntimeError::new_err(format!("serialization error: {msg}"))
        }
        GraphError::Corruption(msg) => {
            PyRuntimeError::new_err(format!("corruption detected: {msg}"))
        }
        GraphError::NotFound(entity) => PyValueError::new_err(format!("{entity} not found")),
        GraphError::InvalidArgument(msg) => PyValueError::new_err(msg),
        GraphError::UnsupportedFeature(feature) => {
            PyRuntimeError::new_err(format!("unsupported feature: {feature}"))
        }
    }
}
