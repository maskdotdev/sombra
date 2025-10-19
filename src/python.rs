use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyModule};
use pyo3::Bound;

use crate::db::{GraphDB, TxId};
use crate::error::GraphError;
use crate::model::{Edge, Node, PropertyValue, NULL_EDGE_ID};

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
    fn from_node(py: Python<'_>, node: Node) -> Self {
        let properties = node
            .properties
            .into_iter()
            .map(|(key, value)| (key, property_value_to_py(py, &value)))
            .collect();

        Self {
            id: node.id,
            labels: node.labels,
            properties,
        }
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
    fn from_edge(py: Python<'_>, edge: Edge) -> Self {
        let properties = edge
            .properties
            .into_iter()
            .map(|(key, value)| (key, property_value_to_py(py, &value)))
            .collect();

        Self {
            id: edge.id,
            source_node_id: edge.source_node_id,
            target_node_id: edge.target_node_id,
            type_name: edge.type_name,
            properties,
        }
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
    inner: Arc<Mutex<GraphDB>>,
}

#[pymethods]
impl PySombraDB {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let db = GraphDB::open(path).map_err(graph_error_to_py)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }

    fn begin_transaction(&self) -> PyResult<PySombraTransaction> {
        let tx_id = {
            let mut db = self.inner.lock().unwrap();
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
            let mut db = self.inner.lock().unwrap();
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
            let mut db = self.inner.lock().unwrap();
            let mut edge = Edge::new(0, source_node_id, target_node_id, &label);
            edge.properties = props;
            db.add_edge(edge).map_err(graph_error_to_py)?
        };
        Ok(edge_id)
    }

    fn get_edge(&self, py: Python<'_>, edge_id: u64) -> PyResult<PySombraEdge> {
        let edge = {
            let mut db = self.inner.lock().unwrap();
            db.load_edge(edge_id).map_err(graph_error_to_py)?
        };
        Ok(PySombraEdge::from_edge(py, edge))
    }

    fn get_outgoing_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.inner.lock().unwrap();
        let node = db.get_node(node_id).map_err(graph_error_to_py)?;
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
        let mut db = self.inner.lock().unwrap();
        let node = db.get_node(node_id).map_err(graph_error_to_py)?;
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
            let mut db = self.inner.lock().unwrap();
            db.get_node(node_id).map_err(graph_error_to_py)?
        };
        Ok(PySombraNode::from_node(py, node))
    }

    fn get_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.lock().unwrap();
            db.get_neighbors(node_id).map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn delete_node(&self, node_id: u64) -> PyResult<()> {
        let mut db = self.inner.lock().unwrap();
        db.delete_node(node_id).map_err(graph_error_to_py)
    }

    fn delete_edge(&self, edge_id: u64) -> PyResult<()> {
        let mut db = self.inner.lock().unwrap();
        db.delete_edge(edge_id).map_err(graph_error_to_py)
    }

    fn flush(&self) -> PyResult<()> {
        let mut db = self.inner.lock().unwrap();
        db.flush().map_err(graph_error_to_py)
    }

    fn checkpoint(&self) -> PyResult<()> {
        let mut db = self.inner.lock().unwrap();
        db.checkpoint().map_err(graph_error_to_py)
    }

    fn get_incoming_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.lock().unwrap();
            db.get_incoming_neighbors(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_two_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.lock().unwrap();
            db.get_neighbors_two_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_three_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.inner.lock().unwrap();
            db.get_neighbors_three_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn bfs_traversal(&self, start_node_id: u64, max_depth: usize) -> PyResult<Vec<PyBfsResult>> {
        let results = {
            let mut db = self.inner.lock().unwrap();
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
            let mut db = self.inner.lock().unwrap();
            db.get_nodes_by_label(label).map_err(graph_error_to_py)?
        };
        Ok(node_ids)
    }

    fn count_outgoing_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.inner.lock().unwrap();
        db.count_outgoing_edges(node_id).map_err(graph_error_to_py)
    }

    fn count_incoming_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.inner.lock().unwrap();
        db.count_incoming_edges(node_id).map_err(graph_error_to_py)
    }
}

#[pyclass(module = "sombra", name = "SombraTransaction", unsendable)]
pub struct PySombraTransaction {
    db: Arc<Mutex<GraphDB>>,
    tx_id: TxId,
    committed: bool,
}

#[pymethods]
impl PySombraTransaction {
    fn id(&self) -> u64 {
        self.tx_id as u64
    }

    fn add_node(
        &self,
        labels: Vec<String>,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = extract_properties(properties)?;
        let node_id = {
            let mut db = self.db.lock().unwrap();
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
            let mut db = self.db.lock().unwrap();
            let mut edge = Edge::new(0, source_node_id, target_node_id, &label);
            edge.properties = props;
            db.add_edge_internal(edge).map_err(graph_error_to_py)?
        };
        Ok(edge_id)
    }

    fn get_edge(&self, py: Python<'_>, edge_id: u64) -> PyResult<PySombraEdge> {
        let edge = {
            let mut db = self.db.lock().unwrap();
            db.load_edge(edge_id).map_err(graph_error_to_py)?
        };
        Ok(PySombraEdge::from_edge(py, edge))
    }

    fn get_outgoing_edges(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let mut db = self.db.lock().unwrap();
        let node = db.get_node(node_id).map_err(graph_error_to_py)?;
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
        let mut db = self.db.lock().unwrap();
        let node = db.get_node(node_id).map_err(graph_error_to_py)?;
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
            let mut db = self.db.lock().unwrap();
            db.get_node(node_id).map_err(graph_error_to_py)?
        };
        Ok(PySombraNode::from_node(py, node))
    }

    fn get_neighbors(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.lock().unwrap();
            db.get_neighbors(node_id).map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn delete_node(&self, node_id: u64) -> PyResult<()> {
        let mut db = self.db.lock().unwrap();
        db.delete_node_internal(node_id).map_err(graph_error_to_py)
    }

    fn delete_edge(&self, edge_id: u64) -> PyResult<()> {
        let mut db = self.db.lock().unwrap();
        db.delete_edge_internal(edge_id).map_err(graph_error_to_py)
    }

    fn commit(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyValueError::new_err(
                "Transaction already committed or rolled back",
            ));
        }

        let mut db = self.db.lock().unwrap();
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

        let mut db = self.db.lock().unwrap();
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
            let mut db = self.db.lock().unwrap();
            db.get_incoming_neighbors(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_two_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.lock().unwrap();
            db.get_neighbors_two_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn get_neighbors_three_hops(&self, node_id: u64) -> PyResult<Vec<u64>> {
        let neighbors = {
            let mut db = self.db.lock().unwrap();
            db.get_neighbors_three_hops(node_id)
                .map_err(graph_error_to_py)?
        };
        Ok(neighbors)
    }

    fn bfs_traversal(&self, start_node_id: u64, max_depth: usize) -> PyResult<Vec<PyBfsResult>> {
        let results = {
            let mut db = self.db.lock().unwrap();
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
            let mut db = self.db.lock().unwrap();
            db.get_nodes_by_label(label).map_err(graph_error_to_py)?
        };
        Ok(node_ids)
    }

    fn count_outgoing_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.db.lock().unwrap();
        db.count_outgoing_edges(node_id).map_err(graph_error_to_py)
    }

    fn count_incoming_edges(&self, node_id: u64) -> PyResult<usize> {
        let mut db = self.db.lock().unwrap();
        db.count_incoming_edges(node_id).map_err(graph_error_to_py)
    }
}

impl PySombraTransaction {
    fn new(db: Arc<Mutex<GraphDB>>, tx_id: TxId) -> Self {
        Self {
            db,
            tx_id,
            committed: false,
        }
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
        return Ok(PropertyValue::Bytes(unsafe { py_byte_array.as_bytes() }.to_vec()));
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

fn property_value_to_py(py: Python<'_>, value: &PropertyValue) -> Py<PyAny> {
    match value {
        PropertyValue::Bool(b) => {
            let bound = b.into_pyobject(py).unwrap();
            bound.as_any().clone().unbind()
        }
        PropertyValue::Int(i) => {
            let bound = i.into_pyobject(py).unwrap();
            bound.as_any().clone().unbind()
        }
        PropertyValue::Float(f) => {
            let bound = f.into_pyobject(py).unwrap();
            bound.as_any().clone().unbind()
        }
        PropertyValue::String(s) => {
            let bound = s.into_pyobject(py).unwrap();
            bound.as_any().clone().unbind()
        }
        PropertyValue::Bytes(bytes) => PyBytes::new(py, bytes).into_any().unbind(),
    }
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
