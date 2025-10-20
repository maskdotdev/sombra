use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::db::{GraphDB, TxId};
use crate::model::{Edge, Node, PropertyValue};

#[napi(js_name = "SombraDB")]
pub struct SombraDB {
    inner: Arc<RwLock<GraphDB>>,
}

#[napi]
impl SombraDB {
    #[napi(constructor)]
    pub fn new(path: String) -> std::result::Result<Self, Error> {
        let db = GraphDB::open(&path).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to open database: {e}"),
            )
        })?;

        Ok(Self {
            inner: Arc::new(RwLock::new(db)),
        })
    }

    #[napi]
    pub fn begin_transaction(&mut self) -> std::result::Result<SombraTransaction, Error> {
        let mut db = self.inner.write();

        let tx_id = db.allocate_tx_id().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to allocate transaction ID: {e}"),
            )
        })?;

        db.enter_transaction(tx_id).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to enter transaction: {e}"),
            )
        })?;

        db.start_tracking();

        Ok(SombraTransaction {
            db: self.inner.clone(),
            tx_id,
            committed: false,
        })
    }

    #[napi]
    pub fn add_node(
        &mut self,
        labels: Vec<String>,
        properties: Option<HashMap<String, SombraPropertyValue>>,
    ) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();

        let mut node = Node::new(0);
        node.labels = labels;

        if let Some(props) = properties {
            for (key, value) in props {
                let prop_value = PropertyValue::try_from(value)?;
                node.properties.insert(key, prop_value);
            }
        }

        let node_id = db
            .add_node(node)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to add node: {e}")))?;

        Ok(node_id as f64)
    }

    #[napi]
    pub fn add_edge(
        &mut self,
        source_node_id: f64,
        target_node_id: f64,
        label: String,
        properties: Option<HashMap<String, SombraPropertyValue>>,
    ) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();

        let mut edge = Edge::new(0, source_node_id as u64, target_node_id as u64, &label);

        if let Some(props) = properties {
            for (key, value) in props {
                let prop_value = PropertyValue::try_from(value)?;
                edge.properties.insert(key, prop_value);
            }
        }

        let edge_id = db
            .add_edge(edge)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to add edge: {e}")))?;

        Ok(edge_id as f64)
    }

    #[napi]
    pub fn get_edge(&mut self, edge_id: f64) -> std::result::Result<SombraEdge, Error> {
        let mut db = self.inner.write();

        let edge = db
            .load_edge(edge_id as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get edge: {e}")))?;

        Ok(SombraEdge::from(edge))
    }

    #[napi]
    pub fn get_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get node: {e}")))?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;

        while edge_id != crate::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(Status::GenericFailure, format!("Failed to load edge: {e}"))
            })?;
            edge_id = edge.next_outgoing_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_incoming_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get node: {e}")))?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;

        while edge_id != crate::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(Status::GenericFailure, format!("Failed to load edge: {e}"))
            })?;
            edge_id = edge.next_incoming_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_node(&mut self, node_id: f64) -> std::result::Result<SombraNode, Error> {
        let mut db = self.inner.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get node: {e}")))?;

        Ok(SombraNode::from(node))
    }

    #[napi]
    pub fn get_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get neighbors: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn delete_node(&mut self, node_id: f64) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.delete_node(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to delete node: {e}"),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn delete_edge(&mut self, edge_id: f64) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.delete_edge(edge_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to delete edge: {e}"),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn flush(&mut self) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.flush()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to flush: {e}")))?;

        Ok(())
    }

    #[napi]
    pub fn checkpoint(&mut self) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.checkpoint().map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to checkpoint: {e}"))
        })?;

        Ok(())
    }

    #[napi]
    pub fn get_incoming_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_incoming_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get incoming neighbors: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_neighbors_two_hops(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_neighbors_two_hops(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get two-hop neighbors: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_neighbors_three_hops(
        &mut self,
        node_id: f64,
    ) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_neighbors_three_hops(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get three-hop neighbors: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn bfs_traversal(
        &mut self,
        start_node_id: f64,
        max_depth: f64,
    ) -> std::result::Result<Vec<BfsResult>, Error> {
        let mut db = self.inner.write();

        let results = db
            .bfs_traversal(start_node_id as u64, max_depth as usize)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to perform BFS traversal: {e}"),
                )
            })?;

        Ok(results
            .into_iter()
            .map(|(node_id, depth)| BfsResult {
                node_id: node_id as f64,
                depth: depth as f64,
            })
            .collect())
    }

    #[napi]
    pub fn get_nodes_by_label(&mut self, label: String) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let node_ids = db.get_nodes_by_label(&label).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get nodes by label: {e}"),
            )
        })?;

        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn count_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();

        let count = db.count_outgoing_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count outgoing edges: {e}"),
            )
        })?;

        Ok(count as f64)
    }

    #[napi]
    pub fn count_incoming_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();

        let count = db.count_incoming_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count incoming edges: {e}"),
            )
        })?;

        Ok(count as f64)
    }
}

#[napi(js_name = "SombraTransaction")]
pub struct SombraTransaction {
    db: Arc<RwLock<GraphDB>>,
    tx_id: TxId,
    committed: bool,
}

#[napi]
impl SombraTransaction {
    #[napi]
    pub fn id(&self) -> f64 {
        self.tx_id as f64
    }

    #[napi]
    pub fn add_node(
        &mut self,
        labels: Vec<String>,
        properties: Option<HashMap<String, SombraPropertyValue>>,
    ) -> std::result::Result<f64, Error> {
        let mut db = self.db.write();

        let mut node = Node::new(0);
        node.labels = labels;

        if let Some(props) = properties {
            for (key, value) in props {
                let prop_value = PropertyValue::try_from(value)?;
                node.properties.insert(key, prop_value);
            }
        }

        let node_id = db.add_node_internal(node).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to add node in transaction: {e}"),
            )
        })?;

        Ok(node_id as f64)
    }

    #[napi]
    pub fn add_edge(
        &mut self,
        source_node_id: f64,
        target_node_id: f64,
        label: String,
        properties: Option<HashMap<String, SombraPropertyValue>>,
    ) -> std::result::Result<f64, Error> {
        let mut db = self.db.write();

        let mut edge = Edge::new(0, source_node_id as u64, target_node_id as u64, &label);

        if let Some(props) = properties {
            for (key, value) in props {
                let prop_value = PropertyValue::try_from(value)?;
                edge.properties.insert(key, prop_value);
            }
        }

        let edge_id = db.add_edge_internal(edge).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to add edge in transaction: {e}"),
            )
        })?;

        Ok(edge_id as f64)
    }

    #[napi]
    pub fn get_edge(&mut self, edge_id: f64) -> std::result::Result<SombraEdge, Error> {
        let mut db = self.db.write();

        let edge = db.load_edge(edge_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get edge in transaction: {e}"),
            )
        })?;

        Ok(SombraEdge::from(edge))
    }

    #[napi]
    pub fn get_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let node = db.get_node(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get node in transaction: {e}"),
            )
        })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;

        while edge_id != crate::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge in transaction: {e}"),
                )
            })?;
            edge_id = edge.next_outgoing_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_incoming_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let node = db.get_node(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get node in transaction: {e}"),
            )
        })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;

        while edge_id != crate::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge in transaction: {e}"),
                )
            })?;
            edge_id = edge.next_incoming_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_node(&mut self, node_id: f64) -> std::result::Result<SombraNode, Error> {
        let mut db = self.db.write();

        let node = db.get_node(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get node in transaction: {e}"),
            )
        })?;

        Ok(SombraNode::from(node))
    }

    #[napi]
    pub fn get_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let neighbors = db.get_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get neighbors in transaction: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn delete_node(&mut self, node_id: f64) -> std::result::Result<(), Error> {
        let mut db = self.db.write();

        db.delete_node_internal(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to delete node in transaction: {e}"),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn delete_edge(&mut self, edge_id: f64) -> std::result::Result<(), Error> {
        let mut db = self.db.write();

        db.delete_edge_internal(edge_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to delete edge in transaction: {e}"),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn commit(&mut self) -> std::result::Result<(), Error> {
        if self.committed {
            return Err(Error::new(
                Status::GenericFailure,
                "Transaction already committed or rolled back",
            ));
        }

        let mut db = self.db.write();

        let dirty_pages = db.take_recent_dirty_pages();

        db.header.last_committed_tx_id = self.tx_id;
        db.write_header().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to write header: {e}"),
            )
        })?;

        let header_dirty = db.take_recent_dirty_pages();
        let mut all_dirty: Vec<_> = dirty_pages.into_iter().chain(header_dirty).collect();
        all_dirty.sort_unstable();
        all_dirty.dedup();

        db.commit_to_wal(self.tx_id, &all_dirty).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to commit to WAL: {e}"),
            )
        })?;

        db.stop_tracking();
        db.exit_transaction();

        self.committed = true;
        Ok(())
    }

    #[napi]
    pub fn rollback(&mut self) -> std::result::Result<(), Error> {
        if self.committed {
            return Err(Error::new(
                Status::GenericFailure,
                "Transaction already committed or rolled back",
            ));
        }

        let mut db = self.db.write();

        let dirty_pages = db.take_recent_dirty_pages();

        db.rollback_transaction(&dirty_pages)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to rollback: {e}")))?;

        db.stop_tracking();
        db.exit_transaction();

        self.committed = true;
        Ok(())
    }

    #[napi]
    pub fn get_incoming_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let neighbors = db.get_incoming_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get incoming neighbors in transaction: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_neighbors_two_hops(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let neighbors = db.get_neighbors_two_hops(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get two-hop neighbors in transaction: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_neighbors_three_hops(
        &mut self,
        node_id: f64,
    ) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let neighbors = db.get_neighbors_three_hops(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get three-hop neighbors in transaction: {e}"),
            )
        })?;

        Ok(neighbors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn bfs_traversal(
        &mut self,
        start_node_id: f64,
        max_depth: f64,
    ) -> std::result::Result<Vec<BfsResult>, Error> {
        let mut db = self.db.write();

        let results = db
            .bfs_traversal(start_node_id as u64, max_depth as usize)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to perform BFS traversal in transaction: {e}"),
                )
            })?;

        Ok(results
            .into_iter()
            .map(|(node_id, depth)| BfsResult {
                node_id: node_id as f64,
                depth: depth as f64,
            })
            .collect())
    }

    #[napi]
    pub fn get_nodes_by_label(&mut self, label: String) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let node_ids = db.get_nodes_by_label(&label).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get nodes by label in transaction: {e}"),
            )
        })?;

        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn count_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.db.write();

        let count = db.count_outgoing_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count outgoing edges in transaction: {e}"),
            )
        })?;

        Ok(count as f64)
    }

    #[napi]
    pub fn count_incoming_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.db.write();

        let count = db.count_incoming_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count incoming edges in transaction: {e}"),
            )
        })?;

        Ok(count as f64)
    }
}

#[napi(object, js_name = "SombraPropertyValue")]
pub struct SombraPropertyValue {
    pub r#type: String,
    pub value: serde_json::Value,
}

impl From<PropertyValue> for SombraPropertyValue {
    fn from(value: PropertyValue) -> Self {
        match value {
            PropertyValue::String(s) => SombraPropertyValue {
                r#type: "string".to_string(),
                value: serde_json::Value::String(s),
            },
            PropertyValue::Int(i) => SombraPropertyValue {
                r#type: "int".to_string(),
                value: serde_json::Value::Number(i.into()),
            },
            PropertyValue::Float(f) => SombraPropertyValue {
                r#type: "float".to_string(),
                value: serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            },
            PropertyValue::Bool(b) => SombraPropertyValue {
                r#type: "bool".to_string(),
                value: serde_json::Value::Bool(b),
            },
            PropertyValue::Bytes(bytes) => SombraPropertyValue {
                r#type: "bytes".to_string(),
                value: serde_json::Value::Array(
                    bytes
                        .into_iter()
                        .map(|b| serde_json::Value::Number(b.into()))
                        .collect(),
                ),
            },
        }
    }
}

impl TryFrom<SombraPropertyValue> for PropertyValue {
    type Error = Error;

    fn try_from(js_value: SombraPropertyValue) -> std::result::Result<Self, Self::Error> {
        match js_value.r#type.as_str() {
            "string" => {
                if let serde_json::Value::String(s) = js_value.value {
                    Ok(PropertyValue::String(s))
                } else {
                    Err(Error::new(Status::InvalidArg, "Invalid string value"))
                }
            }
            "int" => {
                if let serde_json::Value::Number(n) = js_value.value {
                    n.as_i64()
                        .map(PropertyValue::Int)
                        .ok_or_else(|| Error::new(Status::InvalidArg, "Invalid int value"))
                } else {
                    Err(Error::new(Status::InvalidArg, "Invalid int value"))
                }
            }
            "float" => {
                if let serde_json::Value::Number(n) = js_value.value {
                    n.as_f64()
                        .map(PropertyValue::Float)
                        .ok_or_else(|| Error::new(Status::InvalidArg, "Invalid float value"))
                } else {
                    Err(Error::new(Status::InvalidArg, "Invalid float value"))
                }
            }
            "bool" => {
                if let serde_json::Value::Bool(b) = js_value.value {
                    Ok(PropertyValue::Bool(b))
                } else {
                    Err(Error::new(Status::InvalidArg, "Invalid bool value"))
                }
            }
            "bytes" => {
                if let serde_json::Value::Array(arr) = js_value.value {
                    let bytes: std::result::Result<Vec<u8>, _> = arr
                        .into_iter()
                        .map(|v| {
                            v.as_u64()
                                .and_then(|n| u8::try_from(n).ok())
                                .ok_or_else(|| Error::new(Status::InvalidArg, "Invalid byte value"))
                        })
                        .collect();
                    bytes.map(PropertyValue::Bytes)
                } else {
                    Err(Error::new(Status::InvalidArg, "Invalid bytes value"))
                }
            }
            _ => Err(Error::new(
                Status::InvalidArg,
                format!("Unknown property type: {}", js_value.r#type),
            )),
        }
    }
}

#[napi(object, js_name = "SombraNode")]
pub struct SombraNode {
    pub id: f64,
    pub labels: Vec<String>,
    pub properties: HashMap<String, SombraPropertyValue>,
}

impl From<Node> for SombraNode {
    fn from(node: Node) -> Self {
        let properties = node
            .properties
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();

        Self {
            id: node.id as f64,
            labels: node.labels,
            properties,
        }
    }
}

#[napi(object, js_name = "SombraEdge")]
pub struct SombraEdge {
    pub id: f64,
    pub source_node_id: f64,
    pub target_node_id: f64,
    pub type_name: String,
    pub properties: HashMap<String, SombraPropertyValue>,
}

#[napi(object, js_name = "BfsResult")]
pub struct BfsResult {
    pub node_id: f64,
    pub depth: f64,
}

impl From<Edge> for SombraEdge {
    fn from(edge: Edge) -> Self {
        let properties = edge
            .properties
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();

        Self {
            id: edge.id as f64,
            source_node_id: edge.source_node_id as f64,
            target_node_id: edge.target_node_id as f64,
            type_name: edge.type_name,
            properties,
        }
    }
}
