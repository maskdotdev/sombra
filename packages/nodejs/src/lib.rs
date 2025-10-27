#![allow(clippy::uninlined_format_args)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use sombra::db::query::analytics::{DegreeDistribution, DegreeType};
use sombra::db::query::builder::QueryResult;
use sombra::db::query::pattern::{
    EdgePattern, Match, NodePattern, Pattern, PropertyBound, PropertyFilters, PropertyRangeFilter,
};
use sombra::db::query::subgraph::{EdgeTypeFilter, Subgraph};
use sombra::db::{GraphDB, TxId};
use sombra::model::{Edge, EdgeDirection, Node, PropertyValue};

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
                format!("Failed to allocate transaction ID: {}", e),
            )
        })?;

        db.enter_transaction(tx_id).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to enter transaction: {}", e),
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

        let node_id = db.add_node(node).map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to add node: {}", e))
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
        let mut db = self.inner.write();

        let mut edge = Edge::new(0, source_node_id as u64, target_node_id as u64, &label);

        if let Some(props) = properties {
            for (key, value) in props {
                let prop_value = PropertyValue::try_from(value)?;
                edge.properties.insert(key, prop_value);
            }
        }

        let edge_id = db.add_edge(edge).map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to add edge: {}", e))
        })?;

        Ok(edge_id as f64)
    }

    #[napi]
    pub fn get_edge(&mut self, edge_id: f64) -> std::result::Result<SombraEdge, Error> {
        let mut db = self.inner.write();

        let edge = db.load_edge(edge_id as u64).map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to get edge: {}", e))
        })?;

        Ok(SombraEdge::from(edge))
    }

    #[napi]
    pub fn get_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get node: {}", e)))?
            .ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    format!("Node {} not found", node_id),
                )
            })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;

        while edge_id != sombra::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge: {}", e),
                )
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
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to get node: {}", e)))?
            .ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    format!("Node {} not found", node_id),
                )
            })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;

        while edge_id != sombra::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge: {}", e),
                )
            })?;
            edge_id = edge.next_incoming_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_node(&mut self, node_id: f64) -> std::result::Result<Option<SombraNode>, Error> {
        let mut db = self.inner.write();

        let node = db.get_node(node_id as u64).map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to get node: {}", e))
        })?;

        Ok(node.map(SombraNode::from))
    }

    #[napi]
    pub fn get_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get neighbors: {}", e),
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
                format!("Failed to delete node: {}", e),
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
                format!("Failed to delete edge: {}", e),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn set_node_property(
        &mut self,
        node_id: f64,
        key: String,
        value: SombraPropertyValue,
    ) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        let prop_value = PropertyValue::try_from(value)?;

        db.set_node_property(node_id as u64, key, prop_value)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to set node property: {}", e),
                )
            })?;

        Ok(())
    }

    #[napi]
    pub fn remove_node_property(
        &mut self,
        node_id: f64,
        key: String,
    ) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.remove_node_property(node_id as u64, &key).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to remove node property: {}", e),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn flush(&mut self) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.flush()
            .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to flush: {}", e)))?;

        Ok(())
    }

    #[napi]
    pub fn checkpoint(&mut self) -> std::result::Result<(), Error> {
        let mut db = self.inner.write();

        db.checkpoint().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to checkpoint: {}", e),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn get_incoming_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();

        let neighbors = db.get_incoming_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get incoming neighbors: {}", e),
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
                format!("Failed to get two-hop neighbors: {}", e),
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
                format!("Failed to get three-hop neighbors: {}", e),
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
                    format!("Failed to perform BFS traversal: {}", e),
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
                format!("Failed to get nodes by label: {}", e),
            )
        })?;

        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_in_range(
        &mut self,
        start: f64,
        end: f64,
    ) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_nodes_in_range(start as u64, end as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_from(&mut self, start: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_nodes_from(start as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_to(&mut self, end: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_nodes_to(end as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_first_node(&mut self) -> std::result::Result<Option<f64>, Error> {
        let db = self.inner.read();
        Ok(db.get_first_node().map(|id| id as f64))
    }

    #[napi]
    pub fn get_last_node(&mut self) -> std::result::Result<Option<f64>, Error> {
        let db = self.inner.read();
        Ok(db.get_last_node().map(|id| id as f64))
    }

    #[napi]
    pub fn get_first_n_nodes(&mut self, n: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_first_n_nodes(n as usize);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_last_n_nodes(&mut self, n: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_last_n_nodes(n as usize);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_all_node_ids_ordered(&mut self) -> std::result::Result<Vec<f64>, Error> {
        let db = self.inner.read();
        let node_ids = db.get_all_node_ids_ordered();
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn count_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();

        let count = db.count_outgoing_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count outgoing edges: {}", e),
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
                format!("Failed to count incoming edges: {}", e),
            )
        })?;

        Ok(count as f64)
    }

    #[napi]
    pub fn count_nodes_by_label(&mut self) -> std::result::Result<HashMap<String, f64>, Error> {
        let db = self.inner.read();
        let counts = db.count_nodes_by_label();
        Ok(counts.into_iter().map(|(k, v)| (k, v as f64)).collect())
    }

    #[napi]
    pub fn count_edges_by_type(&mut self) -> std::result::Result<HashMap<String, f64>, Error> {
        let mut db = self.inner.write();
        let counts = db.count_edges_by_type().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count edges by type: {}", e),
            )
        })?;
        Ok(counts.into_iter().map(|(k, v)| (k, v as f64)).collect())
    }

    #[napi]
    pub fn get_total_node_count(&mut self) -> std::result::Result<f64, Error> {
        let db = self.inner.read();
        Ok(db.get_total_node_count() as f64)
    }

    #[napi]
    pub fn get_total_edge_count(&mut self) -> std::result::Result<f64, Error> {
        let db = self.inner.read();
        Ok(db.get_total_edge_count() as f64)
    }

    #[napi]
    pub fn degree_distribution(&mut self) -> std::result::Result<JsDegreeDistribution, Error> {
        let mut db = self.inner.write();
        let dist = db.degree_distribution().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get degree distribution: {}", e),
            )
        })?;
        Ok(JsDegreeDistribution::from(dist))
    }

    #[napi]
    pub fn find_hubs(
        &mut self,
        min_degree: f64,
        degree_type: String,
    ) -> std::result::Result<Vec<HubNode>, Error> {
        let dt = match degree_type.as_str() {
            "in" => DegreeType::In,
            "out" => DegreeType::Out,
            "total" => DegreeType::Total,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "degree_type must be 'in', 'out', or 'total'",
                ))
            }
        };
        let mut db = self.inner.write();
        let hubs = db.find_hubs(min_degree as usize, dt).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to find hubs: {}", e),
            )
        })?;
        Ok(hubs
            .into_iter()
            .map(|(node_id, degree)| HubNode {
                node_id: node_id as f64,
                degree: degree as f64,
            })
            .collect())
    }

    #[napi]
    pub fn find_isolated_nodes(&mut self) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();
        let nodes = db.find_isolated_nodes().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to find isolated nodes: {}", e),
            )
        })?;
        Ok(nodes.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn find_leaf_nodes(&mut self, direction: String) -> std::result::Result<Vec<f64>, Error> {
        let dir = match direction.as_str() {
            "incoming" => EdgeDirection::Incoming,
            "outgoing" => EdgeDirection::Outgoing,
            "both" => EdgeDirection::Both,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "direction must be 'incoming', 'outgoing', or 'both'",
                ))
            }
        };
        let mut db = self.inner.write();
        let nodes = db.find_leaf_nodes(dir).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to find leaf nodes: {}", e),
            )
        })?;
        Ok(nodes.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_average_degree(&mut self) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();
        db.get_average_degree().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get average degree: {}", e),
            )
        })
    }

    #[napi]
    pub fn get_density(&mut self) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();
        db.get_density().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get density: {}", e),
            )
        })
    }

    #[napi]
    pub fn count_nodes_with_label(&mut self, label: String) -> std::result::Result<f64, Error> {
        let db = self.inner.read();
        Ok(db.count_nodes_with_label(&label) as f64)
    }

    #[napi]
    pub fn count_edges_with_type(&mut self, edge_type: String) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();
        let count = db.count_edges_with_type(&edge_type).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count edges with type: {}", e),
            )
        })?;
        Ok(count as f64)
    }

    #[napi]
    pub fn extract_subgraph(
        &mut self,
        root_nodes: Vec<f64>,
        depth: f64,
        edge_types: Option<Vec<String>>,
        direction: Option<String>,
    ) -> std::result::Result<JsSubgraph, Error> {
        let edge_filter = if let Some(types) = edge_types {
            let dir = match direction.as_deref() {
                Some("incoming") => EdgeDirection::Incoming,
                Some("outgoing") => EdgeDirection::Outgoing,
                Some("both") => EdgeDirection::Both,
                None => EdgeDirection::Outgoing,
                Some(_) => {
                    return Err(Error::new(
                        Status::InvalidArg,
                        "direction must be 'incoming', 'outgoing', or 'both'",
                    ))
                }
            };
            Some(EdgeTypeFilter::new(types, dir))
        } else {
            None
        };

        let mut db = self.inner.write();
        let root_nodes_u64: Vec<u64> = root_nodes.into_iter().map(|n| n as u64).collect();
        let subgraph = db
            .extract_subgraph(&root_nodes_u64, depth as usize, edge_filter)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to extract subgraph: {}", e),
                )
            })?;
        Ok(JsSubgraph::from(subgraph))
    }

    #[napi]
    pub fn extract_induced_subgraph(
        &mut self,
        node_ids: Vec<f64>,
    ) -> std::result::Result<JsSubgraph, Error> {
        let mut db = self.inner.write();
        let node_ids_u64: Vec<u64> = node_ids.into_iter().map(|n| n as u64).collect();
        let subgraph = db.extract_induced_subgraph(&node_ids_u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to extract induced subgraph: {}", e),
            )
        })?;
        Ok(JsSubgraph::from(subgraph))
    }

    #[napi]
    pub fn query(&self) -> JsQueryBuilder {
        JsQueryBuilder {
            inner: self.inner.clone(),
            start_spec: None,
            edge_types: Vec::new(),
            direction: None,
            depth: None,
            limit_val: None,
        }
    }

    #[napi]
    pub fn find_ancestor_by_label(
        &mut self,
        start_node_id: f64,
        label: String,
        edge_type: String,
    ) -> std::result::Result<Option<f64>, Error> {
        let mut db = self.inner.write();
        let result = db
            .find_ancestor_by_label(start_node_id as u64, &label, &edge_type)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to find ancestor by label: {}", e),
                )
            })?;
        Ok(result.map(|id| id as f64))
    }

    #[napi]
    pub fn get_ancestors(
        &mut self,
        start_node_id: f64,
        edge_type: String,
        max_depth: Option<f64>,
    ) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();
        let max_depth_usize = max_depth.map(|d| d as usize);
        let ancestors = db
            .get_ancestors(start_node_id as u64, &edge_type, max_depth_usize)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to get ancestors: {}", e),
                )
            })?;
        Ok(ancestors.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_descendants(
        &mut self,
        start_node_id: f64,
        edge_type: String,
        max_depth: Option<f64>,
    ) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.inner.write();
        let max_depth_usize = max_depth.map(|d| d as usize);
        let descendants = db
            .get_descendants(start_node_id as u64, &edge_type, max_depth_usize)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to get descendants: {}", e),
                )
            })?;
        Ok(descendants.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_containing_file(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.inner.write();
        let file_id = db.get_containing_file(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get containing file: {}", e),
            )
        })?;
        Ok(file_id as f64)
    }

    #[napi]
    pub fn match_pattern(
        &mut self,
        pattern: JsPattern,
    ) -> std::result::Result<Vec<JsMatch>, Error> {
        let mut db = self.inner.write();
        let pattern = Pattern::try_from(pattern)?;
        let matches = db.match_pattern(&pattern).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to match pattern: {}", e),
            )
        })?;
        Ok(matches.into_iter().map(JsMatch::from).collect())
    }

    #[napi]
    pub fn shortest_path(
        &mut self,
        start: f64,
        end: f64,
        edge_types: Option<Vec<String>>,
    ) -> std::result::Result<Option<Vec<f64>>, Error> {
        let mut db = self.inner.write();
        let edge_type_refs: Option<Vec<&str>> = edge_types
            .as_ref()
            .map(|types| types.iter().map(|s| s.as_str()).collect());
        let path = db
            .shortest_path(start as u64, end as u64, edge_type_refs.as_deref())
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to find shortest path: {}", e),
                )
            })?;
        Ok(path.map(|p| p.into_iter().map(|id| id as f64).collect()))
    }

    #[napi]
    pub fn find_paths(
        &mut self,
        start: f64,
        end: f64,
        min_depth: f64,
        max_depth: f64,
        edge_types: Option<Vec<String>>,
    ) -> std::result::Result<Vec<Vec<f64>>, Error> {
        let mut db = self.inner.write();
        let edge_type_refs: Option<Vec<&str>> = edge_types
            .as_ref()
            .map(|types| types.iter().map(|s| s.as_str()).collect());
        let paths = db
            .find_paths(
                start as u64,
                end as u64,
                min_depth as usize,
                max_depth as usize,
                edge_type_refs.as_deref(),
            )
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to find paths: {}", e),
                )
            })?;
        Ok(paths
            .into_iter()
            .map(|path| path.into_iter().map(|id| id as f64).collect())
            .collect())
    }
}

#[napi(js_name = "QueryResult", object)]
pub struct JsQueryResult {
    pub start_nodes: Vec<f64>,
    pub node_ids: Vec<f64>,
    pub limited: bool,
}

impl From<QueryResult> for JsQueryResult {
    fn from(result: QueryResult) -> Self {
        Self {
            start_nodes: result.start_nodes.into_iter().map(|n| n as f64).collect(),
            node_ids: result.node_ids.into_iter().map(|n| n as f64).collect(),
            limited: result.limited,
        }
    }
}

#[allow(clippy::enum_variant_names)]
enum StartSpec {
    FromNodes(Vec<u64>),
    FromLabel(String),
    FromProperty(String, String, String), // label, key, value
}

#[napi(js_name = "QueryBuilder")]
pub struct JsQueryBuilder {
    inner: Arc<RwLock<GraphDB>>,
    start_spec: Option<StartSpec>,
    edge_types: Vec<String>,
    direction: Option<String>,
    depth: Option<usize>,
    limit_val: Option<usize>,
}

#[napi]
impl JsQueryBuilder {
    #[napi]
    pub fn start_from(&mut self, node_ids: Vec<f64>) -> &Self {
        self.start_spec = Some(StartSpec::FromNodes(
            node_ids.into_iter().map(|n| n as u64).collect(),
        ));
        self
    }

    #[napi]
    pub fn start_from_label(&mut self, label: String) -> &Self {
        self.start_spec = Some(StartSpec::FromLabel(label));
        self
    }

    #[napi]
    pub fn start_from_property(&mut self, label: String, key: String, value: String) -> &Self {
        self.start_spec = Some(StartSpec::FromProperty(label, key, value));
        self
    }

    #[napi]
    pub fn traverse(&mut self, edge_types: Vec<String>, direction: String, depth: f64) -> &Self {
        self.edge_types = edge_types;
        self.direction = Some(direction);
        self.depth = Some(depth as usize);
        self
    }

    #[napi]
    pub fn limit(&mut self, n: f64) -> &Self {
        self.limit_val = Some(n as usize);
        self
    }

    #[napi]
    pub fn get_ids(&self) -> std::result::Result<JsQueryResult, Error> {
        let mut db = self.inner.write();
        let mut builder = db.query();

        match &self.start_spec {
            Some(StartSpec::FromNodes(ids)) => {
                builder = builder.start_from(ids.clone());
            }
            Some(StartSpec::FromLabel(label)) => {
                builder = builder.start_from_label(label);
            }
            Some(StartSpec::FromProperty(label, key, value)) => {
                builder =
                    builder.start_from_property(label, key, PropertyValue::String(value.clone()));
            }
            None => {
                return Err(Error::new(
                    Status::GenericFailure,
                    "No start specification provided",
                ));
            }
        }

        if let (Some(depth), Some(direction)) = (self.depth, &self.direction) {
            let edge_type_refs: Vec<&str> = self.edge_types.iter().map(|s| s.as_str()).collect();
            let dir = match direction.as_str() {
                "incoming" => EdgeDirection::Incoming,
                "outgoing" => EdgeDirection::Outgoing,
                "both" => EdgeDirection::Both,
                _ => EdgeDirection::Outgoing,
            };
            builder = builder.traverse(&edge_type_refs, dir, depth);
        }

        if let Some(limit) = self.limit_val {
            builder = builder.limit(limit);
        }

        let result = builder.get_ids().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Query execution failed: {}", e),
            )
        })?;

        Ok(JsQueryResult::from(result))
    }

    #[napi]
    pub fn get_nodes(&self) -> std::result::Result<Vec<SombraNode>, Error> {
        let mut db = self.inner.write();
        let mut builder = db.query();

        match &self.start_spec {
            Some(StartSpec::FromNodes(ids)) => {
                builder = builder.start_from(ids.clone());
            }
            Some(StartSpec::FromLabel(label)) => {
                builder = builder.start_from_label(label);
            }
            Some(StartSpec::FromProperty(label, key, value)) => {
                builder =
                    builder.start_from_property(label, key, PropertyValue::String(value.clone()));
            }
            None => {
                return Err(Error::new(
                    Status::GenericFailure,
                    "No start specification provided",
                ));
            }
        }

        if let (Some(depth), Some(direction)) = (self.depth, &self.direction) {
            let edge_type_refs: Vec<&str> = self.edge_types.iter().map(|s| s.as_str()).collect();
            let dir = match direction.as_str() {
                "incoming" => EdgeDirection::Incoming,
                "outgoing" => EdgeDirection::Outgoing,
                "both" => EdgeDirection::Both,
                _ => EdgeDirection::Outgoing,
            };
            builder = builder.traverse(&edge_type_refs, dir, depth);
        }

        if let Some(limit) = self.limit_val {
            builder = builder.limit(limit);
        }

        let nodes = builder.get_nodes().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Query execution failed: {}", e),
            )
        })?;

        Ok(nodes.into_iter().map(SombraNode::from).collect())
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
                format!("Failed to add node in transaction: {}", e),
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
                format!("Failed to add edge in transaction: {}", e),
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
                format!("Failed to get edge in transaction: {}", e),
            )
        })?;

        Ok(SombraEdge::from(edge))
    }

    #[napi]
    pub fn get_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to get node in transaction: {}", e),
                )
            })?
            .ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    format!("Node {} not found", node_id),
                )
            })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;

        while edge_id != sombra::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge in transaction: {}", e),
                )
            })?;
            edge_id = edge.next_outgoing_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_incoming_edges(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let node = db
            .get_node(node_id as u64)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to get node in transaction: {}", e),
                )
            })?
            .ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    format!("Node {} not found", node_id),
                )
            })?;

        let mut edges = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;

        while edge_id != sombra::model::NULL_EDGE_ID {
            edges.push(edge_id as f64);
            let edge = db.load_edge(edge_id).map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to load edge in transaction: {}", e),
                )
            })?;
            edge_id = edge.next_incoming_edge_id;
        }

        Ok(edges)
    }

    #[napi]
    pub fn get_node(&mut self, node_id: f64) -> std::result::Result<Option<SombraNode>, Error> {
        let mut db = self.db.write();

        let node = db.get_node(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get node in transaction: {}", e),
            )
        })?;

        Ok(node.map(SombraNode::from))
    }

    #[napi]
    pub fn get_neighbors(&mut self, node_id: f64) -> std::result::Result<Vec<f64>, Error> {
        let mut db = self.db.write();

        let neighbors = db.get_neighbors(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to get neighbors in transaction: {}", e),
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
                format!("Failed to delete node in transaction: {}", e),
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
                format!("Failed to delete edge in transaction: {}", e),
            )
        })?;

        Ok(())
    }

    #[napi]
    pub fn set_node_property(
        &mut self,
        node_id: f64,
        key: String,
        value: SombraPropertyValue,
    ) -> std::result::Result<(), Error> {
        let mut db = self.db.write();

        let prop_value = PropertyValue::try_from(value)?;

        db.set_node_property_internal(node_id as u64, key, prop_value)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to set node property in transaction: {}", e),
                )
            })?;

        Ok(())
    }

    #[napi]
    pub fn remove_node_property(
        &mut self,
        node_id: f64,
        key: String,
    ) -> std::result::Result<(), Error> {
        let mut db = self.db.write();

        db.remove_node_property_internal(node_id as u64, &key)
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to remove node property in transaction: {}", e),
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
                format!("Failed to write header: {}", e),
            )
        })?;

        let header_dirty = db.take_recent_dirty_pages();
        let mut all_dirty: Vec<_> = dirty_pages.into_iter().chain(header_dirty).collect();
        all_dirty.sort_unstable();
        all_dirty.dedup();

        db.commit_to_wal(self.tx_id, &all_dirty).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to commit to WAL: {}", e),
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

        db.rollback_transaction(&dirty_pages).map_err(|e| {
            Error::new(Status::GenericFailure, format!("Failed to rollback: {}", e))
        })?;

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
                format!("Failed to get incoming neighbors in transaction: {}", e),
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
                format!("Failed to get two-hop neighbors in transaction: {}", e),
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
                format!("Failed to get three-hop neighbors in transaction: {}", e),
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
                    format!("Failed to perform BFS traversal in transaction: {}", e),
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
                format!("Failed to get nodes by label in transaction: {}", e),
            )
        })?;

        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_in_range(
        &mut self,
        start: f64,
        end: f64,
    ) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_nodes_in_range(start as u64, end as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_from(&mut self, start: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_nodes_from(start as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_nodes_to(&mut self, end: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_nodes_to(end as u64);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_first_node(&mut self) -> std::result::Result<Option<f64>, Error> {
        let db = self.db.read();
        Ok(db.get_first_node().map(|id| id as f64))
    }

    #[napi]
    pub fn get_last_node(&mut self) -> std::result::Result<Option<f64>, Error> {
        let db = self.db.read();
        Ok(db.get_last_node().map(|id| id as f64))
    }

    #[napi]
    pub fn get_first_n_nodes(&mut self, n: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_first_n_nodes(n as usize);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_last_n_nodes(&mut self, n: f64) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_last_n_nodes(n as usize);
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn get_all_node_ids_ordered(&mut self) -> std::result::Result<Vec<f64>, Error> {
        let db = self.db.read();
        let node_ids = db.get_all_node_ids_ordered();
        Ok(node_ids.into_iter().map(|id| id as f64).collect())
    }

    #[napi]
    pub fn count_outgoing_edges(&mut self, node_id: f64) -> std::result::Result<f64, Error> {
        let mut db = self.db.write();

        let count = db.count_outgoing_edges(node_id as u64).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to count outgoing edges in transaction: {}", e),
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
                format!("Failed to count incoming edges in transaction: {}", e),
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

#[napi(object, js_name = "DegreeEntry")]
pub struct DegreeEntry {
    pub node_id: f64,
    pub degree: f64,
}

#[napi(object)]
pub struct JsDegreeDistribution {
    pub in_degree: Vec<DegreeEntry>,
    pub out_degree: Vec<DegreeEntry>,
    pub total_degree: Vec<DegreeEntry>,
}

impl From<DegreeDistribution> for JsDegreeDistribution {
    fn from(dist: DegreeDistribution) -> Self {
        Self {
            in_degree: dist
                .in_degree
                .into_iter()
                .map(|(node_id, degree)| DegreeEntry {
                    node_id: node_id as f64,
                    degree: degree as f64,
                })
                .collect(),
            out_degree: dist
                .out_degree
                .into_iter()
                .map(|(node_id, degree)| DegreeEntry {
                    node_id: node_id as f64,
                    degree: degree as f64,
                })
                .collect(),
            total_degree: dist
                .total_degree
                .into_iter()
                .map(|(node_id, degree)| DegreeEntry {
                    node_id: node_id as f64,
                    degree: degree as f64,
                })
                .collect(),
        }
    }
}

#[napi(object, js_name = "HubNode")]
pub struct HubNode {
    pub node_id: f64,
    pub degree: f64,
}

#[napi(object)]
pub struct JsSubgraph {
    pub nodes: Vec<SombraNode>,
    pub edges: Vec<SombraEdge>,
    pub boundary_nodes: Vec<f64>,
}

impl From<Subgraph> for JsSubgraph {
    fn from(subgraph: Subgraph) -> Self {
        Self {
            nodes: subgraph.nodes.into_iter().map(SombraNode::from).collect(),
            edges: subgraph.edges.into_iter().map(SombraEdge::from).collect(),
            boundary_nodes: subgraph
                .boundary_nodes
                .into_iter()
                .map(|id| id as f64)
                .collect(),
        }
    }
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

#[napi(object, js_name = "PropertyBound")]
pub struct JsPropertyBound {
    pub value: SombraPropertyValue,
    pub inclusive: bool,
}

impl TryFrom<JsPropertyBound> for PropertyBound {
    type Error = Error;

    fn try_from(value: JsPropertyBound) -> std::result::Result<Self, Self::Error> {
        Ok(PropertyBound {
            value: PropertyValue::try_from(value.value)?,
            inclusive: value.inclusive,
        })
    }
}

#[napi(object, js_name = "PropertyRangeFilter")]
pub struct JsPropertyRangeFilter {
    pub key: String,
    pub min: Option<JsPropertyBound>,
    pub max: Option<JsPropertyBound>,
}

impl TryFrom<JsPropertyRangeFilter> for PropertyRangeFilter {
    type Error = Error;

    fn try_from(value: JsPropertyRangeFilter) -> std::result::Result<Self, Self::Error> {
        Ok(PropertyRangeFilter {
            key: value.key,
            min: value.min.map(PropertyBound::try_from).transpose()?,
            max: value.max.map(PropertyBound::try_from).transpose()?,
        })
    }
}

#[napi(object, js_name = "PropertyFilters")]
pub struct JsPropertyFilters {
    pub equals: Option<HashMap<String, SombraPropertyValue>>,
    pub not_equals: Option<HashMap<String, SombraPropertyValue>>,
    pub ranges: Option<Vec<JsPropertyRangeFilter>>,
}

impl TryFrom<JsPropertyFilters> for PropertyFilters {
    type Error = Error;

    fn try_from(value: JsPropertyFilters) -> std::result::Result<Self, Self::Error> {
        let mut equals = std::collections::BTreeMap::new();
        if let Some(eq_map) = value.equals {
            for (k, v) in eq_map {
                equals.insert(k, PropertyValue::try_from(v)?);
            }
        }

        let mut not_equals = std::collections::BTreeMap::new();
        if let Some(neq_map) = value.not_equals {
            for (k, v) in neq_map {
                not_equals.insert(k, PropertyValue::try_from(v)?);
            }
        }

        let mut ranges = Vec::new();
        if let Some(range_vec) = value.ranges {
            for r in range_vec {
                ranges.push(PropertyRangeFilter::try_from(r)?);
            }
        }

        Ok(PropertyFilters {
            equals,
            not_equals,
            ranges,
        })
    }
}

#[napi(object, js_name = "NodePattern")]
pub struct JsNodePattern {
    pub var_name: String,
    pub labels: Option<Vec<String>>,
    pub properties: Option<JsPropertyFilters>,
}

impl TryFrom<JsNodePattern> for NodePattern {
    type Error = Error;

    fn try_from(value: JsNodePattern) -> std::result::Result<Self, Self::Error> {
        Ok(NodePattern {
            var_name: value.var_name,
            labels: value.labels.unwrap_or_default(),
            properties: value
                .properties
                .map(PropertyFilters::try_from)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

#[napi(object, js_name = "EdgePattern")]
pub struct JsEdgePattern {
    pub from_var: String,
    pub to_var: String,
    pub types: Option<Vec<String>>,
    pub properties: Option<JsPropertyFilters>,
    pub direction: String,
}

impl TryFrom<JsEdgePattern> for EdgePattern {
    type Error = Error;

    fn try_from(value: JsEdgePattern) -> std::result::Result<Self, Self::Error> {
        let direction = match value.direction.as_str() {
            "outgoing" => EdgeDirection::Outgoing,
            "incoming" => EdgeDirection::Incoming,
            "both" => EdgeDirection::Both,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("Invalid edge direction: {}", value.direction),
                ))
            }
        };

        Ok(EdgePattern {
            from_var: value.from_var,
            to_var: value.to_var,
            types: value.types.unwrap_or_default(),
            properties: value
                .properties
                .map(PropertyFilters::try_from)
                .transpose()?
                .unwrap_or_default(),
            direction,
        })
    }
}

#[napi(object)]
pub struct JsPattern {
    pub nodes: Vec<JsNodePattern>,
    pub edges: Vec<JsEdgePattern>,
}

impl TryFrom<JsPattern> for Pattern {
    type Error = Error;

    fn try_from(value: JsPattern) -> std::result::Result<Self, Self::Error> {
        let nodes = value
            .nodes
            .into_iter()
            .map(NodePattern::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let edges = value
            .edges
            .into_iter()
            .map(EdgePattern::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(Pattern { nodes, edges })
    }
}

#[napi(object)]
pub struct JsMatch {
    pub node_bindings: HashMap<String, f64>,
    pub edge_ids: Vec<f64>,
}

impl From<Match> for JsMatch {
    fn from(m: Match) -> Self {
        let node_bindings = m
            .node_bindings
            .into_iter()
            .map(|(k, v)| (k, v as f64))
            .collect();

        let edge_ids = m.edge_ids.into_iter().map(|id| id as f64).collect();

        JsMatch {
            node_bindings,
            edge_ids,
        }
    }
}

// Integrity verification support
#[napi(object, js_name = "IntegrityOptions")]
pub struct IntegrityOptions {
    pub checksum_only: Option<bool>,
    pub verify_indexes: Option<bool>,
    pub verify_adjacency: Option<bool>,
    pub max_errors: Option<u32>,
}

#[napi(object, js_name = "IntegrityReport")]
pub struct IntegrityReport {
    pub checked_pages: f64,
    pub checksum_failures: f64,
    pub record_errors: f64,
    pub index_errors: f64,
    pub adjacency_errors: f64,
    pub errors: Vec<String>,
}

impl From<sombra::db::IntegrityReport> for IntegrityReport {
    fn from(report: sombra::db::IntegrityReport) -> Self {
        Self {
            checked_pages: report.checked_pages as f64,
            checksum_failures: report.checksum_failures as f64,
            record_errors: report.record_errors as f64,
            index_errors: report.index_errors as f64,
            adjacency_errors: report.adjacency_errors as f64,
            errors: report.errors,
        }
    }
}

#[napi]
impl SombraDB {
    #[napi]
    pub fn verify_integrity(&mut self, opts: IntegrityOptions) -> std::result::Result<IntegrityReport, Error> {
        let mut db = self.inner.write();
        
        let options = sombra::IntegrityOptions {
            checksum_only: opts.checksum_only.unwrap_or(false),
            verify_indexes: opts.verify_indexes.unwrap_or(true),
            verify_adjacency: opts.verify_adjacency.unwrap_or(true),
            max_errors: opts.max_errors.unwrap_or(16) as usize,
        };
        
        let report = db.verify_integrity(options).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to verify integrity: {}", e),
            )
        })?;
        
        Ok(IntegrityReport::from(report))
    }
}

// Header state access
#[napi(object, js_name = "HeaderState")]
pub struct HeaderState {
    pub next_node_id: f64,
    pub next_edge_id: f64,
    pub free_page_head: Option<f64>,
    pub last_record_page: Option<f64>,
    pub last_committed_tx_id: f64,
    pub btree_index_page: Option<f64>,
    pub btree_index_size: f64,
}

#[napi]
impl SombraDB {
    #[napi]
    pub fn get_header(&self) -> std::result::Result<HeaderState, Error> {
        let db = self.inner.read();
        
        Ok(HeaderState {
            next_node_id: db.header.next_node_id as f64,
            next_edge_id: db.header.next_edge_id as f64,
            free_page_head: db.header.free_page_head.map(|p| p as f64),
            last_record_page: db.header.last_record_page.map(|p| p as f64),
            last_committed_tx_id: db.header.last_committed_tx_id as f64,
            btree_index_page: db.header.btree_index_page.map(|p| p as f64),
            btree_index_size: db.header.btree_index_size as f64,
        })
    }
}

// Metrics access
#[napi(object, js_name = "Metrics")]
pub struct Metrics {
    pub cache_hits: f64,
    pub cache_misses: f64,
    pub node_lookups: f64,
    pub edge_traversals: f64,
    pub wal_bytes_written: f64,
    pub wal_syncs: f64,
    pub checkpoints_performed: f64,
    pub page_evictions: f64,
    pub transactions_committed: f64,
    pub transactions_rolled_back: f64,
}

#[napi]
impl SombraDB {
    #[napi]
    pub fn get_metrics(&self) -> std::result::Result<Metrics, Error> {
        let db = self.inner.read();
        
        Ok(Metrics {
            cache_hits: db.metrics.cache_hits as f64,
            cache_misses: db.metrics.cache_misses as f64,
            node_lookups: db.metrics.node_lookups as f64,
            edge_traversals: db.metrics.edge_traversals as f64,
            wal_bytes_written: db.metrics.wal_bytes_written as f64,
            wal_syncs: db.metrics.wal_syncs as f64,
            checkpoints_performed: db.metrics.checkpoints_performed as f64,
            page_evictions: db.metrics.page_evictions as f64,
            transactions_committed: db.metrics.transactions_committed as f64,
            transactions_rolled_back: db.metrics.transactions_rolled_back as f64,
        })
    }
}

// Page size constant
#[napi]
pub fn get_default_page_size() -> f64 {
    sombra::pager::DEFAULT_PAGE_SIZE as f64
}
