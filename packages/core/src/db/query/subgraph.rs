use std::collections::{HashMap, HashSet, VecDeque};

use crate::db::GraphDB;
use crate::error::Result;
use crate::model::{Edge, EdgeDirection, EdgeId, Node, NodeId, NULL_EDGE_ID};

/// Filters edges during subgraph extraction.
///
/// The provided `edge_types` list uses OR semantics—any edge whose type matches one
/// of the entries is considered. An empty list matches every edge type. The
/// `direction` determines whether outgoing, incoming, or both sets of edges are
/// traversed from each node.
#[derive(Debug, Clone)]
pub struct EdgeTypeFilter {
    /// Edge types to include (OR semantics). Empty means all types.
    pub edge_types: Vec<String>,
    /// Direction to traverse relative to the current node.
    pub direction: EdgeDirection,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Edge, EdgeDirection, Node, NodeId};

    fn temp_db(name: &str) -> (tempfile::TempPath, std::path::PathBuf) {
        let file = tempfile::Builder::new()
            .prefix(name)
            .suffix(".db")
            .tempfile()
            .expect("create temp file");
        let path = file.path().to_path_buf();
        (file.into_temp_path(), path)
    }

    #[test]
    fn extract_subgraph_respects_depth_and_marks_boundary() {
        let (_guard, path) = temp_db("subgraph_depth_boundary");
        let mut db = GraphDB::open(&path).expect("open db");

        let root = db.add_node(Node::new(0)).expect("add root");
        let mid = db.add_node(Node::new(0)).expect("add mid");
        let leaf = db.add_node(Node::new(0)).expect("add leaf");

        db.add_edge(Edge::new(0, root, mid, "CONTAINS"))
            .expect("root->mid");
        db.add_edge(Edge::new(0, mid, leaf, "CONTAINS"))
            .expect("mid->leaf");

        let filter = EdgeTypeFilter::new(vec!["CONTAINS".to_string()], EdgeDirection::Outgoing);
        let subgraph = db
            .extract_subgraph(&[root], 1, Some(filter))
            .expect("extract subgraph");

        let node_ids: Vec<NodeId> = subgraph.nodes.iter().map(|node| node.id).collect();
        assert_eq!(node_ids, vec![root, mid]);

        let edges: Vec<(NodeId, NodeId)> = subgraph
            .edges
            .iter()
            .map(|edge| (edge.source_node_id, edge.target_node_id))
            .collect();
        assert_eq!(edges, vec![(root, mid)]);

        assert_eq!(subgraph.boundary_nodes, vec![mid]);
    }

    #[test]
    fn extract_subgraph_supports_incoming_direction() {
        let (_guard, path) = temp_db("subgraph_incoming");
        let mut db = GraphDB::open(&path).expect("open db");

        let parent = db.add_node(Node::new(0)).expect("add parent");
        let child = db.add_node(Node::new(0)).expect("add child");

        db.add_edge(Edge::new(0, parent, child, "CONTAINS"))
            .expect("parent->child");

        let filter = EdgeTypeFilter::new(vec!["CONTAINS".to_string()], EdgeDirection::Incoming);
        let subgraph = db
            .extract_subgraph(&[child], 1, Some(filter))
            .expect("extract incoming");

        let node_ids: Vec<NodeId> = subgraph.nodes.iter().map(|node| node.id).collect();
        assert_eq!(node_ids, vec![parent, child]);

        let edges: Vec<(NodeId, NodeId)> = subgraph
            .edges
            .iter()
            .map(|edge| (edge.source_node_id, edge.target_node_id))
            .collect();
        assert_eq!(edges, vec![(parent, child)]);

        assert!(subgraph.boundary_nodes.is_empty());
    }

    #[test]
    fn extract_induced_subgraph_marks_boundary_nodes() {
        let (_guard, path) = temp_db("subgraph_induced");
        let mut db = GraphDB::open(&path).expect("open db");

        let caller = db.add_node(Node::new(0)).expect("add caller");
        let callee = db.add_node(Node::new(0)).expect("add callee");
        let downstream = db.add_node(Node::new(0)).expect("add downstream");
        let upstream = db.add_node(Node::new(0)).expect("add upstream");

        db.add_edge(Edge::new(0, caller, callee, "CALLS"))
            .expect("caller->callee");
        db.add_edge(Edge::new(0, callee, downstream, "CALLS"))
            .expect("callee->downstream");
        db.add_edge(Edge::new(0, upstream, callee, "CALLS"))
            .expect("upstream->callee");

        let subgraph = db
            .extract_induced_subgraph(&[caller, callee])
            .expect("extract induced");

        let node_ids: Vec<NodeId> = subgraph.nodes.iter().map(|node| node.id).collect();
        assert_eq!(node_ids, vec![caller, callee]);

        let edges: Vec<(NodeId, NodeId)> = subgraph
            .edges
            .iter()
            .map(|edge| (edge.source_node_id, edge.target_node_id))
            .collect();
        assert_eq!(edges, vec![(caller, callee)]);

        assert_eq!(subgraph.boundary_nodes, vec![callee]);
    }
}

impl EdgeTypeFilter {
    /// Creates a new filter from edge types and traversal direction.
    pub fn new(edge_types: Vec<String>, direction: EdgeDirection) -> Self {
        Self {
            edge_types,
            direction,
        }
    }
}

/// Result of a subgraph extraction query.
///
/// The `nodes` and `edges` collections are sorted by identifier for deterministic
/// iteration order. `boundary_nodes` contains node identifiers inside the subgraph
/// that still connect (via the filtered edges) to nodes that were not included.
#[derive(Debug, Clone, PartialEq)]
pub struct Subgraph {
    /// Nodes that belong to the extracted subgraph.
    pub nodes: Vec<Node>,
    /// Edges whose endpoints are both inside the subgraph.
    pub edges: Vec<Edge>,
    /// Nodes in the subgraph that connect to neighbors outside of it.
    pub boundary_nodes: Vec<NodeId>,
}

impl Subgraph {
    /// Creates an empty subgraph result.
    pub fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            boundary_nodes: Vec::new(),
        }
    }
}

impl Default for Subgraph {
    fn default() -> Self {
        Self::empty()
    }
}

impl GraphDB {
    /// Extracts a subgraph around a set of root nodes up to a given depth.
    ///
    /// Performs a breadth-first traversal starting from `root_nodes`, following edges
    /// that match the optional `edge_filter`. The traversal stops once `depth` hops
    /// have been explored. Nodes discovered within the depth limit are included in
    /// the returned [`Subgraph`], along with any connecting edges whose endpoints
    /// both fall inside the extracted node set. Nodes that still have matching edges
    /// pointing to nodes outside the subgraph are listed in `boundary_nodes`.
    ///
    /// When no `edge_filter` is provided, the traversal defaults to outgoing edges of
    /// every type. Providing a filter with an empty `edge_types` list is treated the
    /// same as matching all edge types, while still honoring the specified direction.
    ///
    /// # Arguments
    /// * `root_nodes` - Starting node identifiers for the traversal.
    /// * `depth` - Maximum number of hops to explore from each root.
    /// * `edge_filter` - Optional filter controlling edge types and direction.
    ///
    /// # Returns
    /// A [`Subgraph`] containing the visited nodes, included edges, and boundary nodes.
    ///
    /// # Errors
    /// Propagates storage and lookup errors encountered while loading nodes or edges.
    ///
    /// # Time Complexity
    /// O(N + E) where N is the number of nodes visited and E is the number of edges
    /// examined within the depth limit.
    ///
    /// # Space Complexity
    /// O(N + E) for tracking visited nodes, collected edges, and traversal queues.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use sombra::db::query::subgraph::{EdgeTypeFilter, Subgraph};
    /// # use sombra::model::EdgeDirection;
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let db_path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(db_path.path())?;
    /// let root = db.add_node(Node::new(0))?;
    /// let child = db.add_node(Node::new(0))?;
    /// db.add_edge(Edge::new(0, root, child, "CONTAINS"))?;
    ///
    /// let filter = EdgeTypeFilter::new(vec!["CONTAINS".to_string()], EdgeDirection::Outgoing);
    /// let subgraph = db.extract_subgraph(&[root], 1, Some(filter))?;
    /// assert_eq!(subgraph.nodes.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::extract_induced_subgraph`] - Builds a subgraph from an explicit node set.
    pub fn extract_subgraph(
        &mut self,
        root_nodes: &[NodeId],
        depth: usize,
        edge_filter: Option<EdgeTypeFilter>,
    ) -> Result<Subgraph> {
        if root_nodes.is_empty() {
            return Ok(Subgraph::empty());
        }

        let filter_ref = edge_filter.as_ref();
        let mut type_filter_storage: Option<HashSet<&str>> = None;
        let direction = if let Some(filter) = filter_ref {
            if !filter.edge_types.is_empty() {
                type_filter_storage = Some(filter.edge_types.iter().map(|s| s.as_str()).collect());
            }
            filter.direction
        } else {
            EdgeDirection::Outgoing
        };

        let type_filter = type_filter_storage.as_ref();

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for &root in root_nodes {
            if visited.insert(root) {
                queue.push_back((root, 0usize));
            }
        }

        let mut edges: HashMap<EdgeId, Edge> = HashMap::new();
        let mut boundary_nodes = HashSet::new();

        while let Some((node_id, current_depth)) = queue.pop_front() {
            let neighbors = self.collect_neighbor_edges(node_id, direction, type_filter)?;

            for (neighbor_id, edge) in neighbors {
                let already_present = visited.contains(&neighbor_id);
                let can_descend = current_depth < depth;

                if !already_present && !can_descend {
                    boundary_nodes.insert(node_id);
                    continue;
                }

                if !already_present {
                    visited.insert(neighbor_id);
                    queue.push_back((neighbor_id, current_depth + 1));
                }

                edges.entry(edge.id).or_insert(edge);
            }
        }

        let mut node_ids: Vec<NodeId> = visited.into_iter().collect();
        node_ids.sort_unstable();

        let mut nodes = Vec::with_capacity(node_ids.len());
        for node_id in &node_ids {
            nodes.push(self.get_node(*node_id)?);
        }

        let mut edge_list: Vec<Edge> = edges.into_values().collect();
        edge_list.sort_by_key(|edge| edge.id);

        let mut boundary: Vec<NodeId> = boundary_nodes.into_iter().collect();
        boundary.sort_unstable();

        Ok(Subgraph {
            nodes,
            edges: edge_list,
            boundary_nodes: boundary,
        })
    }

    /// Constructs the induced subgraph for a set of node identifiers.
    ///
    /// All nodes referenced in `node_ids` are included in the result, along with every
    /// edge whose endpoints both lie inside that set. Nodes that have edges connecting
    /// to nodes outside the provided set are marked as boundary nodes.
    ///
    /// # Arguments
    /// * `node_ids` - Node identifiers that should appear in the induced subgraph.
    ///
    /// # Returns
    /// A [`Subgraph`] containing the requested nodes, all internal edges, and boundary nodes.
    ///
    /// # Errors
    /// Propagates storage-level errors when loading nodes or edges from disk.
    ///
    /// # Time Complexity
    /// O(N + E) where N is `node_ids.len()` and E is the number of edges touching those nodes.
    ///
    /// # Space Complexity
    /// O(N + E) for caching membership checks and edge storage.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use sombra::db::query::subgraph::Subgraph;
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let db_path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(db_path.path())?;
    /// let a = db.add_node(Node::new(0))?;
    /// let b = db.add_node(Node::new(0))?;
    /// let c = db.add_node(Node::new(0))?;
    /// db.add_edge(Edge::new(0, a, b, "CALLS"))?;
    /// db.add_edge(Edge::new(0, b, c, "CALLS"))?;
    ///
    /// let subgraph = db.extract_induced_subgraph(&[a, b])?;
    /// assert_eq!(subgraph.nodes.len(), 2);
    /// assert_eq!(subgraph.edges.len(), 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::extract_subgraph`] - Depth-bounded traversal around starting nodes.
    pub fn extract_induced_subgraph(&mut self, node_ids: &[NodeId]) -> Result<Subgraph> {
        if node_ids.is_empty() {
            return Ok(Subgraph::empty());
        }

        let node_set: HashSet<NodeId> = node_ids.iter().copied().collect();

        let mut edge_map: HashMap<EdgeId, Edge> = HashMap::new();
        let mut boundary_nodes = HashSet::new();

        for &node_id in &node_set {
            let outgoing = self.collect_neighbor_edges(node_id, EdgeDirection::Outgoing, None)?;
            for (neighbor_id, edge) in outgoing {
                if node_set.contains(&neighbor_id) {
                    edge_map.entry(edge.id).or_insert(edge);
                } else {
                    boundary_nodes.insert(node_id);
                }
            }

            let incoming = self.collect_neighbor_edges(node_id, EdgeDirection::Incoming, None)?;
            for (neighbor_id, edge) in incoming {
                if node_set.contains(&neighbor_id) {
                    edge_map.entry(edge.id).or_insert(edge);
                } else {
                    boundary_nodes.insert(node_id);
                }
            }
        }

        let mut sorted_node_ids: Vec<NodeId> = node_set.iter().copied().collect();
        sorted_node_ids.sort_unstable();

        let mut nodes = Vec::with_capacity(sorted_node_ids.len());
        for node_id in &sorted_node_ids {
            nodes.push(self.get_node(*node_id)?);
        }

        let mut edges: Vec<Edge> = edge_map.into_values().collect();
        edges.sort_by_key(|edge| edge.id);

        let mut boundary: Vec<NodeId> = boundary_nodes.into_iter().collect();
        boundary.sort_unstable();

        Ok(Subgraph {
            nodes,
            edges,
            boundary_nodes: boundary,
        })
    }

    fn collect_neighbor_edges<'a>(
        &'a mut self,
        node_id: NodeId,
        direction: EdgeDirection,
        type_filter: Option<&HashSet<&'a str>>,
    ) -> Result<Vec<(NodeId, Edge)>> {
        let node = self.get_node(node_id)?;
        let mut neighbors = Vec::new();

        if matches!(direction, EdgeDirection::Outgoing | EdgeDirection::Both) {
            let mut edge_id = node.first_outgoing_edge_id;
            while edge_id != NULL_EDGE_ID {
                self.metrics.edge_traversals += 1;
                let edge = self.load_edge(edge_id)?;
                let next_edge_id = edge.next_outgoing_edge_id;
                if type_filter.is_none_or(|set| set.contains(edge.type_name.as_str())) {
                    neighbors.push((edge.target_node_id, edge));
                }
                edge_id = next_edge_id;
            }
        }

        if matches!(direction, EdgeDirection::Incoming | EdgeDirection::Both) {
            let mut edge_id = node.first_incoming_edge_id;
            while edge_id != NULL_EDGE_ID {
                self.metrics.edge_traversals += 1;
                let edge = self.load_edge(edge_id)?;
                let next_edge_id = edge.next_incoming_edge_id;
                if type_filter.is_none_or(|set| set.contains(edge.type_name.as_str())) {
                    neighbors.push((edge.source_node_id, edge));
                }
                edge_id = next_edge_id;
            }
        }

        Ok(neighbors)
    }
}
