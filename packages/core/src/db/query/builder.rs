//! Fluent query builder for composing multi-step graph queries.
//!
//! This module provides a high-level API for assembling complex graph
//! traversals using a chainable builder pattern. Queries can start from
//! labels, explicit node IDs, or indexed properties, apply node/edge
//! predicates, perform bounded traversals, and return the resulting nodes
//! and connecting edges.

use std::collections::{HashMap, HashSet};
use std::mem;

use crate::db::GraphDB;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeDirection, EdgeId, Node, NodeId, PropertyValue};

/// Predicate applied to nodes during query execution.
type NodeFilter = Box<dyn Fn(&Node) -> bool + 'static>;

/// Predicate applied to edges during traversal.
type EdgeFilter = Box<dyn Fn(&Edge) -> bool + 'static>;

#[derive(Debug, Clone)]
struct TraversalSpec {
    edge_types: Vec<String>,
    direction: EdgeDirection,
    depth: usize,
}

#[derive(Debug, Clone)]
enum StartSpec {
    Explicit(Vec<NodeId>),
    Label(String),
    Property {
        label: String,
        key: String,
        value: PropertyValue,
    },
}

enum QueryOp {
    Start(StartSpec),
    FilterNodes(NodeFilter),
    FilterEdges(EdgeFilter),
    Traverse(TraversalSpec),
}

/// Result produced by [`QueryBuilder::get_ids`].
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Nodes that seeded the traversal after all pre-traversal filters.
    pub start_nodes: Vec<NodeId>,
    /// Node identifiers returned by the query (start nodes included).
    pub node_ids: Vec<NodeId>,
    /// Materialized nodes matching `node_ids`, ordered consistently.
    pub nodes: Vec<Node>,
    /// Edges that satisfied edge filters while connecting returned nodes.
    pub edges: Vec<Edge>,
    /// Indicates whether the result set was truncated by [`QueryBuilder::limit`].
    pub limited: bool,
}

/// Chainable builder for composing query plans executed against [`GraphDB`].
///
/// The builder records operations in the order they are declared and replays
/// them during [`get_ids`](Self::get_ids) or [`get_nodes`](Self::get_nodes). 
/// This allows ergonomic construction of queries without immediately borrowing 
/// the database for each step.
///
/// # Examples
/// ```rust,no_run
/// # use sombra::{GraphDB, GraphError, Node, Edge, model::EdgeDirection, PropertyValue};
/// # use tempfile::NamedTempFile;
/// # fn example() -> Result<(), GraphError> {
/// let tmp = NamedTempFile::new()?;
/// let mut db = GraphDB::open(tmp.path())?;
///
/// // Insert data omitted for brevity...
///
/// let result = db
///     .query()
///     .start_from_label("Function")
///     .filter_nodes(|node| {
///         matches!(
///             node.properties.get("name"),
///             Some(PropertyValue::String(name)) if name == "foo"
///         )
///     })
///     .traverse(&["CALLS"], EdgeDirection::Outgoing, 3)
///     .limit(100)
///     .get_ids()?;
/// # Ok(()) }
/// ```
pub struct QueryBuilder<'db> {
    db: &'db mut GraphDB,
    ops: Vec<QueryOp>,
    limit: Option<usize>,
}

impl<'db> QueryBuilder<'db> {
    /// Creates a new builder bound to the provided database.
    ///
    /// # Arguments
    /// * `db` - Database handle used when executing the built query.
    ///
    /// # Returns
    /// A fresh `QueryBuilder` with no recorded operations.
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Space Complexity
    /// O(1)
    ///
    /// # See Also
    /// * [`GraphDB::query`] - Convenience helper to obtain a builder.
    pub fn new(db: &'db mut GraphDB) -> Self {
        Self {
            db,
            ops: Vec::new(),
            limit: None,
        }
    }

    /// Seeds the query with explicit node identifiers.
    ///
    /// This replaces any previously configured starting point.
    ///
    /// # Arguments
    /// * `node_ids` - Nodes that will form the initial frontier.
    ///
    /// # Returns
    /// Updated builder with the start specification recorded.
    ///
    /// # Time Complexity
    /// O(n) to record the supplied node IDs.
    ///
    /// # Space Complexity
    /// O(n) additional storage for the node list.
    ///
    /// # See Also
    /// * [`Self::start_from_label`]
    /// * [`Self::start_from_property`]
    pub fn start_from(mut self, node_ids: Vec<NodeId>) -> Self {
        self.remove_existing_start();
        self.ops.push(QueryOp::Start(StartSpec::Explicit(node_ids)));
        self
    }

    /// Seeds the query using all nodes that possess a label.
    ///
    /// Node IDs are resolved during execution to honour the latest on-disk
    /// state at that time.
    ///
    /// # Arguments
    /// * `label` - Label name used to fetch starting nodes.
    ///
    /// # Returns
    /// Builder with the start specification updated.
    ///
    /// # Time Complexity
    /// O(1) to record the operation (resolution deferred to execution).
    ///
    /// # Space Complexity
    /// O(len(label)) for storing the label string.
    ///
    /// # See Also
    /// * [`GraphDB::get_nodes_by_label`]
    pub fn start_from_label(mut self, label: &str) -> Self {
        self.remove_existing_start();
        self.ops
            .push(QueryOp::Start(StartSpec::Label(label.to_string())));
        self
    }

    /// Seeds the query with nodes whose property equals the provided value.
    ///
    /// During execution the builder leverages property indexes when available
    /// and falls back to label scans otherwise.
    ///
    /// # Arguments
    /// * `label` - Label restricting the property lookup domain.
    /// * `key` - Property key inspected on each node.
    /// * `value` - Target property value.
    ///
    /// # Returns
    /// Builder with the start specification recorded.
    ///
    /// # Time Complexity
    /// O(1) (resolution deferred to execution).
    ///
    /// # Space Complexity
    /// O(len(label) + len(key)) plus the size of `value`.
    ///
    /// # See Also
    /// * [`GraphDB::find_nodes_by_property`]
    pub fn start_from_property(mut self, label: &str, key: &str, value: PropertyValue) -> Self {
        self.remove_existing_start();
        self.ops.push(QueryOp::Start(StartSpec::Property {
            label: label.to_string(),
            key: key.to_string(),
            value,
        }));
        self
    }

    /// Applies a node predicate that must pass for all subsequent stages.
    ///
    /// Filters are evaluated lazily during query execution. Multiple filters
    /// can be chained and are combined with logical AND semantics.
    ///
    /// # Arguments
    /// * `filter` - Predicate returning `true` when a node should remain.
    ///
    /// # Returns
    /// Builder with the filter appended.
    ///
    /// # Time Complexity
    /// O(1) to record the filter (evaluation deferred to execution).
    ///
    /// # Space Complexity
    /// O(1) plus closure capture size.
    pub fn filter_nodes<F>(mut self, filter: F) -> Self
    where
        F: Fn(&Node) -> bool + 'static,
    {
        self.ops.push(QueryOp::FilterNodes(Box::new(filter)));
        self
    }

    /// Applies an edge predicate evaluated during traversals.
    ///
    /// Only edges that satisfy all registered edge filters contribute to
    /// traversal expansion and appear in the result set.
    ///
    /// # Arguments
    /// * `filter` - Predicate returning `true` when an edge is acceptable.
    ///
    /// # Returns
    /// Builder with the filter appended.
    ///
    /// # Time Complexity
    /// O(1) to record the filter.
    ///
    /// # Space Complexity
    /// O(1) plus closure capture size.
    pub fn filter_edges<F>(mut self, filter: F) -> Self
    where
        F: Fn(&Edge) -> bool + 'static,
    {
        self.ops.push(QueryOp::FilterEdges(Box::new(filter)));
        self
    }

    /// Adds a bounded traversal step to the query plan.
    ///
    /// The traversal explores edges matching `edge_types` according to the
    /// provided direction and depth. Node and edge filters recorded prior to
    /// this step are applied before and during traversal respectively.
    ///
    /// # Arguments
    /// * `edge_types` - Edge type names (logical OR) to follow.
    /// * `direction` - Direction in which to consider edges.
    /// * `depth` - Maximum number of hops to explore (0 keeps current frontier).
    ///
    /// # Returns
    /// Builder with the traversal specification appended.
    ///
    /// # Time Complexity
    /// O(k) to clone the edge types, where k is the number provided.
    ///
    /// # Space Complexity
    /// O(k) additional storage.
    pub fn traverse(mut self, edge_types: &[&str], direction: EdgeDirection, depth: usize) -> Self {
        self.ops.push(QueryOp::Traverse(TraversalSpec {
            edge_types: edge_types.iter().map(|s| (*s).to_string()).collect(),
            direction,
            depth,
        }));
        self
    }

    /// Limits the number of nodes returned by the query.
    ///
    /// When invoked multiple times, the smallest supplied limit is honoured.
    ///
    /// # Arguments
    /// * `n` - Maximum number of nodes to return.
    ///
    /// # Returns
    /// Builder with the limit recorded.
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Space Complexity
    /// O(1)
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(match self.limit {
            Some(existing) => existing.min(n),
            None => n,
        });
        self
    }

    /// Executes the recorded operations and returns node IDs with full query metadata.
    ///
    /// Operations are processed in the order they were registered, allowing
    /// filters to shape the traversal frontier incrementally.
    ///
    /// # Returns
    /// * `Ok(QueryResult)` on success.
    /// * `Err(GraphError::InvalidArgument)` if no starting point was provided.
    ///
    /// # Errors
    /// Propagates I/O and data access errors originating from the database
    /// while fetching nodes, edges, or indexes.
    ///
    /// # Time Complexity
    /// O(V + E) relative to the nodes `V` and edges `E` visited given the
    /// configured traversal depth.
    ///
    /// # Space Complexity
    /// O(V + E) for tracking visited nodes, edges, and building the result.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, GraphError, Node, Edge, model::EdgeDirection, PropertyValue};
    /// # use tempfile::NamedTempFile;
    /// # fn demo() -> Result<(), GraphError> {
    /// let tmp = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(tmp.path())?;
    ///
    /// let result = db
    ///     .query()
    ///     .start_from_label("File")
    ///     .traverse(&["CONTAINS"], EdgeDirection::Outgoing, 2)
    ///     .get_ids()?;
    ///
    /// println!("matched nodes: {}", result.node_ids.len());
    /// # Ok(()) }
    /// ```
    ///
    /// # See Also
    /// * [`QueryResult`] - Structure describing the returned data.
    /// * [`Self::get_nodes`] - Returns only the materialized nodes.
    pub fn get_ids(mut self) -> Result<QueryResult> {
        if !self.ops.iter().any(|op| matches!(op, QueryOp::Start(_))) {
            return Err(GraphError::InvalidArgument(
                "QueryBuilder requires a starting point".into(),
            ));
        }

        let mut current_nodes: Vec<NodeId> = Vec::new();
        let mut edge_filters: Vec<EdgeFilter> = Vec::new();
        let mut captured_start: Option<Vec<NodeId>> = None;
        let mut collected_edges: HashMap<EdgeId, Edge> = HashMap::new();

        let ops = mem::take(&mut self.ops);

        for op in ops {
            match op {
                QueryOp::Start(spec) => {
                    current_nodes = self.resolve_start(spec)?;
                }
                QueryOp::FilterNodes(filter) => {
                    current_nodes = self.apply_node_filter(current_nodes, filter)?;
                }
                QueryOp::FilterEdges(filter) => {
                    edge_filters.push(filter);
                }
                QueryOp::Traverse(spec) => {
                    if captured_start.is_none() {
                        captured_start = Some(current_nodes.clone());
                    }
                    let (nodes, edges) =
                        self.execute_traversal(&current_nodes, &spec, &edge_filters)?;
                    current_nodes = nodes;
                    for edge in edges {
                        collected_edges.entry(edge.id).or_insert(edge);
                    }
                }
            }
        }

        let mut node_ids = if current_nodes.is_empty() {
            Vec::new()
        } else {
            let mut seen = HashSet::new();
            let mut ordered = Vec::new();
            for node_id in current_nodes {
                if seen.insert(node_id) {
                    ordered.push(node_id);
                }
            }
            ordered
        };

        if captured_start.is_none() {
            captured_start = Some(node_ids.clone());
        }

        let mut limited = false;
        if let Some(limit) = self.limit {
            if node_ids.len() > limit {
                node_ids.truncate(limit);
                limited = true;
            }
        }

        let node_id_set: HashSet<NodeId> = node_ids.iter().copied().collect();
        let mut edges: Vec<Edge> = collected_edges
            .into_values()
            .filter(|edge| {
                node_id_set.contains(&edge.source_node_id)
                    && node_id_set.contains(&edge.target_node_id)
            })
            .collect();
        edges.sort_by_key(|edge| edge.id);

        let mut nodes = Vec::with_capacity(node_ids.len());
        for node_id in &node_ids {
            if let Some(node) = self.db.get_node(*node_id)? {
                nodes.push(node);
            }
        }

        Ok(QueryResult {
            start_nodes: captured_start.unwrap_or_default(),
            node_ids,
            nodes,
            edges,
            limited,
        })
    }

    /// Executes the query and returns only the materialized nodes.
    ///
    /// This is a convenience method that executes the query and extracts
    /// just the nodes from the result, discarding metadata like edges and
    /// start nodes.
    ///
    /// # Returns
    /// * `Ok(Vec<Node>)` containing all matched nodes.
    /// * `Err(GraphError::InvalidArgument)` if no starting point was provided.
    ///
    /// # Errors
    /// Propagates I/O and data access errors from the database.
    ///
    /// # Time Complexity
    /// O(V + E) where V is visited nodes and E is traversed edges.
    ///
    /// # Space Complexity
    /// O(V) for the returned node vector.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, GraphError};
    /// # use tempfile::NamedTempFile;
    /// # fn demo() -> Result<(), GraphError> {
    /// let tmp = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(tmp.path())?;
    ///
    /// let nodes = db
    ///     .query()
    ///     .start_from_label("User")
    ///     .limit(10)
    ///     .get_nodes()?;
    ///
    /// for node in nodes {
    ///     println!("Node {}: {:?}", node.id, node.labels);
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// # See Also
    /// * [`Self::get_ids`] - Returns full query result with metadata.
    pub fn get_nodes(self) -> Result<Vec<Node>> {
        let result = self.get_ids()?;
        Ok(result.nodes)
    }

    fn resolve_start(&mut self, spec: StartSpec) -> Result<Vec<NodeId>> {
        match spec {
            StartSpec::Explicit(nodes) => Ok(nodes),
            StartSpec::Label(label) => self.db.get_nodes_by_label(&label),
            StartSpec::Property { label, key, value } => {
                self.db.find_nodes_by_property(&label, &key, &value)
            }
        }
    }

    fn apply_node_filter(&mut self, nodes: Vec<NodeId>, filter: NodeFilter) -> Result<Vec<NodeId>> {
        let mut result = Vec::new();
        let predicate = &filter;
        for node_id in nodes {
            if let Some(node) = self.db.get_node(node_id)? {
                if predicate(&node) {
                    result.push(node_id);
                }
            }
        }
        Ok(result)
    }

    fn execute_traversal(
        &mut self,
        start_nodes: &[NodeId],
        spec: &TraversalSpec,
        edge_filters: &[EdgeFilter],
    ) -> Result<(Vec<NodeId>, Vec<Edge>)> {
        if spec.depth == 0 {
            return Ok((start_nodes.to_vec(), Vec::new()));
        }

        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut ordered: Vec<NodeId> = Vec::new();
        let mut frontier: Vec<NodeId> = Vec::new();
        let mut edges = Vec::new();

        for &node_id in start_nodes {
            if visited.insert(node_id) {
                ordered.push(node_id);
                frontier.push(node_id);
            }
        }

        let edge_type_refs: Vec<&str> = spec.edge_types.iter().map(|ty| ty.as_str()).collect();
        let mut depth_remaining = spec.depth;

        while depth_remaining > 0 && !frontier.is_empty() {
            depth_remaining -= 1;
            let mut next_frontier = Vec::new();

            for node_id in frontier {
                for (neighbor, edge) in self.db.get_neighbors_with_edges_by_type(
                    node_id,
                    &edge_type_refs,
                    spec.direction,
                )? {
                    if !self.edge_passes_filters(&edge, edge_filters) {
                        continue;
                    }

                    if visited.insert(neighbor) {
                        ordered.push(neighbor);
                        next_frontier.push(neighbor);
                    }

                    edges.push(edge);
                }
            }

            frontier = next_frontier;
        }

        let mut seen_edge_ids = HashSet::new();
        edges.retain(|edge| seen_edge_ids.insert(edge.id));

        Ok((ordered, edges))
    }

    fn edge_passes_filters(&self, edge: &Edge, filters: &[EdgeFilter]) -> bool {
        filters.iter().all(|filter| filter(edge))
    }

    fn remove_existing_start(&mut self) {
        self.ops.retain(|op| !matches!(op, QueryOp::Start(_)));
    }
}

impl GraphDB {
    /// Convenience helper for creating a [`QueryBuilder`].
    ///
    /// # Returns
    /// A builder pre-populated with this database handle.
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Space Complexity
    /// O(1)
    pub fn query(&mut self) -> QueryBuilder<'_> {
        QueryBuilder::new(self)
    }
}
