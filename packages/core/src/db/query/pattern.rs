use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::db::GraphDB;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeDirection, EdgeId, Node, NodeId, PropertyValue, NULL_EDGE_ID};

/// Inclusive or exclusive bound used by a [`PropertyRangeFilter`].
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyBound {
    /// Boundary value for the range comparison.
    pub value: PropertyValue,
    /// Whether the boundary is inclusive (`>=`/`<=`) or exclusive (`>`/`<`).
    pub inclusive: bool,
}

/// Range-based property filter supporting optional lower and upper bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyRangeFilter {
    /// Property key the range applies to.
    pub key: String,
    /// Optional minimum bound.
    pub min: Option<PropertyBound>,
    /// Optional maximum bound.
    pub max: Option<PropertyBound>,
}

impl PropertyRangeFilter {
    fn matches(&self, value: Option<&PropertyValue>) -> bool {
        let Some(candidate) = value else {
            return false;
        };

        if let Some(bound) = &self.min {
            let Some(ordering) = candidate.partial_cmp_value(&bound.value) else {
                return false;
            };

            match ordering {
                Ordering::Less => return false,
                Ordering::Equal if !bound.inclusive => return false,
                _ => {}
            }
        }

        if let Some(bound) = &self.max {
            let Some(ordering) = candidate.partial_cmp_value(&bound.value) else {
                return false;
            };

            match ordering {
                Ordering::Greater => return false,
                Ordering::Equal if !bound.inclusive => return false,
                _ => {}
            }
        }

        true
    }
}

/// Collection of property-based filters with AND semantics.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PropertyFilters {
    /// Required equality matches (`property == value`).
    pub equals: BTreeMap<String, PropertyValue>,
    /// Disallowed values (`property != value`).
    pub not_equals: BTreeMap<String, PropertyValue>,
    /// Range filters evaluated after equals/not_equals.
    pub ranges: Vec<PropertyRangeFilter>,
}

impl PropertyFilters {
    fn matches(&self, properties: &BTreeMap<String, PropertyValue>) -> bool {
        for (key, expected) in &self.equals {
            match properties.get(key) {
                Some(value) if value == expected => {}
                _ => return false,
            }
        }

        for (key, forbidden) in &self.not_equals {
            if let Some(value) = properties.get(key) {
                if value == forbidden {
                    return false;
                }
            }
        }

        for range in &self.ranges {
            if !range.matches(properties.get(&range.key)) {
                return false;
            }
        }

        true
    }
}

/// Pattern describing a node constraint inside a match expression.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    /// Variable name bound to the node when a match is found.
    pub var_name: String,
    /// Acceptable labels for the node (OR semantics). Empty means any label.
    pub labels: Vec<String>,
    /// Property-based filters that must all succeed (AND semantics).
    pub properties: PropertyFilters,
}

impl NodePattern {
    fn matches(&self, node: &Node) -> bool {
        if !self.labels.is_empty()
            && !node
                .labels
                .iter()
                .any(|label| self.labels.iter().any(|candidate| candidate == label))
        {
            return false;
        }

        self.properties.matches(&node.properties)
    }
}

/// Pattern describing an edge constraint between two node variables.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    /// Variable name of the source/current node in the match traversal.
    pub from_var: String,
    /// Variable name of the destination node reached after following the edge.
    pub to_var: String,
    /// Acceptable edge types (OR semantics). Empty means any type.
    pub types: Vec<String>,
    /// Property-based filters that must all succeed (AND semantics).
    pub properties: PropertyFilters,
    /// Direction to traverse relative to `from_var`.
    pub direction: EdgeDirection,
}

impl EdgePattern {
    fn matches_edge(&self, edge: &Edge) -> bool {
        if !self.types.is_empty()
            && !self
                .types
                .iter()
                .any(|candidate| candidate == edge.type_name.as_str())
        {
            return false;
        }

        self.properties.matches(&edge.properties)
    }
}

/// Simple path pattern consisting of alternating node and edge patterns.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    /// Ordered list of node patterns that must be satisfied.
    pub nodes: Vec<NodePattern>,
    /// Ordered list of edge patterns connecting the nodes.
    pub edges: Vec<EdgePattern>,
}

impl Pattern {
    fn validate(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Err(GraphError::InvalidArgument(
                "pattern must contain at least one node".into(),
            ));
        }

        if self.edges.len() + 1 != self.nodes.len() {
            return Err(GraphError::InvalidArgument(
                "pattern edges must form a simple path (nodes.len() = edges.len() + 1)".into(),
            ));
        }

        let mut seen = HashSet::new();
        for node in &self.nodes {
            if !seen.insert(node.var_name.clone()) {
                return Err(GraphError::InvalidArgument(format!(
                    "duplicate node variable `{}` in pattern",
                    node.var_name
                )));
            }
        }

        for (idx, edge) in self.edges.iter().enumerate() {
            let expected_source = &self.nodes[idx].var_name;
            let expected_target = &self.nodes[idx + 1].var_name;

            if &edge.from_var != expected_source {
                return Err(GraphError::InvalidArgument(format!(
                    "edge {} originates from `{}` but expected `{}`",
                    idx, edge.from_var, expected_source
                )));
            }

            if &edge.to_var != expected_target {
                return Err(GraphError::InvalidArgument(format!(
                    "edge {} targets `{}` but expected `{}`",
                    idx, edge.to_var, expected_target
                )));
            }
        }

        Ok(())
    }

    fn node_index(&self) -> HashMap<&str, usize> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| (node.var_name.as_str(), idx))
            .collect()
    }
}

/// Result of a successful pattern match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    /// Mapping from node variable names to their bound node identifiers.
    pub node_bindings: BTreeMap<String, NodeId>,
    /// Edge identifiers corresponding to `Pattern::edges` in order.
    pub edge_ids: Vec<EdgeId>,
}

impl Match {
    fn empty() -> Self {
        Self {
            node_bindings: BTreeMap::new(),
            edge_ids: Vec::new(),
        }
    }
}

impl GraphDB {
    /// Matches a simple path pattern against the graph and returns all bindings.
    ///
    /// The supplied [`Pattern`] describes alternating node and edge constraints that
    /// must hold along a linear path. Nodes are matched using OR semantics across the
    /// provided labels and AND semantics across the property filters. Edges are matched
    /// using OR semantics across type filters and AND semantics across property filters.
    ///
    /// # Arguments
    /// * `pattern` - Path pattern describing the sequence of node and edge constraints.
    ///
    /// # Returns
    /// * `Ok(Vec<Match>)` - All variable bindings that satisfy the pattern. `Match::edge_ids`
    ///   aligns with the order of `pattern.edges`.
    ///
    /// # Errors
    /// * [`GraphError::InvalidArgument`] - When the pattern is malformed (e.g., not a simple path,
    ///   references unknown node variables, or reuses variable names).
    /// * Propagates storage-level errors while loading nodes or edges.
    ///
    /// # Time Complexity
    /// O(P * (V + E)) in the worst case where `P` is the pattern length, because each step may need
    /// to inspect adjacency lists for candidate nodes. Selective labels and property filters
    /// significantly reduce the search space in practice.
    ///
    /// # Space Complexity
    /// O(P) for recursion stack and binding state.
    ///
    /// # Example
    /// ```rust,no_run
    /// use sombra::db::GraphDB;
    /// use sombra::model::{Edge, Node, PropertyValue, EdgeDirection};
    /// use sombra::db::query::pattern::{
    ///     EdgePattern, Match, NodePattern, Pattern, PropertyBound, PropertyFilters, PropertyRangeFilter,
    /// };
    ///
    /// # fn main() -> Result<(), sombra::GraphError> {
    /// let tmp = tempfile::NamedTempFile::new()?;
    /// let mut db = GraphDB::open(tmp.path())?;
    ///
    /// let mut call = Node::new(0);
    /// call.labels.push("CallExpr".into());
    /// call.properties
    ///     .insert("callee".into(), PropertyValue::String("foo".into()));
    /// let call_id = db.add_node(call)?;
    ///
    /// let mut func = Node::new(0);
    /// func.labels.push("Function".into());
    /// func.properties
    ///     .insert("name".into(), PropertyValue::String("foo".into()));
    /// let func_id = db.add_node(func)?;
    ///
    /// db.add_edge(Edge::new(0, call_id, func_id, "CALLS"))?;
    ///
    /// let mut call_filters = PropertyFilters::default();
    /// call_filters
    ///     .equals
    ///     .insert("callee".into(), PropertyValue::String("foo".into()));
    ///
    /// let mut func_filters = PropertyFilters::default();
    /// func_filters
    ///     .equals
    ///     .insert("name".into(), PropertyValue::String("foo".into()));
    ///
    /// let pattern = Pattern {
    ///     nodes: vec![
    ///         NodePattern {
    ///             var_name: "call".into(),
    ///             labels: vec!["CallExpr".into()],
    ///             properties: call_filters,
    ///         },
    ///         NodePattern {
    ///             var_name: "func".into(),
    ///             labels: vec!["Function".into()],
    ///             properties: func_filters,
    ///         },
    ///     ],
    ///     edges: vec![EdgePattern {
    ///         from_var: "call".into(),
    ///         to_var: "func".into(),
    ///         types: vec!["CALLS".into()],
    ///         properties: PropertyFilters::default(),
    ///         direction: EdgeDirection::Outgoing,
    ///     }],
    /// };
    ///
    /// let matches = db.match_pattern(&pattern)?;
    /// assert_eq!(matches.len(), 1);
    /// assert_eq!(matches[0].node_bindings["call"], call_id);
    /// assert_eq!(matches[0].node_bindings["func"], func_id);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::get_neighbors_with_edges_by_type`] - Lower-level API for filtered traversals.
    /// * [`crate::db::query::subgraph::Subgraph`] - Extract subgraphs after locating interesting paths.
    pub fn match_pattern(&mut self, pattern: &Pattern) -> Result<Vec<Match>> {
        pattern.validate()?;

        let mut results = Vec::new();
        let mut bindings = Match::empty();
        let node_indices = pattern.node_index();

        let start_pattern = &pattern.nodes[0];
        let start_candidates = self.candidate_nodes(start_pattern)?;

        for node_id in start_candidates {
            let Some(node) = self.get_node(node_id)? else {
                continue;
            };
            if !start_pattern.matches(&node) {
                continue;
            }

            bindings
                .node_bindings
                .insert(start_pattern.var_name.clone(), node_id);

            self.match_pattern_from(pattern, &node_indices, 0, &mut bindings, &mut results)?;

            bindings.node_bindings.remove(&start_pattern.var_name);
        }

        Ok(results)
    }

    fn match_pattern_from(
        &mut self,
        pattern: &Pattern,
        node_indices: &HashMap<&str, usize>,
        edge_idx: usize,
        bindings: &mut Match,
        results: &mut Vec<Match>,
    ) -> Result<()> {
        if edge_idx == pattern.edges.len() {
            results.push(bindings.clone());
            return Ok(());
        }

        let edge_pattern = &pattern.edges[edge_idx];
        let Some(&from_node_id) = bindings.node_bindings.get(&edge_pattern.from_var) else {
            return Err(GraphError::InvalidArgument(format!(
                "edge pattern references unbound node `{}`",
                edge_pattern.from_var
            )));
        };

        let candidates = self.edge_candidates(from_node_id, edge_pattern)?;

        let target_index = *node_indices
            .get(edge_pattern.to_var.as_str())
            .ok_or_else(|| {
                GraphError::InvalidArgument(format!(
                    "edge pattern references unknown node `{}`",
                    edge_pattern.to_var
                ))
            })?;
        let target_pattern = &pattern.nodes[target_index];

        for (edge_id, target_node_id) in candidates {
            let mut inserted_binding = false;

            if let Some(existing) = bindings.node_bindings.get(&edge_pattern.to_var) {
                if *existing != target_node_id {
                    continue;
                }
            } else {
                let Some(node) = self.get_node(target_node_id)? else {
                    continue;
                };
                if !target_pattern.matches(&node) {
                    continue;
                }
                bindings
                    .node_bindings
                    .insert(edge_pattern.to_var.clone(), target_node_id);
                inserted_binding = true;
            }

            bindings.edge_ids.push(edge_id);

            self.match_pattern_from(pattern, node_indices, edge_idx + 1, bindings, results)?;

            bindings.edge_ids.pop();

            if inserted_binding {
                bindings.node_bindings.remove(&edge_pattern.to_var);
            }
        }

        Ok(())
    }

    fn candidate_nodes(&mut self, pattern: &NodePattern) -> Result<Vec<NodeId>> {
        let candidates: BTreeSet<NodeId> = if pattern.labels.is_empty() {
            self.node_index
                .iter()
                .into_iter()
                .map(|(node_id, _)| node_id)
                .collect()
        } else {
            let mut set = BTreeSet::new();
            for label in &pattern.labels {
                if let Some(ids) = self.label_index.get(label) {
                    set.extend(ids.iter().map(|id_ref| *id_ref));
                }
            }
            set
        };

        let mut filtered = Vec::new();
        for node_id in candidates {
            if let Some(node) = self.get_node(node_id)? {
                if pattern.matches(&node) {
                    filtered.push(node_id);
                }
            }
        }

        Ok(filtered)
    }

    fn edge_candidates(
        &mut self,
        from_node_id: NodeId,
        pattern: &EdgePattern,
    ) -> Result<Vec<(EdgeId, NodeId)>> {
        let mut candidates = Vec::new();
        let mut seen_edges = HashSet::new();

        let node = self
            .get_node(from_node_id)?
            .ok_or(GraphError::NotFound("node"))?;

        if matches!(
            pattern.direction,
            EdgeDirection::Outgoing | EdgeDirection::Both
        ) {
            let mut edge_id = node.first_outgoing_edge_id;
            while edge_id != NULL_EDGE_ID {
                let edge = self.load_edge(edge_id)?;
                let next_edge_id = edge.next_outgoing_edge_id;

                if seen_edges.insert(edge.id) && pattern.matches_edge(&edge) {
                    candidates.push((edge.id, edge.target_node_id));
                }

                edge_id = next_edge_id;
            }
        }

        if matches!(
            pattern.direction,
            EdgeDirection::Incoming | EdgeDirection::Both
        ) {
            let mut edge_id = node.first_incoming_edge_id;
            while edge_id != NULL_EDGE_ID {
                let edge = self.load_edge(edge_id)?;
                let next_edge_id = edge.next_incoming_edge_id;

                if seen_edges.insert(edge.id) && pattern.matches_edge(&edge) {
                    candidates.push((edge.id, edge.source_node_id));
                }

                edge_id = next_edge_id;
            }
        }

        candidates.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Edge, Node, PropertyValue};

    fn temp_db(name: &str) -> (tempfile::TempPath, std::path::PathBuf) {
        let file = tempfile::Builder::new()
            .prefix(name)
            .suffix(".db")
            .tempfile()
            .expect("create temp db");
        let path = file.path().to_path_buf();
        (file.into_temp_path(), path)
    }

    #[test]
    fn match_pattern_identifies_calls() {
        let (_guard, path) = temp_db("pattern_calls");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut target = Node::new(0);
        target.labels.push("Function".into());
        target
            .properties
            .insert("name".into(), PropertyValue::String("foo".into()));
        let target_id = db.add_node(target).expect("add target");

        let mut call = Node::new(0);
        call.labels.push("CallExpr".into());
        call.properties
            .insert("callee".into(), PropertyValue::String("foo".into()));
        let call_id = db.add_node(call).expect("add call");

        let mut other_func = Node::new(0);
        other_func.labels.push("Function".into());
        other_func
            .properties
            .insert("name".into(), PropertyValue::String("bar".into()));
        let other_func_id = db.add_node(other_func).expect("add other func");

        let mut other_call = Node::new(0);
        other_call.labels.push("CallExpr".into());
        other_call
            .properties
            .insert("callee".into(), PropertyValue::String("bar".into()));
        let other_call_id = db.add_node(other_call).expect("add other call");

        let call_edge_id = db
            .add_edge(Edge::new(0, call_id, target_id, "CALLS"))
            .expect("call->target");
        db.add_edge(Edge::new(0, other_call_id, other_func_id, "CALLS"))
            .expect("other call");

        let mut call_filters = PropertyFilters::default();
        call_filters
            .equals
            .insert("callee".into(), PropertyValue::String("foo".into()));

        let mut target_filters = PropertyFilters::default();
        target_filters
            .equals
            .insert("name".into(), PropertyValue::String("foo".into()));

        let pattern = Pattern {
            nodes: vec![
                NodePattern {
                    var_name: "call".into(),
                    labels: vec!["CallExpr".into()],
                    properties: call_filters,
                },
                NodePattern {
                    var_name: "func".into(),
                    labels: vec!["Function".into()],
                    properties: target_filters,
                },
            ],
            edges: vec![EdgePattern {
                from_var: "call".into(),
                to_var: "func".into(),
                types: vec!["CALLS".into()],
                properties: PropertyFilters::default(),
                direction: EdgeDirection::Outgoing,
            }],
        };

        let matches = db.match_pattern(&pattern).expect("match pattern");
        assert_eq!(matches.len(), 1);
        let binding = &matches[0];
        assert_eq!(binding.node_bindings["call"], call_id);
        assert_eq!(binding.node_bindings["func"], target_id);
        assert_eq!(binding.edge_ids, vec![call_edge_id]);
    }

    #[test]
    fn match_pattern_supports_incoming_edges() {
        let (_guard, path) = temp_db("pattern_incoming");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut parent = Node::new(0);
        parent.labels.push("Module".into());
        parent
            .properties
            .insert("name".into(), PropertyValue::String("core".into()));
        let parent_id = db.add_node(parent).expect("add parent");

        let mut child = Node::new(0);
        child.labels.push("File".into());
        child
            .properties
            .insert("path".into(), PropertyValue::String("src/lib.rs".into()));
        let child_id = db.add_node(child).expect("add child");

        let mut sibling = Node::new(0);
        sibling.labels.push("File".into());
        sibling
            .properties
            .insert("path".into(), PropertyValue::String("src/main.rs".into()));
        let sibling_id = db.add_node(sibling).expect("add sibling");

        let contains_edge_id = db
            .add_edge(Edge::new(0, parent_id, child_id, "CONTAINS"))
            .expect("parent->child");
        db.add_edge(Edge::new(0, parent_id, sibling_id, "CONTAINS"))
            .expect("parent->sibling");

        let mut child_filters = PropertyFilters::default();
        child_filters
            .equals
            .insert("path".into(), PropertyValue::String("src/lib.rs".into()));

        let mut parent_filters = PropertyFilters::default();
        parent_filters
            .not_equals
            .insert("name".into(), PropertyValue::String("test".into()));

        let pattern = Pattern {
            nodes: vec![
                NodePattern {
                    var_name: "file".into(),
                    labels: vec!["File".into()],
                    properties: child_filters,
                },
                NodePattern {
                    var_name: "module".into(),
                    labels: vec!["Module".into(), "Namespace".into()],
                    properties: parent_filters,
                },
            ],
            edges: vec![EdgePattern {
                from_var: "file".into(),
                to_var: "module".into(),
                types: vec!["CONTAINS".into()],
                properties: PropertyFilters::default(),
                direction: EdgeDirection::Incoming,
            }],
        };

        let matches = db.match_pattern(&pattern).expect("match incoming");
        assert_eq!(matches.len(), 1);
        let binding = &matches[0];
        assert_eq!(binding.node_bindings["file"], child_id);
        assert_eq!(binding.node_bindings["module"], parent_id);
        assert_eq!(binding.edge_ids, vec![contains_edge_id]);
    }

    #[test]
    fn match_pattern_applies_property_ranges() {
        let (_guard, path) = temp_db("pattern_ranges");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut root = Node::new(0);
        root.labels.push("File".into());
        root.properties
            .insert("name".into(), PropertyValue::String("a.rs".into()));
        let root_id = db.add_node(root).expect("add root");

        let mut mid = Node::new(0);
        mid.labels.push("File".into());
        mid.properties
            .insert("name".into(), PropertyValue::String("b.rs".into()));
        mid.properties.insert("depth".into(), PropertyValue::Int(3));
        let mid_id = db.add_node(mid).expect("add mid");

        let mut leaf = Node::new(0);
        leaf.labels.push("File".into());
        leaf.properties
            .insert("name".into(), PropertyValue::String("c.rs".into()));
        let leaf_id = db.add_node(leaf).expect("add leaf");

        let mut far_mid = Node::new(0);
        far_mid.labels.push("File".into());
        far_mid
            .properties
            .insert("name".into(), PropertyValue::String("d.rs".into()));
        far_mid
            .properties
            .insert("depth".into(), PropertyValue::Int(7));
        let far_mid_id = db.add_node(far_mid).expect("add far mid");

        let alt_leaf_id = db
            .add_node({
                let mut node = Node::new(0);
                node.labels.push("File".into());
                node.properties
                    .insert("name".into(), PropertyValue::String("alt.rs".into()));
                node
            })
            .expect("add alt leaf");

        let first_edge_id = db
            .add_edge(Edge::new(0, root_id, mid_id, "IMPORTS"))
            .expect("root->mid");
        let second_edge_id = db
            .add_edge(Edge::new(0, mid_id, leaf_id, "IMPORTS"))
            .expect("mid->leaf");

        db.add_edge(Edge::new(0, root_id, far_mid_id, "IMPORTS"))
            .expect("root->far mid");
        db.add_edge(Edge::new(0, far_mid_id, alt_leaf_id, "IMPORTS"))
            .expect("far mid->alt leaf");

        let mut root_filters = PropertyFilters::default();
        root_filters
            .equals
            .insert("name".into(), PropertyValue::String("a.rs".into()));

        let mut mid_filters = PropertyFilters::default();
        mid_filters.ranges.push(PropertyRangeFilter {
            key: "depth".into(),
            min: Some(PropertyBound {
                value: PropertyValue::Int(1),
                inclusive: true,
            }),
            max: Some(PropertyBound {
                value: PropertyValue::Int(5),
                inclusive: false,
            }),
        });

        let mut leaf_filters = PropertyFilters::default();
        leaf_filters
            .equals
            .insert("name".into(), PropertyValue::String("c.rs".into()));

        let pattern = Pattern {
            nodes: vec![
                NodePattern {
                    var_name: "root".into(),
                    labels: vec!["File".into()],
                    properties: root_filters,
                },
                NodePattern {
                    var_name: "mid".into(),
                    labels: vec!["File".into()],
                    properties: mid_filters,
                },
                NodePattern {
                    var_name: "leaf".into(),
                    labels: vec!["File".into()],
                    properties: leaf_filters,
                },
            ],
            edges: vec![
                EdgePattern {
                    from_var: "root".into(),
                    to_var: "mid".into(),
                    types: vec!["IMPORTS".into()],
                    properties: PropertyFilters::default(),
                    direction: EdgeDirection::Outgoing,
                },
                EdgePattern {
                    from_var: "mid".into(),
                    to_var: "leaf".into(),
                    types: vec!["IMPORTS".into()],
                    properties: PropertyFilters::default(),
                    direction: EdgeDirection::Outgoing,
                },
            ],
        };

        let matches = db.match_pattern(&pattern).expect("match range pattern");
        assert_eq!(matches.len(), 1);
        let binding = &matches[0];
        assert_eq!(binding.node_bindings["root"], root_id);
        assert_eq!(binding.node_bindings["mid"], mid_id);
        assert_eq!(binding.node_bindings["leaf"], leaf_id);
        assert_eq!(binding.edge_ids, vec![first_edge_id, second_edge_id]);
    }
}
