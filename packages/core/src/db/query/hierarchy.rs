use std::collections::{HashSet, VecDeque};

use crate::db::GraphDB;
use crate::error::{GraphError, Result};
use crate::model::{EdgeDirection, NodeId};

impl GraphDB {
    /// Finds the nearest ancestor reachable via edges of the provided type that carries a label.
    ///
    /// This performs a breadth-first search that walks incoming edges of `edge_type`
    /// starting from `start` until it encounters a node whose labels contain `label`.
    /// The first match (closest ancestor) is returned.
    ///
    /// # Arguments
    /// * `start` - Node to begin searching from.
    /// * `label` - Target label to match on ancestor nodes (case-sensitive).
    /// * `edge_type` - Edge type to follow when traversing towards parents (e.g. `"CONTAINS"`).
    ///
    /// # Returns
    /// * `Ok(Some(node_id))` when a matching ancestor is found.
    /// * `Ok(None)` when no ancestor with the label exists via the given edge type.
    ///
    /// # Errors
    /// Propagates underlying storage errors such as [`GraphError::NotFound`] if the starting node
    /// or traversed edges are missing, and I/O errors emitted by the pager layer.
    ///
    /// # Time Complexity
    /// O(A) where A is the number of ancestors examined (bounded by reachable nodes via `edge_type`).
    ///
    /// # Space Complexity
    /// O(A) for the visited set and traversal queue.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(path.path())?;
    ///
    /// let mut file = Node::new(0);
    /// file.labels.push("File".into());
    /// let file_id = db.add_node(file)?;
    ///
    /// let mut func = Node::new(0);
    /// func.labels.push("Function".into());
    /// let func_id = db.add_node(func)?;
    ///
    /// db.add_edge(Edge::new(0, file_id, func_id, "CONTAINS"))?;
    ///
    /// let ancestor = db.find_ancestor_by_label(func_id, "File", "CONTAINS")?;
    /// assert_eq!(ancestor, Some(file_id));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::get_ancestors`] - Enumerates all ancestors up to an optional depth.
    /// * [`Self::get_containing_file`] - Convenience wrapper for `"File"` nodes.
    pub fn find_ancestor_by_label(
        &mut self,
        start: NodeId,
        label: &str,
        edge_type: &str,
    ) -> Result<Option<NodeId>> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let edge_types = [edge_type];

        visited.insert(start);
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            for parent in
                self.get_neighbors_by_edge_type(current, &edge_types, EdgeDirection::Incoming)?
            {
                if !visited.insert(parent) {
                    continue;
                }

                let node = self.get_node(parent)?;
                if node.labels.iter().any(|candidate| candidate == label) {
                    return Ok(Some(parent));
                }

                queue.push_back(parent);
            }
        }

        Ok(None)
    }

    /// Collects all ancestors reachable via edges of the provided type.
    ///
    /// The ancestors are returned in breadth-first order, starting with the closest parent.
    /// An optional depth limit (expressed in number of hops) may be supplied to bound the traversal.
    ///
    /// # Arguments
    /// * `start` - Node whose ancestors should be collected.
    /// * `edge_type` - Edge type used to walk towards parents (e.g. `"CONTAINS"`).
    /// * `max_depth` - Optional hop limit (1 for direct parents, 2 for grandparents, etc).
    ///
    /// # Returns
    /// * `Ok(Vec<NodeId>)` containing ancestors ordered by increasing distance from `start`.
    ///
    /// # Errors
    /// Propagates storage-level errors such as [`GraphError::NotFound`] when traversed nodes
    /// or edges are missing, and I/O errors from the pager.
    ///
    /// # Time Complexity
    /// O(A) where A is the number of ancestors discovered (respecting `max_depth` if provided).
    ///
    /// # Space Complexity
    /// O(A) for the visited set, output vector, and traversal queue.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(path.path())?;
    ///
    /// let mut file = Node::new(0);
    /// file.labels.push("File".into());
    /// let file_id = db.add_node(file)?;
    ///
    /// let mut class = Node::new(0);
    /// class.labels.push("Class".into());
    /// let class_id = db.add_node(class)?;
    ///
    /// let mut func = Node::new(0);
    /// func.labels.push("Function".into());
    /// let func_id = db.add_node(func)?;
    ///
    /// db.add_edge(Edge::new(0, file_id, class_id, "CONTAINS"))?;
    /// db.add_edge(Edge::new(0, class_id, func_id, "CONTAINS"))?;
    ///
    /// let ancestors = db.get_ancestors(func_id, "CONTAINS", Some(2))?;
    /// assert_eq!(ancestors, vec![class_id, file_id]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::find_ancestor_by_label`] - Finds a single ancestor that matches a label.
    /// * [`Self::get_descendants`] - Performs the inverse traversal towards children.
    pub fn get_ancestors(
        &mut self,
        start: NodeId,
        edge_type: &str,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>> {
        let mut ancestors = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let edge_types = [edge_type];

        visited.insert(start);
        queue.push_back((start, 0usize));

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(limit) = max_depth {
                if depth >= limit {
                    continue;
                }
            }

            for parent in
                self.get_neighbors_by_edge_type(current, &edge_types, EdgeDirection::Incoming)?
            {
                if visited.insert(parent) {
                    ancestors.push(parent);
                    queue.push_back((parent, depth + 1));
                }
            }
        }

        Ok(ancestors)
    }

    /// Enumerates descendants reachable via edges of the provided type using breadth-first order.
    ///
    /// Descendants are returned closest-first. Supply `max_depth` to cap the expansion radius.
    ///
    /// # Arguments
    /// * `start` - Node whose descendants should be traversed.
    /// * `edge_type` - Edge type to follow towards children (e.g. `"CONTAINS"`).
    /// * `max_depth` - Optional hop limit (1 for direct children, 2 for grandchildren, etc).
    ///
    /// # Returns
    /// * `Ok(Vec<NodeId>)` containing descendant node IDs ordered by increasing distance.
    ///
    /// # Errors
    /// Propagates underlying [`GraphError`] variants when nodes or edges are missing, as well as
    /// pager-related I/O failures.
    ///
    /// # Time Complexity
    /// O(D) where D is the number of descendants visited (bounded by `max_depth` when provided).
    ///
    /// # Space Complexity
    /// O(D) for the visited set, traversal queue, and result vector.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(path.path())?;
    ///
    /// let mut file = Node::new(0);
    /// file.labels.push("File".into());
    /// let file_id = db.add_node(file)?;
    ///
    /// let mut class = Node::new(0);
    /// class.labels.push("Class".into());
    /// let class_id = db.add_node(class)?;
    ///
    /// let mut method = Node::new(0);
    /// method.labels.push("Function".into());
    /// let method_id = db.add_node(method)?;
    ///
    /// db.add_edge(Edge::new(0, file_id, class_id, "CONTAINS"))?;
    /// db.add_edge(Edge::new(0, class_id, method_id, "CONTAINS"))?;
    ///
    /// let descendants = db.get_descendants(file_id, "CONTAINS", None)?;
    /// assert_eq!(descendants, vec![class_id, method_id]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::get_ancestors`] - Traverses in the opposite direction.
    /// * [`Self::get_containing_file`] - Utility for locating enclosing `"File"` nodes.
    pub fn get_descendants(
        &mut self,
        start: NodeId,
        edge_type: &str,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeId>> {
        let mut descendants = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let edge_types = [edge_type];

        visited.insert(start);
        queue.push_back((start, 0usize));

        while let Some((current, depth)) = queue.pop_front() {
            if let Some(limit) = max_depth {
                if depth >= limit {
                    continue;
                }
            }

            for child in
                self.get_neighbors_by_edge_type(current, &edge_types, EdgeDirection::Outgoing)?
            {
                if visited.insert(child) {
                    descendants.push(child);
                    queue.push_back((child, depth + 1));
                }
            }
        }

        Ok(descendants)
    }

    /// Returns the file node that directly or transitively contains the given node.
    ///
    /// This is a convenience wrapper around [`Self::find_ancestor_by_label`] that searches
    /// for a `"File"` node walking `"CONTAINS"` edges.
    ///
    /// # Arguments
    /// * `node_id` - Node whose containing file is required.
    ///
    /// # Returns
    /// * `Ok(node_id)` - Identifier of the containing file node.
    ///
    /// # Errors
    /// * [`GraphError::NotFound`] if no containing file exists along `"CONTAINS"` edges.
    /// * Propagates storage errors (missing nodes/edges, I/O failures).
    ///
    /// # Time Complexity
    /// O(A) where A is the number of ancestors examined until a `"File"` label is found.
    ///
    /// # Space Complexity
    /// O(A) for traversal bookkeeping.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sombra::{GraphDB, Node, Edge, GraphError};
    /// # use tempfile::NamedTempFile;
    /// # fn main() -> Result<(), GraphError> {
    /// let path = NamedTempFile::new()?;
    /// let mut db = GraphDB::open(path.path())?;
    ///
    /// let mut file = Node::new(0);
    /// file.labels.push("File".into());
    /// let file_id = db.add_node(file)?;
    ///
    /// let func_id = db.add_node(Node::new(0))?;
    /// db.add_edge(Edge::new(0, file_id, func_id, "CONTAINS"))?;
    ///
    /// let containing_file = db.get_containing_file(func_id)?;
    /// assert_eq!(containing_file, file_id);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See Also
    /// * [`Self::find_ancestor_by_label`] - Underlying search routine.
    /// * [`Self::get_ancestors`] - Retrieves every ancestor instead of one.
    pub fn get_containing_file(&mut self, node_id: NodeId) -> Result<NodeId> {
        match self.find_ancestor_by_label(node_id, "File", "CONTAINS")? {
            Some(file_id) => Ok(file_id),
            None => Err(GraphError::NotFound("containing file")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Edge, Node};

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
    fn find_ancestor_by_label_returns_closest_match() {
        let (_guard, path) = temp_db("hierarchy_find_ancestor");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut file = Node::new(0);
        file.labels.push("File".to_string());
        let file_id = db.add_node(file).expect("add file");

        let mut module = Node::new(0);
        module.labels.push("Module".to_string());
        let module_id = db.add_node(module).expect("add module");

        let mut class = Node::new(0);
        class.labels.push("Class".to_string());
        let class_id = db.add_node(class).expect("add class");

        let mut func = Node::new(0);
        func.labels.push("Function".to_string());
        let func_id = db.add_node(func).expect("add function");

        db.add_edge(Edge::new(0, file_id, module_id, "CONTAINS"))
            .expect("add file->module");
        db.add_edge(Edge::new(0, module_id, class_id, "CONTAINS"))
            .expect("add module->class");
        db.add_edge(Edge::new(0, class_id, func_id, "CONTAINS"))
            .expect("add class->func");

        let file_ancestor = db
            .find_ancestor_by_label(func_id, "File", "CONTAINS")
            .expect("find file ancestor");
        assert_eq!(file_ancestor, Some(file_id));

        let module_ancestor = db
            .find_ancestor_by_label(func_id, "Module", "CONTAINS")
            .expect("find module ancestor");
        assert_eq!(module_ancestor, Some(module_id));

        let missing_ancestor = db
            .find_ancestor_by_label(func_id, "Interface", "CONTAINS")
            .expect("find missing ancestor");
        assert!(missing_ancestor.is_none());

        let containing_file = db
            .get_containing_file(func_id)
            .expect("get containing file");
        assert_eq!(containing_file, file_id);
    }

    #[test]
    fn get_ancestors_respects_depth_limit() {
        let (_guard, path) = temp_db("hierarchy_get_ancestors");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut root = Node::new(0);
        root.labels.push("File".to_string());
        let root_id = db.add_node(root).expect("add root");

        let mut module = Node::new(0);
        module.labels.push("Module".to_string());
        let module_id = db.add_node(module).expect("add module");

        let mut class = Node::new(0);
        class.labels.push("Class".to_string());
        let class_id = db.add_node(class).expect("add class");

        let mut func = Node::new(0);
        func.labels.push("Function".to_string());
        let func_id = db.add_node(func).expect("add function");

        db.add_edge(Edge::new(0, root_id, module_id, "CONTAINS"))
            .expect("add root->module");
        db.add_edge(Edge::new(0, module_id, class_id, "CONTAINS"))
            .expect("add module->class");
        db.add_edge(Edge::new(0, class_id, func_id, "CONTAINS"))
            .expect("add class->func");

        let ancestors = db
            .get_ancestors(func_id, "CONTAINS", None)
            .expect("get all ancestors");
        assert_eq!(ancestors, vec![class_id, module_id, root_id]);

        let limited = db
            .get_ancestors(func_id, "CONTAINS", Some(2))
            .expect("get ancestors limited");
        assert_eq!(limited, vec![class_id, module_id]);
    }

    #[test]
    fn get_descendants_respects_depth_limit() {
        let (_guard, path) = temp_db("hierarchy_get_descendants");
        let mut db = GraphDB::open(&path).expect("open db");

        let mut file = Node::new(0);
        file.labels.push("File".to_string());
        let file_id = db.add_node(file).expect("add file");

        let mut class_a = Node::new(0);
        class_a.labels.push("Class".to_string());
        let class_a_id = db.add_node(class_a).expect("add class A");

        let mut class_b = Node::new(0);
        class_b.labels.push("Class".to_string());
        let class_b_id = db.add_node(class_b).expect("add class B");

        let method_a_id = db.add_node(Node::new(0)).expect("add method A");
        let method_b_id = db.add_node(Node::new(0)).expect("add method B");

        db.add_edge(Edge::new(0, file_id, class_a_id, "CONTAINS"))
            .expect("add file->class A");
        db.add_edge(Edge::new(0, file_id, class_b_id, "CONTAINS"))
            .expect("add file->class B");
        db.add_edge(Edge::new(0, class_a_id, method_a_id, "CONTAINS"))
            .expect("add class A->method A");
        db.add_edge(Edge::new(0, class_b_id, method_b_id, "CONTAINS"))
            .expect("add class B->method B");

        let mut descendants = db
            .get_descendants(file_id, "CONTAINS", None)
            .expect("get descendants");
        descendants.sort();
        let mut expected = vec![class_a_id, class_b_id, method_a_id, method_b_id];
        expected.sort();
        assert_eq!(descendants, expected);

        let mut limited = db
            .get_descendants(file_id, "CONTAINS", Some(1))
            .expect("get descendants limited");
        limited.sort();
        let mut expected_limited = vec![class_a_id, class_b_id];
        expected_limited.sort();
        assert_eq!(limited, expected_limited);
    }

    #[test]
    fn get_containing_file_reports_missing() {
        let (_guard, path) = temp_db("hierarchy_missing_file");
        let mut db = GraphDB::open(&path).expect("open db");

        let func_id = db.add_node(Node::new(0)).expect("add function");

        let err = db.get_containing_file(func_id).expect_err("missing file");
        assert!(matches!(err, GraphError::NotFound("containing file")));
    }
}
