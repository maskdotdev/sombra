use crate::db::core::GraphDB;
use crate::error::Result;
use crate::model::{NodeId, NULL_EDGE_ID};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegreeType {
    In,
    Out,
    Total,
}

#[derive(Debug, Clone)]
pub struct DegreeDistribution {
    pub in_degree: HashMap<NodeId, usize>,
    pub out_degree: HashMap<NodeId, usize>,
    pub total_degree: HashMap<NodeId, usize>,
}

impl DegreeDistribution {
    pub fn new() -> Self {
        Self {
            in_degree: HashMap::new(),
            out_degree: HashMap::new(),
            total_degree: HashMap::new(),
        }
    }
}

impl Default for DegreeDistribution {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphDB {
    pub fn count_nodes_by_label(&self) -> HashMap<String, usize> {
        self.label_index
            .iter()
            .map(|(label, nodes)| (label.clone(), nodes.len()))
            .collect()
    }

    pub fn count_edges_by_type(&mut self) -> Result<HashMap<String, usize>> {
        let mut counts: HashMap<String, usize> = HashMap::new();

        let edge_ids: Vec<_> = self.edge_index.keys().copied().collect();
        for edge_id in edge_ids {
            let edge = self.load_edge(edge_id)?;
            *counts.entry(edge.type_name.clone()).or_insert(0) += 1;
        }

        Ok(counts)
    }

    pub fn get_total_node_count(&self) -> usize {
        self.node_index.len()
    }

    pub fn get_total_edge_count(&self) -> usize {
        self.edge_index.len()
    }

    pub fn degree_distribution(&mut self) -> Result<DegreeDistribution> {
        let mut dist = DegreeDistribution::new();

        let all_node_ids: Vec<NodeId> = self
            .node_index
            .iter()
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        for &node_id in &all_node_ids {
            let node = self.get_node(node_id)?;

            let mut out_degree = 0;
            let mut edge_id = node.first_outgoing_edge_id;
            while edge_id != NULL_EDGE_ID {
                out_degree += 1;
                let edge = self.load_edge(edge_id)?;
                edge_id = edge.next_outgoing_edge_id;
            }

            let mut in_degree = 0;
            let mut edge_id = node.first_incoming_edge_id;
            while edge_id != NULL_EDGE_ID {
                in_degree += 1;
                let edge = self.load_edge(edge_id)?;
                edge_id = edge.next_incoming_edge_id;
            }

            dist.in_degree.insert(node_id, in_degree);
            dist.out_degree.insert(node_id, out_degree);
            dist.total_degree.insert(node_id, in_degree + out_degree);
        }

        Ok(dist)
    }

    pub fn find_hubs(
        &mut self,
        min_degree: usize,
        degree_type: DegreeType,
    ) -> Result<Vec<(NodeId, usize)>> {
        let dist = self.degree_distribution()?;

        let degree_map = match degree_type {
            DegreeType::In => &dist.in_degree,
            DegreeType::Out => &dist.out_degree,
            DegreeType::Total => &dist.total_degree,
        };

        let mut hubs: Vec<(NodeId, usize)> = degree_map
            .iter()
            .filter(|(_, &degree)| degree >= min_degree)
            .map(|(&node_id, &degree)| (node_id, degree))
            .collect();

        hubs.sort_by(|a, b| b.1.cmp(&a.1));

        Ok(hubs)
    }

    pub fn find_isolated_nodes(&mut self) -> Result<Vec<NodeId>> {
        let dist = self.degree_distribution()?;

        let isolated: Vec<NodeId> = dist
            .total_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(&node_id, _)| node_id)
            .collect();

        Ok(isolated)
    }

    pub fn find_leaf_nodes(
        &mut self,
        direction: crate::model::EdgeDirection,
    ) -> Result<Vec<NodeId>> {
        let dist = self.degree_distribution()?;

        let leaves: Vec<NodeId> = match direction {
            crate::model::EdgeDirection::Outgoing => dist
                .out_degree
                .iter()
                .filter(|(_, &degree)| degree == 0)
                .map(|(&node_id, _)| node_id)
                .collect(),
            crate::model::EdgeDirection::Incoming => dist
                .in_degree
                .iter()
                .filter(|(_, &degree)| degree == 0)
                .map(|(&node_id, _)| node_id)
                .collect(),
            crate::model::EdgeDirection::Both => dist
                .total_degree
                .iter()
                .filter(|(node_id, &degree)| {
                    degree > 0
                        && (dist.in_degree.get(node_id).copied().unwrap_or(0) == 0
                            || dist.out_degree.get(node_id).copied().unwrap_or(0) == 0)
                })
                .map(|(&node_id, _)| node_id)
                .collect(),
        };

        Ok(leaves)
    }

    pub fn get_average_degree(&mut self) -> Result<f64> {
        let dist = self.degree_distribution()?;

        if dist.total_degree.is_empty() {
            return Ok(0.0);
        }

        let total: usize = dist.total_degree.values().sum();
        let count = dist.total_degree.len();

        Ok(total as f64 / count as f64)
    }

    pub fn get_density(&mut self) -> Result<f64> {
        let node_count = self.get_total_node_count();
        let edge_count = self.get_total_edge_count();

        if node_count <= 1 {
            return Ok(0.0);
        }

        let max_edges = node_count * (node_count - 1);
        Ok(edge_count as f64 / max_edges as f64)
    }

    pub fn count_nodes_with_label(&self, label: &str) -> usize {
        self.label_index
            .get(label)
            .map(|nodes| nodes.len())
            .unwrap_or(0)
    }

    pub fn count_edges_with_type(&mut self, edge_type: &str) -> Result<usize> {
        let mut count = 0;

        let edge_ids: Vec<_> = self.edge_index.keys().copied().collect();
        for edge_id in edge_ids {
            let edge = self.load_edge(edge_id)?;
            if edge.type_name == edge_type {
                count += 1;
            }
        }

        Ok(count)
    }

    pub fn get_label_statistics(&self) -> Vec<(String, usize)> {
        let mut stats: Vec<(String, usize)> = self
            .label_index
            .iter()
            .map(|(label, nodes)| (label.clone(), nodes.len()))
            .collect();

        stats.sort_by(|a, b| b.1.cmp(&a.1));
        stats
    }

    pub fn get_edge_type_statistics(&mut self) -> Result<Vec<(String, usize)>> {
        let counts = self.count_edges_by_type()?;

        let mut stats: Vec<(String, usize)> = counts.into_iter().collect();
        stats.sort_by(|a, b| b.1.cmp(&a.1));

        Ok(stats)
    }

    pub fn get_degree_statistics(&mut self) -> Result<DegreeStatistics> {
        let dist = self.degree_distribution()?;

        if dist.total_degree.is_empty() {
            return Ok(DegreeStatistics::default());
        }

        let total_degrees: Vec<usize> = dist.total_degree.values().copied().collect();
        let in_degrees: Vec<usize> = dist.in_degree.values().copied().collect();
        let out_degrees: Vec<usize> = dist.out_degree.values().copied().collect();

        Ok(DegreeStatistics {
            min_total: *total_degrees.iter().min().unwrap_or(&0),
            max_total: *total_degrees.iter().max().unwrap_or(&0),
            avg_total: total_degrees.iter().sum::<usize>() as f64 / total_degrees.len() as f64,
            min_in: *in_degrees.iter().min().unwrap_or(&0),
            max_in: *in_degrees.iter().max().unwrap_or(&0),
            avg_in: in_degrees.iter().sum::<usize>() as f64 / in_degrees.len() as f64,
            min_out: *out_degrees.iter().min().unwrap_or(&0),
            max_out: *out_degrees.iter().max().unwrap_or(&0),
            avg_out: out_degrees.iter().sum::<usize>() as f64 / out_degrees.len() as f64,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DegreeStatistics {
    pub min_total: usize,
    pub max_total: usize,
    pub avg_total: f64,
    pub min_in: usize,
    pub max_in: usize,
    pub avg_in: f64,
    pub min_out: usize,
    pub max_out: usize,
    pub avg_out: f64,
}

impl Default for DegreeStatistics {
    fn default() -> Self {
        Self {
            min_total: 0,
            max_total: 0,
            avg_total: 0.0,
            min_in: 0,
            max_in: 0,
            avg_in: 0.0,
            min_out: 0,
            max_out: 0,
            avg_out: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Edge, Node};
    use tempfile::NamedTempFile;

    fn create_test_db() -> (GraphDB, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let db = GraphDB::open(temp_file.path()).unwrap();
        (db, temp_file)
    }

    #[test]
    fn test_count_nodes_by_label() {
        let (mut db, _temp) = create_test_db();

        let mut node1 = Node::new(0);
        node1.labels.push("Person".to_string());
        db.add_node(node1).unwrap();

        let mut node2 = Node::new(0);
        node2.labels.push("Person".to_string());
        db.add_node(node2).unwrap();

        let mut node3 = Node::new(0);
        node3.labels.push("Company".to_string());
        db.add_node(node3).unwrap();

        let counts = db.count_nodes_by_label();
        assert_eq!(counts.get("Person"), Some(&2));
        assert_eq!(counts.get("Company"), Some(&1));
    }

    #[test]
    fn test_count_edges_by_type() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node3_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node2_id, node3_id, "WORKS_WITH"))
            .unwrap();

        let counts = db.count_edges_by_type().unwrap();
        assert_eq!(counts.get("KNOWS"), Some(&2));
        assert_eq!(counts.get("WORKS_WITH"), Some(&1));
    }

    #[test]
    fn test_degree_distribution() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node3_id, "KNOWS"))
            .unwrap();

        let dist = db.degree_distribution().unwrap();

        assert_eq!(dist.out_degree.get(&node1_id), Some(&2));
        assert_eq!(dist.in_degree.get(&node1_id), Some(&0));
        assert_eq!(dist.total_degree.get(&node1_id), Some(&2));

        assert_eq!(dist.out_degree.get(&node2_id), Some(&0));
        assert_eq!(dist.in_degree.get(&node2_id), Some(&1));
        assert_eq!(dist.total_degree.get(&node2_id), Some(&1));

        assert_eq!(dist.out_degree.get(&node3_id), Some(&0));
        assert_eq!(dist.in_degree.get(&node3_id), Some(&1));
        assert_eq!(dist.total_degree.get(&node3_id), Some(&1));
    }

    #[test]
    fn test_find_hubs() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();
        let node4_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node3_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node4_id, "KNOWS"))
            .unwrap();

        let hubs = db.find_hubs(2, DegreeType::Out).unwrap();
        assert_eq!(hubs.len(), 1);
        assert_eq!(hubs[0].0, node1_id);
        assert_eq!(hubs[0].1, 3);
    }

    #[test]
    fn test_find_isolated_nodes() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();

        let isolated = db.find_isolated_nodes().unwrap();
        assert_eq!(isolated.len(), 1);
        assert!(isolated.contains(&node3_id));
    }

    #[test]
    fn test_find_leaf_nodes() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node3_id, "KNOWS"))
            .unwrap();

        let leaves = db
            .find_leaf_nodes(crate::model::EdgeDirection::Outgoing)
            .unwrap();
        assert_eq!(leaves.len(), 2);
        assert!(leaves.contains(&node2_id));
        assert!(leaves.contains(&node3_id));
    }

    #[test]
    fn test_average_degree() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node2_id, node3_id, "KNOWS"))
            .unwrap();

        let avg = db.get_average_degree().unwrap();
        assert!((avg - 1.333).abs() < 0.01);
    }

    #[test]
    fn test_get_density() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let _node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();

        let density = db.get_density().unwrap();
        assert!((density - 0.1666).abs() < 0.01);
    }

    #[test]
    fn test_label_statistics() {
        let (mut db, _temp) = create_test_db();

        let mut node1 = Node::new(0);
        node1.labels.push("Person".to_string());
        db.add_node(node1).unwrap();

        let mut node2 = Node::new(0);
        node2.labels.push("Person".to_string());
        db.add_node(node2).unwrap();

        let mut node3 = Node::new(0);
        node3.labels.push("Company".to_string());
        db.add_node(node3).unwrap();

        let stats = db.get_label_statistics();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0], ("Person".to_string(), 2));
        assert_eq!(stats[1], ("Company".to_string(), 1));
    }

    #[test]
    fn test_degree_statistics() {
        let (mut db, _temp) = create_test_db();

        let node1_id = db.add_node(Node::new(0)).unwrap();
        let node2_id = db.add_node(Node::new(0)).unwrap();
        let node3_id = db.add_node(Node::new(0)).unwrap();

        db.add_edge(Edge::new(0, node1_id, node2_id, "KNOWS"))
            .unwrap();
        db.add_edge(Edge::new(0, node1_id, node3_id, "KNOWS"))
            .unwrap();

        let stats = db.get_degree_statistics().unwrap();
        assert_eq!(stats.min_total, 1);
        assert_eq!(stats.max_total, 2);
        assert_eq!(stats.max_out, 2);
        assert_eq!(stats.max_in, 1);
    }
}
