use super::graphdb::GraphDB;
use crate::error::Result;
use crate::model::{NodeId, NULL_EDGE_ID};
use std::collections::HashSet;

impl GraphDB {
    pub fn get_neighbors(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        if let Some(neighbors) = self.outgoing_neighbors_cache.get(&node_id) {
            return Ok(neighbors.clone());
        }

        let node = self.get_node(node_id)?;
        let mut neighbors = Vec::new();
        let mut edge_ids = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;
        while edge_id != NULL_EDGE_ID {
            self.metrics.edge_traversals += 1;
            let edge = self.load_edge(edge_id)?;
            neighbors.push(edge.target_node_id);
            edge_ids.push(edge_id);
            edge_id = edge.next_outgoing_edge_id;
        }

        self.outgoing_adjacency.insert(node_id, edge_ids);
        self.outgoing_neighbors_cache
            .insert(node_id, neighbors.clone());
        Ok(neighbors)
    }

    pub fn get_incoming_neighbors(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        if let Some(neighbors) = self.incoming_neighbors_cache.get(&node_id) {
            return Ok(neighbors.clone());
        }

        let node = self.get_node(node_id)?;
        let mut neighbors = Vec::new();
        let mut edge_ids = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;
        while edge_id != NULL_EDGE_ID {
            let edge = self.load_edge(edge_id)?;
            neighbors.push(edge.source_node_id);
            edge_ids.push(edge_id);
            edge_id = edge.next_incoming_edge_id;
        }

        self.incoming_adjacency.insert(node_id, edge_ids);
        self.incoming_neighbors_cache
            .insert(node_id, neighbors.clone());
        Ok(neighbors)
    }

    pub fn get_neighbors_two_hops(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        visited.insert(node_id);

        let first_hop = self.get_neighbors(node_id)?;

        for neighbor_id in &first_hop {
            visited.insert(*neighbor_id);
        }

        let mut all_second_hop_neighbors = HashSet::new();
        for neighbor_id in first_hop {
            let second_hop = self.get_neighbors(neighbor_id)?;
            for second_neighbor_id in second_hop {
                if !visited.contains(&second_neighbor_id) {
                    all_second_hop_neighbors.insert(second_neighbor_id);
                }
            }
        }

        result.extend(all_second_hop_neighbors);
        Ok(result)
    }

    pub fn get_neighbors_three_hops(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        visited.insert(node_id);

        let first_hop = self.get_neighbors(node_id)?;
        for neighbor_id in first_hop {
            visited.insert(neighbor_id);

            let second_hop = self.get_neighbors(neighbor_id)?;
            for second_neighbor_id in second_hop {
                visited.insert(second_neighbor_id);

                let third_hop = self.get_neighbors(second_neighbor_id)?;
                for third_neighbor_id in third_hop {
                    if visited.insert(third_neighbor_id) {
                        result.push(third_neighbor_id);
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn bfs_traversal(
        &mut self,
        start_node_id: NodeId,
        max_depth: usize,
    ) -> Result<Vec<(NodeId, usize)>> {
        let mut visited = HashSet::new();
        let mut current_level = vec![start_node_id];
        let mut result = Vec::new();

        visited.insert(start_node_id);

        for depth in 0..max_depth {
            let mut next_level = Vec::new();

            for node_id in &current_level {
                result.push((*node_id, depth));
            }

            for node_id in current_level.drain(..) {
                let neighbors = self.get_neighbors_fast(node_id)?;
                for target in neighbors {
                    if visited.insert(target) {
                        next_level.push(target);
                    }
                }
            }

            if next_level.is_empty() {
                break;
            }
            current_level = next_level;
        }

        Ok(result)
    }

    fn get_neighbors_fast(&mut self, node_id: NodeId) -> Result<Vec<NodeId>> {
        if let Some(neighbors) = self.outgoing_neighbors_cache.get(&node_id) {
            return Ok(neighbors.clone());
        }

        let node = self.get_node(node_id)?;
        let mut neighbors = Vec::new();
        let mut edge_ids = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;

        while edge_id != NULL_EDGE_ID {
            self.metrics.edge_traversals += 1;
            let edge = self.load_edge(edge_id)?;
            neighbors.push(edge.target_node_id);
            edge_ids.push(edge_id);
            edge_id = edge.next_outgoing_edge_id;
        }

        self.outgoing_adjacency.insert(node_id, edge_ids);
        self.outgoing_neighbors_cache
            .insert(node_id, neighbors.clone());
        Ok(neighbors)
    }
}
