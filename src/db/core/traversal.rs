use super::graphdb::GraphDB;
use crate::error::Result;
use crate::model::{NodeId, NULL_EDGE_ID};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

    pub fn parallel_bfs(
        &mut self,
        start_node_id: NodeId,
        max_depth: usize,
    ) -> Result<Vec<(NodeId, usize)>> {
        let mut visited = HashSet::new();
        let mut current_level = vec![start_node_id];
        let mut result = Vec::new();
        let mut adjacency_cache = HashMap::new();

        visited.insert(start_node_id);

        for depth in 0..max_depth {
            result.extend(current_level.iter().map(|&node_id| (node_id, depth)));

            let neighbor_lists =
                self.collect_neighbors_for_frontier(&current_level, &mut adjacency_cache)?;
            let total_neighbors: usize = neighbor_lists.iter().map(|n| n.len()).sum();

            let next_candidates: Vec<NodeId> =
                if self.should_parallelize_frontier(current_level.len(), total_neighbors) {
                    neighbor_lists
                        .into_par_iter()
                        .flat_map(|neighbors| neighbors)
                        .collect()
                } else {
                    neighbor_lists.into_iter().flatten().collect()
                };

            let mut next_level = Vec::new();
            for node_id in next_candidates {
                if visited.insert(node_id) {
                    next_level.push(node_id);
                }
            }

            if next_level.is_empty() {
                break;
            }
            current_level = next_level;
        }

        Ok(result)
    }

    pub fn parallel_multi_hop_neighbors(
        &mut self,
        node_ids: &[NodeId],
        hops: usize,
    ) -> Result<HashMap<NodeId, Vec<NodeId>>> {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }

        if hops == 0 {
            return Ok(node_ids
                .iter()
                .copied()
                .map(|node_id| (node_id, Vec::new()))
                .collect());
        }

        let mut adjacency_cache = HashMap::new();
        let snapshot = self.build_snapshot_for_roots(node_ids, hops, &mut adjacency_cache)?;
        let snapshot = Arc::new(snapshot);
        let should_parallelize = self.should_parallelize_frontier(node_ids.len(), snapshot.len());

        if should_parallelize {
            Ok(node_ids
                .par_iter()
                .map(|&node_id| {
                    let neighbors = Self::multi_hop_from_snapshot(snapshot.as_ref(), node_id, hops);
                    (node_id, neighbors)
                })
                .collect())
        } else {
            Ok(node_ids
                .iter()
                .map(|&node_id| {
                    let neighbors = Self::multi_hop_from_snapshot(snapshot.as_ref(), node_id, hops);
                    (node_id, neighbors)
                })
                .collect())
        }
    }

    fn collect_neighbors_for_frontier(
        &mut self,
        frontier: &[NodeId],
        cache: &mut HashMap<NodeId, Vec<NodeId>>,
    ) -> Result<Vec<Vec<NodeId>>> {
        let mut neighbor_lists = Vec::with_capacity(frontier.len());
        for &node_id in frontier {
            if let Some(neighbors) = cache.get(&node_id) {
                neighbor_lists.push(neighbors.clone());
            } else {
                let neighbors = self.get_neighbors_fast(node_id)?;
                cache.insert(node_id, neighbors.clone());
                neighbor_lists.push(neighbors);
            }
        }
        Ok(neighbor_lists)
    }

    fn build_snapshot_for_roots(
        &mut self,
        roots: &[NodeId],
        hops: usize,
        cache: &mut HashMap<NodeId, Vec<NodeId>>,
    ) -> Result<HashMap<NodeId, Vec<NodeId>>> {
        let mut visited: HashSet<NodeId> = roots.iter().copied().collect();
        let mut frontier: Vec<NodeId> = roots.to_vec();

        for _ in 0..hops {
            if frontier.is_empty() {
                break;
            }

            let neighbor_lists = self.collect_neighbors_for_frontier(&frontier, cache)?;
            let mut next_frontier = Vec::new();

            for neighbors in neighbor_lists {
                for neighbor in neighbors {
                    if visited.insert(neighbor) {
                        next_frontier.push(neighbor);
                    }
                }
            }

            frontier = next_frontier;
        }

        Ok(cache.clone())
    }

    fn should_parallelize_traversal(&self, workload: usize) -> bool {
        workload >= self.config.parallel_traversal_threshold
    }

    fn should_parallelize_frontier(&self, frontier_len: usize, total_neighbors: usize) -> bool {
        self.should_parallelize_traversal(frontier_len)
            || self.should_parallelize_traversal(total_neighbors)
    }

    fn multi_hop_from_snapshot(
        snapshot: &HashMap<NodeId, Vec<NodeId>>,
        start: NodeId,
        hops: usize,
    ) -> Vec<NodeId> {
        if hops == 0 {
            return Vec::new();
        }

        let mut visited = HashSet::new();
        let mut current_level = vec![start];
        visited.insert(start);

        for _ in 0..hops {
            let mut next_level = Vec::new();
            for node_id in &current_level {
                if let Some(neighbors) = snapshot.get(node_id) {
                    for &neighbor in neighbors {
                        if visited.insert(neighbor) {
                            next_level.push(neighbor);
                        }
                    }
                }
            }

            if next_level.is_empty() {
                break;
            }
            current_level = next_level;
        }

        visited.remove(&start);
        visited.into_iter().collect()
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
