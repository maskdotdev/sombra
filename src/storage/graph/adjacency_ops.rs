use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashSet, VecDeque};

#[cfg(feature = "degree-cache")]
use std::collections::HashMap;
use std::ops::Bound;

use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, PutItem};
use crate::storage::mvcc::{CommitId, VersionedValue, COMMIT_MAX};
use crate::storage::options::AdjacencyBackend;
use crate::storage::{
    profile_timer, record_flush_adj_entries, record_flush_adj_fwd_put, record_flush_adj_fwd_sort,
    record_flush_adj_key_encode, record_flush_adj_rev_put, record_flush_adj_rev_sort,
};
use crate::types::{EdgeId, NodeId, PageId, Result, SombraError, TypeId};

use super::node::{
    self,
    EncodeOpts as NodeEncodeOpts,
    InlineAdjEntry as NodeInlineAdjEntry,
    InlineNodeAdj,
    PropPayload as NodePropPayload,
    DIR_IN,
    DIR_OUT,
};

#[cfg(feature = "degree-cache")]
use super::adjacency::DegreeDir;

use super::adjacency::{self, Dir, ExpandOpts, Neighbor, NeighborCursor};
use super::edge::PropStorage as EdgePropStorage;
use super::graph_types::{BfsOptions, BfsVisit, RootKind};
use super::{Graph, UnitValue};

impl Graph {
    /// Returns neighboring nodes of a given node based on direction and edge type.
    pub fn neighbors(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        opts: ExpandOpts,
    ) -> Result<NeighborCursor> {
        // Use IFA read path if in IfaOnly mode
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            return self.neighbors_ifa(tx, id, dir, ty, opts);
        }

        // Default B-tree path
        let mut neighbors: Vec<Neighbor> = Vec::new();
        let enable_distinct = opts.distinct_nodes || self.distinct_neighbors_default;
        let mut seen_set = enable_distinct.then(HashSet::new);
        if dir.includes_out() {
            self.metrics.adjacency_scan("out");
            self.collect_neighbors(tx, id, ty, true, seen_set.as_mut(), &mut neighbors)?;
        }
        if dir.includes_in() {
            self.metrics.adjacency_scan("in");
            self.collect_neighbors(tx, id, ty, false, seen_set.as_mut(), &mut neighbors)?;
        }
        Ok(NeighborCursor::new(neighbors))
    }

    /// IFA-based neighbor lookup for IfaOnly mode.
    ///
    /// This method supports two paths:
    /// 1. True IFA: If node has `adj_page`, read adjacency directly from that page (O(1))
    /// 2. Fallback: Use B-tree-based IFA store (legacy path)
    fn neighbors_ifa(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        opts: ExpandOpts,
    ) -> Result<NeighborCursor> {
        // Use the cached version with no pre-fetched adj_page
        self.neighbors_ifa_cached(tx, id, dir, ty, opts, None)
    }

    /// IFA-based neighbor lookup with optional pre-fetched adj_page_id.
    ///
    /// If `cached_adj_page` is Some, skips the node lookup and uses the provided page ID.
    /// This is useful for BFS and other traversals where we've already looked up the node.
    fn neighbors_ifa_cached(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        opts: ExpandOpts,
        cached_adj_page: Option<PageId>,
    ) -> Result<NeighborCursor> {
        let ifa = self.ifa.as_ref().ok_or(SombraError::Invalid(
            "IFA not initialized but IfaOnly mode selected",
        ))?;

        let snapshot = Self::reader_snapshot_commit(tx);
        let enable_distinct = opts.distinct_nodes || self.distinct_neighbors_default;
        let mut seen_set = enable_distinct.then(HashSet::new);

        // If we don't have a cached adjacency page, prefer inline adjacency
        // stored directly in the node row.
        if cached_adj_page.is_none() {
            if let Some(versioned) = self.visible_node(tx, id)? {
                if let Some(inline_adj) = &versioned.row.inline_adj {
                    let mut neighbors: Vec<Neighbor> = Vec::new();
                    if dir.includes_out() {
                        self.metrics.adjacency_scan("out");
                    }
                    if dir.includes_in() {
                        self.metrics.adjacency_scan("in");
                    }
                    let ty_filter = ty;
                    for entry in &inline_adj.entries {
                        let entry_dir = match entry.direction {
                            DIR_OUT => Dir::Out,
                            DIR_IN => Dir::In,
                            _ => continue,
                        };
                        let dir_matches = match entry_dir {
                            Dir::Out => dir.includes_out(),
                            Dir::In => dir.includes_in(),
                            Dir::Both => true,
                        };
                        if !dir_matches {
                            continue;
                        }
                        if let Some(filter) = ty_filter {
                            if entry.type_id != filter.0 {
                                continue;
                            }
                        }
                        let neighbor_id = entry.neighbor;
                        if let Some(set) = seen_set.as_mut() {
                            if !set.insert(neighbor_id) {
                                continue;
                            }
                        }
                        neighbors.push(Neighbor {
                            neighbor: neighbor_id,
                            edge: entry.edge,
                            ty: TypeId(entry.type_id),
                        });
                    }
                    return Ok(NeighborCursor::new(neighbors));
                }
            }
        }

        let mut neighbors: Vec<Neighbor> = Vec::new();

        // Use cached adj_page or look it up
        let adj_page_id = match cached_adj_page {
            Some(page_id) => Some(page_id),
            None => {
                // Look up node to get adj_page
                if let Some(versioned) = self.visible_node(tx, id)? {
                    versioned.row.adj_page
                } else {
                    None
                }
            }
        };

        if let Some(adj_page_id) = adj_page_id {
            // True IFA path: O(1) adjacency lookup
            if dir.includes_out() {
                self.metrics.adjacency_scan("out");
                self.collect_neighbors_true_ifa(
                    tx, ifa, adj_page_id, Dir::Out, ty, snapshot,
                    seen_set.as_mut(), &mut neighbors,
                )?;
            }
            if dir.includes_in() {
                self.metrics.adjacency_scan("in");
                self.collect_neighbors_true_ifa(
                    tx, ifa, adj_page_id, Dir::In, ty, snapshot,
                    seen_set.as_mut(), &mut neighbors,
                )?;
            }
            return Ok(NeighborCursor::new(neighbors));
        }

        // Fallback to B-tree-based IFA (legacy path)
        if dir.includes_out() {
            self.metrics.adjacency_scan("out");
            self.collect_neighbors_ifa(
                tx,
                ifa,
                id,
                Dir::Out,
                ty,
                snapshot,
                seen_set.as_mut(),
                &mut neighbors,
            )?;
        }
        if dir.includes_in() {
            self.metrics.adjacency_scan("in");
            self.collect_neighbors_ifa(
                tx,
                ifa,
                id,
                Dir::In,
                ty,
                snapshot,
                seen_set.as_mut(),
                &mut neighbors,
            )?;
        }

        Ok(NeighborCursor::new(neighbors))
    }

    /// Collects neighbors using true IFA path (direct page read).
    /// 
    /// Note: Visibility is filtered at the IFA layer using per-entry xmin/xmax,
    /// eliminating the need for expensive B-tree edge lookups.
    fn collect_neighbors_true_ifa(
        &self,
        tx: &ReadGuard,
        ifa: &super::ifa::IfaAdjacency,
        adj_page_id: PageId,
        dir: Dir,
        ty: Option<TypeId>,
        snapshot: CommitId,
        mut seen: Option<&mut HashSet<NodeId>>,
        out: &mut Vec<Neighbor>,
    ) -> Result<()> {
        match ty {
            Some(type_id) => {
                // Query specific type - visibility already filtered by IFA
                let entries = ifa.get_neighbors_true_ifa(tx, adj_page_id, dir, type_id, snapshot)?;
                for (neighbor, edge) in entries {
                    if let Some(set) = seen.as_deref_mut() {
                        if !set.insert(neighbor) {
                            continue;
                        }
                    }
                    out.push(Neighbor {
                        neighbor,
                        edge,
                        ty: type_id,
                    });
                }
            }
            None => {
                // Query all types - visibility already filtered by IFA
                let entries = ifa.get_all_neighbors_true_ifa(tx, adj_page_id, dir, snapshot)?;
                for (neighbor, edge, type_id) in entries {
                    if let Some(set) = seen.as_deref_mut() {
                        if !set.insert(neighbor) {
                            continue;
                        }
                    }
                    out.push(Neighbor {
                        neighbor,
                        edge,
                        ty: type_id,
                    });
                }
            }
        }
        Ok(())
    }

    /// Collects neighbors from IFA for a specific direction.
    fn collect_neighbors_ifa(
        &self,
        tx: &ReadGuard,
        ifa: &super::ifa::IfaAdjacency,
        node: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        snapshot: CommitId,
        mut seen: Option<&mut HashSet<NodeId>>,
        out: &mut Vec<Neighbor>,
    ) -> Result<()> {
        match ty {
            Some(type_id) => {
                // Query specific type
                let entries = ifa.get_neighbors_read(tx, node, dir, type_id, snapshot)?;
                for (neighbor, edge) in entries {
                    // Verify edge is visible
                    if self.visible_edge(tx, edge)?.is_none() {
                        continue;
                    }
                    if let Some(set) = seen.as_deref_mut() {
                        if !set.insert(neighbor) {
                            continue;
                        }
                    }
                    out.push(Neighbor {
                        neighbor,
                        edge,
                        ty: type_id,
                    });
                }
            }
            None => {
                // Query all types
                let entries = ifa.get_all_neighbors_read(tx, node, dir, snapshot)?;
                for (neighbor, edge, type_id) in entries {
                    // Verify edge is visible
                    if self.visible_edge(tx, edge)?.is_none() {
                        continue;
                    }
                    if let Some(set) = seen.as_deref_mut() {
                        if !set.insert(neighbor) {
                            continue;
                        }
                    }
                    out.push(Neighbor {
                        neighbor,
                        edge,
                        ty: type_id,
                    });
                }
            }
        }
        Ok(())
    }

    /// Performs a breadth-first traversal from `start`, returning visited nodes up to `max_depth`.
    pub fn bfs(&self, tx: &ReadGuard, start: NodeId, opts: &BfsOptions) -> Result<Vec<BfsVisit>> {
        if !self.node_exists(tx, start)? {
            return Err(SombraError::NotFound);
        }
        
        // For IFA mode, use optimized BFS with adj_page caching
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            return self.bfs_ifa_optimized(tx, start, opts);
        }
        
        // Default B-tree path
        let mut queue: VecDeque<(NodeId, u32)> = VecDeque::new();
        let mut seen: HashSet<NodeId> = HashSet::new();
        let mut visits: Vec<BfsVisit> = Vec::new();
        queue.push_back((start, 0));
        seen.insert(start);
        let type_filters = opts.edge_types.as_deref();
        while let Some((node, depth)) = queue.pop_front() {
            visits.push(BfsVisit { node, depth });
            if let Some(limit) = opts.max_results {
                if visits.len() >= limit {
                    break;
                }
            }
            if depth >= opts.max_depth {
                continue;
            }
            match type_filters {
                Some(types) if !types.is_empty() => {
                    for ty in types {
                        self.enqueue_bfs_neighbors(
                            tx,
                            node,
                            opts.direction,
                            Some(*ty),
                            depth + 1,
                            &mut seen,
                            &mut queue,
                        )?;
                    }
                }
                _ => {
                    self.enqueue_bfs_neighbors(
                        tx,
                        node,
                        opts.direction,
                        None,
                        depth + 1,
                        &mut seen,
                        &mut queue,
                    )?;
                }
            }
        }
        Ok(visits)
    }

    /// Optimized BFS for IFA mode with adj_page caching.
    /// 
    /// Caches adj_page lookups to avoid redundant node B-tree reads during traversal.
    fn bfs_ifa_optimized(&self, tx: &ReadGuard, start: NodeId, opts: &BfsOptions) -> Result<Vec<BfsVisit>> {
        // Queue now includes optional cached adj_page_id
        let mut queue: VecDeque<(NodeId, u32, Option<PageId>)> = VecDeque::new();
        let mut seen: HashSet<NodeId> = HashSet::new();
        let mut visits: Vec<BfsVisit> = Vec::new();
        
        // Look up start node's adj_page
        let start_adj_page = self.visible_node(tx, start)?
            .and_then(|v| v.row.adj_page);
        
        queue.push_back((start, 0, start_adj_page));
        seen.insert(start);
        let type_filters = opts.edge_types.as_deref();
        
        while let Some((node, depth, cached_adj_page)) = queue.pop_front() {
            visits.push(BfsVisit { node, depth });
            
            if let Some(limit) = opts.max_results {
                if visits.len() >= limit {
                    break;
                }
            }
            if depth >= opts.max_depth {
                continue;
            }
            
            match type_filters {
                Some(types) if !types.is_empty() => {
                    for ty in types {
                        self.enqueue_bfs_neighbors_ifa(
                            tx,
                            node,
                            opts.direction,
                            Some(*ty),
                            depth + 1,
                            cached_adj_page,
                            &mut seen,
                            &mut queue,
                        )?;
                    }
                }
                _ => {
                    self.enqueue_bfs_neighbors_ifa(
                        tx,
                        node,
                        opts.direction,
                        None,
                        depth + 1,
                        cached_adj_page,
                        &mut seen,
                        &mut queue,
                    )?;
                }
            }
        }
        Ok(visits)
    }

    /// Enqueues neighbors for IFA-optimized BFS.
    /// 
    /// Uses cached adj_page_id to avoid redundant node lookups.
    /// When discovering new neighbors, looks up their adj_page for future use.
    fn enqueue_bfs_neighbors_ifa(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty_filter: Option<TypeId>,
        next_depth: u32,
        cached_adj_page: Option<PageId>,
        seen: &mut HashSet<NodeId>,
        queue: &mut VecDeque<(NodeId, u32, Option<PageId>)>,
    ) -> Result<()> {
        let cursor = self.neighbors_ifa_cached(
            tx,
            node,
            dir,
            ty_filter,
            ExpandOpts { distinct_nodes: false },
            cached_adj_page,
        )?;
        
        for neighbor in cursor {
            if seen.insert(neighbor.neighbor) {
                // Look up neighbor's adj_page for future traversal
                let neighbor_adj_page = self.visible_node(tx, neighbor.neighbor)?
                    .and_then(|v| v.row.adj_page);
                queue.push_back((neighbor.neighbor, next_depth, neighbor_adj_page));
            }
        }
        Ok(())
    }

    /// Computes the degree (number of edges) for a node in a given direction.
    pub fn degree(&self, tx: &ReadGuard, id: NodeId, dir: Dir, ty: Option<TypeId>) -> Result<u64> {
        // Use IFA read path if in IfaOnly mode
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            return self.degree_ifa(tx, id, dir, ty);
        }
        
        let result = match dir {
            Dir::Out => self.degree_single(tx, id, true, ty)?,
            Dir::In => self.degree_single(tx, id, false, ty)?,
            Dir::Both => {
                let out = self.degree_single(tx, id, true, ty)?;
                let inn = self.degree_single(tx, id, false, ty)?;
                let loops = self.count_loop_edges(tx, id, ty)?;
                out + inn - loops
            }
        };
        let direction_str = match dir {
            Dir::Out => "out",
            Dir::In => "in",
            Dir::Both => "both",
        };
        let cached = self.degree_has_cache_entry(tx, id, dir, ty)?;
        self.metrics.degree_query(direction_str, cached);
        Ok(result)
    }
    
    /// IFA-based degree calculation for IfaOnly mode.
    fn degree_ifa(&self, tx: &ReadGuard, id: NodeId, dir: Dir, ty: Option<TypeId>) -> Result<u64> {
        let ifa = self.ifa.as_ref().ok_or(SombraError::Invalid(
            "IFA not initialized but IfaOnly mode selected",
        ))?;
        
        let snapshot = Self::reader_snapshot_commit(tx);
        
        // Look up node row to check for inline adjacency or external adj_page
        let result = if let Some(versioned) = self.visible_node(tx, id)? {
            if let Some(inline_adj) = &versioned.row.inline_adj {
                // Compute degree from inline adjacency stored in the node row.
                let mut out = 0u64;
                let mut inn = 0u64;
                let mut loops = 0u64;
                for entry in &inline_adj.entries {
                    let matches_ty = ty.map_or(true, |t| t.0 == entry.type_id);
                    if !matches_ty {
                        continue;
                    }
                    let is_loop = entry.neighbor == id;
                    match entry.direction {
                        DIR_OUT => {
                            out = out.saturating_add(1);
                            if is_loop {
                                loops = loops.saturating_add(1);
                            }
                        }
                        DIR_IN => {
                            inn = inn.saturating_add(1);
                            if is_loop {
                                loops = loops.saturating_add(1);
                            }
                        }
                        _ => {}
                    }
                }
                match dir {
                    Dir::Out => out,
                    Dir::In => inn,
                    Dir::Both => out + inn - loops,
                }
            } else if let Some(adj_page_id) = versioned.row.adj_page {
                // True IFA path: count from adj_page
                self.degree_true_ifa(tx, ifa, adj_page_id, dir, ty, snapshot)?
            } else {
                0
            }
        } else {
            0
        };
        
        let direction_str = match dir {
            Dir::Out => "out",
            Dir::In => "in",
            Dir::Both => "both",
        };
        self.metrics.degree_query(direction_str, false);
        Ok(result)
    }
    
    /// Counts edges using true IFA path (direct page read).
    fn degree_true_ifa(
        &self,
        tx: &ReadGuard,
        ifa: &super::ifa::IfaAdjacency,
        adj_page_id: PageId,
        dir: Dir,
        ty: Option<TypeId>,
        snapshot: CommitId,
    ) -> Result<u64> {
        let mut count = 0u64;
        
        if dir.includes_out() {
            count += self.count_neighbors_true_ifa(tx, ifa, adj_page_id, Dir::Out, ty, snapshot)?;
        }
        if dir.includes_in() {
            let in_count = self.count_neighbors_true_ifa(tx, ifa, adj_page_id, Dir::In, ty, snapshot)?;
            count += in_count;
        }
        
        // For Dir::Both, we need to subtract loop edges to avoid double-counting
        if dir == Dir::Both {
            let loops = self.count_loop_edges_ifa(tx, ifa, adj_page_id, ty, snapshot)?;
            count = count.saturating_sub(loops);
        }
        
        Ok(count)
    }
    
    /// Counts neighbors for a single direction using true IFA.
    fn count_neighbors_true_ifa(
        &self,
        tx: &ReadGuard,
        ifa: &super::ifa::IfaAdjacency,
        adj_page_id: PageId,
        dir: Dir,
        ty: Option<TypeId>,
        snapshot: CommitId,
    ) -> Result<u64> {
        match ty {
            Some(type_id) => {
                let entries = ifa.get_neighbors_true_ifa(tx, adj_page_id, dir, type_id, snapshot)?;
                Ok(entries.len() as u64)
            }
            None => {
                let entries = ifa.get_all_neighbors_true_ifa(tx, adj_page_id, dir, snapshot)?;
                Ok(entries.len() as u64)
            }
        }
    }
    
    /// Counts loop edges (self-referential) using true IFA.
    fn count_loop_edges_ifa(
        &self,
        tx: &ReadGuard,
        ifa: &super::ifa::IfaAdjacency,
        adj_page_id: PageId,
        ty: Option<TypeId>,
        snapshot: CommitId,
    ) -> Result<u64> {
        // Get outgoing edges
        let entries = match ty {
            Some(type_id) => {
                let e = ifa.get_neighbors_true_ifa(tx, adj_page_id, Dir::Out, type_id, snapshot)?;
                e.into_iter().map(|(n, e)| (n, e, type_id)).collect::<Vec<_>>()
            }
            None => {
                ifa.get_all_neighbors_true_ifa(tx, adj_page_id, Dir::Out, snapshot)?
            }
        };
        
        // Read the adj_page to get the owner node
        let adj_page = ifa.read_adj_page(tx, adj_page_id)?;
        let owner = adj_page.owner();
        
        // Count edges where neighbor == owner (self-loops)
        let loops = entries.iter().filter(|(neighbor, _, _)| *neighbor == owner).count();
        Ok(loops as u64)
    }

    // =========================================================================
    // True IFA Write Path Support
    // =========================================================================

    /// Gets a node's adj_page during a write transaction.
    ///
    /// Returns None if the node doesn't exist or doesn't have an adj_page.
    fn get_node_adj_page(&self, tx: &mut WriteGuard<'_>, node_id: NodeId) -> Result<Option<PageId>> {
        let Some(bytes) = self.nodes.get_with_write(tx, &node_id.0)? else {
            return Ok(None);
        };
        let versioned = node::decode(&bytes)?;
        Ok(versioned.row.adj_page)
    }

    /// Ensures a node has an IFA adjacency page allocated.
    ///
    /// If the node already has `adj_page`, returns it.
    /// Otherwise, allocates a new adjacency page, updates the node row, and returns the page ID.
    ///
    /// This is used for True IFA mode where adjacency headers are stored directly
    /// in per-node pages rather than in B-trees.
    #[allow(dead_code)]
    fn ensure_node_has_adj_page(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: NodeId,
        ifa: &super::ifa::IfaAdjacency,
    ) -> Result<PageId> {
        // Read current node row
        let Some(bytes) = self.nodes.get_with_write(tx, &node_id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = node::decode(&bytes)?;
        
        // If already has adj_page, return it
        if let Some(adj_page) = versioned.row.adj_page {
            return Ok(adj_page);
        }
        
        // Node doesn't have adj_page - allocate one
        let adj_page_id = ifa.allocate_adj_page(tx, node_id)?;
        
        // Re-encode and update node row with adj_page
        // Note: We update the node row in-place without creating a new MVCC version
        // because adj_page is internal metadata, not user-visible data.
        let row = versioned.row;
        let prop_payload = match &row.props {
            super::node::PropStorage::Inline(bytes) => NodePropPayload::Inline(bytes),
            super::node::PropStorage::VRef(vref) => NodePropPayload::VRef(*vref),
        };
        
        // Build encode opts with adj_page
        let mut opts = NodeEncodeOpts::new(self.row_hash_header);
        opts = opts.with_adj_page(adj_page_id);
        
        let encoded = node::encode(
            &row.labels,
            prop_payload,
            opts,
            versioned.header, // Keep same MVCC header
            versioned.prev_ptr,
            versioned.inline_history.as_deref(),
        )?;
        
        // Update node in B-tree
        // Note: We don't persist tree root here - caller should batch persists
        self.nodes.put(tx, &node_id.0, &encoded.bytes)?;
        
        Ok(adj_page_id)
    }

    pub(crate) fn insert_adjacencies(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // For IfaOnly mode, skip B-tree adjacency writes entirely
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            if let Some(ifa) = &self.ifa {
                self.insert_adjacencies_true_ifa(tx, ifa, entries, commit)?;
            }
            return Ok(());
        }

        // B-tree adjacency writes (for BTree and Ifa modes)
        // Profile key encoding
        let key_encode_start = profile_timer();
        let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (src, dst, ty, edge) in entries {
            let fwd_key = adjacency::encode_fwd_key(*src, *ty, *dst, *edge);
            let rev_key = adjacency::encode_rev_key(*dst, *ty, *src, *edge);
            keys.push((fwd_key, rev_key));
        }
        if let Some(start) = key_encode_start {
            record_flush_adj_key_encode(start.elapsed().as_nanos() as u64);
        }
        record_flush_adj_entries(entries.len() as u64);
        let versioned_unit = Self::adjacency_value_for_commit(commit, false);
        {
            // Profile forward adjacency sort
            let fwd_sort_start = profile_timer();
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(fwd, _)| fwd).collect();
            refs.sort_unstable();
            if let Some(start) = fwd_sort_start {
                record_flush_adj_fwd_sort(start.elapsed().as_nanos() as u64);
            }
            // Profile forward adjacency put_many
            let fwd_put_start = profile_timer();
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            self.adj_fwd.put_many(tx, iter)?;
            self.persist_tree_root(tx, RootKind::AdjFwd)?;
            if let Some(start) = fwd_put_start {
                record_flush_adj_fwd_put(start.elapsed().as_nanos() as u64);
            }
        }
        {
            // Profile reverse adjacency sort
            let rev_sort_start = profile_timer();
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(_, rev)| rev).collect();
            refs.sort_unstable();
            if let Some(start) = rev_sort_start {
                record_flush_adj_rev_sort(start.elapsed().as_nanos() as u64);
            }
            // Profile reverse adjacency put_many
            let rev_put_start = profile_timer();
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            if let Err(err) = self.adj_rev.put_many(tx, iter) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
            self.persist_tree_root(tx, RootKind::AdjRev)?;
            if let Some(start) = rev_put_start {
                record_flush_adj_rev_put(start.elapsed().as_nanos() as u64);
            }
        }
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            let mut deltas: BTreeMap<(NodeId, DegreeDir, TypeId), i64> = BTreeMap::new();
            for (src, dst, ty, _edge) in entries {
                *deltas.entry((*src, DegreeDir::Out, *ty)).or_default() += 1;
                *deltas.entry((*dst, DegreeDir::In, *ty)).or_default() += 1;
            }
            if let Err(err) = self.apply_degree_batch(tx, &deltas) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
        }

        // Shadow-write to IFA for Ifa mode (dual-write)
        if self.adjacency_backend == AdjacencyBackend::Ifa {
            if let Some(ifa) = &self.ifa {
                for (src, dst, ty, edge) in entries {
                    ifa.insert_edge(tx, *src, *dst, *ty, *edge, commit)?;
                }
                // Persist IFA roots
                self.persist_tree_root(tx, RootKind::IfaAdjOut)?;
                self.persist_tree_root(tx, RootKind::IfaAdjIn)?;
                self.persist_tree_root(tx, RootKind::IfaOverflow)?;
            }
        }

        // No finalization needed - adjacency values are written finalized directly
        Ok(())
    }

    /// Inserts adjacencies using True IFA path with per-node adjacency pages.
    ///
    /// OPTIMIZED: Uses bulk page allocation to minimize allocation overhead:
    /// 1. Collect all unique nodes and ensure they have adj_pages
    /// 2. Group edges by (node, direction, type) to count segments needed
    /// 3. Bulk-allocate all segment pages upfront using extent allocation
    /// 4. For each (node, dir) group, use pre-allocated pages for CoW operations
    ///
    /// This is much faster than per-edge or even per-node allocation because:
    /// - Bulk extent allocation takes one lock vs thousands of individual allocations
    /// - Only one CoW clone per (node, dir, type) instead of per-edge
    /// - Only one adj_page read/write per node instead of per-edge
    /// - All entries of same type are inserted together
    fn insert_adjacencies_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        ifa: &super::ifa::IfaAdjacency,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // ---------------------------------------------------------------------
        // 1) Group edges by node (both directions) in sorted NodeId order
        // ---------------------------------------------------------------------
        #[derive(Clone, Copy, Debug)]
        enum EdgeOp {
            Out(NodeId, TypeId, EdgeId),
            In(NodeId, TypeId, EdgeId),
        }

        impl EdgeOp {
            fn to_inline_entry(self) -> NodeInlineAdjEntry {
                match self {
                    EdgeOp::Out(neighbor, ty, edge) => NodeInlineAdjEntry {
                        direction: DIR_OUT,
                        type_id: ty.0,
                        neighbor,
                        edge,
                    },
                    EdgeOp::In(neighbor, ty, edge) => NodeInlineAdjEntry {
                        direction: DIR_IN,
                        type_id: ty.0,
                        neighbor,
                        edge,
                    },
                }
            }
        }

        let mut node_edges: BTreeMap<NodeId, Vec<EdgeOp>> = BTreeMap::new();
        for (src, dst, ty, edge) in entries {
            node_edges
                .entry(*src)
                .or_default()
                .push(EdgeOp::Out(*dst, *ty, *edge));
            node_edges
                .entry(*dst)
                .or_default()
                .push(EdgeOp::In(*src, *ty, *edge));
        }

        // ---------------------------------------------------------------------
        // 2) Batch read all affected node rows
        // ---------------------------------------------------------------------
        let mut node_rows: BTreeMap<NodeId, node::VersionedNodeRow> = BTreeMap::new();
        for node_id in node_edges.keys() {
            let Some(bytes) = self.nodes.get_with_write(tx, &node_id.0)? else {
                return Err(SombraError::NotFound);
            };
            let row = node::decode(&bytes)?;
            node_rows.insert(*node_id, row);
        }

        // ---------------------------------------------------------------------
        // 3) In-memory modification: inline-first, promotion when capacity exceeded
        // ---------------------------------------------------------------------
        let mut promoted_nodes: Vec<(NodeId, Vec<NodeInlineAdjEntry>, Vec<EdgeOp>)> = Vec::new();
        let mut external_appends: BTreeMap<NodeId, Vec<EdgeOp>> = BTreeMap::new();

        for (node_id, ops) in &node_edges {
            let versioned = node_rows
                .get_mut(node_id)
                .expect("node row should be loaded");

            if versioned.row.adj_page.is_some() {
                // Already using external adjacency page – append there.
                external_appends
                    .entry(*node_id)
                    .or_default()
                    .extend_from_slice(ops);
                continue;
            }

            let inline = versioned
                .row
                .inline_adj
                .get_or_insert_with(InlineNodeAdj::new);

            if inline.needs_promotion(ops.len()) {
                // Promotion: move existing inline entries plus new ops to external page.
                let existing = std::mem::take(&mut inline.entries);
                // Clear inline_adj to maintain mutual exclusivity once promoted.
                versioned.row.inline_adj = None;
                promoted_nodes.push((*node_id, existing, ops.clone()));
            } else {
                // Stay inline – append new entries directly into node row.
                for op in ops {
                    inline.add(op.to_inline_entry());
                }
            }
        }

        // ---------------------------------------------------------------------
        // 4) Handle promotions: allocate adj_page and move edges to external storage
        // ---------------------------------------------------------------------
        for (node_id, existing_inline, new_ops) in promoted_nodes {
            let versioned = node_rows
                .get_mut(&node_id)
                .expect("promoted node row missing");

            // Allocate a new adjacency page if needed.
            let adj_page_id = match versioned.row.adj_page {
                Some(page) => page,
                None => {
                    let page = ifa.allocate_adj_page(tx, node_id)?;
                    versioned.row.adj_page = Some(page);
                    page
                }
            };

            // Build per-direction batches from existing inline entries and new ops.
            let mut out_edges: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();
            let mut in_edges: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();

            for entry in existing_inline {
                let ty = TypeId(entry.type_id);
                let neighbor = entry.neighbor;
                let edge = entry.edge;
                match entry.direction {
                    DIR_OUT => out_edges.push((neighbor, ty, edge)),
                    DIR_IN => in_edges.push((neighbor, ty, edge)),
                    _ => {}
                }
            }

            for op in &new_ops {
                match *op {
                    EdgeOp::Out(neighbor, ty, edge) => out_edges.push((neighbor, ty, edge)),
                    EdgeOp::In(neighbor, ty, edge) => in_edges.push((neighbor, ty, edge)),
                }
            }

            if !out_edges.is_empty() {
                ifa.insert_edges_batch_true_ifa(
                    tx,
                    adj_page_id,
                    node_id,
                    Dir::Out,
                    &out_edges,
                    commit,
                )?;
            }
            if !in_edges.is_empty() {
                ifa.insert_edges_batch_true_ifa(
                    tx,
                    adj_page_id,
                    node_id,
                    Dir::In,
                    &in_edges,
                    commit,
                )?;
            }
        }

        // ---------------------------------------------------------------------
        // 5) Append edges for nodes that already had external adjacency pages
        // ---------------------------------------------------------------------
        for (node_id, ops) in external_appends {
            let versioned = node_rows
                .get(&node_id)
                .expect("external node row missing");
            let adj_page_id = versioned
                .row
                .adj_page
                .expect("external nodes must have adj_page");

            let mut out_edges: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();
            let mut in_edges: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();

            for op in ops {
                match op {
                    EdgeOp::Out(neighbor, ty, edge) => out_edges.push((neighbor, ty, edge)),
                    EdgeOp::In(neighbor, ty, edge) => in_edges.push((neighbor, ty, edge)),
                }
            }

            if !out_edges.is_empty() {
                ifa.insert_edges_batch_true_ifa(
                    tx,
                    adj_page_id,
                    node_id,
                    Dir::Out,
                    &out_edges,
                    commit,
                )?;
            }
            if !in_edges.is_empty() {
                ifa.insert_edges_batch_true_ifa(
                    tx,
                    adj_page_id,
                    node_id,
                    Dir::In,
                    &in_edges,
                    commit,
                )?;
            }
        }

        // ---------------------------------------------------------------------
        // 6) Batch-write updated node rows via put_many (sorted by NodeId)
        // ---------------------------------------------------------------------
        let mut keys: Vec<u64> = Vec::with_capacity(node_rows.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(node_rows.len());

        for (node_id, versioned) in &node_rows {
            let row = &versioned.row;
            let prop_payload = match &row.props {
                node::PropStorage::Inline(bytes) => NodePropPayload::Inline(bytes),
                node::PropStorage::VRef(vref) => NodePropPayload::VRef(*vref),
            };

            // Preserve existing row-hash usage: if the row currently has a hash,
            // keep it; otherwise follow graph-wide configuration.
            let mut encode_opts = NodeEncodeOpts::new(self.row_hash_header);
            if let Some(adj) = row.adj_page {
                encode_opts = encode_opts.with_adj_page(adj);
            }
            if let Some(inline_adj) = row.inline_adj.as_ref() {
                encode_opts = encode_opts.with_inline_adj(inline_adj);
            }

            let encoded = node::encode(
                &row.labels,
                prop_payload,
                encode_opts,
                versioned.header,
                versioned.prev_ptr,
                versioned.inline_history.as_deref(),
            )?;

            keys.push(node_id.0);
            values.push(encoded.bytes);
        }

        let mut items: Vec<PutItem<u64, Vec<u8>>> = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            items.push(PutItem {
                key: &keys[i],
                value: &values[i],
            });
        }

        self.nodes.put_many(tx, items)?;
        self.persist_tree_root(tx, RootKind::Nodes)?;

        Ok(())
    }

    /// Helper to insert a directed edge using true IFA.
    /// 
    /// NOTE: This is kept for backward compatibility but the batched version
    /// `insert_edges_batch_true_ifa` should be preferred for bulk inserts.
    #[allow(dead_code)]
    fn insert_directed_edge_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        ifa: &super::ifa::IfaAdjacency,
        adj_page_id: PageId,
        owner: NodeId,
        neighbor: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: CommitId,
    ) -> Result<()> {
        // Read the adjacency page
        let mut adj_page = ifa.read_adj_page_mut(tx, adj_page_id)?;
        
        // Get header for IN direction
        let header = adj_page.header_mut(Dir::In);
        
        // Get old segment pointer - check inline first, then overflow
        let old_ptr = header.lookup_inline(type_id);
        
        // If not found inline and has overflow, search overflow
        let (old_ptr, found_in_overflow) = if old_ptr.is_none() && header.has_overflow() {
            let store = ifa.ifa_store();
            let overflow_ptr = store.search_overflow_chain_for_type(tx, owner, Dir::In, type_id)?;
            (overflow_ptr, overflow_ptr.is_some())
        } else {
            (old_ptr, false)
        };
        
        // Use segment manager to insert edge (CoW)
        let new_ptr = ifa.segment_manager().insert_edge(
            tx,
            old_ptr,
            owner,
            Dir::In,
            type_id,
            neighbor,
            edge_id,
            xmin,
        )?;
        
        // Update type mapping
        if found_in_overflow {
            // Type is in overflow - update overflow block
            let store = ifa.ifa_store();
            store.update_overflow_type(tx, owner, Dir::In, type_id, new_ptr)?;
        } else {
            // Try inline first
            match header.insert_inline(type_id, new_ptr) {
                Ok(()) => {}
                Err(_) => {
                    // Inline buckets full - insert into overflow
                    let store = ifa.ifa_store();
                    store.insert_overflow(tx, owner, Dir::In, type_id, new_ptr, header)?;
                }
            }
        }
        
        // Write updated NodeAdjPage back
        ifa.write_adj_page(tx, adj_page_id, &adj_page)?;
        
        // Mark old segment as superseded (if it existed)
        if let Some(old) = old_ptr {
            if !old.is_null() {
                ifa.segment_manager().mark_superseded(tx, old, xmin)?;
            }
        }
        
        Ok(())
    }

    pub(crate) fn remove_adjacency(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        ty: TypeId,
        edge: EdgeId,
        commit: CommitId,
    ) -> Result<()> {
        // For IfaOnly mode, skip B-tree adjacency writes entirely
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            if let Some(ifa) = &self.ifa {
                // First, attempt to remove from inline adjacency stored in node rows.
                let mut nodes_changed = false;

                // Helper closure to remove inline adjacency for a single node/direction.
                let mut remove_inline_for_node = |node_id: NodeId,
                                                  neighbor: NodeId,
                                                  dir_flag: u8|
                 -> Result<bool> {
                    let Some(bytes) = self.nodes.get_with_write(tx, &node_id.0)? else {
                        return Ok(false);
                    };
                    let mut versioned = node::decode(&bytes)?;
                    let Some(inline_adj) = &mut versioned.row.inline_adj else {
                        return Ok(false);
                    };

                    let before_len = inline_adj.len();
                    inline_adj.entries.retain(|e| {
                        if e.direction != dir_flag {
                            return true;
                        }
                        if e.neighbor != neighbor {
                            return true;
                        }
                        if e.type_id != ty.0 {
                            return true;
                        }
                        if e.edge != edge {
                            return true;
                        }
                        // Drop this entry.
                        false
                    });

                    if inline_adj.len() == before_len {
                        // Nothing removed.
                        return Ok(false);
                    }

                    if inline_adj.is_empty() {
                        versioned.row.inline_adj = None;
                    }

                    let prop_payload = match &versioned.row.props {
                        node::PropStorage::Inline(bytes) => NodePropPayload::Inline(bytes),
                        node::PropStorage::VRef(vref) => NodePropPayload::VRef(*vref),
                    };
                    let mut encode_opts = NodeEncodeOpts::new(self.row_hash_header);
                    if let Some(adj) = versioned.row.adj_page {
                        encode_opts = encode_opts.with_adj_page(adj);
                    }
                    if let Some(inline) = versioned.row.inline_adj.as_ref() {
                        encode_opts = encode_opts.with_inline_adj(inline);
                    }
                    let encoded = node::encode(
                        &versioned.row.labels,
                        prop_payload,
                        encode_opts,
                        versioned.header,
                        versioned.prev_ptr,
                        versioned.inline_history.as_deref(),
                    )?;
                    self.nodes.put(tx, &node_id.0, &encoded.bytes)?;
                    Ok(true)
                };

                // Remove OUT from src and IN from dst.
                if remove_inline_for_node(src, dst, DIR_OUT)? {
                    nodes_changed = true;
                }
                if remove_inline_for_node(dst, src, DIR_IN)? {
                    nodes_changed = true;
                }

                if nodes_changed {
                    self.persist_tree_root(tx, RootKind::Nodes)?;
                }

                // Then, remove from external adjacency pages if they exist.
                let src_adj_page = self.get_node_adj_page(tx, src)?;
                let dst_adj_page = self.get_node_adj_page(tx, dst)?;
                
                if let (Some(src_page), Some(dst_page)) = (src_adj_page, dst_adj_page) {
                    ifa.remove_edge_true_ifa(tx, src_page, dst_page, src, dst, ty, edge, commit)?;
                }
                // Persist IFA roots
                self.persist_tree_root(tx, RootKind::IfaAdjOut)?;
                self.persist_tree_root(tx, RootKind::IfaAdjIn)?;
                self.persist_tree_root(tx, RootKind::IfaOverflow)?;
            }
            return Ok(());
        }

        // B-tree adjacency writes (for BTree and Ifa modes)
        let fwd_key = adjacency::encode_fwd_key(src, ty, dst, edge);
        let rev_key = adjacency::encode_rev_key(dst, ty, src, edge);
        let mut retire_entry =
            |tree: &BTree<Vec<u8>, VersionedValue<UnitValue>>, key: &Vec<u8>| -> Result<()> {
                let Some(mut current) = tree.get_with_write(tx, key)? else {
                    return Err(SombraError::Corruption(
                        "adjacency entry missing during delete",
                    ));
                };
                if current.header.end != COMMIT_MAX {
                    return Err(SombraError::Corruption("adjacency entry already retired"));
                }
                current.header.end = commit;
                tree.put(tx, key, &current)
            };
        retire_entry(&self.adj_fwd, &fwd_key)?;
        retire_entry(&self.adj_rev, &rev_key)?;
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            self.bump_degree(tx, src, DegreeDir::Out, ty, -1)?;
            self.bump_degree(tx, dst, DegreeDir::In, ty, -1)?;
        }

        // Shadow-write to IFA for Ifa mode (dual-write)
        if self.adjacency_backend == AdjacencyBackend::Ifa {
            if let Some(ifa) = &self.ifa {
                ifa.remove_edge(tx, src, dst, ty, edge, commit)?;
                // Persist IFA roots
                self.persist_tree_root(tx, RootKind::IfaAdjOut)?;
                self.persist_tree_root(tx, RootKind::IfaAdjIn)?;
                self.persist_tree_root(tx, RootKind::IfaOverflow)?;
            }
        }

        Ok(())
    }

    pub(crate) fn collect_incident_edges(
        &self,
        read: &ReadGuard,
        node: NodeId,
    ) -> Result<HashSet<EdgeId>> {
        // For IfaOnly mode, use IFA path
        if self.adjacency_backend == AdjacencyBackend::IfaOnly {
            return self.collect_incident_edges_ifa(read, node);
        }

        // B-tree path
        let mut edges = HashSet::new();
        self.collect_adjacent_edges(read, node, true, &mut edges)?;
        self.collect_adjacent_edges(read, node, false, &mut edges)?;
        Ok(edges)
    }

    /// Collects incident edges using IFA path.
    fn collect_incident_edges_ifa(
        &self,
        read: &ReadGuard,
        node: NodeId,
    ) -> Result<HashSet<EdgeId>> {
        let ifa = self.ifa.as_ref().ok_or(SombraError::Invalid(
            "IFA not initialized but IfaOnly mode selected",
        ))?;

        let snapshot = Self::reader_snapshot_commit(read);
        let mut edges = HashSet::new();

        // Get node row to inspect inline and external adjacency
        let versioned = match self.visible_node(read, node)? {
            Some(v) => v,
            None => return Ok(edges),
        };

        // Collect edges from inline adjacency, if present
        if let Some(inline_adj) = &versioned.row.inline_adj {
            for entry in &inline_adj.entries {
                edges.insert(entry.edge);
            }
        }

        // Collect edges from external adj_page, if present
        if let Some(adj_page_id) = versioned.row.adj_page {
            for dir in [Dir::Out, Dir::In] {
                let neighbors = ifa.get_all_neighbors_true_ifa(read, adj_page_id, dir, snapshot)?;
                for (_neighbor, edge_id, _type_id) in neighbors {
                    edges.insert(edge_id);
                }
            }
        }

        Ok(edges)
    }

    fn collect_adjacent_edges(
        &self,
        read: &ReadGuard,
        node: NodeId,
        forward: bool,
        edges: &mut HashSet<EdgeId>,
    ) -> Result<()> {
        let (lo, hi) = adjacency_bounds_for_node(node);
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(read);
        let mut cursor = tree.range(read, Bound::Included(lo), Bound::Included(hi))?;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded = if forward {
                adjacency::decode_fwd_key(&key)
            } else {
                adjacency::decode_rev_key(&key)
            }
            .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            let edge_id = decoded.3;
            if self.visible_edge(read, edge_id)?.is_none() {
                continue;
            }
            edges.insert(edge_id);
        }
        Ok(())
    }

    fn collect_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty_filter: Option<TypeId>,
        forward: bool,
        seen: Option<&mut HashSet<NodeId>>,
        out: &mut Vec<Neighbor>,
    ) -> Result<()> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty_filter)
        } else {
            adjacency::rev_bounds(node, ty_filter)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(tx);
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut seen = seen;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            if forward {
                let (src, ty, dst, edge) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(src, node);
                match src.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if self.visible_edge(tx, edge)?.is_none() {
                    continue;
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(dst) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: dst,
                    edge,
                    ty,
                });
            } else {
                let (dst, ty, src, edge) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(dst, node);
                match dst.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if self.visible_edge(tx, edge)?.is_none() {
                    continue;
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(src) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: src,
                    edge,
                    ty,
                });
            }
        }
        Ok(())
    }

    fn enqueue_bfs_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty_filter: Option<TypeId>,
        next_depth: u32,
        seen: &mut HashSet<NodeId>,
        queue: &mut VecDeque<(NodeId, u32)>,
    ) -> Result<()> {
        let cursor = self.neighbors(
            tx,
            node,
            dir,
            ty_filter,
            ExpandOpts {
                distinct_nodes: false,
            },
        )?;
        for neighbor in cursor {
            if seen.insert(neighbor.neighbor) {
                queue.push_back((neighbor.neighbor, next_depth));
            }
        }
        Ok(())
    }

    fn degree_single(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        forward: bool,
        ty: Option<TypeId>,
    ) -> Result<u64> {
        #[cfg(feature = "degree-cache")]
        {
            if self.degree_cache_enabled {
                if let Some(tree) = &self.degree {
                    let dir = if forward {
                        DegreeDir::Out
                    } else {
                        DegreeDir::In
                    };
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, dir, ty);
                            if let Some(value) = tree.get(tx, &key)? {
                                return Ok(value);
                            }
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, dir, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            let mut total = 0u64;
                            while let Some((_key, value)) = cursor.next()? {
                                total = total.saturating_add(value);
                            }
                            return Ok(total);
                        }
                    }
                }
            }
        }
        self.count_adjacent_edges(tx, node, ty, forward)
    }

    fn degree_has_cache_entry(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
    ) -> Result<bool> {
        #[cfg(feature = "degree-cache")]
        {
            if !self.degree_cache_enabled {
                return Ok(false);
            }
            let Some(tree) = &self.degree else {
                return Ok(false);
            };
            let result = match dir {
                Dir::Out => {
                    let tag = DegreeDir::Out;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::In => {
                    let tag = DegreeDir::In;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::Both => {
                    let out_has = self.degree_has_cache_entry(tx, node, Dir::Out, ty)?;
                    let in_has = self.degree_has_cache_entry(tx, node, Dir::In, ty)?;
                    out_has && in_has
                }
            };
            Ok(result)
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            let _ = (tx, node, dir, ty);
            Ok(false)
        }
    }

    fn count_adjacent_edges(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty: Option<TypeId>,
        forward: bool,
    ) -> Result<u64> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty)
        } else {
            adjacency::rev_bounds(node, ty)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut count = 0u64;
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((_key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn count_loop_edges(&self, tx: &ReadGuard, node: NodeId, ty: Option<TypeId>) -> Result<u64> {
        let (lo, hi) = adjacency::fwd_bounds(node, ty);
        let mut cursor = self
            .adj_fwd
            .range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut loops = 0u64;
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let (src, ty_val, dst, _edge) = adjacency::decode_fwd_key(&key)
                .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            debug_assert_eq!(src, node);
            if dst == node && ty.map(|t| t == ty_val).unwrap_or(true) {
                loops = loops.saturating_add(1);
            }
        }
        Ok(loops)
    }

    pub(crate) fn free_edge_props(
        &self,
        tx: &mut WriteGuard<'_>,
        props: EdgePropStorage,
    ) -> Result<()> {
        match props {
            EdgePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            EdgePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    #[cfg(feature = "degree-cache")]
    fn apply_degree_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        deltas: &BTreeMap<(NodeId, DegreeDir, TypeId), i64>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut applied: Vec<(NodeId, DegreeDir, TypeId, i64)> = Vec::new();
        for ((node, dir, ty), delta) in deltas {
            if *delta == 0 {
                continue;
            }
            if let Err(err) = self.bump_degree(tx, *node, *dir, *ty, *delta) {
                for (node_applied, dir_applied, ty_applied, delta_applied) in
                    applied.into_iter().rev()
                {
                    let _ =
                        self.bump_degree(tx, node_applied, dir_applied, ty_applied, -delta_applied);
                }
                return Err(err);
            }
            applied.push((*node, *dir, *ty, *delta));
        }
        Ok(())
    }

    fn rollback_adjacency_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        keys: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        for (fwd, rev) in keys {
            let _ = self.adj_fwd.delete(tx, fwd);
            let _ = self.adj_rev.delete(tx, rev);
        }
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    fn bump_degree(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: DegreeDir,
        ty: TypeId,
        delta: i64,
    ) -> Result<()> {
        if !self.degree_cache_enabled {
            return Ok(());
        }
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let key = adjacency::encode_degree_key(node, dir, ty);
        let current = tree.get_with_write(tx, &key)?;
        let current_val = current.unwrap_or(0);
        let new_val = if delta.is_negative() {
            let abs = delta.unsigned_abs();
            if abs > current_val {
                return Err(SombraError::Corruption("degree underflow"));
            }
            current_val - abs
        } else {
            current_val.saturating_add(delta as u64)
        };
        if new_val == 0 {
            let removed = tree.delete(tx, &key)?;
            if delta.is_negative() && !removed {
                return Err(SombraError::Corruption(
                    "degree entry missing during delete",
                ));
            }
        } else {
            tree.put(tx, &key, &new_val)?;
        }
        self.persist_tree_root(tx, RootKind::Degree)?;
        Ok(())
    }

    /// Collects all forward adjacency entries for debugging purposes.
    pub fn debug_collect_adj_fwd(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_fwd_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    /// Collects all reverse adjacency entries for debugging purposes.
    pub fn debug_collect_adj_rev(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_rev_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    #[cfg(feature = "degree-cache")]
    /// Returns every stored degree cache entry for debugging purposes.
    pub fn debug_collect_degree(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, DegreeDir, TypeId, u64)>> {
        let Some(tree) = &self.degree else {
            return Ok(Vec::new());
        };
        let mut cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, value)) = cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode"))?;
            rows.push((node, dir, ty, value));
        }
        Ok(rows)
    }

    #[cfg(feature = "degree-cache")]
    /// Verifies that the degree cache matches the adjacency trees.
    pub fn validate_degree_cache(&self, tx: &ReadGuard) -> Result<()> {
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let mut actual: HashMap<(NodeId, DegreeDir, TypeId), u64> = HashMap::new();
        {
            let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (src, ty, _, _) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual.entry((src, DegreeDir::Out, ty)).or_insert(0) += 1;
            }
        }
        {
            let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (dst, ty, _, _) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual.entry((dst, DegreeDir::In, ty)).or_insert(0) += 1;
            }
        }

        let mut degree_cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        while let Some((key, stored)) = degree_cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode failed"))?;
            let actual_count = actual.remove(&(node, dir, ty)).unwrap_or(0);
            if actual_count != stored {
                return Err(SombraError::Corruption("degree cache mismatch"));
            }
        }
        if actual.values().any(|count| *count > 0) {
            return Err(SombraError::Corruption(
                "degree cache missing entry for adjacency",
            ));
        }
        Ok(())
    }
}

fn adjacency_bounds_for_node(node: NodeId) -> (Vec<u8>, Vec<u8>) {
    const SUFFIX_LEN: usize = 4 + 8 + 8;
    let mut lower = Vec::with_capacity(8 + SUFFIX_LEN);
    lower.extend_from_slice(&node.0.to_be_bytes());
    lower.extend_from_slice(&[0u8; SUFFIX_LEN]);
    let mut upper = Vec::with_capacity(8 + SUFFIX_LEN);
    upper.extend_from_slice(&node.0.to_be_bytes());
    upper.extend_from_slice(&[0xFF; SUFFIX_LEN]);
    (lower, upper)
}
