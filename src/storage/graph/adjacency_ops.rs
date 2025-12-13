use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashSet, VecDeque};

#[cfg(feature = "degree-cache")]
use std::collections::{BTreeMap, HashMap};
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

use super::node::{self, EncodeOpts as NodeEncodeOpts, PropPayload as NodePropPayload};

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
        let mut neighbors: Vec<Neighbor> = Vec::new();
        let enable_distinct = opts.distinct_nodes || self.distinct_neighbors_default;
        let mut seen_set = enable_distinct.then(HashSet::new);

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
                    seen_set.as_mut(), &mut neighbors
                )?;
            }
            if dir.includes_in() {
                self.metrics.adjacency_scan("in");
                self.collect_neighbors_true_ifa(
                    tx, ifa, adj_page_id, Dir::In, ty, snapshot,
                    seen_set.as_mut(), &mut neighbors
                )?;
            }
            return Ok(NeighborCursor::new(neighbors));
        }

        // Fallback to B-tree-based IFA (legacy path)
        if dir.includes_out() {
            self.metrics.adjacency_scan("out");
            self.collect_neighbors_ifa(tx, ifa, id, Dir::Out, ty, snapshot, seen_set.as_mut(), &mut neighbors)?;
        }
        if dir.includes_in() {
            self.metrics.adjacency_scan("in");
            self.collect_neighbors_ifa(tx, ifa, id, Dir::In, ty, snapshot, seen_set.as_mut(), &mut neighbors)?;
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

    // =========================================================================
    // True IFA Write Path Support
    // =========================================================================

    /// Ensures a node has an IFA adjacency page allocated.
    ///
    /// If the node already has `adj_page`, returns it.
    /// Otherwise, allocates a new adjacency page, updates the node row, and returns the page ID.
    ///
    /// This is used for True IFA mode where adjacency headers are stored directly
    /// in per-node pages rather than in B-trees.
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
        let adj_page_id = ifa.allocate_adj_page(tx)?;
        
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
        self.nodes.put(tx, &node_id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Nodes)?;
        
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

        // Shadow-write to IFA if enabled (Ifa or IfaOnly mode)
        if self.adjacency_backend != AdjacencyBackend::BTree {
            if let Some(ifa) = &self.ifa {
                // Use True IFA path for IfaOnly mode - allocate adj_page per node
                if self.adjacency_backend == AdjacencyBackend::IfaOnly {
                    self.insert_adjacencies_true_ifa(tx, ifa, entries, commit)?;
                } else {
                    // Ifa mode: use B-tree-based IFA (shadow writes)
                    for (src, dst, ty, edge) in entries {
                        ifa.insert_edge(tx, *src, *dst, *ty, *edge, commit)?;
                    }
                    // Persist IFA roots
                    self.persist_tree_root(tx, RootKind::IfaAdjOut)?;
                    self.persist_tree_root(tx, RootKind::IfaAdjIn)?;
                    self.persist_tree_root(tx, RootKind::IfaOverflow)?;
                }
            }
        }

        // No finalization needed - adjacency values are written finalized directly
        Ok(())
    }

    /// Inserts adjacencies using True IFA path with per-node adjacency pages.
    ///
    /// For each edge (src -> dst), we:
    /// 1. Ensure src has adj_page allocated (for OUT direction)
    /// 2. Ensure dst has adj_page allocated (for IN direction)  
    /// 3. Insert edge into both adjacency pages
    fn insert_adjacencies_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        ifa: &super::ifa::IfaAdjacency,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        use std::collections::HashMap;
        
        // Cache adj_page lookups to avoid repeated node reads
        let mut adj_page_cache: HashMap<NodeId, PageId> = HashMap::new();
        
        for (src, dst, ty, edge) in entries {
            // Get or allocate adj_page for source node (OUT direction)
            let src_adj_page = match adj_page_cache.get(src) {
                Some(&page) => page,
                None => {
                    let page = self.ensure_node_has_adj_page(tx, *src, ifa)?;
                    adj_page_cache.insert(*src, page);
                    page
                }
            };
            
            // Get or allocate adj_page for destination node (IN direction)
            let dst_adj_page = match adj_page_cache.get(dst) {
                Some(&page) => page,
                None => {
                    let page = self.ensure_node_has_adj_page(tx, *dst, ifa)?;
                    adj_page_cache.insert(*dst, page);
                    page
                }
            };
            
            // Insert outgoing edge: src -> dst (stored in src's adj_page)
            ifa.insert_edge_true_ifa(tx, src_adj_page, *src, *dst, *ty, *edge, commit)?;
            
            // Insert incoming edge: dst <- src (stored in dst's adj_page)
            // Note: We need to call the directed version for the IN direction
            self.insert_directed_edge_true_ifa(tx, ifa, dst_adj_page, *dst, *src, *ty, *edge, commit)?;
        }
        
        Ok(())
    }

    /// Helper to insert a directed edge using true IFA.
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
        
        // Get old segment pointer (if any)
        let old_ptr = header.lookup_inline(type_id);
        
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
        
        // Update type mapping in header
        match header.insert_inline(type_id, new_ptr) {
            Ok(()) => {}
            Err(_) => {
                return Err(SombraError::Invalid("adjacency inline buckets full, overflow not implemented"));
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

        // Shadow-write to IFA if enabled (Ifa or IfaOnly mode)
        if self.adjacency_backend != AdjacencyBackend::BTree {
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
        let mut edges = HashSet::new();
        self.collect_adjacent_edges(read, node, true, &mut edges)?;
        self.collect_adjacent_edges(read, node, false, &mut edges)?;
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
