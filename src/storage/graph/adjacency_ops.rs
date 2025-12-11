use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashSet, VecDeque};

#[cfg(feature = "degree-cache")]
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, PutItem};
use crate::storage::mvcc::{CommitId, VersionedValue, COMMIT_MAX};
use crate::storage::{
    profile_timer, record_flush_adj_entries, record_flush_adj_fwd_put, record_flush_adj_fwd_sort,
    record_flush_adj_key_encode, record_flush_adj_rev_put, record_flush_adj_rev_sort,
};
use crate::types::{EdgeId, NodeId, Result, SombraError, TypeId};

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

    /// Performs a breadth-first traversal from `start`, returning visited nodes up to `max_depth`.
    pub fn bfs(&self, tx: &ReadGuard, start: NodeId, opts: &BfsOptions) -> Result<Vec<BfsVisit>> {
        if !self.node_exists(tx, start)? {
            return Err(SombraError::NotFound);
        }
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
        // No finalization needed - adjacency values are written finalized directly
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
