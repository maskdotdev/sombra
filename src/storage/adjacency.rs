use std::convert::TryInto;

use crate::types::{EdgeId, NodeId, TypeId};

#[cfg(feature = "degree-cache")]
pub const DEGREE_DIR_OUT: u8 = 0;
#[cfg(feature = "degree-cache")]
pub const DEGREE_DIR_IN: u8 = 1;

#[cfg(feature = "degree-cache")]
/// Direction of the edges tracked inside the degree cache.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum DegreeDir {
    /// Outgoing edge counts.
    Out,
    /// Incoming edge counts.
    In,
}

#[cfg(feature = "degree-cache")]
impl DegreeDir {
    /// Encodes this direction into its on-disk byte representation.
    pub fn into_u8(self) -> u8 {
        match self {
            DegreeDir::Out => DEGREE_DIR_OUT,
            DegreeDir::In => DEGREE_DIR_IN,
        }
    }

    /// Decodes a direction byte from the degree cache key.
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            DEGREE_DIR_OUT => Some(DegreeDir::Out),
            DEGREE_DIR_IN => Some(DegreeDir::In),
            _ => None,
        }
    }
}

pub fn encode_fwd_key(src: NodeId, ty: TypeId, dst: NodeId, edge: EdgeId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 4 + 8 + 8);
    buf.extend_from_slice(&src.0.to_be_bytes());
    buf.extend_from_slice(&ty.0.to_be_bytes());
    buf.extend_from_slice(&dst.0.to_be_bytes());
    buf.extend_from_slice(&edge.0.to_be_bytes());
    buf
}

pub fn encode_rev_key(dst: NodeId, ty: TypeId, src: NodeId, edge: EdgeId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 4 + 8 + 8);
    buf.extend_from_slice(&dst.0.to_be_bytes());
    buf.extend_from_slice(&ty.0.to_be_bytes());
    buf.extend_from_slice(&src.0.to_be_bytes());
    buf.extend_from_slice(&edge.0.to_be_bytes());
    buf
}

#[cfg(feature = "degree-cache")]
pub fn encode_degree_key(node: NodeId, dir: DegreeDir, ty: TypeId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 1 + 4);
    buf.extend_from_slice(&node.0.to_be_bytes());
    buf.push(dir.into_u8());
    buf.extend_from_slice(&ty.0.to_be_bytes());
    buf
}

pub(crate) fn decode_fwd_key(bytes: &[u8]) -> Option<(NodeId, TypeId, NodeId, EdgeId)> {
    if bytes.len() != 8 + 4 + 8 + 8 {
        return None;
    }
    let mut offset = 0usize;
    let src = NodeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    offset += 8;
    let ty = TypeId(u32::from_be_bytes(
        bytes[offset..offset + 4].try_into().ok()?,
    ));
    offset += 4;
    let dst = NodeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    offset += 8;
    let edge = EdgeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    Some((src, ty, dst, edge))
}

pub(crate) fn decode_rev_key(bytes: &[u8]) -> Option<(NodeId, TypeId, NodeId, EdgeId)> {
    if bytes.len() != 8 + 4 + 8 + 8 {
        return None;
    }
    let mut offset = 0usize;
    let dst = NodeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    offset += 8;
    let ty = TypeId(u32::from_be_bytes(
        bytes[offset..offset + 4].try_into().ok()?,
    ));
    offset += 4;
    let src = NodeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    offset += 8;
    let edge = EdgeId(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().ok()?,
    ));
    Some((dst, ty, src, edge))
}

#[cfg(feature = "degree-cache")]
pub(crate) fn decode_degree_key(bytes: &[u8]) -> Option<(NodeId, DegreeDir, TypeId)> {
    if bytes.len() != 8 + 1 + 4 {
        return None;
    }
    let node = NodeId(u64::from_be_bytes(bytes[0..8].try_into().ok()?));
    let dir = DegreeDir::from_u8(bytes[8])?;
    let ty = TypeId(u32::from_be_bytes(bytes[9..13].try_into().ok()?));
    Some((node, dir, ty))
}

/// Direction for graph traversal and adjacency queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dir {
    /// Outgoing edges from a node.
    Out,
    /// Incoming edges to a node.
    In,
    /// Both incoming and outgoing edges.
    Both,
}

impl Dir {
    /// Returns true if this direction includes outgoing edges.
    pub fn includes_out(self) -> bool {
        matches!(self, Dir::Out | Dir::Both)
    }

    /// Returns true if this direction includes incoming edges.
    pub fn includes_in(self) -> bool {
        matches!(self, Dir::In | Dir::Both)
    }
}

/// Options for neighbor expansion queries.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExpandOpts {
    /// Whether to deduplicate nodes in the result set.
    pub distinct_nodes: bool,
}

/// A neighboring node in the graph with its connecting edge information.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Neighbor {
    /// The neighboring node ID.
    pub neighbor: NodeId,
    /// The edge connecting to this neighbor.
    pub edge: EdgeId,
    /// The type of the connecting edge.
    pub ty: TypeId,
}

/// Cursor for iterating through neighbors of a node.
pub struct NeighborCursor {
    neighbors: Vec<Neighbor>,
    index: usize,
}

impl NeighborCursor {
    pub(crate) fn new(neighbors: Vec<Neighbor>) -> Self {
        Self {
            neighbors,
            index: 0,
        }
    }

    /// Returns the number of neighbors in this cursor.
    pub fn len(&self) -> usize {
        self.neighbors.len()
    }

    /// Returns true if there are no neighbors.
    pub fn is_empty(&self) -> bool {
        self.neighbors.is_empty()
    }
}

impl Iterator for NeighborCursor {
    type Item = Neighbor;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.neighbors.len() {
            return None;
        }
        let item = self.neighbors[self.index];
        self.index += 1;
        Some(item)
    }
}

pub fn fwd_bounds(node: NodeId, ty: Option<TypeId>) -> (Vec<u8>, Vec<u8>) {
    let mut lower = Vec::with_capacity(8 + 4 + 8 + 8);
    lower.extend_from_slice(&node.0.to_be_bytes());
    match ty {
        Some(t) => lower.extend_from_slice(&t.0.to_be_bytes()),
        None => lower.extend_from_slice(&[0u8; 4]),
    }
    lower.extend_from_slice(&[0u8; 8 + 8]);

    let mut upper = Vec::with_capacity(8 + 4 + 8 + 8);
    upper.extend_from_slice(&node.0.to_be_bytes());
    match ty {
        Some(t) => upper.extend_from_slice(&t.0.to_be_bytes()),
        None => upper.extend_from_slice(&[0xFF; 4]),
    }
    upper.extend_from_slice(&[0xFF; 8 + 8]);
    (lower, upper)
}

pub fn rev_bounds(node: NodeId, ty: Option<TypeId>) -> (Vec<u8>, Vec<u8>) {
    fwd_bounds(node, ty)
}

#[cfg(feature = "degree-cache")]
pub fn degree_bounds(node: NodeId, dir: DegreeDir, ty: Option<TypeId>) -> (Vec<u8>, Vec<u8>) {
    let mut lower = Vec::with_capacity(8 + 1 + 4);
    lower.extend_from_slice(&node.0.to_be_bytes());
    lower.push(dir.into_u8());
    match ty {
        Some(t) => lower.extend_from_slice(&t.0.to_be_bytes()),
        None => lower.extend_from_slice(&[0u8; 4]),
    }
    let mut upper = Vec::with_capacity(8 + 1 + 4);
    upper.extend_from_slice(&node.0.to_be_bytes());
    upper.push(dir.into_u8());
    match ty {
        Some(t) => upper.extend_from_slice(&t.0.to_be_bytes()),
        None => upper.extend_from_slice(&[0xFF; 4]),
    }
    (lower, upper)
}
