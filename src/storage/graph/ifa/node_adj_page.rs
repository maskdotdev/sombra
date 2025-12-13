//! Per-node adjacency page for true Index-Free Adjacency.
//!
//! Each node can have a dedicated adjacency page that stores both OUT and IN
//! NodeAdjHeaders. This enables O(1) neighbor lookups by storing a single
//! `adj_page_id` in the node row, eliminating B-tree lookups for adjacency.
//!
//! # Page Layout (v2)
//!
//! ```text
//! +------------------+
//! | Page Header (8B) |  - magic, version, flags
//! +------------------+
//! | Owner NodeId(8B) |  - node that owns this adjacency page
//! +------------------+
//! | OUT Header (72B) |  - NodeAdjHeader for outgoing edges
//! +------------------+
//! | IN Header (72B)  |  - NodeAdjHeader for incoming edges
//! +------------------+
//! | Reserved         |  - future expansion (overflow, stats, etc.)
//! +------------------+
//! ```
//!
//! Total fixed size: 160 bytes, fits easily in a 4KB page.

use crate::storage::adjacency::Dir;
use crate::types::{NodeId, PageId, Result, SombraError};

use super::types::{NodeAdjHeader, NODE_ADJ_HEADER_LEN};

/// Magic number for node adjacency pages.
const NODE_ADJ_PAGE_MAGIC: u32 = 0x4E414450; // "NADP"

/// Version of the node adjacency page format.
/// v2 adds owner NodeId field for hybrid overflow support.
const NODE_ADJ_PAGE_VERSION: u16 = 2;

/// Size of the page header (magic + version + flags).
const PAGE_HEADER_LEN: usize = 8;

/// Size of the owner field.
const OWNER_LEN: usize = 8;

/// Offset of owner NodeId in the page.
const OWNER_OFFSET: usize = PAGE_HEADER_LEN;

/// Offset of OUT header in the page.
const OUT_HEADER_OFFSET: usize = OWNER_OFFSET + OWNER_LEN;

/// Offset of IN header in the page.
const IN_HEADER_OFFSET: usize = OUT_HEADER_OFFSET + NODE_ADJ_HEADER_LEN;

/// Total size of the node adjacency data (header + owner + both directions).
pub const NODE_ADJ_PAGE_DATA_LEN: usize = PAGE_HEADER_LEN + OWNER_LEN + 2 * NODE_ADJ_HEADER_LEN;

/// A node's adjacency page containing headers for both directions.
#[derive(Clone, Debug)]
pub struct NodeAdjPage {
    /// The node that owns this adjacency page.
    /// Used for hybrid overflow lookup via B-tree store.
    pub owner: NodeId,
    /// Header for outgoing edges.
    pub out_header: NodeAdjHeader,
    /// Header for incoming edges.
    pub in_header: NodeAdjHeader,
}

impl Default for NodeAdjPage {
    fn default() -> Self {
        Self::new(NodeId(0))
    }
}

impl NodeAdjPage {
    /// Creates a new empty node adjacency page for the given owner.
    pub fn new(owner: NodeId) -> Self {
        Self {
            owner,
            out_header: NodeAdjHeader::new(),
            in_header: NodeAdjHeader::new(),
        }
    }

    /// Returns the owner node ID.
    #[inline]
    pub fn owner(&self) -> NodeId {
        self.owner
    }

    /// Gets the header for the specified direction.
    pub fn header(&self, dir: Dir) -> &NodeAdjHeader {
        match dir {
            Dir::Out => &self.out_header,
            Dir::In => &self.in_header,
            Dir::Both => panic!("Dir::Both not valid for single header lookup"),
        }
    }

    /// Gets mutable header for the specified direction.
    pub fn header_mut(&mut self, dir: Dir) -> &mut NodeAdjHeader {
        match dir {
            Dir::Out => &mut self.out_header,
            Dir::In => &mut self.in_header,
            Dir::Both => panic!("Dir::Both not valid for single header lookup"),
        }
    }

    /// Encodes the page to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(NODE_ADJ_PAGE_DATA_LEN);
        
        // Page header
        buf.extend_from_slice(&NODE_ADJ_PAGE_MAGIC.to_be_bytes());
        buf.extend_from_slice(&NODE_ADJ_PAGE_VERSION.to_be_bytes());
        buf.extend_from_slice(&[0u8; 2]); // flags/reserved
        
        // Owner NodeId
        buf.extend_from_slice(&self.owner.0.to_be_bytes());
        
        // OUT header
        buf.extend_from_slice(&self.out_header.encode());
        
        // IN header
        buf.extend_from_slice(&self.in_header.encode());
        
        buf
    }

    /// Decodes a page from bytes.
    /// 
    /// Supports both v1 (without owner) and v2 (with owner) formats.
    /// v1 pages will have owner set to NodeId(0).
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < PAGE_HEADER_LEN {
            return Err(SombraError::Corruption("node adj page too short"));
        }

        // Verify magic
        let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
        if magic != NODE_ADJ_PAGE_MAGIC {
            return Err(SombraError::Corruption("invalid node adj page magic"));
        }

        // Check version
        let version = u16::from_be_bytes(data[4..6].try_into().unwrap());
        
        match version {
            1 => Self::decode_v1(data),
            2 => Self::decode_v2(data),
            _ => Err(SombraError::Corruption("unsupported node adj page version")),
        }
    }
    
    /// Decodes v1 format (without owner field).
    fn decode_v1(data: &[u8]) -> Result<Self> {
        const V1_OUT_OFFSET: usize = PAGE_HEADER_LEN;
        const V1_IN_OFFSET: usize = V1_OUT_OFFSET + NODE_ADJ_HEADER_LEN;
        const V1_DATA_LEN: usize = PAGE_HEADER_LEN + 2 * NODE_ADJ_HEADER_LEN;
        
        if data.len() < V1_DATA_LEN {
            return Err(SombraError::Corruption("node adj page v1 too short"));
        }
        
        let out_header = NodeAdjHeader::decode(&data[V1_OUT_OFFSET..V1_OUT_OFFSET + NODE_ADJ_HEADER_LEN])?;
        let in_header = NodeAdjHeader::decode(&data[V1_IN_OFFSET..V1_IN_OFFSET + NODE_ADJ_HEADER_LEN])?;
        
        Ok(Self {
            owner: NodeId(0), // v1 didn't have owner, default to 0
            out_header,
            in_header,
        })
    }
    
    /// Decodes v2 format (with owner field).
    fn decode_v2(data: &[u8]) -> Result<Self> {
        if data.len() < NODE_ADJ_PAGE_DATA_LEN {
            return Err(SombraError::Corruption("node adj page v2 too short"));
        }
        
        // Decode owner
        let owner = NodeId(u64::from_be_bytes(data[OWNER_OFFSET..OWNER_OFFSET + 8].try_into().unwrap()));
        
        // Decode headers
        let out_header = NodeAdjHeader::decode(&data[OUT_HEADER_OFFSET..OUT_HEADER_OFFSET + NODE_ADJ_HEADER_LEN])?;
        let in_header = NodeAdjHeader::decode(&data[IN_HEADER_OFFSET..IN_HEADER_OFFSET + NODE_ADJ_HEADER_LEN])?;

        Ok(Self {
            owner,
            out_header,
            in_header,
        })
    }
}

/// Pointer to a node's adjacency page.
///
/// Zero indicates no adjacency page allocated (node has no edges yet).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeAdjPagePtr(pub PageId);

impl Default for NodeAdjPagePtr {
    fn default() -> Self {
        Self::null()
    }
}

impl NodeAdjPagePtr {
    /// Returns a null pointer (no adjacency page).
    #[inline]
    pub const fn null() -> Self {
        Self(PageId(0))
    }

    /// Returns true if this pointer is null.
    #[inline]
    #[allow(dead_code)]
    pub const fn is_null(self) -> bool {
        self.0.0 == 0
    }

    /// Creates a pointer from a PageId.
    #[inline]
    #[allow(dead_code)]
    pub const fn from_page(page: PageId) -> Self {
        Self(page)
    }

    /// Returns the PageId.
    #[inline]
    #[allow(dead_code)]
    pub const fn page_id(self) -> PageId {
        self.0
    }

    /// Encodes as big-endian bytes.
    #[inline]
    #[allow(dead_code)]
    pub fn to_bytes(self) -> [u8; 8] {
        self.0.0.to_be_bytes()
    }

    /// Decodes from big-endian bytes.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(SombraError::Corruption("node adj page ptr truncated"));
        }
        let arr: [u8; 8] = bytes[..8].try_into().unwrap();
        Ok(Self(PageId(u64::from_be_bytes(arr))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::graph::ifa::types::SegmentPtr;
    use crate::types::TypeId;

    #[test]
    fn empty_page_roundtrip() {
        let page = NodeAdjPage::new(NodeId(42));
        let encoded = page.encode();
        let decoded = NodeAdjPage::decode(&encoded).unwrap();
        
        assert_eq!(decoded.owner(), NodeId(42));
        assert_eq!(decoded.out_header.active_count(), 0);
        assert_eq!(decoded.in_header.active_count(), 0);
    }

    #[test]
    fn page_with_types_roundtrip() {
        let mut page = NodeAdjPage::new(NodeId(123));
        
        // Add some types to OUT header
        page.out_header.insert_inline(TypeId(1), SegmentPtr::from_page(PageId(100))).unwrap();
        page.out_header.insert_inline(TypeId(2), SegmentPtr::from_page(PageId(200))).unwrap();
        
        // Add some types to IN header
        page.in_header.insert_inline(TypeId(1), SegmentPtr::from_page(PageId(150))).unwrap();
        
        let encoded = page.encode();
        let decoded = NodeAdjPage::decode(&encoded).unwrap();
        
        assert_eq!(decoded.owner(), NodeId(123));
        assert_eq!(decoded.out_header.active_count(), 2);
        assert_eq!(decoded.in_header.active_count(), 1);
        
        assert_eq!(decoded.out_header.lookup_inline(TypeId(1)), Some(SegmentPtr::from_page(PageId(100))));
        assert_eq!(decoded.out_header.lookup_inline(TypeId(2)), Some(SegmentPtr::from_page(PageId(200))));
        assert_eq!(decoded.in_header.lookup_inline(TypeId(1)), Some(SegmentPtr::from_page(PageId(150))));
    }

    #[test]
    fn ptr_null_check() {
        let null_ptr = NodeAdjPagePtr::null();
        assert!(null_ptr.is_null());
        
        let valid_ptr = NodeAdjPagePtr::from_page(PageId(42));
        assert!(!valid_ptr.is_null());
        assert_eq!(valid_ptr.page_id(), PageId(42));
    }

    #[test]
    fn ptr_roundtrip() {
        let ptr = NodeAdjPagePtr::from_page(PageId(12345));
        let bytes = ptr.to_bytes();
        let decoded = NodeAdjPagePtr::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, ptr);
    }
    
    #[test]
    fn owner_accessor() {
        let page = NodeAdjPage::new(NodeId(999));
        assert_eq!(page.owner(), NodeId(999));
    }
}
