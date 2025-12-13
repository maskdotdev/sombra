//! Per-node adjacency page for true Index-Free Adjacency.
//!
//! Each node can have a dedicated adjacency page that stores both OUT and IN
//! NodeAdjHeaders. This enables O(1) neighbor lookups by storing a single
//! `adj_page_id` in the node row, eliminating B-tree lookups for adjacency.
//!
//! # Page Layout
//!
//! ```text
//! +------------------+
//! | Page Header (8B) |  - magic, version, flags
//! +------------------+
//! | OUT Header (72B) |  - NodeAdjHeader for outgoing edges
//! +------------------+
//! | IN Header (72B)  |  - NodeAdjHeader for incoming edges
//! +------------------+
//! | Reserved         |  - future expansion (overflow, stats, etc.)
//! +------------------+
//! ```
//!
//! Total fixed size: 152 bytes, fits easily in a 4KB page.

use crate::storage::adjacency::Dir;
use crate::types::{PageId, Result, SombraError};

use super::types::{NodeAdjHeader, NODE_ADJ_HEADER_LEN};

/// Magic number for node adjacency pages.
const NODE_ADJ_PAGE_MAGIC: u32 = 0x4E414450; // "NADP"

/// Version of the node adjacency page format.
const NODE_ADJ_PAGE_VERSION: u16 = 1;

/// Size of the page header.
const PAGE_HEADER_LEN: usize = 8;

/// Offset of OUT header in the page.
const OUT_HEADER_OFFSET: usize = PAGE_HEADER_LEN;

/// Offset of IN header in the page.
const IN_HEADER_OFFSET: usize = OUT_HEADER_OFFSET + NODE_ADJ_HEADER_LEN;

/// Total size of the node adjacency data (header + both directions).
pub const NODE_ADJ_PAGE_DATA_LEN: usize = PAGE_HEADER_LEN + 2 * NODE_ADJ_HEADER_LEN;

/// A node's adjacency page containing headers for both directions.
#[derive(Clone, Debug)]
pub struct NodeAdjPage {
    /// Header for outgoing edges.
    pub out_header: NodeAdjHeader,
    /// Header for incoming edges.
    pub in_header: NodeAdjHeader,
}

impl Default for NodeAdjPage {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeAdjPage {
    /// Creates a new empty node adjacency page.
    pub fn new() -> Self {
        Self {
            out_header: NodeAdjHeader::new(),
            in_header: NodeAdjHeader::new(),
        }
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
        
        // OUT header
        buf.extend_from_slice(&self.out_header.encode());
        
        // IN header
        buf.extend_from_slice(&self.in_header.encode());
        
        buf
    }

    /// Decodes a page from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < NODE_ADJ_PAGE_DATA_LEN {
            return Err(SombraError::Corruption("node adj page too short"));
        }

        // Verify magic
        let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
        if magic != NODE_ADJ_PAGE_MAGIC {
            return Err(SombraError::Corruption("invalid node adj page magic"));
        }

        // Verify version
        let version = u16::from_be_bytes(data[4..6].try_into().unwrap());
        if version != NODE_ADJ_PAGE_VERSION {
            return Err(SombraError::Corruption("unsupported node adj page version"));
        }

        // Decode headers
        let out_header = NodeAdjHeader::decode(&data[OUT_HEADER_OFFSET..OUT_HEADER_OFFSET + NODE_ADJ_HEADER_LEN])?;
        let in_header = NodeAdjHeader::decode(&data[IN_HEADER_OFFSET..IN_HEADER_OFFSET + NODE_ADJ_HEADER_LEN])?;

        Ok(Self {
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
    pub const fn is_null(self) -> bool {
        self.0.0 == 0
    }

    /// Creates a pointer from a PageId.
    #[inline]
    pub const fn from_page(page: PageId) -> Self {
        Self(page)
    }

    /// Returns the PageId.
    #[inline]
    pub const fn page_id(self) -> PageId {
        self.0
    }

    /// Encodes as big-endian bytes.
    #[inline]
    pub fn to_bytes(self) -> [u8; 8] {
        self.0.0.to_be_bytes()
    }

    /// Decodes from big-endian bytes.
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
        let page = NodeAdjPage::new();
        let encoded = page.encode();
        let decoded = NodeAdjPage::decode(&encoded).unwrap();
        
        assert_eq!(decoded.out_header.active_count(), 0);
        assert_eq!(decoded.in_header.active_count(), 0);
    }

    #[test]
    fn page_with_types_roundtrip() {
        let mut page = NodeAdjPage::new();
        
        // Add some types to OUT header
        page.out_header.insert_inline(TypeId(1), SegmentPtr::from_page(PageId(100))).unwrap();
        page.out_header.insert_inline(TypeId(2), SegmentPtr::from_page(PageId(200))).unwrap();
        
        // Add some types to IN header
        page.in_header.insert_inline(TypeId(1), SegmentPtr::from_page(PageId(150))).unwrap();
        
        let encoded = page.encode();
        let decoded = NodeAdjPage::decode(&encoded).unwrap();
        
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
}
