//! Tests for Index-Free Adjacency structures.

#![allow(unused_imports)]

use super::types::*;
use super::segment::*;
use super::*;
use crate::storage::adjacency::Dir;
use crate::types::{EdgeId, NodeId, PageId, TypeId};

#[test]
fn segment_ptr_basics() {
    let null = SegmentPtr::null();
    assert!(null.is_null());
    assert_eq!(null.0, 0);

    let ptr = SegmentPtr(42);
    assert!(!ptr.is_null());
    assert_eq!(ptr.to_page(), PageId(42));

    let from_page = SegmentPtr::from_page(PageId(100));
    assert_eq!(from_page.0, 100);
}

#[test]
fn segment_ptr_roundtrip() {
    let ptr = SegmentPtr(0x123456789ABCDEF0);
    let bytes = ptr.to_bytes();
    let decoded = SegmentPtr::from_bytes(&bytes).unwrap();
    assert_eq!(ptr, decoded);
}

#[test]
fn type_bucket_basics() {
    let empty = TypeBucket::empty();
    assert!(empty.is_empty());
    assert!(!empty.is_overflow());

    let bucket = TypeBucket::new(TypeId(5), SegmentPtr(100));
    assert!(!bucket.is_empty());
    assert!(!bucket.is_overflow());
    assert_eq!(bucket.type_id, TypeId(5));

    let overflow = TypeBucket::overflow(SegmentPtr(200));
    assert!(!overflow.is_empty());
    assert!(overflow.is_overflow());
    assert_eq!(overflow.type_id, OVERFLOW_TAG);
}

#[test]
fn type_bucket_roundtrip() {
    let bucket = TypeBucket::new(TypeId(42), SegmentPtr(12345));
    let encoded = bucket.encode();
    let decoded = TypeBucket::decode(&encoded).unwrap();
    assert_eq!(bucket, decoded);
}

#[test]
fn node_adj_header_empty() {
    let header = NodeAdjHeader::new();
    assert_eq!(header.active_count(), 0);
    assert!(!header.has_overflow());
    assert!(header.overflow_ptr().is_none());
    assert!(header.lookup_inline(TypeId(1)).is_none());
}

#[test]
fn node_adj_header_inline_operations() {
    let mut header = NodeAdjHeader::new();

    // Insert types
    header.insert_inline(TypeId(5), SegmentPtr(100)).unwrap();
    header.insert_inline(TypeId(10), SegmentPtr(200)).unwrap();
    header.insert_inline(TypeId(15), SegmentPtr(300)).unwrap();

    assert_eq!(header.active_count(), 3);
    assert_eq!(header.lookup_inline(TypeId(5)), Some(SegmentPtr(100)));
    assert_eq!(header.lookup_inline(TypeId(10)), Some(SegmentPtr(200)));
    assert_eq!(header.lookup_inline(TypeId(15)), Some(SegmentPtr(300)));
    assert_eq!(header.lookup_inline(TypeId(99)), None);

    // Update existing
    header.insert_inline(TypeId(10), SegmentPtr(250)).unwrap();
    assert_eq!(header.lookup_inline(TypeId(10)), Some(SegmentPtr(250)));
    assert_eq!(header.active_count(), 3);

    // Remove
    let old = header.remove_inline(TypeId(10));
    assert_eq!(old, Some(SegmentPtr(250)));
    assert_eq!(header.active_count(), 2);
    assert!(header.lookup_inline(TypeId(10)).is_none());
}

#[test]
fn node_adj_header_overflow_trigger() {
    let mut header = NodeAdjHeader::new();

    // Fill all K-1 slots (last is reserved for overflow)
    // Start from TypeId(1) because TypeId(0) with SegmentPtr(0) looks like empty
    for i in 0..INLINE_BUCKET_COUNT - 1 {
        header
            .insert_inline(TypeId(i as u32 + 1), SegmentPtr((i as u64 + 1) * 100))
            .unwrap();
    }

    assert_eq!(header.active_count(), INLINE_BUCKET_COUNT - 1);

    // Next insert should fail (needs overflow)
    let result = header.insert_inline(TypeId(100), SegmentPtr(1000));
    assert!(result.is_err());
}

#[test]
fn node_adj_header_with_overflow() {
    let mut header = NodeAdjHeader::new();

    // Add some inline types
    header.insert_inline(TypeId(1), SegmentPtr(100)).unwrap();
    header.insert_inline(TypeId(2), SegmentPtr(200)).unwrap();

    // Set overflow
    header.set_overflow(SegmentPtr(9999));
    assert!(header.has_overflow());
    assert_eq!(header.overflow_ptr(), Some(SegmentPtr(9999)));

    // Inline lookups still work
    assert_eq!(header.lookup_inline(TypeId(1)), Some(SegmentPtr(100)));
    assert_eq!(header.lookup_inline(TypeId(2)), Some(SegmentPtr(200)));

    // Unknown type returns None (caller should check overflow)
    assert!(header.lookup_inline(TypeId(99)).is_none());

    // Clear overflow
    header.clear_overflow();
    assert!(!header.has_overflow());
}

#[test]
fn node_adj_header_roundtrip() {
    let mut header = NodeAdjHeader::new();
    header.insert_inline(TypeId(5), SegmentPtr(100)).unwrap();
    header.insert_inline(TypeId(10), SegmentPtr(200)).unwrap();
    header.set_overflow(SegmentPtr(9999));

    let encoded = header.encode();
    let decoded = NodeAdjHeader::decode(&encoded).unwrap();

    assert_eq!(header, decoded);
}

#[test]
fn node_adj_header_iter_types() {
    let mut header = NodeAdjHeader::new();
    header.insert_inline(TypeId(5), SegmentPtr(100)).unwrap();
    header.insert_inline(TypeId(10), SegmentPtr(200)).unwrap();
    header.insert_inline(TypeId(15), SegmentPtr(300)).unwrap();

    let types: Vec<_> = header.iter_types().collect();
    assert_eq!(types.len(), 3);
    assert!(types.contains(&(TypeId(5), SegmentPtr(100))));
    assert!(types.contains(&(TypeId(10), SegmentPtr(200))));
    assert!(types.contains(&(TypeId(15), SegmentPtr(300))));
}

#[test]
fn overflow_block_empty() {
    let block = OverflowBlock::new();
    assert!(block.is_empty());
    assert!(!block.is_full());
    assert!(block.lookup(TypeId(1)).is_none());
}

#[test]
fn overflow_block_insert_lookup() {
    let mut block = OverflowBlock::new();

    // Insert in non-sorted order
    block.insert(TypeId(30), SegmentPtr(300)).unwrap();
    block.insert(TypeId(10), SegmentPtr(100)).unwrap();
    block.insert(TypeId(20), SegmentPtr(200)).unwrap();

    assert_eq!(block.entry_count, 3);
    assert_eq!(block.lookup(TypeId(10)), Some(SegmentPtr(100)));
    assert_eq!(block.lookup(TypeId(20)), Some(SegmentPtr(200)));
    assert_eq!(block.lookup(TypeId(30)), Some(SegmentPtr(300)));
    assert_eq!(block.lookup(TypeId(99)), None);

    // Verify sorted order
    let entries: Vec<_> = block.iter().collect();
    assert_eq!(entries[0].0, TypeId(10));
    assert_eq!(entries[1].0, TypeId(20));
    assert_eq!(entries[2].0, TypeId(30));
}

#[test]
fn overflow_block_update() {
    let mut block = OverflowBlock::new();

    block.insert(TypeId(10), SegmentPtr(100)).unwrap();
    block.insert(TypeId(10), SegmentPtr(150)).unwrap();

    assert_eq!(block.entry_count, 1);
    assert_eq!(block.lookup(TypeId(10)), Some(SegmentPtr(150)));
}

#[test]
fn overflow_block_remove() {
    let mut block = OverflowBlock::new();

    block.insert(TypeId(10), SegmentPtr(100)).unwrap();
    block.insert(TypeId(20), SegmentPtr(200)).unwrap();
    block.insert(TypeId(30), SegmentPtr(300)).unwrap();

    let old = block.remove(TypeId(20));
    assert_eq!(old, Some(SegmentPtr(200)));
    assert_eq!(block.entry_count, 2);
    assert!(block.lookup(TypeId(20)).is_none());

    // Remaining entries still sorted
    let entries: Vec<_> = block.iter().collect();
    assert_eq!(entries[0].0, TypeId(10));
    assert_eq!(entries[1].0, TypeId(30));
}

#[test]
fn overflow_block_full() {
    let mut block = OverflowBlock::new();

    // Fill the block
    for i in 0..OVERFLOW_BLOCK_ENTRIES {
        block.insert(TypeId(i as u32), SegmentPtr(i as u64)).unwrap();
    }

    assert!(block.is_full());

    // New insert should fail
    let result = block.insert(TypeId(9999), SegmentPtr(9999));
    assert!(result.is_err());

    // But update should still work
    block.insert(TypeId(5), SegmentPtr(5555)).unwrap();
    assert_eq!(block.lookup(TypeId(5)), Some(SegmentPtr(5555)));
}

#[test]
fn overflow_block_roundtrip() {
    let mut block = OverflowBlock::new();
    block.next = SegmentPtr(12345);
    block.insert(TypeId(10), SegmentPtr(100)).unwrap();
    block.insert(TypeId(20), SegmentPtr(200)).unwrap();

    let encoded = block.encode();
    let decoded = OverflowBlock::decode(&encoded).unwrap();

    assert_eq!(block.next, decoded.next);
    assert_eq!(block.entry_count, decoded.entry_count);
    for i in 0..block.entry_count as usize {
        assert_eq!(block.entries[i], decoded.entries[i]);
    }
}

#[test]
fn constants_sanity() {
    // Verify our constants make sense
    assert_eq!(SEGMENT_PTR_LEN, 8);
    assert_eq!(TYPE_BUCKET_LEN, 12); // 4 + 8
    assert_eq!(NODE_ADJ_HEADER_LEN, INLINE_BUCKET_COUNT * TYPE_BUCKET_LEN);
    assert_eq!(ADJ_ENTRY_LEN, 32); // neighbor(8) + edge(8) + xmin(8) + xmax(8)
    assert_eq!(ADJ_SEGMENT_HEADER_LEN, 50);

    // K=6 should fit in a cache line (64 bytes) with some room
    assert!(NODE_ADJ_HEADER_LEN <= 128);
}
