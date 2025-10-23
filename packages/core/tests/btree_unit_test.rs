#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::index::BTreeIndex;
use sombra::storage::RecordPointer;

#[test]
fn test_btree_serialize_1000_entries() {
    let mut index = BTreeIndex::new();

    // Insert 1000 entries
    for i in 1..=1000 {
        index.insert(
            i,
            RecordPointer {
                page_id: i as u32,
                slot_index: (i % 100) as u16,
                byte_offset: (i % 50) as u16,
            },
        );
    }

    println!("Created index with {} entries", index.len());
    assert_eq!(index.len(), 1000);

    // Serialize
    let serialized = index.serialize().unwrap();
    println!("Serialized to {} bytes", serialized.len());

    // Deserialize
    let deserialized = BTreeIndex::deserialize(&serialized).unwrap();
    println!("Deserialized index has {} entries", deserialized.len());

    // Verify all entries
    assert_eq!(deserialized.len(), 1000);
    for i in 1..=1000 {
        let original = index.get(&i).unwrap();
        let restored = deserialized.get(&i).unwrap();
        assert_eq!(
            original, restored,
            "Node {i} mismatch: {original:?} != {restored:?}"
        );
    }

    println!("All 1000 entries verified!");
}
