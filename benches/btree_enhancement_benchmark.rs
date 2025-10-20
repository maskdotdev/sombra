use sombra::index::{BTreeIndex, CustomBTree};
use sombra::storage::RecordPointer;
use std::time::Instant;

fn benchmark_persistence(count: usize) {
    let mut btree = BTreeIndex::new();

    for i in 0..count {
        btree.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: (i % 1000) as u16,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    let serialized = btree.serialize().unwrap();
    let serialize_time = start.elapsed();

    let start = Instant::now();
    let _deserialized = BTreeIndex::deserialize(&serialized).unwrap();
    let deserialize_time = start.elapsed();

    println!("BTreeIndex Persistence (n={count})");
    println!("  Serialize:   {serialize_time:?}");
    println!("  Deserialize: {deserialize_time:?}");
    println!("  Data size:   {} bytes", serialized.len());
}

fn benchmark_range_queries(count: usize) {
    let mut btree = BTreeIndex::new();

    for i in 0..count {
        btree.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    let results: Vec<_> = btree
        .range(count as u64 / 4, count as u64 * 3 / 4)
        .into_iter()
        .collect();
    let range_time = start.elapsed();

    let start = Instant::now();
    let from_results: Vec<_> = btree.range_from(count as u64 / 2).into_iter().collect();
    let range_from_time = start.elapsed();

    let start = Instant::now();
    let to_results: Vec<_> = btree.range_to(count as u64 / 2).into_iter().collect();
    let range_to_time = start.elapsed();

    println!("BTreeIndex Range Queries (n={count})");
    println!(
        "  range(25%, 75%): {:?} ({} results)",
        range_time,
        results.len()
    );
    println!(
        "  range_from(50%): {:?} ({} results)",
        range_from_time,
        from_results.len()
    );
    println!(
        "  range_to(50%):   {:?} ({} results)",
        range_to_time,
        to_results.len()
    );
}

fn benchmark_bulk_operations(count: usize) {
    let entries: Vec<_> = (0..count)
        .map(|i| {
            (
                i as u64,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: 0,
                    byte_offset: 0,
                },
            )
        })
        .collect();

    let mut btree = BTreeIndex::new();

    let start = Instant::now();
    btree.batch_insert(entries.clone());
    let insert_time = start.elapsed();

    let keys_to_remove: Vec<_> = (0..count / 2).map(|i| i as u64).collect();

    let start = Instant::now();
    let removed = btree.batch_remove(&keys_to_remove);
    let remove_time = start.elapsed();

    println!("BTreeIndex Bulk Operations (n={count})");
    println!("  batch_insert: {insert_time:?}");
    println!(
        "  batch_remove: {:?} ({} removed)",
        remove_time,
        removed.len()
    );
}

fn benchmark_custom_btree(count: usize) {
    let mut custom = CustomBTree::new();

    let start = Instant::now();
    for i in 0..count {
        custom.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }
    let insert_time = start.elapsed();

    let start = Instant::now();
    for i in 0..count {
        let _ = custom.get(&(i as u64));
    }
    let lookup_time = start.elapsed();

    let start = Instant::now();
    let _: Vec<_> = custom.iter().collect();
    let iter_time = start.elapsed();

    println!("CustomBTree (256-ary) Performance (n={count})");
    println!("  insert: {insert_time:?}");
    println!("  lookup: {lookup_time:?}");
    println!("  iter:   {iter_time:?}");
}

fn compare_btree_implementations(count: usize) {
    let mut std_btree = BTreeIndex::new();
    let mut custom_btree = CustomBTree::new();

    let start = Instant::now();
    for i in 0..count {
        std_btree.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }
    let std_insert = start.elapsed();

    let start = Instant::now();
    for i in 0..count {
        custom_btree.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }
    let custom_insert = start.elapsed();

    let start = Instant::now();
    for i in 0..count {
        let _ = std_btree.get(&(i as u64));
    }
    let std_lookup = start.elapsed();

    let start = Instant::now();
    for i in 0..count {
        let _ = custom_btree.get(&(i as u64));
    }
    let custom_lookup = start.elapsed();

    println!("BTree Implementation Comparison (n={count})");
    println!("  Standard BTreeMap:");
    println!("    insert: {std_insert:?}");
    println!("    lookup: {std_lookup:?}");
    println!("  Custom 256-ary BTree:");
    println!(
        "    insert: {:?} ({:.2}x)",
        custom_insert,
        std_insert.as_nanos() as f64 / custom_insert.as_nanos() as f64
    );
    println!(
        "    lookup: {:?} ({:.2}x)",
        custom_lookup,
        std_lookup.as_nanos() as f64 / custom_lookup.as_nanos() as f64
    );
}

fn main() {
    println!("=== B-tree Enhancement Benchmarks ===\n");

    println!("--- On-Disk Persistence ---");
    benchmark_persistence(10_000);
    println!();
    benchmark_persistence(100_000);
    println!();

    println!("--- Range Queries ---");
    benchmark_range_queries(10_000);
    println!();
    benchmark_range_queries(100_000);
    println!();

    println!("--- Bulk Operations ---");
    benchmark_bulk_operations(10_000);
    println!();
    benchmark_bulk_operations(100_000);
    println!();

    println!("--- Custom B-tree (256-ary) ---");
    benchmark_custom_btree(10_000);
    println!();
    benchmark_custom_btree(100_000);
    println!();

    println!("--- Implementation Comparison ---");
    compare_btree_implementations(10_000);
    println!();
    compare_btree_implementations(100_000);
    println!();
}
