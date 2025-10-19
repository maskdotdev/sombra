use sombra::index::BTreeIndex;
use sombra::storage::RecordPointer;
use std::collections::HashMap;
use std::time::Instant;

fn benchmark_btree_insert(count: usize) -> u128 {
    let mut index = BTreeIndex::new();
    let start = Instant::now();

    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    start.elapsed().as_micros()
}

fn benchmark_hashmap_insert(count: usize) -> u128 {
    let mut index = HashMap::new();
    let start = Instant::now();

    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    start.elapsed().as_micros()
}

fn benchmark_btree_lookup(count: usize, lookups: usize) -> u128 {
    let mut index = BTreeIndex::new();
    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    for i in 0..lookups {
        let key = (i % count) as u64;
        let _ = index.get(&key);
    }
    start.elapsed().as_micros()
}

fn benchmark_hashmap_lookup(count: usize, lookups: usize) -> u128 {
    let mut index = HashMap::new();
    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    for i in 0..lookups {
        let key = (i % count) as u64;
        let _ = index.get(&key);
    }
    start.elapsed().as_micros()
}

fn benchmark_btree_iteration(count: usize) -> u128 {
    let mut index = BTreeIndex::new();
    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    let _: Vec<_> = index.iter().collect();
    start.elapsed().as_micros()
}

fn benchmark_hashmap_iteration(count: usize) -> u128 {
    let mut index = HashMap::new();
    for i in 0..count {
        index.insert(
            i as u64,
            RecordPointer {
                page_id: i as u32,
                slot_index: 0,
                byte_offset: 0,
            },
        );
    }

    let start = Instant::now();
    let _: Vec<_> = index.iter().collect();
    start.elapsed().as_micros()
}

fn main() {
    println!("=== Index Performance Benchmark ===\n");

    let sizes = vec![100, 1000, 10000, 100000];

    println!("## Insert Performance");
    println!(
        "{:<10} {:<15} {:<15} {:<15}",
        "Size", "BTree (µs)", "HashMap (µs)", "Ratio"
    );
    println!("{:-<55}", "");
    for &size in &sizes {
        let btree_time = benchmark_btree_insert(size);
        let hashmap_time = benchmark_hashmap_insert(size);
        let ratio = btree_time as f64 / hashmap_time as f64;
        println!(
            "{:<10} {:<15} {:<15} {:<15.2}x",
            size, btree_time, hashmap_time, ratio
        );
    }

    println!("\n## Lookup Performance (10K lookups)");
    println!(
        "{:<10} {:<15} {:<15} {:<15}",
        "Size", "BTree (µs)", "HashMap (µs)", "Ratio"
    );
    println!("{:-<55}", "");
    for &size in &sizes {
        let btree_time = benchmark_btree_lookup(size, 10000);
        let hashmap_time = benchmark_hashmap_lookup(size, 10000);
        let ratio = btree_time as f64 / hashmap_time as f64;
        println!(
            "{:<10} {:<15} {:<15} {:<15.2}x",
            size, btree_time, hashmap_time, ratio
        );
    }

    println!("\n## Iteration Performance");
    println!(
        "{:<10} {:<15} {:<15} {:<15}",
        "Size", "BTree (µs)", "HashMap (µs)", "Ratio"
    );
    println!("{:-<55}", "");
    for &size in &sizes {
        let btree_time = benchmark_btree_iteration(size);
        let hashmap_time = benchmark_hashmap_iteration(size);
        let ratio = btree_time as f64 / hashmap_time as f64;
        println!(
            "{:<10} {:<15} {:<15} {:<15.2}x",
            size, btree_time, hashmap_time, ratio
        );
    }

    println!("\n## Memory Characteristics");
    println!("BTree:");
    println!("  - Better cache locality (sorted keys)");
    println!("  - Lower memory overhead per entry");
    println!("  - Predictable iteration order");
    println!("\nHashMap:");
    println!("  - Constant-time lookups (expected)");
    println!("  - Higher memory overhead (hash buckets)");
    println!("  - Random iteration order");

    println!("\n=== Benchmark Complete ===");
}
