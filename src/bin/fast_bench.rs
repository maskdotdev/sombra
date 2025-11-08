//! Fast micro-benchmarks for Sombra DB
//!
//! Provides quick performance measurements with minimal overhead.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{PageStore, Pager, PagerOptions};
use sombra::storage::btree::{BTree, BTreeOptions};

/// Benchmark result
#[derive(Debug)]
struct BenchResult {
    name: String,
    conc: Option<usize>,
    docs: usize,
    time: Duration,
}

impl BenchResult {
    fn print_header(section: &str) {
        println!("\n{}", section.to_uppercase());
        println!(
            "{:<20} {:>10} {:>10} {:>15}",
            "BENCHMARK", "CONC", "DOCS", "TIME"
        );
    }

    fn print(&self) {
        let conc_str = self.conc.map_or("-".to_string(), |c| c.to_string());
        let time_str = format_duration(self.time);

        println!(
            "{:<20} {:>10} {:>10} {:>15}",
            self.name, conc_str, self.docs, time_str
        );
    }
}

fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1_000 {
        format!("{} µs", micros)
    } else if micros < 1_000_000 {
        format!("{:.2} ms", micros as f64 / 1_000.0)
    } else {
        format!("{:.2} s", micros as f64 / 1_000_000.0)
    }
}

/// Run a benchmark and measure time
fn bench<F>(name: &str, conc: Option<usize>, docs: usize, f: F) -> BenchResult
where
    F: FnOnce(),
{
    // Run benchmark
    let start = Instant::now();
    f();
    let elapsed = start.elapsed();

    BenchResult {
        name: name.to_string(),
        conc,
        docs,
        time: elapsed,
    }
}

/// BTree benchmarks
fn bench_btree_insert(docs: usize) -> BenchResult {
    bench("Insert", None, docs, || {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("btree.sombra");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

        let mut write = pager.begin_write().unwrap();
        for i in 0..docs {
            tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
        }
        pager.commit(write).unwrap();
    })
}

fn bench_btree_find(docs: usize) -> BenchResult {
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

    // Pre-populate
    let mut write = pager.begin_write().unwrap();
    for i in 0..docs {
        tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
    }
    pager.commit(write).unwrap();

    bench("Find", None, docs, move || {
        let read = pager.begin_read().unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        for _ in 0..docs {
            let key = rng.gen_range(0..docs) as u64;
            let _ = tree.get(&read, &key).unwrap();
        }
    })
}

fn bench_btree_read(docs: usize) -> BenchResult {
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

    // Pre-populate
    let mut write = pager.begin_write().unwrap();
    for i in 0..docs {
        tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
    }
    pager.commit(write).unwrap();

    bench("Read", None, docs, move || {
        let read = pager.begin_read().unwrap();
        for i in 0..docs {
            let _ = tree.get(&read, &(i as u64)).unwrap();
        }
    })
}

fn bench_btree_mixed(docs: usize) -> BenchResult {
    bench("Mixed", None, docs, || {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("btree.sombra");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let mut write = pager.begin_write().unwrap();

        for i in 0..docs {
            // 70% write, 30% read
            if rng.gen_bool(0.7) {
                tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
            } else if i > 0 {
                let key = rng.gen_range(0..i) as u64;
                pager.commit(write).unwrap();
                let read = pager.begin_read().unwrap();
                let _ = tree.get(&read, &key).unwrap();
                write = pager.begin_write().unwrap();
            }
        }
        pager.commit(write).unwrap();
    })
}

fn main() {
    println!("=== Sombra DB Fast Benchmarks ===");

    // BTree Insert benchmarks
    BenchResult::print_header("btree insert");
    bench_btree_insert(1).print();
    bench_btree_insert(1_000).print();
    bench_btree_insert(10_000).print();

    // BTree Find benchmarks
    BenchResult::print_header("btree find");
    bench_btree_find(2_500).print();
    bench_btree_find(10_000).print();

    // BTree Read benchmarks
    BenchResult::print_header("btree read");
    bench_btree_read(1_500).print();
    bench_btree_read(2_500).print();
    bench_btree_read(5_000).print();

    // BTree Mixed benchmarks
    BenchResult::print_header("btree mixed");
    bench_btree_mixed(1_000).print();
    bench_btree_mixed(2_000).print();

    println!("\n=== Benchmarks Complete ===");
}
