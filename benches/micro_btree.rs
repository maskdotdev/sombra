//! Micro benchmarks for the on-disk B-Tree implementation.
#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use std::ops::Bound;
use std::sync::Arc;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{PageStore, Pager, PagerOptions};
use sombra::storage::btree::{BTree, BTreeOptions};
use tempfile::TempDir;

const INSERT_COUNT: u64 = 32_768;
const LOOKUP_SAMPLES: usize = 4_096;
const RANGE_WIDTH: u64 = 512;

fn micro_btree(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/btree");
    group.sample_size(30);

    group.throughput(Throughput::Elements(INSERT_COUNT));
    group.bench_function("sequential_insert", |b| {
        b.iter_batched(
            FreshTree::new,
            |mut tree| {
                tree.insert_sequence(0, INSERT_COUNT);
                black_box(tree.tree.root_page());
            },
            BatchSize::SmallInput,
        );
    });

    let mut random_keys: Vec<u64> = (0..INSERT_COUNT).collect();
    random_keys.shuffle(&mut ChaCha8Rng::seed_from_u64(0xBEEF_F00D));
    group.throughput(Throughput::Elements(INSERT_COUNT));
    group.bench_function("random_insert", |b| {
        b.iter_batched(
            FreshTree::new,
            |mut tree| {
                tree.insert_keys(&random_keys);
                black_box(tree.tree.root_page());
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(INSERT_COUNT));
    group.bench_function("delete_random", |b| {
        b.iter_batched(
            || {
                let mut tree = FreshTree::new();
                tree.insert_sequence(0, INSERT_COUNT);
                tree
            },
            |mut tree| {
                tree.delete_keys(&random_keys);
                black_box(tree.tree.root_page());
            },
            BatchSize::SmallInput,
        );
    });

    let mut lookup_harness = LoadedTree::new(INSERT_COUNT);
    group.throughput(Throughput::Elements(LOOKUP_SAMPLES as u64));
    group.bench_function(BenchmarkId::new("point_lookup", LOOKUP_SAMPLES), |b| {
        b.iter(|| lookup_harness.point_lookup(LOOKUP_SAMPLES));
    });

    group.throughput(Throughput::Elements(RANGE_WIDTH));
    group.bench_function(BenchmarkId::new("range_scan", RANGE_WIDTH), |b| {
        b.iter(|| lookup_harness.range_scan(RANGE_WIDTH));
    });

    group.finish();
}

struct FreshTree {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    tree: BTree<u64, u64>,
}

impl FreshTree {
    fn new() -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("btree.sombra");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let tree = BTree::open_or_create(&store, BTreeOptions::default()).expect("tree");
        Self {
            _tmpdir: tmpdir,
            pager,
            tree,
        }
    }

    fn insert_sequence(&mut self, start: u64, count: u64) {
        let mut write = self.pager.begin_write().expect("write");
        for key in start..(start + count) {
            self.tree.put(&mut write, &key, &key).expect("insert");
        }
        self.pager.commit(write).expect("commit");
    }

    fn insert_keys(&mut self, keys: &[u64]) {
        let mut write = self.pager.begin_write().expect("write");
        for key in keys {
            self.tree.put(&mut write, key, key).expect("insert");
        }
        self.pager.commit(write).expect("commit");
    }

    fn delete_keys(&mut self, keys: &[u64]) {
        let mut write = self.pager.begin_write().expect("write");
        for key in keys {
            self.tree.delete(&mut write, key).expect("delete");
        }
        self.pager.commit(write).expect("commit");
    }
}

struct LoadedTree {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    tree: BTree<u64, u64>,
    max_key: u64,
    rng: ChaCha8Rng,
}

impl LoadedTree {
    fn new(count: u64) -> Self {
        let mut fresh = FreshTree::new();
        fresh.insert_sequence(0, count);
        let FreshTree {
            _tmpdir,
            pager,
            tree,
        } = fresh;
        Self {
            _tmpdir,
            pager,
            tree,
            max_key: count,
            rng: ChaCha8Rng::seed_from_u64(0xFEED_FACE),
        }
    }

    fn point_lookup(&mut self, samples: usize) {
        let read = self.pager.begin_read().expect("read");
        for _ in 0..samples {
            let key = self.rng.gen_range(0..self.max_key);
            black_box(self.tree.get(&read, &key).expect("get"));
        }
    }

    fn range_scan(&mut self, width: u64) {
        let read = self.pager.begin_read().expect("read");
        for _ in 0..16 {
            let start = self.rng.gen_range(0..(self.max_key - width));
            let mut cursor = self
                .tree
                .range(
                    &read,
                    Bound::Included(start),
                    Bound::Included(start + width),
                )
                .expect("range");
            while let Some((k, v)) = cursor.next().expect("cursor") {
                black_box((k, v));
            }
        }
    }
}

criterion_group!(benches, micro_btree);
criterion_main!(benches);
