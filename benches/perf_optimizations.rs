//! Benchmarks for performance optimizations.
//!
//! These benchmarks measure the impact of various performance optimizations:
//! - Profiling overhead (when enabled vs disabled)
//! - B-tree put_many buffer reuse
//! - is_sorted guards for bulk operations
#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{PageStore, Pager, PagerOptions};
use sombra::storage::btree::{BTree, BTreeOptions, PutItem};

const BATCH_SIZE: usize = 10_000;
const SAMPLE_SIZE: usize = 20;

/// Benchmark put_many with sequential keys (tests buffer reuse).
fn bench_put_many_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_opts/put_many");
    group.sample_size(SAMPLE_SIZE);
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));

    group.bench_function("sequential_keys", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().expect("tmpdir");
                let path = tmpdir.path().join("bench.sombra");
                let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
                let store: Arc<dyn PageStore> = pager.clone();
                let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())
                    .expect("tree");

                let items: Vec<(u64, u64)> = (0..BATCH_SIZE as u64).map(|i| (i, i * 2)).collect();
                (tmpdir, pager, tree, items)
            },
            |(tmpdir, pager, tree, items)| {
                let mut write = pager.begin_write().expect("write");
                tree.put_many(
                    &mut write,
                    items.iter().map(|(k, v)| PutItem { key: k, value: v }),
                )
                .expect("put_many");
                pager.commit(write).expect("commit");
                black_box(tree.root_page());
                drop(tree);
                drop(pager);
                drop(tmpdir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("random_keys", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().expect("tmpdir");
                let path = tmpdir.path().join("bench.sombra");
                let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
                let store: Arc<dyn PageStore> = pager.clone();
                let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())
                    .expect("tree");

                let mut rng = ChaCha8Rng::seed_from_u64(0xDEAD_BEEF);
                let mut items: Vec<(u64, u64)> = (0..BATCH_SIZE)
                    .map(|_| {
                        let k = rng.gen::<u64>();
                        (k, k * 2)
                    })
                    .collect();
                // Sort to make put_many happy (it requires sorted input)
                items.sort_by_key(|(k, _)| *k);
                items.dedup_by_key(|(k, _)| *k);
                (tmpdir, pager, tree, items)
            },
            |(tmpdir, pager, tree, items)| {
                let mut write = pager.begin_write().expect("write");
                tree.put_many(
                    &mut write,
                    items.iter().map(|(k, v)| PutItem { key: k, value: v }),
                )
                .expect("put_many");
                pager.commit(write).expect("commit");
                black_box(tree.root_page());
                drop(tree);
                drop(pager);
                drop(tmpdir);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark repeated single puts vs put_many to show buffer reuse benefit.
fn bench_put_single_vs_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_opts/put_comparison");
    group.sample_size(SAMPLE_SIZE);

    let count = 1000usize;
    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("individual_puts", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().expect("tmpdir");
                let path = tmpdir.path().join("bench.sombra");
                let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
                let store: Arc<dyn PageStore> = pager.clone();
                let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())
                    .expect("tree");
                (tmpdir, pager, tree)
            },
            |(tmpdir, pager, tree)| {
                let mut write = pager.begin_write().expect("write");
                for i in 0..count as u64 {
                    tree.put(&mut write, &i, &(i * 2)).expect("put");
                }
                pager.commit(write).expect("commit");
                black_box(tree.root_page());
                drop(tree);
                drop(pager);
                drop(tmpdir);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("put_many_batch", |b| {
        b.iter_batched(
            || {
                let tmpdir = tempfile::tempdir().expect("tmpdir");
                let path = tmpdir.path().join("bench.sombra");
                let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
                let store: Arc<dyn PageStore> = pager.clone();
                let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())
                    .expect("tree");
                let items: Vec<(u64, u64)> = (0..count as u64).map(|i| (i, i * 2)).collect();
                (tmpdir, pager, tree, items)
            },
            |(tmpdir, pager, tree, items)| {
                let mut write = pager.begin_write().expect("write");
                tree.put_many(
                    &mut write,
                    items.iter().map(|(k, v)| PutItem { key: k, value: v }),
                )
                .expect("put_many");
                pager.commit(write).expect("commit");
                black_box(tree.root_page());
                drop(tree);
                drop(pager);
                drop(tmpdir);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark is_sorted check overhead.
fn bench_is_sorted(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf_opts/is_sorted");
    group.sample_size(50);

    for size in [100usize, 1000, 10000] {
        let sorted: Vec<u64> = (0..size as u64).collect();
        let mut unsorted = sorted.clone();
        unsorted[size / 2] = 0; // Make it unsorted

        group.throughput(Throughput::Elements(size as u64));

        group.bench_function(format!("sorted_{size}"), |b| {
            b.iter(|| {
                let is_sorted = sorted.windows(2).all(|w| w[0] <= w[1]);
                black_box(is_sorted)
            });
        });

        group.bench_function(format!("unsorted_{size}"), |b| {
            b.iter(|| {
                let is_sorted = unsorted.windows(2).all(|w| w[0] <= w[1]);
                black_box(is_sorted)
            });
        });
    }

    group.finish();
}

criterion_group!(
    perf_opts_benches,
    bench_put_many_sequential,
    bench_put_single_vs_many,
    bench_is_sorted,
);

criterion_main!(perf_opts_benches);
