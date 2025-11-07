use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

use sombra_pager::{PageStore, Pager, PagerOptions};
use sombra_storage::{Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec};
use sombra_types::{LabelId, NodeId, TypeId};

const SEED: u64 = 0x5eed_cafe;
const NODE_COUNT: usize = 1_000;
const EDGE_COUNT: usize = 5_000;

fn setup_graph() -> (Arc<Pager>, Graph) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bench.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
    let store: Arc<dyn sombra_pager::PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store)).expect("graph");

    // Keep directory alive for the duration of the benchmark by leaking tempdir.
    std::mem::forget(dir);

    (pager, graph)
}

fn seed_graph(pager: &Arc<Pager>, graph: &Graph) -> Vec<NodeId> {
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    {
        let mut write = pager.begin_write().expect("write txn");
        for _ in 0..NODE_COUNT {
            let node = graph
                .create_node(
                    &mut write,
                    NodeSpec {
                        labels: &[LabelId(rng.gen_range(0..16))],
                        props: &[],
                    },
                )
                .expect("create node");
            nodes.push(node);
        }

        for _ in 0..EDGE_COUNT {
            let src = nodes[rng.gen_range(0..nodes.len())];
            let dst = nodes[rng.gen_range(0..nodes.len())];
            let ty = TypeId(rng.gen_range(0..32));
            graph
                .create_edge(
                    &mut write,
                    EdgeSpec {
                        src,
                        dst,
                        ty,
                        props: &[],
                    },
                )
                .expect("create edge");
        }
        pager.commit(write).expect("commit");
    }
    pager
        .checkpoint(sombra_pager::CheckpointMode::Force)
        .expect("checkpoint");
    nodes
}

fn bench_neighbors(c: &mut Criterion) {
    let (pager, graph) = setup_graph();
    let nodes = seed_graph(&pager, &graph);

    let mut group = c.benchmark_group("neighbors");
    group.sample_size(50);
    let read = pager.begin_read().expect("read txn");
    for dir in [Dir::Out, Dir::In, Dir::Both] {
        group.bench_with_input(
            BenchmarkId::new("all", format!("{dir:?}")),
            &dir,
            |b, dir| {
                b.iter(|| {
                    let node = nodes[(black_box(SEED) as usize) % nodes.len()];
                    let cursor = graph
                        .neighbors(&read, node, *dir, None, ExpandOpts::default())
                        .expect("neighbors");
                    let count = cursor.count();
                    black_box(count);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("distinct", format!("{dir:?}")),
            &dir,
            |b, dir| {
                b.iter(|| {
                    let node = nodes[(black_box(SEED + 1) as usize) % nodes.len()];
                    let cursor = graph
                        .neighbors(
                            &read,
                            node,
                            *dir,
                            None,
                            ExpandOpts {
                                distinct_nodes: true,
                            },
                        )
                        .expect("neighbors");
                    black_box(cursor.count());
                });
            },
        );
    }
    group.finish();
}

fn bench_degree(c: &mut Criterion) {
    let (pager, graph) = setup_graph();
    let nodes = seed_graph(&pager, &graph);

    let mut group = c.benchmark_group("degree");
    group.sample_size(50);
    let read = pager.begin_read().expect("read txn");
    for dir in [Dir::Out, Dir::In, Dir::Both] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{dir:?}")),
            &dir,
            |b, dir| {
                b.iter(|| {
                    let node = nodes[(black_box(SEED + 2) as usize) % nodes.len()];
                    let degree = graph.degree(&read, node, *dir, None).expect("degree");
                    black_box(degree);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_neighbors, bench_degree);
criterion_main!(benches);
