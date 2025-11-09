#![forbid(unsafe_code)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions, Synchronous};
use sombra::storage::{Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec};
use sombra::types::{LabelId, NodeId, TypeId};
use tempfile::TempDir;

const NODE_COUNT: usize = 8_192;
const EDGE_COUNT: usize = 65_536;

fn micro_adjacency(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/adjacency");
    group.sample_size(40);
    group.throughput(Throughput::Elements(1));

    let mut harness = GraphHarness::new(NODE_COUNT, EDGE_COUNT, Synchronous::Normal);
    for dir in [Dir::Out, Dir::In, Dir::Both] {
        group.bench_with_input(
            BenchmarkId::new("neighbors_default", format!("{dir:?}")),
            &dir,
            |b, dir| {
                b.iter(|| black_box(harness.expand(*dir, false)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("neighbors_distinct", format!("{dir:?}")),
            &dir,
            |b, dir| {
                b.iter(|| black_box(harness.expand(*dir, true)));
            },
        );
    }
    group.finish();
}

struct GraphHarness {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    graph: Graph,
    nodes: Vec<NodeId>,
    cursor: usize,
}

impl GraphHarness {
    fn new(node_count: usize, edge_count: usize, synchronous: Synchronous) -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("adjacency.sombra");
        let mut pager_opts = PagerOptions::default();
        pager_opts.synchronous = synchronous;
        let pager = Arc::new(Pager::create(&path, pager_opts).expect("pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store).btree_inplace(true)).expect("graph");
        let mut harness = Self {
            _tmpdir: tmpdir,
            pager,
            graph,
            nodes: Vec::with_capacity(node_count),
            cursor: 0,
        };
        harness.seed(node_count, edge_count);
        harness
    }

    fn seed(&mut self, node_count: usize, edge_count: usize) {
        let mut write = self.pager.begin_write().expect("write");
        for _ in 0..node_count {
            let node = self
                .graph
                .create_node(
                    &mut write,
                    NodeSpec {
                        labels: &[LabelId(1)],
                        props: &[],
                    },
                )
                .expect("node");
            self.nodes.push(node);
        }

        let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
        for _ in 0..edge_count {
            let src = self.nodes[rng.gen_range(0..self.nodes.len())];
            let dst = self.nodes[rng.gen_range(0..self.nodes.len())];
            self.graph
                .create_edge(
                    &mut write,
                    EdgeSpec {
                        src,
                        dst,
                        ty: TypeId(1),
                        props: &[],
                    },
                )
                .expect("edge");
        }
        self.pager.commit(write).expect("commit");
        self.pager
            .checkpoint(CheckpointMode::Force)
            .expect("checkpoint");
    }

    fn expand(&mut self, dir: Dir, distinct: bool) -> usize {
        let node = self.next_node();
        let read = self.pager.begin_read().expect("read");
        let cursor = self
            .graph
            .neighbors(
                &read,
                node,
                dir,
                None,
                ExpandOpts {
                    distinct_nodes: distinct,
                    ..Default::default()
                },
            )
            .expect("neighbors");
        cursor.count()
    }

    fn next_node(&mut self) -> NodeId {
        if self.cursor >= self.nodes.len() {
            self.cursor = 0;
        }
        let node = self.nodes[self.cursor];
        self.cursor += 1;
        node
    }
}

criterion_group!(benches, micro_adjacency);
criterion_main!(benches);
