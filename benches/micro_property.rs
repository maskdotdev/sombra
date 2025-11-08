#![forbid(unsafe_code)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{PageStore, Pager, PagerOptions};
use sombra::storage::index::{intersect_sorted, IndexDef, IndexKind, TypeTag, VecPostingStream};
use sombra::storage::{Graph, GraphOptions, NodeSpec, PropEntry, PropValue, PropValueOwned};
use sombra::types::{LabelId, PropId};
use tempfile::TempDir;

const NODE_COUNT: usize = 16_384;
const VALUE_DOMAIN: i64 = 10_000;

fn micro_property(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/property_index");
    group.sample_size(40);
    let mut harness = PropertyHarness::new(NODE_COUNT, VALUE_DOMAIN);

    group.throughput(Throughput::Elements(1));
    group.bench_function("eq_lookup", |b| {
        b.iter(|| black_box(harness.eq_lookup()));
    });

    group.throughput(Throughput::Elements(256));
    group.bench_function("range_lookup", |b| {
        b.iter(|| black_box(harness.range_lookup(256)));
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("posting_intersect", |b| {
        b.iter(|| black_box(harness.intersect()));
    });

    group.finish();
}

struct PropertyHarness {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    graph: Graph,
    label: LabelId,
    prop: PropId,
    domain: i64,
    rng: ChaCha8Rng,
}

impl PropertyHarness {
    fn new(node_count: usize, domain: i64) -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("property.sombra");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store)).expect("graph");
        let label = LabelId(1);
        let prop = PropId(1);
        let mut rng = ChaCha8Rng::seed_from_u64(0xC0FFEE);
        {
            let mut write = pager.begin_write().expect("write");
            for _ in 0..node_count {
                let score = rng.gen_range(0..domain);
                let props = [PropEntry::new(prop, PropValue::Int(score))];
                graph
                    .create_node(
                        &mut write,
                        NodeSpec {
                            labels: &[label],
                            props: &props,
                        },
                    )
                    .expect("node");
            }
            graph
                .create_property_index(
                    &mut write,
                    IndexDef {
                        label,
                        prop,
                        kind: IndexKind::Chunked,
                        ty: TypeTag::Int,
                    },
                )
                .expect("index");
            pager.commit(write).expect("commit");
        }
        Self {
            _tmpdir: tmpdir,
            pager,
            graph,
            label,
            prop,
            domain,
            rng,
        }
    }

    fn eq_lookup(&mut self) -> usize {
        let target = self.random_value();
        let read = self.pager.begin_read().expect("read");
        let rows = self
            .graph
            .property_scan_eq(&read, self.label, self.prop, &target)
            .expect("eq scan");
        rows.len()
    }

    fn range_lookup(&mut self, width: i64) -> usize {
        let start = self.rng.gen_range(0..(self.domain - width - 1));
        let end = start + width;
        let start_val = PropValueOwned::Int(start);
        let end_val = PropValueOwned::Int(end);
        let read = self.pager.begin_read().expect("read");
        let rows = self
            .graph
            .property_scan_range(&read, self.label, self.prop, &start_val, &end_val)
            .expect("range");
        rows.len()
    }

    fn intersect(&mut self) -> usize {
        let left = self.random_value();
        let right = self.random_value();
        let read = self.pager.begin_read().expect("read");
        let left_rows = self
            .graph
            .property_scan_eq(&read, self.label, self.prop, &left)
            .expect("left");
        let right_rows = self
            .graph
            .property_scan_eq(&read, self.label, self.prop, &right)
            .expect("right");
        let mut out = Vec::new();
        let mut left_stream = VecPostingStream::new(&left_rows);
        let mut right_stream = VecPostingStream::new(&right_rows);
        intersect_sorted(&mut left_stream, &mut right_stream, &mut out).expect("intersect");
        out.len()
    }

    fn random_value(&mut self) -> PropValueOwned {
        let value = self.rng.gen_range(0..self.domain);
        PropValueOwned::Int(value)
    }
}

criterion_group!(benches, micro_property);
criterion_main!(benches);
