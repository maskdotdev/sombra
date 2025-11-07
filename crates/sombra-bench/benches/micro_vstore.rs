#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sombra_pager::{PageStore, Pager, PagerOptions};
use sombra_types::VRef;
use sombra_vstore::VStore;
use tempfile::TempDir;

const WRITE_BATCH: usize = 64;
const READ_BATCH: usize = 64;
const SIZES: [usize; 3] = [1 * 1024, 16 * 1024, 1 * 1024 * 1024];

fn micro_vstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/vstore");
    group.sample_size(30);
    let mut harness = VStoreHarness::new();

    for size in SIZES {
        group.throughput(Throughput::Bytes((size * WRITE_BATCH) as u64));
        group.bench_with_input(BenchmarkId::new("write", size), &size, |b, &bytes| {
            b.iter(|| black_box(harness.write_batch(bytes, WRITE_BATCH)));
        });
        group.throughput(Throughput::Bytes((size * READ_BATCH) as u64));
        group.bench_with_input(BenchmarkId::new("read", size), &size, |b, &bytes| {
            b.iter(|| black_box(harness.read_batch(bytes, READ_BATCH)));
        });
    }
    group.finish();
}

struct VStoreHarness {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    vstore: VStore,
    payloads: HashMap<usize, Vec<u8>>,
    refs: HashMap<usize, Vec<VRef>>,
    positions: HashMap<usize, usize>,
}

impl VStoreHarness {
    fn new() -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("vstore.sombra");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let vstore = VStore::open(store).expect("vstore");
        Self {
            _tmpdir: tmpdir,
            pager,
            vstore,
            payloads: HashMap::new(),
            refs: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    fn payload(&mut self, size: usize) -> &[u8] {
        self.payloads
            .entry(size)
            .or_insert_with(|| vec![0xAB; size])
    }

    fn write_batch(&mut self, size: usize, count: usize) -> usize {
        let payload = self.payload(size).to_vec();
        let mut write = self.pager.begin_write().expect("write");
        let mut produced = Vec::with_capacity(count);
        for _ in 0..count {
            let vref = self.vstore.write(&mut write, &payload).expect("write");
            produced.push(vref);
        }
        self.pager.commit(write).expect("commit");
        self.refs.entry(size).or_default().extend(produced);
        count
    }

    fn read_batch(&mut self, size: usize, count: usize) -> usize {
        self.ensure_refs(size, count);
        let refs = self.refs.get(&size).expect("refs");
        let read = self.pager.begin_read().expect("read");
        let pos = self.positions.entry(size).or_insert(0);
        for idx in 0..count {
            let cursor = (*pos + idx) % refs.len();
            let vref = refs[cursor];
            let bytes = self.vstore.read(&read, vref).expect("read");
            black_box(bytes.len());
        }
        *pos = (*pos + count) % refs.len();
        count
    }

    fn ensure_refs(&mut self, size: usize, count: usize) {
        let available = self.refs.get(&size).map(|r| r.len()).unwrap_or(0);
        if available < count {
            let needed = count - available;
            self.write_batch(size, needed.max(WRITE_BATCH));
        }
    }
}

criterion_group!(benches, micro_vstore);
criterion_main!(benches);
