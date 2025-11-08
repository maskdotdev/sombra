#![forbid(unsafe_code)]

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions, Synchronous};
use tempfile::TempDir;

const PAGES_PER_BATCH: usize = 64;

fn micro_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("micro/wal");
    group.sample_size(25);
    for mode in [Synchronous::Full, Synchronous::Normal] {
        let mut harness = WalHarness::new(mode);
        group.throughput(Throughput::Elements(PAGES_PER_BATCH as u64));
        group.bench_with_input(
            BenchmarkId::new("append", format!("{mode:?}")),
            &mode,
            |b, _| {
                b.iter(|| harness.append_batch(PAGES_PER_BATCH));
            },
        );
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("checkpoint", format!("{mode:?}")),
            &mode,
            |b, _| {
                b.iter(|| harness.checkpoint());
            },
        );
    }
    group.finish();
}

struct WalHarness {
    _tmpdir: TempDir,
    pager: Arc<Pager>,
    payload: Vec<u8>,
    counter: u64,
}

impl WalHarness {
    fn new(mode: Synchronous) -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join(format!("wal_{mode:?}.sombra"));
        let mut opts = PagerOptions::default();
        opts.synchronous = mode;
        opts.cache_pages = 2048;
        let pager = Arc::new(Pager::create(&path, opts).expect("pager"));
        let payload = vec![0xCD; pager.page_size() as usize];
        Self {
            _tmpdir: tmpdir,
            pager,
            payload,
            counter: 0,
        }
    }

    fn append_batch(&mut self, count: usize) {
        let mut write = self.pager.begin_write().expect("write");
        for _ in 0..count {
            let page = write.allocate_page().expect("allocate");
            {
                let mut frame = write.page_mut(page).expect("page");
                let data = frame.data_mut();
                data.copy_from_slice(&self.payload);
                data[..8].copy_from_slice(&self.counter.to_le_bytes());
            }
            self.counter += 1;
        }
        self.pager.commit(write).expect("commit");
    }

    fn checkpoint(&mut self) {
        self.pager
            .checkpoint(CheckpointMode::Force)
            .expect("checkpoint");
    }
}

criterion_group!(benches, micro_wal);
criterion_main!(benches);
