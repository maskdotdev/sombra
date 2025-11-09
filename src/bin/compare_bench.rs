//! Comparative benchmarks between Sombra DB and SQLite.
//!
//! Provides configurable workloads so we can capture perf baselines
//! with consistent knobs (mode, commit cadence, transaction semantics).

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::{params, Connection, Transaction};
use sombra::primitives::pager::{PageStore, Pager, PagerOptions, PagerStats, WriteGuard};
use sombra::storage::btree::{BTree, BTreeOptions, PutItem};
use sombra::storage::{profile_snapshot, StorageProfileSnapshot};
use tempfile::tempdir;

fn main() {
    // Ensure the profiling counters are available for Sombra runs.
    std::env::set_var("SOMBRA_PROFILE", "1");
    if let Err(err) = try_main() {
        eprintln!("compare-bench failed: {err}");
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let cfg = BenchConfig::from(args);

    let mut results = Vec::new();
    if cfg.should_run_sombra() {
        results.push(run_sombra_bench(&cfg));
    }
    if cfg.should_run_sqlite() {
        results.push(run_sqlite_bench(&cfg));
    }

    BenchResult::print_header(cfg.mode.label());
    for result in &results {
        result.print();
        result.print_telemetry();
    }

    Ok(())
}

#[derive(Parser, Debug)]
struct Args {
    /// Benchmark mode: reads-only, inserts-only, or mixed workload.
    #[arg(long, value_enum, default_value_t = BenchMode::Mixed)]
    mode: BenchMode,

    /// Number of logical operations to perform (docs inserted/looked up).
    #[arg(long, default_value_t = 10_000)]
    docs: usize,

    /// Commit every N writes when tx-mode=commit.
    #[arg(long, default_value_t = 1)]
    commit_every: usize,

    /// Transaction semantics for the workload.
    #[arg(long, value_enum, default_value_t = TxMode::Commit)]
    tx_mode: TxMode,

    /// RNG seed for repeatable mixed/read workloads.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Enable in-place B-tree leaf edits for the Sombra workload.
    #[arg(long, default_value_t = false)]
    btree_inplace: bool,

    /// Sombra insert API to exercise (pointwise put vs. batched put_many).
    #[arg(long, value_enum, default_value_t = SombraInsertApi::Pointwise)]
    sombra_insert_api: SombraInsertApi,

    /// Number of edges per source node when batching via put_many.
    #[arg(long, default_value_t = 64)]
    put_many_group: usize,

    /// Database implementations to benchmark (both, sombra-only, sqlite-only).
    #[arg(long, value_enum, default_value_t = DatabaseSelection::Both)]
    databases: DatabaseSelection,
}

#[derive(Clone, Copy, Debug, ValueEnum, Eq, PartialEq)]
#[value(rename_all = "kebab_case")]
enum BenchMode {
    ReadsOnly,
    InsertsOnly,
    Mixed,
}

impl BenchMode {
    fn label(self) -> &'static str {
        match self {
            BenchMode::ReadsOnly => "reads-only",
            BenchMode::InsertsOnly => "inserts-only",
            BenchMode::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Eq, PartialEq)]
#[value(rename_all = "kebab_case")]
enum TxMode {
    Commit,
    ReadWithWrite,
}

impl fmt::Display for TxMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TxMode::Commit => "commit",
                TxMode::ReadWithWrite => "read-with-write",
            }
        )
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Eq, PartialEq)]
#[value(rename_all = "kebab_case")]
enum SombraInsertApi {
    Pointwise,
    PutMany,
}

#[derive(Clone, Copy, Debug, ValueEnum, Eq, PartialEq)]
#[value(rename_all = "kebab_case")]
enum DatabaseSelection {
    Both,
    SombraOnly,
    SqliteOnly,
}

#[derive(Clone, Debug)]
struct BenchConfig {
    docs: usize,
    seed: u64,
    commit_every: usize,
    mode: BenchMode,
    tx_mode: TxMode,
    btree_inplace: bool,
    insert_api: SombraInsertApi,
    put_many_group: usize,
    databases: DatabaseSelection,
}

impl From<Args> for BenchConfig {
    fn from(value: Args) -> Self {
        assert!(
            value.commit_every > 0,
            "--commit-every must be greater than zero"
        );
        assert!(
            value.put_many_group > 0,
            "--put-many-group must be greater than zero"
        );
        if value.sombra_insert_api == SombraInsertApi::PutMany {
            assert!(
                value.put_many_group <= u32::MAX as usize,
                "--put-many-group must fit within 32 bits"
            );
        }
        Self {
            docs: value.docs,
            seed: value.seed,
            commit_every: value.commit_every,
            mode: value.mode,
            tx_mode: value.tx_mode,
            btree_inplace: value.btree_inplace,
            insert_api: value.sombra_insert_api,
            put_many_group: value.put_many_group,
            databases: value.databases,
        }
    }
}

impl BenchConfig {
    fn btree_options(&self) -> BTreeOptions {
        let mut opts = BTreeOptions::default();
        opts.in_place_leaf_edits = self.btree_inplace;
        opts
    }

    fn uses_put_many(&self) -> bool {
        self.insert_api == SombraInsertApi::PutMany
    }

    fn should_run_sombra(&self) -> bool {
        matches!(
            self.databases,
            DatabaseSelection::Both | DatabaseSelection::SombraOnly
        )
    }

    fn should_run_sqlite(&self) -> bool {
        matches!(
            self.databases,
            DatabaseSelection::Both | DatabaseSelection::SqliteOnly
        )
    }
}

#[derive(Debug, Default, Clone)]
struct BenchTelemetry {
    profile: Option<StorageProfileSnapshot>,
    pager: Option<PagerStats>,
}

impl BenchTelemetry {
    fn none() -> Self {
        Self::default()
    }

    fn from_sombra(pager: &Pager) -> Self {
        Self {
            profile: profile_snapshot(true),
            pager: Some(pager.stats()),
        }
    }
}

#[derive(Debug)]
struct BenchResult {
    db: &'static str,
    mode_label: &'static str,
    docs: usize,
    time: Duration,
    telemetry: BenchTelemetry,
}

enum DatabaseKind {
    Sombra,
    Sqlite,
}

impl DatabaseKind {
    fn as_str(&self) -> &'static str {
        match self {
            DatabaseKind::Sombra => "Sombra",
            DatabaseKind::Sqlite => "SQLite",
        }
    }
}

impl BenchResult {
    fn print_header(section: &str) {
        println!("\n{}", section.to_uppercase());
        println!(
            "{:<12} {:<14} {:>10} {:>15} {:>15} {:>12}",
            "DATABASE", "MODE", "DOCS", "TIME", "OPS/SEC", "µS/OP"
        );
        println!("{}", "-".repeat(88));
    }

    fn print(&self) {
        let ops_per_sec = self.docs as f64 / self.time.as_secs_f64();
        let us_per_op = self.time.as_micros() as f64 / self.docs as f64;
        println!(
            "{:<12} {:<14} {:>10} {:>15} {:>15} {:>12.1}",
            self.db,
            self.mode_label,
            self.docs,
            format_duration(self.time),
            ops_per_sec as u64,
            us_per_op,
        );
    }

    fn print_telemetry(&self) {
        if let Some(profile) = &self.telemetry.profile {
            let commit_avg_ms = avg_ms(profile.pager_commit_ns, profile.pager_commit_count);
            let search_avg_us = avg_us(
                profile.btree_leaf_search_ns,
                profile.btree_leaf_search_count,
            );
            let insert_avg_us = avg_us(
                profile.btree_leaf_insert_ns,
                profile.btree_leaf_insert_count,
            );
            let slot_extent_avg_us = avg_us(
                profile.btree_slot_extent_ns,
                profile.btree_slot_extent_count,
            );
            let slot_extent_ns_per_slot = avg_per_unit(
                profile.btree_slot_extent_ns,
                profile.btree_slot_extent_slots,
            );
            let allocator_compactions = profile.btree_leaf_allocator_compactions;
            let allocator_bytes_moved = profile.btree_leaf_allocator_bytes_moved;
            let allocator_failures = profile.btree_leaf_allocator_failures;
            let allocator_avg_bytes = avg_per_unit(
                profile.btree_leaf_allocator_bytes_moved,
                profile.btree_leaf_allocator_compactions,
            );
            println!(
                "    metrics: wal_frames={} wal_bytes={} fsyncs={} key_decodes={} key_cmps={} memcopy_bytes={} rebalance_in_place={} rebalance_rebuilds={} allocator_compactions={} allocator_failures={} allocator_bytes_moved={} allocator_avg_bytes={:.1} commit_avg_ms={:.3} search_avg_us={:.3} insert_avg_us={:.3} slot_extent_avg_us={:.3} slot_extent_ns_per_slot={:.1}",
                profile.pager_wal_frames,
                profile.pager_wal_bytes,
                profile.pager_fsync_count,
                profile.btree_leaf_key_decodes,
                profile.btree_leaf_key_cmps,
                profile.btree_leaf_memcopy_bytes,
                profile.btree_leaf_rebalance_in_place,
                profile.btree_leaf_rebalance_rebuilds,
                allocator_compactions,
                allocator_failures,
                allocator_bytes_moved,
                allocator_avg_bytes,
                commit_avg_ms,
                search_avg_us,
                insert_avg_us,
                slot_extent_avg_us,
                slot_extent_ns_per_slot,
            );
        }
        if let Some(stats) = &self.telemetry.pager {
            println!(
                "    pager: hits={} misses={} evictions={} dirty_writebacks={}",
                stats.hits, stats.misses, stats.evictions, stats.dirty_writebacks
            );
        }
    }
}

fn avg_ms(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    (total_ns as f64 / 1_000_000.0) / count as f64
}

fn avg_us(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    (total_ns as f64 / 1_000.0) / count as f64
}

fn avg_per_unit(total_ns: u64, units: u64) -> f64 {
    if units == 0 {
        return 0.0;
    }
    total_ns as f64 / units as f64
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

fn bench<F>(db: DatabaseKind, mode_label: &'static str, docs: usize, f: F) -> BenchResult
where
    F: FnOnce() -> BenchTelemetry,
{
    let start = Instant::now();
    let telemetry = f();
    let elapsed = start.elapsed();
    BenchResult {
        db: db.as_str(),
        mode_label,
        docs,
        time: elapsed,
        telemetry,
    }
}

fn reset_profile_counters() {
    let _ = profile_snapshot(true);
}

fn run_sombra_bench(cfg: &BenchConfig) -> BenchResult {
    match cfg.mode {
        BenchMode::ReadsOnly => bench_sombra_reads(cfg),
        BenchMode::InsertsOnly => bench_sombra_inserts(cfg),
        BenchMode::Mixed => bench_sombra_mixed(cfg),
    }
}

fn bench_sombra_reads(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, cfg.btree_options()).unwrap();

    populate_tree(&pager, &tree, cfg);
    reset_profile_counters();

    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        let read = pager.begin_read().unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        for _ in 0..cfg.docs {
            let idx = rng.gen_range(0..cfg.docs) as usize;
            let key = sombra_key_for_doc(cfg, idx);
            let _ = tree.get(&read, &key).unwrap();
        }
        BenchTelemetry::from_sombra(&pager)
    })
}

fn bench_sombra_inserts(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, cfg.btree_options()).unwrap();

    reset_profile_counters();
    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => sombra_insert_with_commits(&pager, &tree, cfg),
            TxMode::ReadWithWrite => sombra_insert_single_commit(&pager, &tree, cfg),
        }
        BenchTelemetry::from_sombra(&pager)
    })
}

fn bench_sombra_mixed(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, cfg.btree_options()).unwrap();

    reset_profile_counters();
    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => sombra_mixed_with_commits(&pager, &tree, cfg),
            TxMode::ReadWithWrite => sombra_mixed_read_with_write(&pager, &tree, cfg),
        }
        BenchTelemetry::from_sombra(&pager)
    })
}

fn populate_tree(pager: &Arc<Pager>, tree: &BTree<u64, u64>, cfg: &BenchConfig) {
    let mut write = pager.begin_write().unwrap();
    for i in 0..cfg.docs {
        let key = sombra_key_for_doc(cfg, i);
        let value = i as u64;
        tree.put(&mut write, &key, &value).unwrap();
    }
    pager.commit(write).unwrap();
}

fn sombra_key_for_doc(cfg: &BenchConfig, index: usize) -> u64 {
    if cfg.uses_put_many() {
        let group = cfg.put_many_group;
        let source = (index / group) as u64;
        let ordinal = (index % group) as u64;
        (source << 32) | ordinal
    } else {
        index as u64
    }
}

struct PutManyBatch {
    group_size: usize,
    entries: Vec<(u64, u64)>,
}

impl PutManyBatch {
    fn new(group_size: usize) -> Self {
        Self {
            group_size,
            entries: Vec::with_capacity(group_size),
        }
    }

    fn push(&mut self, tree: &BTree<u64, u64>, tx: &mut WriteGuard<'_>, key: u64, value: u64) {
        self.entries.push((key, value));
        if self.entries.len() >= self.group_size {
            self.flush(tree, tx);
        }
    }

    fn flush(&mut self, tree: &BTree<u64, u64>, tx: &mut WriteGuard<'_>) {
        if self.entries.is_empty() {
            return;
        }
        let iter = self
            .entries
            .iter()
            .map(|(key, value)| PutItem { key, value });
        tree.put_many(tx, iter).unwrap();
        self.entries.clear();
    }
}

fn sombra_insert_with_commits(pager: &Arc<Pager>, tree: &BTree<u64, u64>, cfg: &BenchConfig) {
    let mut write: Option<_> = None;
    let mut pending = 0usize;
    let use_put_many = cfg.uses_put_many();
    let mut batch = if use_put_many {
        Some(PutManyBatch::new(cfg.put_many_group))
    } else {
        None
    };
    for i in 0..cfg.docs {
        if write.is_none() {
            write = Some(pager.begin_write().unwrap());
        }
        let key = sombra_key_for_doc(cfg, i);
        let value = i as u64;
        {
            let guard = write.as_mut().unwrap();
            if use_put_many {
                batch.as_mut().unwrap().push(tree, guard, key, value);
            } else {
                tree.put(guard, &key, &value).unwrap();
            }
        }
        pending += 1;
        if pending == cfg.commit_every {
            if use_put_many {
                let guard = write.as_mut().unwrap();
                batch.as_mut().unwrap().flush(tree, guard);
            }
            let guard = write.take().unwrap();
            pager.commit(guard).unwrap();
            pending = 0;
        }
    }
    if let Some(guard) = write.as_mut() {
        if use_put_many {
            batch.as_mut().unwrap().flush(tree, guard);
        }
    }
    if let Some(guard) = write.take() {
        pager.commit(guard).unwrap();
    }
}

fn sombra_insert_single_commit(pager: &Arc<Pager>, tree: &BTree<u64, u64>, cfg: &BenchConfig) {
    let mut write = pager.begin_write().unwrap();
    let use_put_many = cfg.uses_put_many();
    let mut batch = if use_put_many {
        Some(PutManyBatch::new(cfg.put_many_group))
    } else {
        None
    };
    for i in 0..cfg.docs {
        if use_put_many {
            batch
                .as_mut()
                .unwrap()
                .push(tree, &mut write, sombra_key_for_doc(cfg, i), i as u64);
        } else {
            let key = sombra_key_for_doc(cfg, i);
            let value = i as u64;
            tree.put(&mut write, &key, &value).unwrap();
        }
    }
    if use_put_many {
        batch.as_mut().unwrap().flush(tree, &mut write);
    }
    pager.commit(write).unwrap();
}

fn sombra_mixed_with_commits(pager: &Arc<Pager>, tree: &BTree<u64, u64>, cfg: &BenchConfig) {
    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
    let mut write: Option<_> = None;
    let mut pending = 0usize;
    let use_put_many = cfg.uses_put_many();
    let mut batch = if use_put_many {
        Some(PutManyBatch::new(cfg.put_many_group))
    } else {
        None
    };
    for i in 0..cfg.docs {
        let do_write = rng.gen_bool(0.7);
        if do_write {
            if write.is_none() {
                write = Some(pager.begin_write().unwrap());
            }
            let key = sombra_key_for_doc(cfg, i);
            let value = i as u64;
            {
                let guard = write.as_mut().unwrap();
                if use_put_many {
                    batch.as_mut().unwrap().push(tree, guard, key, value);
                } else {
                    tree.put(guard, &key, &value).unwrap();
                }
            }
            pending += 1;
            if pending == cfg.commit_every {
                if use_put_many {
                    let guard = write.as_mut().unwrap();
                    batch.as_mut().unwrap().flush(tree, guard);
                }
                let guard = write.take().unwrap();
                pager.commit(guard).unwrap();
                pending = 0;
            }
        } else if i > 0 {
            if write.is_some() {
                if use_put_many {
                    let guard = write.as_mut().unwrap();
                    batch.as_mut().unwrap().flush(tree, guard);
                }
                let guard = write.take().unwrap();
                pager.commit(guard).unwrap();
                pending = 0;
            }
            let read = pager.begin_read().unwrap();
            let doc_idx = rng.gen_range(0..i) as usize;
            let key = sombra_key_for_doc(cfg, doc_idx);
            let _ = tree.get(&read, &key).unwrap();
        }
    }
    if let Some(guard) = write.as_mut() {
        if use_put_many {
            batch.as_mut().unwrap().flush(tree, guard);
        }
    }
    if let Some(guard) = write.take() {
        pager.commit(guard).unwrap();
    }
}

fn sombra_mixed_read_with_write(pager: &Arc<Pager>, tree: &BTree<u64, u64>, cfg: &BenchConfig) {
    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
    let mut write = pager.begin_write().unwrap();
    let use_put_many = cfg.uses_put_many();
    let mut batch = if use_put_many {
        Some(PutManyBatch::new(cfg.put_many_group))
    } else {
        None
    };
    for i in 0..cfg.docs {
        if rng.gen_bool(0.7) {
            if use_put_many {
                batch.as_mut().unwrap().push(
                    tree,
                    &mut write,
                    sombra_key_for_doc(cfg, i),
                    i as u64,
                );
            } else {
                let key = sombra_key_for_doc(cfg, i);
                let value = i as u64;
                tree.put(&mut write, &key, &value).unwrap();
            }
        } else if i > 0 {
            if use_put_many {
                batch.as_mut().unwrap().flush(tree, &mut write);
            }
            let doc_idx = rng.gen_range(0..i) as usize;
            let key = sombra_key_for_doc(cfg, doc_idx);
            let _ = tree.get_with_write(&mut write, &key).unwrap();
        }
    }
    if use_put_many {
        batch.as_mut().unwrap().flush(tree, &mut write);
    }
    pager.commit(write).unwrap();
}

fn run_sqlite_bench(cfg: &BenchConfig) -> BenchResult {
    match cfg.mode {
        BenchMode::ReadsOnly => bench_sqlite_reads(cfg),
        BenchMode::InsertsOnly => bench_sqlite_inserts(cfg),
        BenchMode::Mixed => bench_sqlite_mixed(cfg),
    }
}

fn configure_sqlite(conn: &Connection) {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\
         PRAGMA synchronous=FULL;\
         PRAGMA page_size=4096;\
         PRAGMA cache_size=128;",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v INTEGER)",
        [],
    )
    .unwrap();
}

fn bench_sqlite_reads(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("sqlite.db");
    let conn = Connection::open(&path).unwrap();
    configure_sqlite(&conn);
    {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..cfg.docs {
            tx.execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                params![i as i64, i as i64],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    bench(DatabaseKind::Sqlite, cfg.mode.label(), cfg.docs, || {
        let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        for _ in 0..cfg.docs {
            let key = rng.gen_range(0..cfg.docs) as i64;
            let _ = conn
                .query_row("SELECT v FROM kv WHERE k = ?1", params![key], |_row| Ok(()))
                .ok();
        }
        BenchTelemetry::none()
    })
}

fn bench_sqlite_inserts(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("sqlite.db");
    let conn = Connection::open(&path).unwrap();
    configure_sqlite(&conn);
    bench(DatabaseKind::Sqlite, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => sqlite_insert_with_commits(&conn, cfg.docs, cfg.commit_every),
            TxMode::ReadWithWrite => sqlite_insert_single_commit(&conn, cfg.docs),
        }
        BenchTelemetry::none()
    })
}

fn bench_sqlite_mixed(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("sqlite.db");
    let conn = Connection::open(&path).unwrap();
    configure_sqlite(&conn);
    bench(DatabaseKind::Sqlite, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => {
                sqlite_mixed_with_commits(&conn, cfg.docs, cfg.commit_every, cfg.seed)
            }
            TxMode::ReadWithWrite => sqlite_mixed_read_with_write(&conn, cfg.docs, cfg.seed),
        }
        BenchTelemetry::none()
    })
}

fn sqlite_insert_with_commits(conn: &Connection, docs: usize, commit_every: usize) {
    let mut tx: Option<Transaction<'_>> = None;
    let mut pending = 0usize;
    for i in 0..docs {
        if tx.is_none() {
            tx = Some(conn.unchecked_transaction().unwrap());
        }
        let tx_ref = tx.as_mut().unwrap();
        tx_ref
            .execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                params![i as i64, i as i64],
            )
            .unwrap();
        pending += 1;
        if pending == commit_every {
            tx.take().unwrap().commit().unwrap();
            pending = 0;
        }
    }
    if let Some(tx) = tx.take() {
        tx.commit().unwrap();
    }
}

fn sqlite_insert_single_commit(conn: &Connection, docs: usize) {
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..docs {
        tx.execute(
            "INSERT INTO kv (k, v) VALUES (?1, ?2)",
            params![i as i64, i as i64],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn sqlite_mixed_with_commits(conn: &Connection, docs: usize, commit_every: usize, seed: u64) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut tx: Option<Transaction<'_>> = None;
    let mut pending = 0usize;
    for i in 0..docs {
        if rng.gen_bool(0.7) {
            if tx.is_none() {
                tx = Some(conn.unchecked_transaction().unwrap());
            }
            let tx_ref = tx.as_mut().unwrap();
            tx_ref
                .execute(
                    "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                    params![i as i64, i as i64],
                )
                .unwrap();
            pending += 1;
            if pending == commit_every {
                tx.take().unwrap().commit().unwrap();
                pending = 0;
            }
        } else if i > 0 {
            if let Some(tx_active) = tx.take() {
                tx_active.commit().unwrap();
                pending = 0;
            }
            let key = rng.gen_range(0..i) as i64;
            let _ = conn
                .query_row("SELECT v FROM kv WHERE k = ?1", params![key], |_row| Ok(()))
                .ok();
        }
    }
    if let Some(tx) = tx.take() {
        tx.commit().unwrap();
    }
}

fn sqlite_mixed_read_with_write(conn: &Connection, docs: usize, seed: u64) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..docs {
        if rng.gen_bool(0.7) {
            tx.execute(
                "INSERT INTO kv (k, v) VALUES (?1, ?2)",
                params![i as i64, i as i64],
            )
            .unwrap();
        } else if i > 0 {
            let key = rng.gen_range(0..i) as i64;
            let _ = tx
                .query_row("SELECT v FROM kv WHERE k = ?1", params![key], |_row| Ok(()))
                .ok();
        }
    }
    tx.commit().unwrap();
}
