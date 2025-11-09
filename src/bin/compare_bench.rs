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
use sombra::primitives::pager::{PageStore, Pager, PagerOptions, PagerStats};
use sombra::storage::btree::{BTree, BTreeOptions};
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
    results.push(run_sombra_bench(&cfg));
    results.push(run_sqlite_bench(&cfg));

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

#[derive(Clone, Debug)]
struct BenchConfig {
    docs: usize,
    seed: u64,
    commit_every: usize,
    mode: BenchMode,
    tx_mode: TxMode,
}

impl From<Args> for BenchConfig {
    fn from(value: Args) -> Self {
        assert!(
            value.commit_every > 0,
            "--commit-every must be greater than zero"
        );
        Self {
            docs: value.docs,
            seed: value.seed,
            commit_every: value.commit_every,
            mode: value.mode,
            tx_mode: value.tx_mode,
        }
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
            println!(
                "    metrics: wal_frames={} wal_bytes={} fsyncs={} key_decodes={} key_cmps={} memcopy_bytes={} rebalance_in_place={} rebalance_rebuilds={} commit_avg_ms={:.3} search_avg_us={:.3} insert_avg_us={:.3}",
                profile.pager_wal_frames,
                profile.pager_wal_bytes,
                profile.pager_fsync_count,
                profile.btree_leaf_key_decodes,
                profile.btree_leaf_key_cmps,
                profile.btree_leaf_memcopy_bytes,
                profile.btree_leaf_rebalance_in_place,
                profile.btree_leaf_rebalance_rebuilds,
                commit_avg_ms,
                search_avg_us,
                insert_avg_us,
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
    let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

    populate_tree(&pager, &tree, cfg.docs);
    reset_profile_counters();

    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        let read = pager.begin_read().unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        for _ in 0..cfg.docs {
            let key = rng.gen_range(0..cfg.docs) as u64;
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
    let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

    reset_profile_counters();
    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => sombra_insert_with_commits(&pager, &tree, cfg.docs, cfg.commit_every),
            TxMode::ReadWithWrite => sombra_insert_single_commit(&pager, &tree, cfg.docs),
        }
        BenchTelemetry::from_sombra(&pager)
    })
}

fn bench_sombra_mixed(cfg: &BenchConfig) -> BenchResult {
    let tmpdir = tempdir().unwrap();
    let path = tmpdir.path().join("btree.sombra");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::open_or_create(&store, BTreeOptions::default()).unwrap();

    reset_profile_counters();
    bench(DatabaseKind::Sombra, cfg.mode.label(), cfg.docs, || {
        match cfg.tx_mode {
            TxMode::Commit => {
                sombra_mixed_with_commits(&pager, &tree, cfg.docs, cfg.commit_every, cfg.seed)
            }
            TxMode::ReadWithWrite => {
                sombra_mixed_read_with_write(&pager, &tree, cfg.docs, cfg.seed)
            }
        }
        BenchTelemetry::from_sombra(&pager)
    })
}

fn populate_tree(pager: &Arc<Pager>, tree: &BTree<u64, u64>, docs: usize) {
    let mut write = pager.begin_write().unwrap();
    for i in 0..docs {
        tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
    }
    pager.commit(write).unwrap();
}

fn sombra_insert_with_commits(
    pager: &Arc<Pager>,
    tree: &BTree<u64, u64>,
    docs: usize,
    commit_every: usize,
) {
    let mut write: Option<_> = None;
    let mut pending = 0usize;
    for i in 0..docs {
        if write.is_none() {
            write = Some(pager.begin_write().unwrap());
        }
        let guard = write.as_mut().unwrap();
        tree.put(guard, &(i as u64), &(i as u64)).unwrap();
        pending += 1;
        if pending == commit_every {
            let guard = write.take().unwrap();
            pager.commit(guard).unwrap();
            pending = 0;
        }
    }
    if let Some(guard) = write.take() {
        pager.commit(guard).unwrap();
    }
}

fn sombra_insert_single_commit(pager: &Arc<Pager>, tree: &BTree<u64, u64>, docs: usize) {
    let mut write = pager.begin_write().unwrap();
    for i in 0..docs {
        tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
    }
    pager.commit(write).unwrap();
}

fn sombra_mixed_with_commits(
    pager: &Arc<Pager>,
    tree: &BTree<u64, u64>,
    docs: usize,
    commit_every: usize,
    seed: u64,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut write: Option<_> = None;
    let mut pending = 0usize;
    for i in 0..docs {
        let do_write = rng.gen_bool(0.7);
        if do_write {
            if write.is_none() {
                write = Some(pager.begin_write().unwrap());
            }
            let guard = write.as_mut().unwrap();
            tree.put(guard, &(i as u64), &(i as u64)).unwrap();
            pending += 1;
            if pending == commit_every {
                let guard = write.take().unwrap();
                pager.commit(guard).unwrap();
                pending = 0;
            }
        } else if i > 0 {
            if let Some(guard) = write.take() {
                pager.commit(guard).unwrap();
                pending = 0;
            }
            let read = pager.begin_read().unwrap();
            let key = rng.gen_range(0..i) as u64;
            let _ = tree.get(&read, &key).unwrap();
        }
    }
    if let Some(guard) = write.take() {
        pager.commit(guard).unwrap();
    }
}

fn sombra_mixed_read_with_write(
    pager: &Arc<Pager>,
    tree: &BTree<u64, u64>,
    docs: usize,
    seed: u64,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut write = pager.begin_write().unwrap();
    for i in 0..docs {
        if rng.gen_bool(0.7) {
            tree.put(&mut write, &(i as u64), &(i as u64)).unwrap();
        } else if i > 0 {
            let key = rng.gen_range(0..i) as u64;
            let _ = tree.get_with_write(&mut write, &key).unwrap();
        }
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
