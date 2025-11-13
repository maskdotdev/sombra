//! CRUD workload benchmarks for the FFI layer.
#![forbid(unsafe_code)]
#![allow(missing_docs)]

#[cfg(not(feature = "ffi-benches"))]
fn main() {
    eprintln!("Enable the `ffi-benches` feature to build the CRUD benchmark.");
}

#[cfg(feature = "ffi-benches")]
mod bench {
    use std::collections::{HashSet, VecDeque};
    use std::sync::{Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
    use serde_json::Value;
    use sombra::{
        ffi::{
            Database, DatabaseOptions, MatchSpec, MutationOp, MutationSpec, MutationSummary,
            PayloadValue, PredicateSpec, ProjectionSpec, QuerySpec,
        },
        primitives::pager::Synchronous,
    };
    use tempfile::TempDir;

    const LABEL_USER: &str = "User";
    const EDGE_TYPE_FOLLOWS: &str = "FOLLOWS";
    const EDGE_ANCHOR_COUNT: usize = 16;
    const DEFAULT_OPS_PER_BATCH: usize = 256;
    const DEFAULT_PREFILL_BATCHES: usize = 512;
    const DEFAULT_SAMPLE_SIZE: usize = 30;
    const FAST_ENV_VAR: &str = "SOMBRA_BENCH_FAST";
    const READ_USER_COUNT_ENV_VAR: &str = "SOMBRA_BENCH_READ_USERS";
    const FAST_OPS_PER_BATCH: usize = 32;
    const FAST_PREFILL_BATCHES: usize = 8;
    const FAST_SAMPLE_SIZE: usize = 10;
    const FAST_READ_USER_COUNT: usize = 10_000;
    const DEFAULT_READ_USER_COUNT: usize = 100_000;

    #[derive(Clone, Copy)]
    struct BenchTuning {
        sample_size: usize,
        ops_per_batch: usize,
        prefill_batches: usize,
        read_user_count: usize,
    }

    impl BenchTuning {
        fn detect() -> Self {
            let fast = std::env::var_os(FAST_ENV_VAR).is_some();
            let mut tuning = if fast {
                BenchTuning {
                    sample_size: FAST_SAMPLE_SIZE,
                    ops_per_batch: FAST_OPS_PER_BATCH,
                    prefill_batches: FAST_PREFILL_BATCHES,
                    read_user_count: FAST_READ_USER_COUNT,
                }
            } else {
                BenchTuning {
                    sample_size: DEFAULT_SAMPLE_SIZE,
                    ops_per_batch: DEFAULT_OPS_PER_BATCH,
                    prefill_batches: DEFAULT_PREFILL_BATCHES,
                    read_user_count: DEFAULT_READ_USER_COUNT,
                }
            };

            if let Ok(value) = std::env::var(READ_USER_COUNT_ENV_VAR) {
                match value.trim().parse::<usize>() {
                    Ok(parsed) if parsed > 0 => tuning.read_user_count = parsed,
                    Ok(_) => eprintln!(
                        "[sombra-bench] Ignoring {READ_USER_COUNT_ENV_VAR}=0; using {fallback} users instead.",
                        fallback = tuning.read_user_count
                    ),
                    Err(err) => eprintln!(
                        "[sombra-bench] Failed to parse {READ_USER_COUNT_ENV_VAR}='{value}': {err}; using {fallback} users.",
                        fallback = tuning.read_user_count
                    ),
                }
            }

            tuning
        }
    }

    pub fn crud_benchmarks(c: &mut Criterion) {
        let tuning = BenchTuning::detect();
        let profiles = [
            (Synchronous::Full, "synchronous_full"),
            (Synchronous::Normal, "synchronous_normal"),
        ];
        for (mode, label) in profiles {
            let mut group = c.benchmark_group(format!("crud/{label}"));
            group.sample_size(tuning.sample_size);

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("create_node_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter(|| harness.create_node_batch());
            });

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("update_node_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter(|| harness.update_node_batch());
            });

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("delete_node_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter_custom(|iters| harness.measure_delete_nodes(iters));
            });

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("create_edge_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter(|| harness.create_edge_batch());
            });

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("update_edge_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter(|| harness.update_edge_batch());
            });

            group.throughput(Throughput::Elements(tuning.ops_per_batch as u64));
            group.bench_function("delete_edge_batch", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                b.iter_custom(|iters| harness.measure_delete_edges(iters));
            });

            group.throughput(Throughput::Elements(1));
            group.bench_function("read_users", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                harness.log_dataset_summary("read_users", label, "full label scan (rows/iter)");
                b.iter(|| harness.read_users_op());
            });

            let read_user_target = tuning.read_user_count;
            group.throughput(Throughput::Elements(1));
            group.bench_function("read_user_by_name", |b| {
                let mut harness = CrudHarness::new(mode, label, tuning);
                harness.populate_users_for_read_bench(read_user_target);
                harness.log_dataset_summary(
                    "read_user_by_name",
                    label,
                    "point lookup (1 lookup/iter)",
                );
                b.iter(|| harness.read_user_by_name_op());
            });

            group.finish();
        }
    }

    struct CrudHarness {
        db: Database,
        _tmpdir: TempDir,
        node_update_target: u64,
        edge_update_target: u64,
        node_delete_pool: VecDeque<u64>,
        edge_delete_pool: VecDeque<u64>,
        edge_anchor_nodes: Vec<u64>,
        counter: u64,
        ops_per_batch: usize,
        prefill_batches: usize,
        lookup_keys: Vec<String>,
        lookup_cursor: usize,
    }

    static LOGGED_BANNERS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    static POPULATED_READ_DATASET: OnceLock<Mutex<bool>> = OnceLock::new();

    struct DatasetStats {
        total_users: usize,
        lookup_keys: usize,
        ops_per_batch: usize,
        prefill_batches: usize,
    }

    impl CrudHarness {
        fn new(synchronous: Synchronous, label: &str, tuning: BenchTuning) -> Self {
            let tmpdir = tempfile::tempdir().expect("tempdir");
            let path = tmpdir.path().join(format!("{label}.sombra"));
            let mut opts = DatabaseOptions {
                create_if_missing: true,
                distinct_neighbors_default: false,
                ..DatabaseOptions::default()
            };
            // Modify pager options through the DatabaseOptions to use the same types
            opts.pager.synchronous = synchronous;
            opts.pager.wal_commit_coalesce_ms = 5;
            opts.pager.cache_pages = 4096;
            let db = Database::open(&path, opts).expect("open database");
            db.seed_demo().expect("seed demo");
            let mut harness = Self {
                db,
                _tmpdir: tmpdir,
                node_update_target: 0,
                edge_update_target: 0,
                node_delete_pool: VecDeque::new(),
                edge_delete_pool: VecDeque::new(),
                edge_anchor_nodes: Vec::new(),
                counter: 0,
                ops_per_batch: tuning.ops_per_batch,
                prefill_batches: tuning.prefill_batches,
                lookup_keys: Vec::new(),
                lookup_cursor: 0,
            };
            harness.bootstrap();
            harness
        }

        fn bootstrap(&mut self) {
            if self.edge_anchor_nodes.is_empty() {
                for i in 0..EDGE_ANCHOR_COUNT {
                    let name = format!("edge-anchor-{i}");
                    let id = self.create_user(name.clone());
                    self.edge_anchor_nodes.push(id);
                    self.lookup_keys.push(name);
                }
            }
            self.node_update_target = self.edge_anchor_nodes[0];
            self.edge_update_target = self.create_edge_between(
                self.edge_anchor_nodes[0],
                self.edge_anchor_nodes[1 % EDGE_ANCHOR_COUNT],
            );
            self.prefill_delete_pools();
        }

        fn prefill_delete_pools(&mut self) {
            let target = self.ops_per_batch * self.prefill_batches;
            if self.node_delete_pool.len() < target {
                let needed = target - self.node_delete_pool.len();
                let ids = self.create_users(needed);
                self.node_delete_pool.extend(ids);
            }
            if self.edge_delete_pool.len() < target {
                let needed = target - self.edge_delete_pool.len();
                let ids = self.create_edges(needed);
                self.edge_delete_pool.extend(ids);
            }
        }

        fn create_node_batch(&mut self) {
            let ops = (0..self.ops_per_batch)
                .map(|_| {
                    let name = format!("bench-node-{}", self.bump_counter());
                    MutationOp::CreateNode {
                        labels: vec![LABEL_USER.to_string()],
                        props: single_prop("name", serde_json::Value::String(name)),
                    }
                })
                .collect();
            let summary = self.run_mutations(ops);
            self.node_delete_pool
                .extend(summary.created_nodes.iter().copied());
            black_box(summary.created_nodes.len());
        }

        fn update_node_batch(&mut self) {
            let ops = (0..self.ops_per_batch)
                .map(|_| {
                    let bio = format!("bio-{}", self.bump_counter());
                    MutationOp::UpdateNode {
                        id: self.node_update_target,
                        set: single_prop("bio", serde_json::Value::String(bio)),
                        unset: Vec::new(),
                    }
                })
                .collect();
            self.run_mutations(ops);
        }

        fn measure_delete_nodes(&mut self, iterations: u64) -> Duration {
            let mut total = Duration::ZERO;
            for _ in 0..iterations {
                self.ensure_node_delete_capacity();
                let mut ops = Vec::with_capacity(self.ops_per_batch);
                for _ in 0..self.ops_per_batch {
                    if let Some(id) = self.node_delete_pool.pop_front() {
                        ops.push(MutationOp::DeleteNode { id, cascade: true });
                    }
                }
                if ops.is_empty() {
                    break;
                }
                let start = Instant::now();
                self.run_mutations(ops);
                total += start.elapsed();
            }
            total
        }

        fn create_edge_batch(&mut self) {
            let ops = (0..self.ops_per_batch)
                .map(|_| {
                    let (src, dst) = self.next_edge_pair();
                    MutationOp::CreateEdge {
                        src,
                        dst,
                        ty: EDGE_TYPE_FOLLOWS.to_string(),
                        props: serde_json::Map::new(),
                    }
                })
                .collect();
            let summary = self.run_mutations(ops);
            self.edge_delete_pool
                .extend(summary.created_edges.iter().copied());
            black_box(summary.created_edges.len());
        }

        fn update_edge_batch(&mut self) {
            let ops = (0..self.ops_per_batch)
                .map(|_| {
                    let weight = (self.bump_counter() % 1_000) as i64;
                    MutationOp::UpdateEdge {
                        id: self.edge_update_target,
                        set: single_prop("weight", serde_json::Value::Number(weight.into())),
                        unset: Vec::new(),
                    }
                })
                .collect();
            self.run_mutations(ops);
        }

        fn measure_delete_edges(&mut self, iterations: u64) -> Duration {
            let mut total = Duration::ZERO;
            for _ in 0..iterations {
                self.ensure_edge_delete_capacity();
                let mut ops = Vec::with_capacity(self.ops_per_batch);
                for _ in 0..self.ops_per_batch {
                    if let Some(id) = self.edge_delete_pool.pop_front() {
                        ops.push(MutationOp::DeleteEdge { id });
                    }
                }
                if ops.is_empty() {
                    break;
                }
                let start = Instant::now();
                self.run_mutations(ops);
                total += start.elapsed();
            }
            total
        }

        fn read_users_op(&mut self) {
            let spec = user_scan_spec();
            let result = self.db.execute(spec).expect("execute query");
            black_box(rows_len(&result));
        }

        fn read_user_by_name_op(&mut self) {
            let name = self.next_lookup_key();
            let spec = user_lookup_spec(name);
            let result = self
                .db
                .execute(spec)
                .expect("execute lookup query");
            let count = rows_len(&result);
            if count == 0 {
                panic!("lookup query returned no rows");
            }
            black_box(count);
        }

        fn dataset_stats(&self) -> DatasetStats {
            let result = self
                .db
                .execute(user_scan_spec())
                .expect("count users for stats");
            let total_users = rows_len(&result);
            DatasetStats {
                total_users,
                lookup_keys: self.lookup_keys.len(),
                ops_per_batch: self.ops_per_batch,
                prefill_batches: self.prefill_batches,
            }
        }

        fn log_dataset_summary(&self, bench: &str, profile: &str, workload: &str) {
            let key = format!("{profile}:{bench}");
            let cache = LOGGED_BANNERS.get_or_init(|| Mutex::new(HashSet::new()));
            {
                let mut seen = cache.lock().expect("banner cache lock");
                if !seen.insert(key) {
                    return;
                }
            }
            let stats = self.dataset_stats();
            let ops_per_iter = if bench == "read_users" {
                stats.total_users
            } else {
                1
            };
            println!(
                "[crud/{profile}/{bench}] Total Users: {total}, Lookup Key Pool: {keys}, Prefill: {prefill} batches × {ops} ops \
                 -> {ops_iter} operations per iteration ({workload}); Criterion throughput reports ops/sec.",
                total = stats.total_users,
                keys = stats.lookup_keys,
                prefill = stats.prefill_batches,
                ops = stats.ops_per_batch,
                ops_iter = ops_per_iter,
                workload = workload,
            );
        }

        fn ensure_node_delete_capacity(&mut self) {
            if self.node_delete_pool.len() < self.ops_per_batch {
                let ids = self.create_users(self.ops_per_batch * 2);
                self.node_delete_pool.extend(ids);
            }
        }

        fn ensure_edge_delete_capacity(&mut self) {
            if self.edge_delete_pool.len() < self.ops_per_batch {
                let ids = self.create_edges(self.ops_per_batch * 2);
                self.edge_delete_pool.extend(ids);
            }
        }

        fn create_users(&mut self, count: usize) -> Vec<u64> {
            let mut created = Vec::with_capacity(count);
            let mut remaining = count;
            while remaining > 0 {
                let chunk = remaining.min(self.ops_per_batch);
                let ops = (0..chunk)
                    .map(|_| {
                        let name = format!("delete-pool-{}", self.bump_counter());
                        MutationOp::CreateNode {
                            labels: vec![LABEL_USER.to_string()],
                            props: single_prop("name", serde_json::Value::String(name)),
                        }
                    })
                    .collect();
                let summary = self.run_mutations(ops);
                created.extend(summary.created_nodes.iter().copied());
                remaining -= chunk;
            }
            created
        }

        fn create_edges(&mut self, count: usize) -> Vec<u64> {
            let mut created = Vec::with_capacity(count);
            let mut remaining = count;
            while remaining > 0 {
                let chunk = remaining.min(self.ops_per_batch);
                let ops = (0..chunk)
                    .map(|_| {
                        let (src, dst) = self.next_edge_pair();
                        MutationOp::CreateEdge {
                            src,
                            dst,
                            ty: EDGE_TYPE_FOLLOWS.to_string(),
                            props: serde_json::Map::new(),
                        }
                    })
                    .collect();
                let summary = self.run_mutations(ops);
                created.extend(summary.created_edges.iter().copied());
                remaining -= chunk;
            }
            created
        }

        fn next_edge_pair(&mut self) -> (u64, u64) {
            if self.edge_anchor_nodes.len() < 2 {
                panic!("edge anchor set must contain at least two nodes");
            }
            let idx = (self.bump_counter() as usize) % self.edge_anchor_nodes.len();
            let src = self.edge_anchor_nodes[idx];
            let dst = self.edge_anchor_nodes[(idx + 1) % self.edge_anchor_nodes.len()];
            (src, dst)
        }

        fn create_user(&self, name: String) -> u64 {
            let op = MutationOp::CreateNode {
                labels: vec![LABEL_USER.to_string()],
                props: single_prop("name", serde_json::Value::String(name)),
            };
            let summary = self.run_mutation(op);
            *summary
                .created_nodes
                .last()
                .expect("create node should return id")
        }

        fn create_edge_between(&self, src: u64, dst: u64) -> u64 {
            let op = MutationOp::CreateEdge {
                src,
                dst,
                ty: EDGE_TYPE_FOLLOWS.to_string(),
                props: serde_json::Map::new(),
            };
            let summary = self.run_mutation(op);
            *summary
                .created_edges
                .last()
                .expect("create edge should return id")
        }

        fn run_mutations(&self, ops: Vec<MutationOp>) -> MutationSummary {
            let spec = MutationSpec { ops };
            self.db.mutate(spec).expect("mutation result")
        }

        fn run_mutation(&self, op: MutationOp) -> MutationSummary {
            self.run_mutations(vec![op])
        }

        fn bump_counter(&mut self) -> u64 {
            let current = self.counter;
            self.counter += 1;
            current
        }

        fn next_lookup_key(&mut self) -> String {
            if self.lookup_keys.is_empty() {
                panic!("lookup key set cannot be empty");
            }
            let idx = self.lookup_cursor % self.lookup_keys.len();
            self.lookup_cursor = (self.lookup_cursor + 1) % self.lookup_keys.len();
            self.lookup_keys[idx].clone()
        }

        fn populate_users_for_read_bench(&mut self, target_count: usize) {
            // Check current user count
            let result = self
                .db
                .execute(user_scan_spec())
                .expect("count users");
            let current_count = rows_len(&result);

            if current_count >= target_count {
                let needed = target_count.saturating_sub(self.lookup_keys.len());
                if needed > 0 {
                    println!(
                        "Lookup pool missing {needed} keys; creating fresh users so lookups succeed..."
                    );
                    self.create_named_users(needed, None);
                    println!("Lookup pool now has {} keys.", self.lookup_keys.len());
                }
                return;
            }

            // Use a static flag to avoid printing "Populating..." multiple times during warmup
            // But we still need to populate each fresh database
            let should_log = {
                let flag = POPULATED_READ_DATASET.get_or_init(|| Mutex::new(false));
                let mut populated = flag.lock().expect("populated flag lock");
                if !*populated {
                    *populated = true;
                    true
                } else {
                    false
                }
            };

            if should_log {
                println!(
                    "Populating {target_count} users for read benchmark (current: {current_count})..."
                );
            }
            let mut remaining = target_count - current_count;
            let mut created = 0;
            while remaining > 0 {
                let chunk = remaining.min(self.ops_per_batch);
                created += self.create_named_users(chunk, Some(should_log));
                remaining = remaining.saturating_sub(chunk);
                if should_log && created % 10_000 == 0 {
                    println!("  Created {created} users...");
                }
            }
            if should_log {
                println!(
                    "Finished populating {created} users. Lookup pool size: {}",
                    self.lookup_keys.len()
                );
            }
        }

    fn create_named_users(&mut self, count: usize, progress: Option<bool>) -> usize {
        if count == 0 {
            return 0;
        }
            let mut names = Vec::with_capacity(count);
            let ops = (0..count)
                .map(|_| {
                    let name = format!("user-{}", self.bump_counter());
                    names.push(name.clone());
                    MutationOp::CreateNode {
                        labels: vec![LABEL_USER.to_string()],
                        props: single_prop("name", serde_json::Value::String(name)),
                    }
                })
                .collect();
            let summary = self.run_mutations(ops);
            let created = summary.created_nodes.len();
            self.lookup_keys.extend(names.into_iter().take(created));
            if progress == Some(true) && created % 10_000 == 0 {
                println!("  Created {created} users...");
            }
            created
        }
    }

    fn rows_len(value: &Value) -> usize {
        value
            .get("rows")
            .and_then(|rows| rows.as_array())
            .map(|rows| rows.len())
            .unwrap_or(0)
    }

    fn user_scan_spec() -> QuerySpec {
        QuerySpec {
            schema_version: Some(1),
            request_id: None,
            matches: vec![MatchSpec {
                var: "a".to_string(),
                label: Some(LABEL_USER.to_string()),
            }],
            edges: Vec::new(),
            predicate: None,
            projections: vec![ProjectionSpec::Var {
                var: "a".to_string(),
                alias: None,
            }],
            distinct: false,
        }
    }

    fn user_lookup_spec(name: String) -> QuerySpec {
        QuerySpec {
            schema_version: Some(1),
            request_id: None,
            matches: vec![MatchSpec {
                var: "a".to_string(),
                label: Some(LABEL_USER.to_string()),
            }],
            edges: Vec::new(),
            predicate: Some(PredicateSpec::Eq {
                var: "a".to_string(),
                prop: "name".to_string(),
                value: PayloadValue::String(name),
            }),
            projections: vec![ProjectionSpec::Var {
                var: "a".to_string(),
                alias: None,
            }],
            distinct: false,
        }
    }

    fn single_prop(
        key: &str,
        value: serde_json::Value,
    ) -> serde_json::Map<String, serde_json::Value> {
        // Construct via JSON deserialization to ensure compatibility with sombra_ffi's serde_json version
        use serde_json::json;
        let json_value = json!({ key: value });
        // Deserialize through MutationOp to get the correct serde_json::Map type
        let json_str = serde_json::to_string(&json_value).unwrap();
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&json_str).unwrap()
    }
}

#[cfg(feature = "ffi-benches")]
use criterion::{criterion_group, criterion_main};

#[cfg(feature = "ffi-benches")]
use bench::crud_benchmarks;

#[cfg(feature = "ffi-benches")]
criterion_group!(benches, crud_benchmarks);
#[cfg(feature = "ffi-benches")]
criterion_main!(benches);
