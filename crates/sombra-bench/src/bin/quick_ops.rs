#![forbid(unsafe_code)]

use clap::{Parser, ValueEnum};
use rand::{thread_rng, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value};
use sombra_ffi::{
    profile_snapshot, Database, DatabaseOptions, DirectionSpec, EdgeSpec, LiteralSpec, MatchSpec,
    MutationOp, MutationSpec, PredicateSpec, ProjectionSpec, QuerySpec,
};
use sombra_pager::Synchronous;
use std::time::{Duration, Instant};
use tempfile::TempDir;

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Debug)]
#[command(
    name = "quick-ops",
    about = "Lightweight ops/sec benchmarks for CRUD and traversal workloads"
)]
struct Args {
    /// Operations to execute; defaults to the full suite.
    #[arg(long = "op", value_enum)]
    ops: Vec<Operation>,

    /// Number of iterations per operation (default varies by op when zero).
    #[arg(long, default_value_t = 0)]
    iterations: usize,

    /// Target dataset size used for read/update/delete workloads.
    #[arg(long, default_value_t = 10_000)]
    user_count: usize,

    /// Outgoing edges to create per user (used for traversal ops).
    #[arg(long, default_value_t = 2)]
    edges_per_user: usize,

    /// Batch size for bulk create/delete operations.
    #[arg(long, default_value_t = 256)]
    batch_size: usize,

    /// Pager synchronous mode (controls durability cost).
    #[arg(long, value_enum, default_value_t = PagerMode::Normal)]
    pager: PagerMode,

    /// Optional RNG seed for reproducible lookups.
    #[arg(long)]
    seed: Option<u64>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum PagerMode {
    Off,
    Normal,
    Full,
}

impl From<PagerMode> for Synchronous {
    fn from(value: PagerMode) -> Self {
        match value {
            PagerMode::Off => Synchronous::Off,
            PagerMode::Normal => Synchronous::Normal,
            PagerMode::Full => Synchronous::Full,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum Operation {
    CreateUsers,
    UpdateUsers,
    DeleteUsers,
    ReadUsers,
    ReadUserByName,
    ExpandOneHop,
    ExpandTwoHop,
}

impl Operation {
    fn all() -> Vec<Self> {
        vec![
            Operation::CreateUsers,
            Operation::UpdateUsers,
            Operation::DeleteUsers,
            Operation::ReadUsers,
            Operation::ReadUserByName,
            Operation::ExpandOneHop,
            Operation::ExpandTwoHop,
        ]
    }

    fn label(self) -> &'static str {
        match self {
            Operation::CreateUsers => "create_users",
            Operation::UpdateUsers => "update_users",
            Operation::DeleteUsers => "delete_users",
            Operation::ReadUsers => "read_users",
            Operation::ReadUserByName => "read_user_by_name",
            Operation::ExpandOneHop => "expand_1hop",
            Operation::ExpandTwoHop => "expand_2hop",
        }
    }

    fn default_iterations(self) -> usize {
        match self {
            Operation::CreateUsers => 10_000,
            Operation::UpdateUsers => 10_000,
            Operation::DeleteUsers => 5_000,
            Operation::ReadUsers => 200,
            Operation::ReadUserByName => 20_000,
            Operation::ExpandOneHop => 10_000,
            Operation::ExpandTwoHop => 5_000,
        }
    }
}

fn main() -> BenchResult<()> {
    let args = Args::parse();
    let ops = if args.ops.is_empty() {
        Operation::all()
    } else {
        args.ops.clone()
    };

    println!(
        "quick-ops starting (user_count={}, edges/user={}, batch={}, pager={:?})",
        args.user_count, args.edges_per_user, args.batch_size, args.pager
    );

    for op in ops {
        run_operation(&args, op)?;
    }

    Ok(())
}

fn run_operation(args: &Args, op: Operation) -> BenchResult<()> {
    let iterations = if args.iterations == 0 {
        op.default_iterations()
    } else {
        args.iterations
    };
    println!("\n== {:<18} | iterations={} ==", op.label(), iterations);

    let mut fixture = Fixture::new(args, op)?;

    match op {
        Operation::CreateUsers => {
            let stats = fixture.run_create(iterations)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::UpdateUsers => {
            fixture.ensure_users(args.user_count.max(1000))?;
            let stats = fixture.run_update(iterations)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::DeleteUsers => {
            let target = args.user_count.max(iterations + 100);
            fixture.ensure_users(target)?;
            let stats = fixture.run_delete(iterations)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::ReadUsers => {
            fixture.ensure_users(args.user_count.max(1000))?;
            let stats = fixture.run_read_label(iterations)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::ReadUserByName => {
            fixture.ensure_users(args.user_count.max(1000))?;
            let stats = fixture.run_read_by_name(iterations)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::ExpandOneHop => {
            fixture.ensure_users(args.user_count.max(1000))?;
            fixture.ensure_edges(args.edges_per_user)?;
            let stats = fixture.run_expand(iterations, 1)?;
            stats.print(op);
            report_profile(op.label());
        }
        Operation::ExpandTwoHop => {
            fixture.ensure_users(args.user_count.max(1000))?;
            fixture.ensure_edges(args.edges_per_user.max(2))?;
            let stats = fixture.run_expand(iterations, 2)?;
            stats.print(op);
            report_profile(op.label());
        }
    }

    Ok(())
}

struct Fixture {
    db: Database,
    _tmpdir: TempDir,
    users: Vec<UserRecord>,
    rng: ChaCha8Rng,
    counter: u64,
    batch_size: usize,
    edges_built: bool,
}

struct UserRecord {
    id: u64,
    name: String,
}

impl Fixture {
    fn new(args: &Args, op: Operation) -> BenchResult<Self> {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join(format!("{}.sombra", op.label()));
        let mut opts = DatabaseOptions::default();
        opts.pager.synchronous = args.pager.into();
        opts.pager.cache_pages = 2048;
        let db = Database::open(&db_path, opts)?;
        db.seed_demo()?;
        let seed = args.seed.unwrap_or_else(|| thread_rng().gen());
        Ok(Self {
            db,
            _tmpdir: tmpdir,
            users: Vec::new(),
            rng: ChaCha8Rng::seed_from_u64(seed),
            counter: 0,
            batch_size: args.batch_size.max(1),
            edges_built: false,
        })
    }

    fn ensure_users(&mut self, target: usize) -> BenchResult<()> {
        while self.users.len() < target {
            let remaining = target - self.users.len();
            let chunk = remaining.min(self.batch_size);
            self.create_named_users(chunk)?;
        }
        Ok(())
    }

    fn create_named_users(&mut self, count: usize) -> BenchResult<()> {
        if count == 0 {
            return Ok(());
        }
        let mut names = Vec::with_capacity(count);
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let name = format!("user-{}", self.counter);
            self.counter += 1;
            names.push(name.clone());
            ops.push(MutationOp::CreateNode {
                labels: vec!["User".to_string()],
                props: single_prop("name", Value::String(name)),
            });
        }
        let summary = self.db.mutate(MutationSpec { ops }).map_err(to_err)?;
        for (id, name) in summary.created_nodes.into_iter().zip(names.into_iter()) {
            self.users.push(UserRecord { id, name });
        }
        Ok(())
    }

    fn ensure_edges(&mut self, fanout: usize) -> BenchResult<()> {
        if self.edges_built || fanout == 0 || self.users.len() < 2 {
            return Ok(());
        }
        let mut ops = Vec::new();
        for (idx, src) in self.users.iter().enumerate() {
            for offset in 1..=fanout {
                let dst = &self.users[(idx + offset) % self.users.len()];
                ops.push(MutationOp::CreateEdge {
                    src: src.id,
                    dst: dst.id,
                    ty: "FOLLOWS".to_string(),
                    props: Map::new(),
                });
                if ops.len() == self.batch_size {
                    self.db
                        .mutate(MutationSpec {
                            ops: std::mem::take(&mut ops),
                        })
                        .map_err(to_err)?;
                }
            }
        }
        if !ops.is_empty() {
            self.db.mutate(MutationSpec { ops }).map_err(to_err)?;
        }
        self.edges_built = true;
        Ok(())
    }

    fn run_create(&mut self, iterations: usize) -> BenchResult<OpStats> {
        let start = Instant::now();
        let mut remaining = iterations;
        while remaining > 0 {
            let chunk = remaining.min(self.batch_size);
            self.create_named_users(chunk)?;
            remaining -= chunk;
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn run_update(&mut self, iterations: usize) -> BenchResult<OpStats> {
        if self.users.is_empty() {
            return Err("no users to update".into());
        }
        let start = Instant::now();
        let mut idx = 0usize;
        for _ in 0..iterations {
            let user = &self.users[idx];
            idx = (idx + 1) % self.users.len();
            let bio = format!("bio-{}", self.counter);
            self.counter += 1;
            let op = MutationOp::UpdateNode {
                id: user.id,
                set: single_prop("bio", Value::String(bio)),
                unset: Vec::new(),
            };
            self.db
                .mutate(MutationSpec { ops: vec![op] })
                .map_err(to_err)?;
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn run_delete(&mut self, iterations: usize) -> BenchResult<OpStats> {
        if self.users.len() < iterations {
            return Err("not enough users to delete".into());
        }
        let start = Instant::now();
        for _ in 0..iterations {
            let user = self.users.pop().expect("user list not empty");
            let op = MutationOp::DeleteNode {
                id: user.id,
                cascade: false,
            };
            self.db
                .mutate(MutationSpec { ops: vec![op] })
                .map_err(to_err)?;
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn run_read_label(&mut self, iterations: usize) -> BenchResult<OpStats> {
        let start = Instant::now();
        for _ in 0..iterations {
            let rows = self.db.execute(user_scan_spec()).map_err(to_err)?;
            if rows.is_empty() {
                return Err("label scan returned zero rows".into());
            }
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn run_read_by_name(&mut self, iterations: usize) -> BenchResult<OpStats> {
        if self.users.is_empty() {
            return Err("no users to query".into());
        }
        let start = Instant::now();
        for _ in 0..iterations {
            let name = self.random_lookup_key();
            let rows = self.db.execute(user_lookup_spec(name)).map_err(to_err)?;
            if rows.is_empty() {
                return Err("lookup returned zero rows".into());
            }
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn run_expand(&mut self, iterations: usize, hops: usize) -> BenchResult<OpStats> {
        if self.users.is_empty() {
            return Err("no users available for traversal".into());
        }
        let start = Instant::now();
        for _ in 0..iterations {
            let name = self.random_lookup_key();
            let spec = match hops {
                1 => expand_one_spec(name),
                2 => expand_two_spec(name),
                _ => return Err("unsupported hop count".into()),
            };
            let rows = self.db.execute(spec).map_err(to_err)?;
            if rows.is_empty() {
                return Err("expand query returned zero rows".into());
            }
        }
        Ok(OpStats::new(iterations, start.elapsed()))
    }

    fn random_lookup_key(&mut self) -> String {
        let idx = self.rng.gen_range(0..self.users.len());
        self.users[idx].name.clone()
    }
}

struct OpStats {
    ops: usize,
    elapsed: Duration,
}

impl OpStats {
    fn new(ops: usize, elapsed: Duration) -> Self {
        Self { ops, elapsed }
    }

    fn ops_per_second(&self) -> f64 {
        if self.elapsed.as_secs_f64() == 0.0 {
            return self.ops as f64;
        }
        self.ops as f64 / self.elapsed.as_secs_f64()
    }

    fn print(&self, op: Operation) {
        println!(
            "  {:<18} {:>10} ops in {:>8.3?} -> {:>12.0} ops/sec",
            op.label(),
            self.ops,
            self.elapsed,
            self.ops_per_second()
        );
    }
}

fn report_profile(label: &str) {
    if std::env::var_os("SOMBRA_PROFILE").is_none() {
        return;
    }
    if let Some(snapshot) = profile_snapshot(true) {
        if snapshot.plan_count == 0 && snapshot.exec_count == 0 && snapshot.serde_count == 0 {
            return;
        }
        println!(
            "[profile/{label}] planner: {:>8.3} ms total ({:>8.3} µs/op across {:>6} iters) | executor: {:>8.3} ms total ({:>8.3} \
             µs/op across {:>6} iters) | serialize: {:>8.3} ms total ({:>8.3} µs/op across {:>6} iters)",
            nanos_to_ms(snapshot.plan_ns),
            avg_us(snapshot.plan_ns, snapshot.plan_count),
            snapshot.plan_count,
            nanos_to_ms(snapshot.exec_ns),
            avg_us(snapshot.exec_ns, snapshot.exec_count),
            snapshot.exec_count,
            nanos_to_ms(snapshot.serde_ns),
            avg_us(snapshot.serde_ns, snapshot.serde_count),
            snapshot.serde_count
        );
        let query_counts = [
            snapshot.query_read_guard_count,
            snapshot.query_stream_build_count,
            snapshot.query_stream_iter_count,
            snapshot.query_prop_index_count,
            snapshot.query_expand_count,
            snapshot.query_filter_count,
        ];
        if query_counts.iter().any(|&count| count > 0) {
            println!(
                "                query: read_guard {:>8.3} ms ({:>8.3} µs/op) | stream_build {:>8.3} ms ({:>8.3} µs/op) | \
                 stream_iter {:>8.3} ms ({:>8.3} µs/op) | prop_index {:>8.3} ms ({:>8.3} µs/op) | expand {:>8.3} ms ({:>8.3} µs/op) | \
                 filter {:>8.3} ms ({:>8.3} µs/op)",
                nanos_to_ms(snapshot.query_read_guard_ns),
                avg_us(snapshot.query_read_guard_ns, snapshot.query_read_guard_count),
                nanos_to_ms(snapshot.query_stream_build_ns),
                avg_us(snapshot.query_stream_build_ns, snapshot.query_stream_build_count),
                nanos_to_ms(snapshot.query_stream_iter_ns),
                avg_us(snapshot.query_stream_iter_ns, snapshot.query_stream_iter_count),
                nanos_to_ms(snapshot.query_prop_index_ns),
                avg_us(snapshot.query_prop_index_ns, snapshot.query_prop_index_count),
                nanos_to_ms(snapshot.query_expand_ns),
                avg_us(snapshot.query_expand_ns, snapshot.query_expand_count),
                nanos_to_ms(snapshot.query_filter_ns),
                avg_us(snapshot.query_filter_ns, snapshot.query_filter_count),
            );
            let prop_detail_counts = [
                snapshot.query_prop_index_lookup_count,
                snapshot.query_prop_index_encode_count,
                snapshot.query_prop_index_stream_build_count,
                snapshot.query_prop_index_stream_iter_count,
            ];
            if prop_detail_counts.iter().any(|&count| count > 0) {
                println!(
                    "                prop_index detail: lookup {:>8.3} ms ({:>8.3} µs/op) | encode {:>8.3} ms ({:>8.3} µs/op) | \
                     stream_build {:>8.3} ms ({:>8.3} µs/op) | stream_iter {:>8.3} ms ({:>8.3} µs/op)",
                    nanos_to_ms(snapshot.query_prop_index_lookup_ns),
                    avg_us(
                        snapshot.query_prop_index_lookup_ns,
                        snapshot.query_prop_index_lookup_count
                    ),
                    nanos_to_ms(snapshot.query_prop_index_encode_ns),
                    avg_us(
                        snapshot.query_prop_index_encode_ns,
                        snapshot.query_prop_index_encode_count
                    ),
                    nanos_to_ms(snapshot.query_prop_index_stream_build_ns),
                    avg_us(
                        snapshot.query_prop_index_stream_build_ns,
                        snapshot.query_prop_index_stream_build_count
                    ),
                    nanos_to_ms(snapshot.query_prop_index_stream_iter_ns),
                    avg_us(
                        snapshot.query_prop_index_stream_iter_ns,
                        snapshot.query_prop_index_stream_iter_count
                    ),
                );
            }
        }
    }
}

fn nanos_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

fn avg_us(ns: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        (ns as f64 / count as f64) / 1_000.0
    }
}

fn user_scan_spec() -> QuerySpec {
    QuerySpec {
        matches: vec![MatchSpec {
            var: "a".to_string(),
            label: Some("User".to_string()),
        }],
        edges: Vec::new(),
        predicates: Vec::new(),
        distinct: false,
        projections: vec![ProjectionSpec::Var {
            var: "a".to_string(),
            alias: None,
        }],
    }
}

fn user_lookup_spec(name: String) -> QuerySpec {
    QuerySpec {
        matches: vec![MatchSpec {
            var: "a".to_string(),
            label: Some("User".to_string()),
        }],
        edges: Vec::new(),
        predicates: vec![PredicateSpec::Eq {
            var: "a".to_string(),
            prop: "name".to_string(),
            value: LiteralSpec::String(name),
        }],
        distinct: false,
        projections: vec![ProjectionSpec::Var {
            var: "a".to_string(),
            alias: None,
        }],
    }
}

fn expand_one_spec(anchor: String) -> QuerySpec {
    QuerySpec {
        matches: vec![
            MatchSpec {
                var: "a".to_string(),
                label: Some("User".to_string()),
            },
            MatchSpec {
                var: "b".to_string(),
                label: Some("User".to_string()),
            },
        ],
        edges: vec![EdgeSpec {
            from: "a".to_string(),
            to: "b".to_string(),
            edge_type: Some("FOLLOWS".to_string()),
            direction: DirectionSpec::Out,
        }],
        predicates: vec![PredicateSpec::Eq {
            var: "a".to_string(),
            prop: "name".to_string(),
            value: LiteralSpec::String(anchor),
        }],
        distinct: false,
        projections: vec![ProjectionSpec::Var {
            var: "b".to_string(),
            alias: None,
        }],
    }
}

fn expand_two_spec(anchor: String) -> QuerySpec {
    QuerySpec {
        matches: vec![
            MatchSpec {
                var: "a".to_string(),
                label: Some("User".to_string()),
            },
            MatchSpec {
                var: "b".to_string(),
                label: Some("User".to_string()),
            },
            MatchSpec {
                var: "c".to_string(),
                label: Some("User".to_string()),
            },
        ],
        edges: vec![
            EdgeSpec {
                from: "a".to_string(),
                to: "b".to_string(),
                edge_type: Some("FOLLOWS".to_string()),
                direction: DirectionSpec::Out,
            },
            EdgeSpec {
                from: "b".to_string(),
                to: "c".to_string(),
                edge_type: Some("FOLLOWS".to_string()),
                direction: DirectionSpec::Out,
            },
        ],
        predicates: vec![PredicateSpec::Eq {
            var: "a".to_string(),
            prop: "name".to_string(),
            value: LiteralSpec::String(anchor),
        }],
        distinct: false,
        projections: vec![ProjectionSpec::Var {
            var: "c".to_string(),
            alias: None,
        }],
    }
}

fn single_prop(key: &str, value: Value) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert(key.to_string(), value);
    map
}

fn to_err(err: sombra_ffi::FfiError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(err)
}
