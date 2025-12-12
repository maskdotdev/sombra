//! Graph operations benchmark (lookup + traversals)
//!
//! Run with for example:
//!
//! ```bash
//! # Default config, both sync modes
//! cargo run --release --bin graph_ops_bench
//!
//! # Explicit sizes and single sync mode
//! NODES=50000 EDGES=200000 LOOKUPS=20000 NEIGHBORS=20000 BFS_RUNS=5000 \
//!   SYNC_MODE=full cargo run --release --bin graph_ops_bench
//! ```

use std::env;
use std::time::{Duration, Instant};

use serde_json::Value;
use sombra::ffi::{
    Database, DatabaseOptions, MatchSpec, PayloadValue, PredicateSpec, ProjectionSpec, QuerySpec,
    TypedBatchSpec, TypedEdgeSpec, TypedNodeRef, TypedNodeSpec, TypedPropEntry,
};
use sombra::primitives::pager::Synchronous;
use sombra::storage::Dir;
use tempfile::TempDir;

fn main() {
    if let Err(err) = run() {
        eprintln!("graph_ops_bench error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = BenchConfig::from_env();
    println!(
        "Graph ops bench config: NODES={}, EDGES={}, LOOKUPS={}, NEIGHBORS={}, BFS_RUNS={}, BFS_DEPTH={}",
        cfg.nodes, cfg.edges, cfg.lookups, cfg.neighbor_expansions, cfg.bfs_runs, cfg.bfs_depth
    );

    let modes = parse_modes_from_env();
    for mode in modes {
        println!("\n==============================");
        println!("Sync mode: {mode:?}");
        println!("==============================");
        run_for_mode(mode, &cfg)?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    nodes: usize,
    edges: usize,
    lookups: usize,
    neighbor_expansions: usize,
    bfs_runs: usize,
    bfs_depth: u32,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            nodes: env_usize("NODES", 50_000),
            edges: env_usize("EDGES", 200_000),
            lookups: env_usize("LOOKUPS", 10_000),
            neighbor_expansions: env_usize("NEIGHBORS", 10_000),
            bfs_runs: env_usize("BFS_RUNS", 5_000),
            bfs_depth: env_usize("BFS_DEPTH", 3) as u32,
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(val) => val
            .trim()
            .parse::<usize>()
            .unwrap_or_else(|_| {
                eprintln!(
                    "Invalid {name}='{}', falling back to {}",
                    val.trim(), default
                );
                default
            }),
        Err(_) => default,
    }
}

fn parse_modes_from_env() -> Vec<Synchronous> {
    match env::var("SYNC_MODE") {
        Ok(val) => match parse_sync_mode(&val) {
            Some(mode) => vec![mode],
            None => {
                eprintln!(
                    "Unknown SYNC_MODE='{}', expected full|normal|off; defaulting to [Full, Normal]",
                    val
                );
                vec![Synchronous::Full, Synchronous::Normal]
            }
        },
        Err(_) => vec![Synchronous::Full, Synchronous::Normal],
    }
}

fn parse_sync_mode(value: &str) -> Option<Synchronous> {
    match value.to_ascii_lowercase().as_str() {
        "full" => Some(Synchronous::Full),
        "normal" => Some(Synchronous::Normal),
        "off" => Some(Synchronous::Off),
        _ => None,
    }
}

fn run_for_mode(mode: Synchronous, cfg: &BenchConfig) -> Result<(), Box<dyn std::error::Error>> {
    let tmpdir = TempDir::new()?;
    let db_path = tmpdir.path().join("graph_ops.sombra");
    println!("DB path: {:?}", db_path);

    let mut opts = DatabaseOptions {
        create_if_missing: true,
        distinct_neighbors_default: true,
        ..DatabaseOptions::default()
    };
    opts.pager.synchronous = mode;
    // Disable commit coalescing so sync mode differences are clearer.
    opts.pager.group_commit_max_wait_ms = 0;
    opts.pager.cache_pages = 16_384;

    let db = Database::open(&db_path, opts)?;

    // Write phase: create code-like nodes and LINKS edges.
    let (node_ids, names, write_time) = create_users_and_edges(&db, cfg.nodes, cfg.edges)?;

    // Ensure indexes on Node.name for realistic lookup performance.
    let _ = db.ensure_label_indexes(&vec!["Node".to_string()])?;
    let _ = db.ensure_property_index("Node", "name", "chunked", "string")?;

    println!(
        "Writes: nodes={} edges={} in {:.1} ms (nodes: {:.0}/s, edges: {:.0}/s)",
        node_ids.len(),
        cfg.edges,
        ms(write_time),
        node_ids.len() as f64 / write_time.as_secs_f64(),
        cfg.edges as f64 / write_time.as_secs_f64()
    );

    // Lookup by name (property index on Node.name).
    let lookup_stats = lookup_by_name(&db, &names, cfg.lookups)?;
    println!(
        "Lookup by name: {} lookups in {:.1} ms (qps: {:.0}), avg per lookup: {:.3} ms, rows/lookup: {:.2}",
        lookup_stats.count,
        ms(lookup_stats.time),
        lookup_stats.count as f64 / lookup_stats.time.as_secs_f64(),
        ms(lookup_stats.time) / lookup_stats.count as f64,
        lookup_stats.rows_per_lookup
    );

    // One-hop neighbor expansions.
    let neighbor_stats = neighbor_expansions(&db, &node_ids, cfg.neighbor_expansions)?;
    println!(
        "One-hop neighbors: {} expansions in {:.1} ms (qps: {:.0}), neighbors/expansion: {:.2}",
        neighbor_stats.count,
        ms(neighbor_stats.time),
        neighbor_stats.count as f64 / neighbor_stats.time.as_secs_f64(),
        neighbor_stats.neighbors_per_expansion
    );

    // BFS traversals up to configured depth.
    let bfs_stats = bfs_traversals(&db, &node_ids, cfg.bfs_runs, cfg.bfs_depth)?;
    println!(
        "BFS traversals (depth <= {}): {} runs in {:.1} ms (qps: {:.0}), visits/run: {:.2}",
        cfg.bfs_depth,
        bfs_stats.count,
        ms(bfs_stats.time),
        bfs_stats.count as f64 / bfs_stats.time.as_secs_f64(),
        bfs_stats.visits_per_run
    );

    Ok(())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

fn make_string_prop(key: &str, value: String) -> TypedPropEntry {
    TypedPropEntry {
        key: key.to_string(),
        kind: "string".to_string(),
        bool_value: None,
        int_value: None,
        float_value: None,
        string_value: Some(value),
        bytes_value: None,
    }
}

fn make_int_prop(key: &str, value: i64) -> TypedPropEntry {
    TypedPropEntry {
        key: key.to_string(),
        kind: "int".to_string(),
        bool_value: None,
        int_value: Some(value),
        float_value: None,
        string_value: None,
        bytes_value: None,
    }
}

fn make_float_prop(key: &str, value: f64) -> TypedPropEntry {
    TypedPropEntry {
        key: key.to_string(),
        kind: "float".to_string(),
        bool_value: None,
        int_value: None,
        float_value: Some(value),
        string_value: None,
        bytes_value: None,
    }
}

fn make_handle_ref(index: usize) -> TypedNodeRef {
    TypedNodeRef {
        kind: "handle".to_string(),
        alias: None,
        handle: Some(index as u32),
        id: None,
    }
}

fn create_users_and_edges(
    db: &Database,
    node_count: usize,
    edge_count: usize,
) -> Result<(Vec<u64>, Vec<String>, Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();

    // Code-like dataset: functions with file/position/metadata.
    let code_texts = [
        "function foo() { return 1; }",
        "const bar = () => { console.log('hello'); };",
        "class Baz { constructor() {} }",
        "export function qux() { return true; }",
        "const quux = (x, y) => x + y;",
    ];

    let metadata = [
        r#"{"type": "function", "exported": true}"#,
        r#"{"type": "variable", "exported": false}"#,
        r#"{"type": "class", "exported": true}"#,
    ];

    let mut node_ids: Vec<u64> = Vec::with_capacity(node_count);
    let mut names: Vec<String> = Vec::with_capacity(node_count);

    let chunk_nodes = node_count.max(1);
    let chunk_edges = edge_count.max(1);

    let mut created_nodes = 0usize;
    let mut created_edges = 0usize;

    while created_nodes < node_count {
        let n = usize::min(chunk_nodes, node_count - created_nodes);
        let e = if created_edges >= edge_count {
            0
        } else {
            usize::min(chunk_edges, edge_count - created_edges)
        };

        let nodes: Vec<TypedNodeSpec> = (0..n)
            .map(|i| {
                let idx = created_nodes + i;
                let name = format!("fn_{idx}");
                names.push(name.clone());
                TypedNodeSpec {
                    label: "Node".to_string(),
                    props: vec![
                        make_string_prop("name", name),
                        make_string_prop("filePath", format!("/tmp/file_{}.ts", idx / 50)),
                        make_int_prop("startLine", idx as i64),
                        make_int_prop("endLine", (idx + 5) as i64),
                        make_string_prop(
                            "codeText",
                            code_texts[idx % code_texts.len()].to_string(),
                        ),
                        make_string_prop("language", "typescript".to_string()),
                        make_string_prop(
                            "metadata",
                            metadata[idx % metadata.len()].to_string(),
                        ),
                    ],
                    alias: None,
                }
            })
            .collect();

        let edges: Vec<TypedEdgeSpec> = (0..e)
            .map(|i| TypedEdgeSpec {
                ty: "LINKS".to_string(),
                src: make_handle_ref(i % n.max(1)),
                dst: make_handle_ref((i * 13 + 7) % n.max(1)),
                props: vec![
                    make_float_prop("weight", (i % 10) as f64 / 10.0),
                    make_string_prop(
                        "linkKind",
                        if i % 2 == 0 {
                            "call".to_string()
                        } else {
                            "reference".to_string()
                        },
                    ),
                ],
            })
            .collect();

        let spec = TypedBatchSpec { nodes, edges };
        let result = db.create_typed_batch(&spec)?;
        node_ids.extend(result.node_ids.iter().map(|id| id.0));

        created_nodes += n;
        created_edges += e;
    }

    let elapsed = start.elapsed();
    Ok((node_ids, names, elapsed))
}

struct LookupStats {
    count: usize,
    time: Duration,
    rows_per_lookup: f64,
}

fn lookup_by_name(
    db: &Database,
    names: &[String],
    count: usize,
) -> Result<LookupStats, Box<dyn std::error::Error>> {
    if names.is_empty() || count == 0 {
        return Ok(LookupStats {
            count: 0,
            time: Duration::ZERO,
            rows_per_lookup: 0.0,
        });
    }

    let mut total_rows = 0usize;
    let mut idx = 0usize;
    let start = Instant::now();

    for _ in 0..count {
        let name = &names[idx % names.len()];
        idx = (idx + 1) % names.len();
        let spec = user_lookup_spec(name.clone());
        let result = db.execute(spec)?;
        total_rows += rows_len(&result);
    }

    let elapsed = start.elapsed();
    let rows_per_lookup = if count > 0 {
        total_rows as f64 / count as f64
    } else {
        0.0
    };

    Ok(LookupStats {
        count,
        time: elapsed,
        rows_per_lookup,
    })
}

fn rows_len(value: &Value) -> usize {
    value
        .get("rows")
        .and_then(|rows| rows.as_array())
        .map(|rows| rows.len())
        .unwrap_or(0)
}

fn user_lookup_spec(name: String) -> QuerySpec {
    QuerySpec {
        schema_version: Some(1),
        request_id: None,
        matches: vec![MatchSpec {
            var: "n".to_string(),
            label: Some("Node".to_string()),
        }],
        edges: Vec::new(),
        predicate: Some(PredicateSpec::Eq {
            var: "n".to_string(),
            prop: "name".to_string(),
            value: PayloadValue::String(name),
        }),
        projections: vec![ProjectionSpec::Var {
            var: "n".to_string(),
            alias: None,
        }],
        distinct: false,
    }
}

struct NeighborStats {
    count: usize,
    time: Duration,
    neighbors_per_expansion: f64,
}

fn neighbor_expansions(
    db: &Database,
    node_ids: &[u64],
    expansions: usize,
) -> Result<NeighborStats, Box<dyn std::error::Error>> {
    if node_ids.is_empty() || expansions == 0 {
        return Ok(NeighborStats {
            count: 0,
            time: Duration::ZERO,
            neighbors_per_expansion: 0.0,
        });
    }

    let mut total_neighbors = 0usize;
    let mut idx = 0usize;
    let start = Instant::now();

    for _ in 0..expansions {
        let id = node_ids[idx % node_ids.len()];
        idx = (idx + 1) % node_ids.len();
        let neighbors = db.neighbors_with_options(id, Dir::Out, Some("LINKS"), true)?;
        total_neighbors += neighbors.len();
    }

    let elapsed = start.elapsed();
    let neighbors_per_expansion = if expansions > 0 {
        total_neighbors as f64 / expansions as f64
    } else {
        0.0
    };

    Ok(NeighborStats {
        count: expansions,
        time: elapsed,
        neighbors_per_expansion,
    })
}

struct BfsStats {
    count: usize,
    time: Duration,
    visits_per_run: f64,
}

fn bfs_traversals(
    db: &Database,
    node_ids: &[u64],
    runs: usize,
    max_depth: u32,
) -> Result<BfsStats, Box<dyn std::error::Error>> {
    if node_ids.is_empty() || runs == 0 {
        return Ok(BfsStats {
            count: 0,
            time: Duration::ZERO,
            visits_per_run: 0.0,
        });
    }

    let mut total_visits = 0usize;
    let mut idx = 0usize;
    let start = Instant::now();

    for _ in 0..runs {
        let id = node_ids[idx % node_ids.len()];
        idx = (idx + 1) % node_ids.len();
        let visits = db.bfs_traversal(
            id,
            Dir::Out,
            max_depth,
            Some(&["LINKS".to_string()]),
            None,
        )?;
        total_visits += visits.len();
    }

    let elapsed = start.elapsed();
    let visits_per_run = if runs > 0 {
        total_visits as f64 / runs as f64
    } else {
        0.0
    };

    Ok(BfsStats {
        count: runs,
        time: elapsed,
        visits_per_run,
    })
}
