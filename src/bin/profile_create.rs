//! Profile create_node and create_edge performance
//!
//! This benchmark measures where time is spent during node/edge creation
//! to identify optimization opportunities.
//!
//! Run with: cargo run --release --bin profile_create
//! Or with env vars: NODES=50000 EDGES=50000 cargo run --release --bin profile_create

use std::time::{Duration, Instant};

use sombra::ffi::{
    Database, DatabaseOptions, TypedBatchSpec, TypedEdgeSpec, TypedNodeRef, TypedNodeSpec,
    TypedPropEntry,
};
use sombra::primitives::pager::Synchronous;
use sombra::storage::{storage_profile_snapshot, StorageProfileSnapshot};
use tempfile::TempDir;

const DEFAULT_WARMUP_NODES: usize = 1000;
const DEFAULT_BENCH_NODES: usize = 10000;
const DEFAULT_BENCH_EDGES: usize = 10000;

fn main() {
    let warmup_nodes: usize = std::env::var("WARMUP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_WARMUP_NODES);
    let bench_nodes: usize = std::env::var("NODES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BENCH_NODES);
    let bench_edges: usize = std::env::var("EDGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BENCH_EDGES);

    println!("=== Create Path Profiling ===");
    println!("Config: warmup={warmup_nodes}, nodes={bench_nodes}, edges={bench_edges}\n");

    // Run different scenarios
    profile_nodes_no_props(warmup_nodes, bench_nodes);
    profile_nodes_with_props(warmup_nodes, bench_nodes);
    profile_nodes_with_btree_index_only(warmup_nodes, bench_nodes);
    profile_nodes_with_chunked_unique_index(warmup_nodes, bench_nodes);
    profile_nodes_with_props_and_indexes(warmup_nodes, bench_nodes);
    profile_edges_no_props(bench_edges);
    profile_edges_with_props(bench_edges);
    profile_mixed_batch(bench_nodes);
    profile_realistic_like(bench_nodes, bench_edges);

    println!("\n=== Profiling Complete ===");
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

fn open_db(path: &std::path::Path) -> Database {
    let mut opts = DatabaseOptions {
        create_if_missing: true,
        ..DatabaseOptions::default()
    };
    opts.pager.synchronous = Synchronous::Off; // Disable fsync for pure CPU measurement
    opts.pager.group_commit_max_wait_ms = 0;
    opts.pager.cache_pages = 16384;

    Database::open(path, opts).expect("open database")
}

fn profile_nodes_no_props(warmup_nodes: usize, bench_nodes: usize) {
    println!("--- Nodes (no properties) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("nodes_no_props.db"));

    // Warmup
    {
        let spec = TypedBatchSpec {
            nodes: (0..warmup_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark
    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_node (no props)", bench_nodes, elapsed, snapshot);
}

fn profile_nodes_with_props(warmup_nodes: usize, bench_nodes: usize) {
    println!("\n--- Nodes (with properties, no indexes) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("nodes_with_props.db"));

    // Warmup
    {
        let spec = TypedBatchSpec {
            nodes: (0..warmup_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![
                        make_string_prop("name", format!("Person{i}")),
                        make_int_prop("age", i as i64),
                        make_string_prop("email", format!("person{i}@example.com")),
                    ],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark
    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![
                        make_string_prop("name", format!("Person{i}")),
                        make_int_prop("age", i as i64),
                        make_string_prop("email", format!("person{i}@example.com")),
                    ],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_node (3 props)", bench_nodes, elapsed, snapshot);
}

fn profile_nodes_with_btree_index_only(warmup_nodes: usize, bench_nodes: usize) {
    println!("\n--- Nodes (BTree index only - age, 100 unique values) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("nodes_btree_only.db"));

    // Warmup
    {
        let spec = TypedBatchSpec {
            nodes: (0..warmup_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![make_int_prop("age", i as i64)],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Create only the BTree index
    db.ensure_property_index("Person", "age", "btree", "int")
        .expect("create age index");

    let _ = storage_profile_snapshot(true);

    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![make_int_prop("age", (i % 100) as i64)],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_node (btree idx)", bench_nodes, elapsed, snapshot);
}

fn profile_nodes_with_chunked_unique_index(warmup_nodes: usize, bench_nodes: usize) {
    println!("\n--- Nodes (Chunked index - name, 10k unique values) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("nodes_chunked_unique.db"));

    // Warmup
    {
        let spec = TypedBatchSpec {
            nodes: (0..warmup_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![make_string_prop("name", format!("Person{i}"))],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Create only the Chunked index  
    db.ensure_property_index("Person", "name", "chunked", "string")
        .expect("create name index");

    let _ = storage_profile_snapshot(true);

    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![make_string_prop("name", format!("BenchPerson{i}"))],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_node (chunked unique)", bench_nodes, elapsed, snapshot);
}

fn profile_nodes_with_props_and_indexes(warmup_nodes: usize, bench_nodes: usize) {
    println!("\n--- Nodes (with properties + 3 property indexes) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("nodes_with_props_indexed.db"));

    // Warmup: create initial nodes to establish the label
    {
        let spec = TypedBatchSpec {
            nodes: (0..warmup_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![
                        make_string_prop("name", format!("Person{i}")),
                        make_int_prop("age", i as i64),
                        make_string_prop("email", format!("person{i}@example.com")),
                    ],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Create property indexes on the "Person" label
    // Using BTree for int (age) and Chunked for strings (name, email)
    db.ensure_property_index("Person", "name", "chunked", "string")
        .expect("create name index");
    db.ensure_property_index("Person", "age", "btree", "int")
        .expect("create age index");
    db.ensure_property_index("Person", "email", "chunked", "string")
        .expect("create email index");

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark: create nodes that will trigger deferred property index updates
    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![
                        make_string_prop("name", format!("BenchPerson{i}")),
                        make_int_prop("age", (i % 100) as i64),
                        make_string_prop("email", format!("bench{i}@example.com")),
                    ],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_node (3 props + indexes)", bench_nodes, elapsed, snapshot);
}

fn profile_edges_no_props(bench_edges: usize) {
    println!("\n--- Edges (no properties) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("edges_no_props.db"));

    // Create nodes for edges first
    let num_nodes = bench_edges + 1;
    {
        let spec = TypedBatchSpec {
            nodes: (0..num_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark: create edges in a single batch
    let start = Instant::now();
    {
        // Create nodes in the same batch so we can reference them by handle
        let spec = TypedBatchSpec {
            nodes: (0..num_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Node".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: (0..bench_edges)
                .map(|i| TypedEdgeSpec {
                    ty: "KNOWS".to_string(),
                    src: make_handle_ref(i),
                    dst: make_handle_ref(i + 1),
                    props: vec![],
                })
                .collect(),
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    // Total ops = nodes + edges
    print_results(
        "create_edge (no props)",
        bench_edges,
        elapsed,
        snapshot,
    );
}

fn profile_edges_with_props(bench_edges: usize) {
    println!("\n--- Edges (with properties) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("edges_with_props.db"));

    let num_nodes = bench_edges + 1;

    // Warmup batch (not measured) - to match edges_no_props benchmark
    {
        let spec = TypedBatchSpec {
            nodes: (0..num_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: vec![],
        };
        db.create_typed_batch(&spec).unwrap();
    }

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark: create nodes and edges with properties in a single batch
    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..num_nodes)
                .map(|_| TypedNodeSpec {
                    label: "Node".to_string(),
                    props: vec![],
                    alias: None,
                })
                .collect(),
            edges: (0..bench_edges)
                .map(|i| TypedEdgeSpec {
                    ty: "KNOWS".to_string(),
                    src: make_handle_ref(i),
                    dst: make_handle_ref(i + 1),
                    props: vec![
                        make_int_prop("since", 2020 + (i % 5) as i64),
                        make_float_prop("weight", (i as f64) / 100.0),
                    ],
                })
                .collect(),
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results("create_edge (2 props)", bench_edges, elapsed, snapshot);
}

fn profile_mixed_batch(bench_nodes: usize) {
    println!("\n--- Mixed Batch (nodes + edges in single transaction) ---");
    let tmpdir = TempDir::new().unwrap();
    let db = open_db(&tmpdir.path().join("mixed_batch.db"));

    // Reset counters
    let _ = storage_profile_snapshot(true);

    // Benchmark: create nodes and edges in same transaction
    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Person".to_string(),
                    props: vec![make_string_prop("name", format!("Person{i}"))],
                    alias: None,
                })
                .collect(),
            edges: (0..bench_nodes - 1)
                .map(|i| TypedEdgeSpec {
                    ty: "KNOWS".to_string(),
                    src: make_handle_ref(i),
                    dst: make_handle_ref(i + 1),
                    props: vec![],
                })
                .collect(),
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results(
        "mixed (node+edge)",
        bench_nodes + bench_nodes - 1,
        elapsed,
        snapshot,
    );
}

fn profile_realistic_like(bench_nodes: usize, bench_edges: usize) {
    if bench_nodes == 0 {
        println!("\n--- Realistic-like batch skipped (nodes=0) ---");
        return;
    }

    println!("\n--- Realistic-like batch (7 node props, 2 edge props) ---");
    let tmpdir = TempDir::new().unwrap();
    let db_path = tmpdir.path().join("realistic_like.db");

    // Match realistic_bench pager settings (fsync on, no group wait)
    let mut opts = DatabaseOptions {
        create_if_missing: true,
        ..DatabaseOptions::default()
    };
    opts.pager.synchronous = Synchronous::Normal;
    opts.pager.group_commit_max_wait_ms = 0;
    opts.pager.cache_pages = 16384;

    let db = Database::open(&db_path, opts).expect("open database");

    let code_texts = [
        "function foo() { return 1; }",
        "const bar = () => { console.log('hello'); };",
        "class Baz { constructor() {} }",
        "export function qux() { return true; }",
        "const quux = (x, y) => x + y;",
    ];

    let metadata = [
        "{\"type\": \"function\", \"exported\": true}",
        "{\"type\": \"variable\", \"exported\": false}",
        "{\"type\": \"class\", \"exported\": true}",
    ];

    // Reset counters
    let _ = storage_profile_snapshot(true);

    let start = Instant::now();
    {
        let spec = TypedBatchSpec {
            nodes: (0..bench_nodes)
                .map(|i| TypedNodeSpec {
                    label: "Node".to_string(),
                    props: vec![
                        make_string_prop("name", format!("fn_{i}")),
                        make_string_prop("filePath", format!("/tmp/file_{}.ts", i / 50)),
                        make_int_prop("startLine", i as i64),
                        make_int_prop("endLine", (i + 5) as i64),
                        make_string_prop("codeText", code_texts[i % code_texts.len()].to_string()),
                        make_string_prop("language", "typescript".to_string()),
                        make_string_prop("metadata", metadata[i % metadata.len()].to_string()),
                    ],
                    alias: None,
                })
                .collect(),
            edges: (0..bench_edges)
                .map(|i| TypedEdgeSpec {
                    ty: "LINKS".to_string(),
                    src: make_handle_ref(i % bench_nodes),
                    dst: make_handle_ref((i * 13 + 7) % bench_nodes),
                    props: vec![
                        make_float_prop("weight", (i % 10) as f64 / 10.0),
                        make_string_prop(
                            "linkKind",
                            if i % 2 == 0 { "call" } else { "reference" }.to_string(),
                        ),
                    ],
                })
                .collect(),
        };
        db.create_typed_batch(&spec).unwrap();
    }
    let elapsed = start.elapsed();

    let snapshot = storage_profile_snapshot(true);
    print_results(
        "realistic_like (7 node props + 2 edge props)",
        bench_nodes + bench_edges,
        elapsed,
        snapshot,
    );
}

fn print_results(
    _name: &str,
    ops: usize,
    elapsed: Duration,
    snapshot: Option<StorageProfileSnapshot>,
) {
    let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
    let us_per_op = elapsed.as_micros() as f64 / ops as f64;

    println!(
        "  Total: {:?} ({:.0} ops/sec, {:.2} µs/op)",
        elapsed, ops_per_sec, us_per_op
    );

    if let Some(s) = snapshot {
        println!("\n  Breakdown (time spent in each phase):");

        // FFI layer breakdown
        if s.ffi_create_batch_count > 0 {
            let total_ns = s.ffi_create_batch_ns;
            println!(
                "\n  FFI CREATE_BATCH ({} calls, {:.3} ms total):",
                s.ffi_create_batch_count,
                total_ns as f64 / 1_000_000.0
            );
            print_phase(
                "    dict_resolve",
                s.dict_resolve_ns,
                s.dict_resolve_count,
                total_ns,
            );
            print_phase(
                "    props_convert",
                s.ffi_typed_props_convert_ns,
                s.ffi_typed_props_convert_count,
                total_ns,
            );
        }

        // Create node breakdown
        if s.create_node_count > 0 {
            let total_ns = s.create_node_ns;
            println!(
                "\n  CREATE_NODE ({} calls, {:.3} ms total, {:.0} ns/call):",
                s.create_node_count,
                total_ns as f64 / 1_000_000.0,
                total_ns as f64 / s.create_node_count.max(1) as f64
            );
            print_phase(
                "    encode_props",
                s.create_node_encode_props_ns,
                s.create_node_encode_props_count,
                total_ns,
            );
            print_phase(
                "    btree_insert",
                s.create_node_btree_ns,
                s.create_node_btree_count,
                total_ns,
            );
            print_phase(
                "    label_index",
                s.create_node_label_index_ns,
                s.create_node_label_index_count,
                total_ns,
            );
            print_phase(
                "    prop_index",
                s.create_node_prop_index_ns,
                s.create_node_prop_index_count,
                total_ns,
            );
            let accounted = s.create_node_encode_props_ns
                + s.create_node_btree_ns
                + s.create_node_label_index_ns
                + s.create_node_prop_index_ns;
            let other = total_ns.saturating_sub(accounted);
            println!(
                "    other:        {:>10} ns ({:>5.1}%)",
                other,
                pct(other, total_ns)
            );
        }

        // Create edge breakdown
        if s.create_edge_count > 0 {
            let total_ns = s.create_edge_ns;
            println!(
                "\n  CREATE_EDGE ({} calls, {:.3} ms total, {:.0} ns/call):",
                s.create_edge_count,
                total_ns as f64 / 1_000_000.0,
                total_ns as f64 / s.create_edge_count.max(1) as f64
            );
            print_phase(
                "    encode_props",
                s.create_edge_encode_props_ns,
                s.create_edge_encode_props_count,
                total_ns,
            );
            print_phase(
                "    btree_insert",
                s.create_edge_btree_ns,
                s.create_edge_btree_count,
                total_ns,
            );
            print_phase(
                "    adjacency",
                s.create_edge_adjacency_ns,
                s.create_edge_adjacency_count,
                total_ns,
            );
            let accounted =
                s.create_edge_encode_props_ns + s.create_edge_btree_ns + s.create_edge_adjacency_ns;
            let other = total_ns.saturating_sub(accounted);
            println!(
                "    other:        {:>10} ns ({:>5.1}%)",
                other,
                pct(other, total_ns)
            );
        }

        // BTree leaf operations (detailed)
        if s.btree_leaf_insert_count > 0 {
            println!("\n  BTREE LEAF OPS:");
            println!(
                "    leaf_insert:  {:>10} ns ({} calls, {:.0} ns/call)",
                s.btree_leaf_insert_ns,
                s.btree_leaf_insert_count,
                s.btree_leaf_insert_ns as f64 / s.btree_leaf_insert_count.max(1) as f64
            );
            println!(
                "    leaf_search:  {:>10} ns ({} calls)",
                s.btree_leaf_search_ns, s.btree_leaf_search_count
            );
            println!(
                "    slot_extent:  {:>10} ns ({} calls, {} slots)",
                s.btree_slot_extent_ns, s.btree_slot_extent_count, s.btree_slot_extent_slots
            );
            println!(
                "    key_decodes:  {:>10} ({} cmps, {} bytes copied)",
                s.btree_leaf_key_decodes, s.btree_leaf_key_cmps, s.btree_leaf_memcopy_bytes
            );

            // Granular leaf insert breakdown
            println!("\n  LEAF INSERT BREAKDOWN:");
            let leaf_total = s.btree_leaf_insert_ns;
            println!(
                "    binary_search: {:>9} ns ({:>5.1}%, {} calls, {:.0} ns/call)",
                s.btree_leaf_binary_search_ns,
                pct(s.btree_leaf_binary_search_ns, leaf_total),
                s.btree_leaf_binary_search_count,
                s.btree_leaf_binary_search_ns as f64
                    / s.btree_leaf_binary_search_count.max(1) as f64
            );
            println!(
                "    record_encode: {:>9} ns ({:>5.1}%, {} calls, {:.0} ns/call)",
                s.btree_leaf_record_encode_ns,
                pct(s.btree_leaf_record_encode_ns, leaf_total),
                s.btree_leaf_record_encode_count,
                s.btree_leaf_record_encode_ns as f64
                    / s.btree_leaf_record_encode_count.max(1) as f64
            );
            println!(
                "    slot_alloc:    {:>9} ns ({:>5.1}%, {} calls, {:.0} ns/call)",
                s.btree_leaf_slot_alloc_ns,
                pct(s.btree_leaf_slot_alloc_ns, leaf_total),
                s.btree_leaf_slot_alloc_count,
                s.btree_leaf_slot_alloc_ns as f64 / s.btree_leaf_slot_alloc_count.max(1) as f64
            );
            println!(
                "    alloc_cache:   {:>9} ns ({:>5.1}%, {} calls, {:.0} ns/call)",
                s.btree_leaf_allocator_cache_ns,
                pct(s.btree_leaf_allocator_cache_ns, leaf_total),
                s.btree_leaf_allocator_cache_count,
                s.btree_leaf_allocator_cache_ns as f64
                    / s.btree_leaf_allocator_cache_count.max(1) as f64
            );
            // Show allocator build stats (subset of alloc_cache time when cache misses)
            println!(
                "      -> alloc_build: {:>6} ns ({} builds, {:.0} ns/build, {} free_regions)",
                s.btree_leaf_allocator_build_ns,
                s.btree_leaf_allocator_build_count,
                s.btree_leaf_allocator_build_ns as f64
                    / s.btree_leaf_allocator_build_count.max(1) as f64,
                s.btree_leaf_allocator_build_free_regions
            );
            let leaf_accounted = s.btree_leaf_binary_search_ns
                + s.btree_leaf_record_encode_ns
                + s.btree_leaf_slot_alloc_ns
                + s.btree_leaf_allocator_cache_ns;
            let leaf_other = leaf_total.saturating_sub(leaf_accounted);
            println!(
                "    other:         {:>9} ns ({:>5.1}%)",
                leaf_other,
                pct(leaf_other, leaf_total)
            );
            println!(
                "    in_place_ok:   {:>9} / {} splits",
                s.btree_leaf_in_place_success, s.btree_leaf_splits
            );
        }

        // MVCC/Commit
        println!("\n  MVCC/COMMIT:");
        println!(
            "    write_begin:  {:>10} ns ({} calls, p50={} ns)",
            s.mvcc_write_begin_ns, s.mvcc_write_begin_count, s.mvcc_write_begin_p50_ns
        );
        println!(
            "    flush_def:    {:>10} ns ({} calls)",
            s.flush_deferred_ns, s.flush_deferred_count
        );

        // Flush deferred adjacency breakdown
        if s.flush_adj_entries > 0 || s.flush_deferred_indexes_ns > 0 {
            let flush_total = s.flush_deferred_ns;
            println!("\n  FLUSH DEFERRED BREAKDOWN ({} adj entries):", s.flush_adj_entries);
            println!(
                "    key_encode:   {:>10} ns ({:>5.1}%, {} calls, {:.0} ns/call)",
                s.flush_adj_key_encode_ns,
                pct(s.flush_adj_key_encode_ns, flush_total),
                s.flush_adj_key_encode_count,
                s.flush_adj_key_encode_ns as f64 / s.flush_adj_key_encode_count.max(1) as f64
            );
            println!(
                "    fwd_sort:     {:>10} ns ({:>5.1}%, {} calls)",
                s.flush_adj_fwd_sort_ns,
                pct(s.flush_adj_fwd_sort_ns, flush_total),
                s.flush_adj_fwd_sort_count
            );
            println!(
                "    fwd_put_many: {:>10} ns ({:>5.1}%, {} calls)",
                s.flush_adj_fwd_put_ns,
                pct(s.flush_adj_fwd_put_ns, flush_total),
                s.flush_adj_fwd_put_count
            );
            println!(
                "    rev_sort:     {:>10} ns ({:>5.1}%, {} calls)",
                s.flush_adj_rev_sort_ns,
                pct(s.flush_adj_rev_sort_ns, flush_total),
                s.flush_adj_rev_sort_count
            );
            println!(
                "    rev_put_many: {:>10} ns ({:>5.1}%, {} calls)",
                s.flush_adj_rev_put_ns,
                pct(s.flush_adj_rev_put_ns, flush_total),
                s.flush_adj_rev_put_count
            );
            println!(
                "    finalize:     {:>10} ns ({:>5.1}%, {} ops, {:.0} ns/op)",
                s.flush_adj_finalize_ns,
                pct(s.flush_adj_finalize_ns, flush_total),
                s.flush_adj_finalize_count,
                s.flush_adj_finalize_ns as f64 / s.flush_adj_finalize_count.max(1) as f64
            );
            println!(
                "    def_indexes:  {:>10} ns ({:>5.1}%, {} calls)",
                s.flush_deferred_indexes_ns,
                pct(s.flush_deferred_indexes_ns, flush_total),
                s.flush_deferred_indexes_count
            );
            let flush_accounted = s.flush_adj_key_encode_ns
                + s.flush_adj_fwd_sort_ns
                + s.flush_adj_fwd_put_ns
                + s.flush_adj_rev_sort_ns
                + s.flush_adj_rev_put_ns
                + s.flush_adj_finalize_ns
                + s.flush_deferred_indexes_ns;
            let flush_other = flush_total.saturating_sub(flush_accounted);
            println!(
                "    other:        {:>10} ns ({:>5.1}%)",
                flush_other,
                pct(flush_other, flush_total)
            );
        }

        println!(
            "    commit:       {:>10} ns ({} calls)",
            s.mvcc_commit_ns, s.mvcc_commit_count
        );
        println!(
            "    pager_commit: {:>10} ns (p50={} ns)",
            s.pager_commit_ns, s.pager_commit_p50_ns
        );

        // WAL
        if s.pager_wal_frames > 0 {
            println!("\n  WAL:");
            println!(
                "    frames:       {:>10} ({} bytes)",
                s.pager_wal_frames, s.pager_wal_bytes
            );
            println!("    fsync_count:  {:>10}", s.pager_fsync_count);
            println!("    frame_build:  {:>10} ns", s.commit_frame_build_ns);
            println!("    wal_write:    {:>10} ns", s.commit_wal_write_ns);
            println!("    fsync:        {:>10} ns", s.commit_fsync_ns);
        }

        // Summary: time breakdown as percentages
        let total_wall_ns = elapsed.as_nanos() as u64;
        println!("\n  TIME BREAKDOWN (% of wall clock):");

        let ffi_overhead = s
            .ffi_create_batch_ns
            .saturating_sub(s.create_node_ns + s.create_edge_ns);
        println!(
            "    FFI overhead: {:>5.1}% ({:.3} ms)",
            pct(ffi_overhead, total_wall_ns),
            ffi_overhead as f64 / 1_000_000.0
        );
        println!(
            "    create_node:  {:>5.1}% ({:.3} ms)",
            pct(s.create_node_ns, total_wall_ns),
            s.create_node_ns as f64 / 1_000_000.0
        );
        println!(
            "    create_edge:  {:>5.1}% ({:.3} ms)",
            pct(s.create_edge_ns, total_wall_ns),
            s.create_edge_ns as f64 / 1_000_000.0
        );
        println!(
            "    flush_defer:  {:>5.1}% ({:.3} ms)",
            pct(s.flush_deferred_ns, total_wall_ns),
            s.flush_deferred_ns as f64 / 1_000_000.0
        );
        println!(
            "    commit:       {:>5.1}% ({:.3} ms)",
            pct(s.mvcc_commit_ns, total_wall_ns),
            s.mvcc_commit_ns as f64 / 1_000_000.0
        );

        let accounted_total = s.ffi_create_batch_ns + s.mvcc_commit_ns;
        let unaccounted = total_wall_ns.saturating_sub(accounted_total);
        println!(
            "    other:        {:>5.1}% ({:.3} ms)",
            pct(unaccounted, total_wall_ns),
            unaccounted as f64 / 1_000_000.0
        );
    }
    println!();
}

fn print_phase(name: &str, ns: u64, count: u64, total_ns: u64) {
    let avg = if count > 0 { ns / count } else { 0 };
    println!(
        "{}:{:>10} ns ({:>5.1}%, {} calls, {} ns/call)",
        name,
        ns,
        pct(ns, total_ns),
        count,
        avg
    );
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}
