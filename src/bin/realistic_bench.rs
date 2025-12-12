//! Realistic benchmark comparing Rust core vs Node.js bindings
//! Run with: cargo run --release --bin realistic_bench

use sombra::ffi::{
    BulkLoadOptions, Database, DatabaseOptions, TypedBatchSpec, TypedEdgeSpec, TypedNodeRef,
    TypedNodeSpec, TypedPropEntry,
};
use sombra::primitives::pager::Synchronous;
use std::{fs, path::Path, time::Instant};
use tempfile::TempDir;

fn total_dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += total_dir_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
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

fn main() {
    let node_count: usize = std::env::var("NODES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    let edge_count: usize = std::env::var("EDGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20000);
    let read_count: usize = std::env::var("READS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);
    let read_mode = std::env::var("READ_MODE").unwrap_or_else(|_| "SNAPSHOT_BATCH".to_string());
    let read_batch_size: usize = std::env::var("READ_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let chunk_nodes: Option<usize> = std::env::var("CHUNK_NODES")
        .ok()
        .and_then(|s| s.parse().ok());
    let chunk_edges: Option<usize> = std::env::var("CHUNK_EDGES")
        .ok()
        .and_then(|s| s.parse().ok());
    let load_mode = std::env::var("LOAD_MODE").unwrap_or_else(|_| "TYPED_BATCH".to_string());
    let no_fsync = std::env::var("NO_FSYNC").map(|v| v == "1").unwrap_or(false);

    let tmpdir = TempDir::new().expect("tempdir");
    let db_path = tmpdir.path().join("bench.sombra");
    println!("📂 temp db: {db_path:?}");

    let mut opts = DatabaseOptions {
        create_if_missing: true,
        ..DatabaseOptions::default()
    };
    opts.pager.synchronous = if no_fsync {
        Synchronous::Off
    } else {
        Synchronous::Normal
    };
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
        r#"{"type": "function", "exported": true}"#,
        r#"{"type": "variable", "exported": false}"#,
        r#"{"type": "class", "exported": true}"#,
    ];

    // Create nodes AND edges using typed batches by default.
    // Supports chunking via CHUNK_NODES / CHUNK_EDGES to avoid single huge txns.
    let mut node_ids: Vec<u64> = Vec::with_capacity(node_count);

    let write_start = Instant::now();

    if load_mode.eq_ignore_ascii_case("BULK") {
        let mut opts = BulkLoadOptions::default();
        if let Some(n) = chunk_nodes {
            if n > 0 {
                opts.node_chunk_size = n;
            }
        }
        if let Some(e) = chunk_edges {
            if e > 0 {
                opts.edge_chunk_size = e;
            }
        }

        println!(
            "Creating {node_count} nodes and {edge_count} edges via BulkLoadHandle (node_chunk_size={}, edge_chunk_size={})...",
            opts.node_chunk_size, opts.edge_chunk_size
        );

        // Build all node specs up front (no aliases in bulk mode).
        let nodes: Vec<TypedNodeSpec> = (0..node_count)
            .map(|idx| TypedNodeSpec {
                label: "Node".to_string(),
                props: vec![
                    make_string_prop("name", format!("fn_{idx}")),
                    make_string_prop("filePath", format!("/tmp/file_{}.ts", idx / 50)),
                    make_int_prop("startLine", idx as i64),
                    make_int_prop("endLine", (idx + 5) as i64),
                    make_string_prop(
                        "codeText",
                        code_texts[idx % code_texts.len()].to_string(),
                    ),
                    make_string_prop("language", "typescript".to_string()),
                    make_string_prop("metadata", metadata[idx % metadata.len()].to_string()),
                ],
                alias: None,
            })
            .collect();

        let mut bulk = db.begin_bulk_load(opts);
        let created = bulk.load_nodes(&nodes).expect("bulk load nodes");
        node_ids = created.iter().map(|id| id.0).collect();

        // Build all edges referencing node IDs directly.
        let edges: Vec<TypedEdgeSpec> = (0..edge_count)
            .map(|i| {
                let src_idx = i % node_count.max(1);
                let dst_idx = (i * 13 + 7) % node_count.max(1);
                TypedEdgeSpec {
                    ty: "LINKS".to_string(),
                    src: TypedNodeRef {
                        kind: "id".to_string(),
                        alias: None,
                        handle: None,
                        id: Some(node_ids[src_idx]),
                    },
                    dst: TypedNodeRef {
                        kind: "id".to_string(),
                        alias: None,
                        handle: None,
                        id: Some(node_ids[dst_idx]),
                    },
                    props: vec![
                        make_float_prop("weight", (i % 10) as f64 / 10.0),
                        make_string_prop(
                            "linkKind", if i % 2 == 0 { "call" } else { "reference" }.to_string(),
                        ),
                    ],
                }
            })
            .collect();

        let _edge_ids = bulk.load_edges(&edges).expect("bulk load edges");
        let _stats = bulk.finish();
    } else {
        let chunk_nodes = chunk_nodes.unwrap_or(node_count);
        let chunk_edges = chunk_edges.unwrap_or(edge_count);

        println!(
            "Creating {node_count} nodes and {edge_count} edges in chunks of {chunk_nodes} nodes / {chunk_edges} edges via typed batches..."
        );

        let mut created_nodes = 0usize;
        let mut created_edges = 0usize;

        while created_nodes < node_count {
            let n = std::cmp::min(chunk_nodes, node_count - created_nodes);
            let e = if created_edges >= edge_count {
                0
            } else {
                std::cmp::min(chunk_edges, edge_count - created_edges)
            };

            let nodes: Vec<TypedNodeSpec> = (0..n)
                .map(|i| {
                    let idx = created_nodes + i;
                    TypedNodeSpec {
                        label: "Node".to_string(),
                        props: vec![
                            make_string_prop("name", format!("fn_{idx}")),
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
                            if i % 2 == 0 { "call" } else { "reference" }.to_string(),
                        ),
                    ],
                })
                .collect();

            let spec = TypedBatchSpec { nodes, edges };
            let result = db
                .create_typed_batch(&spec)
                .expect("create nodes and edges");
            node_ids.extend(result.node_ids.iter().map(|id| id.0));

            created_nodes += n;
            created_edges += e;
        }
    }

    let write_time = write_start.elapsed();
    println!("create total: {:.1} ms", write_time.as_secs_f64() * 1000.0);

    // Random reads using get_node
    println!("Running {read_count} reads...");
    let read_start = Instant::now();

    let mut total_props = 0usize;
    let mut first_20_times = Vec::with_capacity(20);

    if read_mode == "SNAPSHOT_BATCH" {
        // Precompute IDs to keep the access pattern identical.
        let mut read_ids = Vec::with_capacity(read_count);
        for i in 0..read_count {
            let id = node_ids[(i * 17) % node_ids.len()];
            read_ids.push(id);
        }

        let mut idx = 0usize;
        while idx < read_count {
            let end = usize::min(idx + read_batch_size, read_count);
            let batch = &read_ids[idx..end];

            let batch_start = if idx < 20 { Some(Instant::now()) } else { None };
            let nodes = db.get_nodes(batch).expect("get nodes (typed)");

            for (offset, node_opt) in nodes.into_iter().enumerate() {
                let node = node_opt.expect("node exists");
                total_props += node.props.len();

                let global_i = idx + offset;
                if global_i < 20 {
                    if let Some(batch_start) = batch_start {
                        let elapsed = batch_start.elapsed();
                        let per = elapsed / (end - idx) as u32;
                        first_20_times.push(per);
                    }
                }
            }

            idx = end;
        }
    } else if read_mode == "HOT" {
        // Hot read mode: only count properties without materializing values.
        let mut read_ids = Vec::with_capacity(read_count);
        for i in 0..read_count {
            let id = node_ids[(i * 17) % node_ids.len()];
            read_ids.push(id);
        }

        let mut idx = 0usize;
        while idx < read_count {
            let end = usize::min(idx + read_batch_size, read_count);
            let batch = &read_ids[idx..end];

            let batch_start = if idx < 20 { Some(Instant::now()) } else { None };
            let counts = db
                .get_node_prop_counts(batch)
                .expect("get node prop counts (hot)");

            for (offset, count_opt) in counts.into_iter().enumerate() {
                let count = count_opt.expect("node exists");
                total_props += count;

                let global_i = idx + offset;
                if global_i < 20 {
                    if let Some(batch_start) = batch_start {
                        let elapsed = batch_start.elapsed();
                        let per = elapsed / (end - idx) as u32;
                        first_20_times.push(per);
                    }
                }
            }

            idx = end;
        }
    } else if read_mode == "VERY_HOT" {
        // Very hot read mode: only test node existence.
        let mut read_ids = Vec::with_capacity(read_count);
        for i in 0..read_count {
            let id = node_ids[(i * 17) % node_ids.len()];
            read_ids.push(id);
        }

        let mut idx = 0usize;
        while idx < read_count {
            let end = usize::min(idx + read_batch_size, read_count);
            let batch = &read_ids[idx..end];

            let batch_start = if idx < 20 { Some(Instant::now()) } else { None };
            let exists_flags = db
                .nodes_exist(batch)
                .expect("nodes_exist (very hot)");

            for (offset, exists) in exists_flags.into_iter().enumerate() {
                if exists {
                    total_props += 1;
                }

                let global_i = idx + offset;
                if global_i < 20 {
                    if let Some(batch_start) = batch_start {
                        let elapsed = batch_start.elapsed();
                        let per = elapsed / (end - idx) as u32;
                        first_20_times.push(per);
                    }
                }
            }

            idx = end;
        }
    } else {
        for i in 0..read_count {
            let id = node_ids[(i * 17) % node_ids.len()];
            let single_start = Instant::now();
            let node = db
                .get_node_data(id)
                .expect("get node (typed)")
                .expect("node exists");
            total_props += node.props.len(); // Force reading the data
            if i < 20 {
                first_20_times.push(single_start.elapsed());
            }
        }
    }

    let read_time = read_start.elapsed();
    println!(
        "random reads: {:.1} ms (total props read: {})",
        read_time.as_secs_f64() * 1000.0,
        total_props
    );
    println!(
        "First 20 read times (µs): {:?}",
        first_20_times
            .iter()
            .map(|d| d.as_micros())
            .collect::<Vec<_>>()
    );

    let dir_size_bytes = total_dir_size(tmpdir.path()).ok();
    if let Some(bytes) = dir_size_bytes {
        let mb = bytes as f64 / 1_048_576.0;
        println!("Disk usage (db dir): {:.2} MB at {:?}", mb, tmpdir.path());
    }

    println!("\n📊 Benchmark Summary (Rust Core):");
    println!("- Read mode: {read_mode}");
    println!(
        "- Nodes: {} ({:.0} nodes/sec)",
        node_count,
        node_count as f64 / write_time.as_secs_f64()
    );
    println!(
        "- Edges: {} ({:.0} edges/sec)",
        edge_count,
        edge_count as f64 / write_time.as_secs_f64()
    );
    println!(
        "- Reads: {} ({:.1}ms, {:.0} reads/sec)",
        read_count,
        read_time.as_secs_f64() * 1000.0,
        read_count as f64 / read_time.as_secs_f64()
    );
    println!(
        "- Total write time: {:.1}ms",
        write_time.as_secs_f64() * 1000.0
    );

    if std::env::var("KEEP_DB").map(|v| v == "1").unwrap_or(false) {
        println!("Keeping temp db at {:?}", tmpdir.path());
        std::mem::forget(tmpdir);
    }
}
