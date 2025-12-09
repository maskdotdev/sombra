//! Realistic benchmark comparing Rust core vs Node.js bindings
//! Run with: cargo run --release --bin realistic_bench

use sombra::ffi::{Database, DatabaseOptions};
use sombra::primitives::pager::Synchronous;
use std::time::Instant;
use tempfile::TempDir;

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

    let tmpdir = TempDir::new().expect("tempdir");
    let db_path = tmpdir.path().join("bench.sombra");
    println!("📂 temp db: {:?}", db_path);

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
        r#"{"type": "function", "exported": true}"#,
        r#"{"type": "variable", "exported": false}"#,
        r#"{"type": "class", "exported": true}"#,
    ];

    // Create nodes AND edges in a single transaction (like Node.js benchmark)
    // Using create_json which accepts handles for edges
    println!("Creating {} nodes and {} edges in single transaction...", node_count, edge_count);
    let write_start = Instant::now();
    
    // Build JSON spec like Node.js does
    let nodes: Vec<serde_json::Value> = (0..node_count)
        .map(|i| {
            serde_json::json!({
                "labels": ["Node"],
                "props": {
                    "name": format!("fn_{}", i),
                    "filePath": format!("/tmp/file_{}.ts", i / 50),
                    "startLine": i,
                    "endLine": i + 5,
                    "codeText": code_texts[i % code_texts.len()],
                    "language": "typescript",
                    "metadata": metadata[i % metadata.len()],
                }
            })
        })
        .collect();
    
    let edges: Vec<serde_json::Value> = (0..edge_count)
        .map(|i| {
            let src_handle = i % node_count;
            let dst_handle = (i * 13 + 7) % node_count;
            serde_json::json!({
                "src": { "kind": "handle", "index": src_handle },
                "ty": "LINKS",
                "dst": { "kind": "handle", "index": dst_handle },
                "props": {
                    "weight": (i % 10) as f64 / 10.0,
                    "linkKind": if i % 2 == 0 { "call" } else { "reference" },
                }
            })
        })
        .collect();
    
    let spec = serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    });
    
    let result = db.create_json(&spec).expect("create nodes and edges");
    let node_ids: Vec<u64> = result["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .map(|v| v.as_u64().expect("node id"))
        .collect();
    
    let write_time = write_start.elapsed();
    println!("create total: {:.1} ms", write_time.as_secs_f64() * 1000.0);

    // Random reads using get_node
    println!("Running {} reads...", read_count);
    let read_start = Instant::now();
    
    let mut total_props = 0usize;
    let mut first_20_times = Vec::new();
    for i in 0..read_count {
        let id = node_ids[(i * 17) % node_ids.len()];
        let single_start = Instant::now();
        let node = db.get_node_record(id).expect("get node").expect("node exists");
        total_props += node.properties.len(); // Force reading the data
        if i < 20 {
            first_20_times.push(single_start.elapsed());
        }
    }
    
    let read_time = read_start.elapsed();
    println!("random reads: {:.1} ms (total props read: {})", read_time.as_secs_f64() * 1000.0, total_props);
    println!("First 20 read times (µs): {:?}", first_20_times.iter().map(|d| d.as_micros()).collect::<Vec<_>>());

    println!("\n📊 Benchmark Summary (Rust Core):");
    println!("- Nodes: {} ({:.0} nodes/sec)", node_count, node_count as f64 / write_time.as_secs_f64());
    println!("- Edges: {} ({:.0} edges/sec)", edge_count, edge_count as f64 / write_time.as_secs_f64());
    println!("- Reads: {} ({:.1}ms, {:.0} reads/sec)", read_count, read_time.as_secs_f64() * 1000.0, read_count as f64 / read_time.as_secs_f64());
    println!("- Total write time: {:.1}ms", write_time.as_secs_f64() * 1000.0);
}
