use parking_lot::RwLock;
use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn setup_social_graph(path: &str, node_count: usize, avg_edges: usize) -> (GraphDB, Vec<u64>) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut db = GraphDB::open_with_config(path, Config::balanced()).unwrap();

    let mut node_ids = Vec::new();
    for i in 0..node_count {
        let mut node = Node::new(i as u64);
        node.labels.push("User".to_string());
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    use rand::Rng;
    let mut rng = rand::thread_rng();
    for i in 0..node_count {
        let edge_count = rng.gen_range(avg_edges / 2..avg_edges * 2);
        for _ in 0..edge_count {
            let target_idx = rng.gen_range(0..node_count);
            if target_idx != i {
                db.add_edge(Edge::new(0, node_ids[i], node_ids[target_idx], "follows"))
                    .ok();
            }
        }
    }

    db.checkpoint().unwrap();
    (db, node_ids)
}

fn bench_concurrent_read_only() {
    println!("\n=== Concurrent Read-Only Workload ===");
    
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent_bench.db");
    let (db, node_ids) = setup_social_graph(path.to_str().unwrap(), 1000, 20);
    
    let db = Arc::new(RwLock::new(db));
    let node_ids = Arc::new(node_ids);
    
    for num_threads in [1, 2, 4, 8] {
        let operations_per_thread = 1000;
        let mut handles = vec![];
        
        let start = Instant::now();
        
        for _ in 0..num_threads {
            let db = db.clone();
            let node_ids = node_ids.clone();
            
            let handle = thread::spawn(move || {
                let mut local_ops = 0;
                for _ in 0..operations_per_thread {
                    let mut db = db.write();
                    let idx = local_ops % node_ids.len();
                    let node_id = node_ids[idx];
                    let _ = db.get_neighbors(node_id);
                    local_ops += 1;
                }
                local_ops
            });
            
            handles.push(handle);
        }
        
        let mut total_ops = 0;
        for handle in handles {
            total_ops += handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        
        println!(
            "  {} threads: {:.2}ms total, {:.0} ops/sec, {:.3}ms avg latency",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            ops_per_sec,
            duration.as_secs_f64() * 1000.0 / total_ops as f64
        );
    }
    
    let db = db.write();
    println!("\nConcurrency Metrics:");
    db.concurrency_metrics.print_report();
}

fn bench_mixed_read_write() {
    println!("\n=== Mixed Read/Write Workload (80% reads, 20% writes) ===");
    
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mixed_bench.db");
    let (db, node_ids) = setup_social_graph(path.to_str().unwrap(), 500, 15);
    
    let db = Arc::new(RwLock::new(db));
    let node_ids = Arc::new(node_ids);
    
    for num_threads in [1, 2, 4, 8] {
        let operations_per_thread = 500;
        let mut handles = vec![];
        
        let start = Instant::now();
        
        for thread_id in 0..num_threads {
            let db = db.clone();
            let node_ids = node_ids.clone();
            
            let handle = thread::spawn(move || {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let mut read_ops = 0;
                let mut write_ops = 0;
                
                for _ in 0..operations_per_thread {
                    let is_read = rng.gen_bool(0.8);
                    
                    if is_read {
                        let mut db = db.write();
                        let idx = rng.gen_range(0..node_ids.len());
                        let node_id = node_ids[idx];
                        let _ = db.get_neighbors(node_id);
                        read_ops += 1;
                    } else {
                        let mut db = db.write();
                        let source_idx = rng.gen_range(0..node_ids.len());
                        let target_idx = rng.gen_range(0..node_ids.len());
                        if source_idx != target_idx {
                            let edge_id = thread_id as u64 * 1000000 + write_ops;
                            let _ = db.add_edge(Edge::new(
                                edge_id,
                                node_ids[source_idx],
                                node_ids[target_idx],
                                "new_edge",
                            ));
                        }
                        write_ops += 1;
                    }
                }
                (read_ops, write_ops)
            });
            
            handles.push(handle);
        }
        
        let mut total_reads = 0;
        let mut total_writes = 0;
        for handle in handles {
            let (reads, writes) = handle.join().unwrap();
            total_reads += reads;
            total_writes += writes;
        }
        
        let duration = start.elapsed();
        let total_ops = total_reads + total_writes;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        
        println!(
            "  {} threads: {:.2}ms total, {} reads, {} writes, {:.0} ops/sec",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            total_reads,
            total_writes,
            ops_per_sec
        );
    }
    
    let db = db.write();
    println!("\nConcurrency Metrics:");
    db.concurrency_metrics.print_report();
}

fn bench_parallel_traversal() {
    println!("\n=== Parallel Traversal Performance ===");
    
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("traversal_bench.db");
    let (mut db, node_ids) = setup_social_graph(path.to_str().unwrap(), 2000, 25);
    
    let start_node = node_ids[0];
    
    println!("\n  Multi-hop Neighbors (sequential vs parallel):");
    
    let sequential_start = Instant::now();
    for _ in 0..10 {
        let _ = db.get_neighbors_two_hops(start_node);
    }
    let sequential_time = sequential_start.elapsed();
    
    let parallel_start = Instant::now();
    for _ in 0..10 {
        let _ = db.parallel_multi_hop_neighbors(&[start_node], 2);
    }
    let parallel_time = parallel_start.elapsed();
    
    let speedup = sequential_time.as_secs_f64() / parallel_time.as_secs_f64();
    
    println!(
        "    Sequential: {:.2}ms per op",
        sequential_time.as_secs_f64() * 1000.0 / 10.0
    );
    println!(
        "    Parallel:   {:.2}ms per op",
        parallel_time.as_secs_f64() * 1000.0 / 10.0
    );
    println!("    Speedup:    {:.2}x", speedup);
    
    println!("\n  Multi-hop Batch Query (50 nodes, 2 hops):");
    let query_nodes: Vec<u64> = node_ids.iter().take(50).copied().collect();
    
    let sequential_start = Instant::now();
    for _ in 0..10 {
        for &node_id in &query_nodes {
            let _ = db.get_neighbors_two_hops(node_id);
        }
    }
    let sequential_time = sequential_start.elapsed();
    
    let parallel_start = Instant::now();
    for _ in 0..10 {
        let _ = db.parallel_multi_hop_neighbors(&query_nodes, 2);
    }
    let parallel_time = parallel_start.elapsed();
    
    let speedup = sequential_time.as_secs_f64() / parallel_time.as_secs_f64();
    
    println!(
        "    Sequential: {:.2}ms per batch",
        sequential_time.as_secs_f64() * 1000.0 / 10.0
    );
    println!(
        "    Parallel:   {:.2}ms per batch",
        parallel_time.as_secs_f64() * 1000.0 / 10.0
    );
    println!("    Speedup:    {:.2}x", speedup);
    
    println!("\nConcurrency Metrics:");
    db.concurrency_metrics.print_report();
}

fn bench_lock_contention() {
    println!("\n=== Lock Contention Analysis ===");
    
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("contention_bench.db");
    let (db, node_ids) = setup_social_graph(path.to_str().unwrap(), 100, 10);
    
    let db = Arc::new(RwLock::new(db));
    let node_ids = Arc::new(node_ids);
    
    println!("\n  Hot Node Contention (all threads access same node):");
    for num_threads in [1, 2, 4, 8, 16] {
        let hot_node = node_ids[0];
        let operations_per_thread = 1000;
        let mut handles = vec![];
        
        db.read().concurrency_metrics.reset();
        
        let start = Instant::now();
        
        for _ in 0..num_threads {
            let db = db.clone();
            
            let handle = thread::spawn(move || {
                for _ in 0..operations_per_thread {
                    let lock_start = Instant::now();
                    let mut db_guard = db.write();
                    let lock_time = lock_start.elapsed();
                    
                    db_guard.concurrency_metrics.record_writer_wait(lock_time.as_nanos() as u64);
                    let _ = db_guard.get_neighbors(hot_node);
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = num_threads * operations_per_thread;
        
        let db_guard = db.write();
        let avg_wait_us = db_guard.concurrency_metrics.get_avg_writer_wait_us();
        
        println!(
            "    {} threads: {:.2}ms total, {:.0} ops/sec, {:.2}μs avg wait",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            total_ops as f64 / duration.as_secs_f64(),
            avg_wait_us
        );
    }
    
    println!("\n  Distributed Access (threads access different nodes):");
    for num_threads in [1, 2, 4, 8, 16] {
        let operations_per_thread = 1000;
        let mut handles = vec![];
        
        db.read().concurrency_metrics.reset();
        
        let start = Instant::now();
        
        for thread_id in 0..num_threads {
            let db = db.clone();
            let node_ids = node_ids.clone();
            
            let handle = thread::spawn(move || {
                let base_idx = thread_id * (node_ids.len() / num_threads.max(1));
                for i in 0..operations_per_thread {
                    let lock_start = Instant::now();
                    let mut db_guard = db.write();
                    let lock_time = lock_start.elapsed();
                    
                    db_guard.concurrency_metrics.record_writer_wait(lock_time.as_nanos() as u64);
                    let idx = (base_idx + i) % node_ids.len();
                    let _ = db_guard.get_neighbors(node_ids[idx]);
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = num_threads * operations_per_thread;
        
        let db_guard = db.write();
        let avg_wait_us = db_guard.concurrency_metrics.get_avg_writer_wait_us();
        
        println!(
            "    {} threads: {:.2}ms total, {:.0} ops/sec, {:.2}μs avg wait",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            total_ops as f64 / duration.as_secs_f64(),
            avg_wait_us
        );
    }
}

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║         Sombra Concurrency Performance Benchmark          ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    
    bench_concurrent_read_only();
    bench_mixed_read_write();
    bench_parallel_traversal();
    bench_lock_contention();
    
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
