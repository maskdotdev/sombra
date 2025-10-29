#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use rand::{rngs::StdRng, Rng, SeedableRng};
use sombra::{ConcurrentGraphDB, Config, Edge, Node, PropertyValue};
use std::cmp::min;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct StressStats {
    target_runtime: Duration,
    actual_runtime: Duration,
    threads: usize,
    total_transactions: u64,
    write_transactions: u64,
    read_transactions: u64,
    read_misses: u64,
    edges_created: u64,
    max_node_id: u64,
}

fn run_concurrency_stress(duration: Duration) -> StressStats {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("concurrency_stress.db");

    let mut config = Config::balanced();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(512);

    let db = ConcurrentGraphDB::open_with_config(&path, config)
        .expect("concurrent graph database should open");

    // Seed the database with a baseline graph that readers and writers can share.
    let mut seed_tx = db
        .begin_transaction()
        .expect("seed transaction should start");
    let mut highest_seed_id = 0u64;
    for i in 0..256 {
        let mut node = Node::new(0);
        node.labels.push("Seed".to_string());
        node.properties.insert(
            "seed_index".to_string(),
            PropertyValue::Int(i as i64),
        );
        let node_id = seed_tx.add_node(node).expect("seed node insert");
        highest_seed_id = highest_seed_id.max(node_id);
    }
    seed_tx.commit().expect("seed transaction commit");

    let max_node_id = Arc::new(AtomicU64::new(highest_seed_id));
    let total_transactions = Arc::new(AtomicU64::new(0));
    let write_transactions = Arc::new(AtomicU64::new(0));
    let read_transactions = Arc::new(AtomicU64::new(0));
    let read_misses = Arc::new(AtomicU64::new(0));
    let edges_created = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let deadline = start + duration;
    let thread_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .map(|n| n.clamp(4, 32))
        .unwrap_or(8);

    std::thread::scope(|scope| {
        for thread_index in 0..thread_count {
            let db = db.clone();
            let max_node_id = Arc::clone(&max_node_id);
            let total_transactions = Arc::clone(&total_transactions);
            let write_transactions = Arc::clone(&write_transactions);
            let read_transactions = Arc::clone(&read_transactions);
            let read_misses = Arc::clone(&read_misses);
            let edges_created = Arc::clone(&edges_created);
            let deadline = deadline;

            scope.spawn(move || {
                let mut rng =
                    StdRng::seed_from_u64((thread_index as u64 + 1).wrapping_mul(0x9E3779B97F4A7C15));
                let mut local_sequence: u64 = 0;

                while Instant::now() < deadline {
                    let choose_write = rng.gen_ratio(3, 5); // ~60% write, 40% read

                    if choose_write {
                        let mut tx = db.begin_transaction().expect("write tx start");

                        let mut node = Node::new(0);
                        node.labels.push("ConcurrentNode".to_string());
                        node.properties.insert(
                            "thread".to_string(),
                            PropertyValue::Int(thread_index as i64),
                        );
                        node.properties.insert(
                            "seq".to_string(),
                            PropertyValue::Int(local_sequence as i64),
                        );
                        node.properties.insert(
                            "payload".to_string(),
                            PropertyValue::String(format!(
                                "payload-{}-{}",
                                thread_index, local_sequence
                            )),
                        );

                        let node_id = tx.add_node(node).expect("write node insert");

                        let max_id_snapshot = max_node_id.load(Ordering::Relaxed);
                        if max_id_snapshot > 0 {
                            let target_id = rng.gen_range(1..=max_id_snapshot);
                            if let Ok(Some(_)) = tx.get_node(target_id) {
                                let edge = Edge::new(0, target_id, node_id, "CONCURRENT_LOAD");
                                if tx.add_edge(edge).is_ok() {
                                    edges_created.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                read_misses.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        tx.commit().expect("write tx commit");
                        max_node_id.fetch_max(node_id, Ordering::Relaxed);
                        write_transactions.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let mut tx = db.begin_transaction().expect("read tx start");
                        let max_id_snapshot = max_node_id.load(Ordering::Relaxed);

                        if max_id_snapshot > 0 {
                            let attempts = min(8, max_id_snapshot as usize);
                            let mut observed = false;
                            for _ in 0..attempts {
                                let candidate = rng.gen_range(1..=max_id_snapshot);
                                match tx.get_node(candidate) {
                                    Ok(Some(_)) => {
                                        observed = true;
                                        break;
                                    }
                                    Ok(None) => {
                                        read_misses.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        read_misses.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }

                            if !observed {
                                // Fall back to checking the first seed node to exercise the hot path.
                                let _ = tx.get_node(1);
                            }
                        }

                        tx.commit().expect("read tx commit");
                        read_transactions.fetch_add(1, Ordering::Relaxed);
                    }

                    total_transactions.fetch_add(1, Ordering::Relaxed);
                    local_sequence = local_sequence.wrapping_add(1);
                }
            });
        }
    });

    let actual_runtime = start.elapsed();

    // Spot check a handful of nodes to ensure the database is still responding.
    let verification_max = max_node_id.load(Ordering::Relaxed);
    if verification_max > 0 {
        let mut verify_tx = db.begin_transaction().expect("verification tx start");
        for id in 1..=min(verification_max, 16) {
            let _ = verify_tx
                .get_node(id)
                .expect("verification reads should not fail");
        }
        verify_tx.commit().expect("verification commit");
    }

    StressStats {
        target_runtime: duration,
        actual_runtime,
        threads: thread_count,
        total_transactions: total_transactions.load(Ordering::Relaxed),
        write_transactions: write_transactions.load(Ordering::Relaxed),
        read_transactions: read_transactions.load(Ordering::Relaxed),
        read_misses: read_misses.load(Ordering::Relaxed),
        edges_created: edges_created.load(Ordering::Relaxed),
        max_node_id: max_node_id.load(Ordering::Relaxed),
    }
}

fn assert_stats(stats: &StressStats) {
    assert!(
        stats.total_transactions > 0,
        "stress test should execute at least one transaction"
    );
    assert_eq!(
        stats.total_transactions,
        stats.read_transactions + stats.write_transactions,
        "transaction accounting mismatch: {stats:?}"
    );
}

#[test]
#[ignore]
fn concurrency_stress_1_minute() {
    let stats = run_concurrency_stress(Duration::from_secs(60));
    println!("1-minute concurrency stress stats: {stats:?}");
    assert_stats(&stats);
}

#[test]
#[ignore]
fn concurrency_stress_3_minutes() {
    let stats = run_concurrency_stress(Duration::from_secs(3 * 60));
    println!("3-minute concurrency stress stats: {stats:?}");
    assert_stats(&stats);
}

#[test]
#[ignore]
fn concurrency_stress_5_minutes() {
    let stats = run_concurrency_stress(Duration::from_secs(5 * 60));
    println!("5-minute concurrency stress stats: {stats:?}");
    assert_stats(&stats);
}
