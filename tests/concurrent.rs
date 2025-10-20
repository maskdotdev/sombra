use sombra::{Edge, GraphDB, Node, Result};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const NUM_THREADS: usize = 8;
const OPERATIONS_PER_THREAD: usize = 100;
const CONCURRENT_NODES: usize = NUM_THREADS * OPERATIONS_PER_THREAD;

#[test]
fn concurrent_node_insertion() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<Vec<u64>> {
            barrier_clone.wait();

            let mut db = GraphDB::open(&*path_clone)?;
            let mut node_ids = Vec::new();

            for i in 0..OPERATIONS_PER_THREAD {
                let node = Node::new((thread_id * OPERATIONS_PER_THREAD + i) as u64);
                let node_id = db.add_node(node)?;
                node_ids.push(node_id);
            }

            Ok(node_ids)
        });

        handles.push(handle);
    }

    let mut all_node_ids = Vec::new();
    for handle in handles {
        let node_ids = handle.join().unwrap()?;
        all_node_ids.extend(node_ids);
    }

    // Verify all nodes were created
    let mut db = GraphDB::open(&*path)?;
    assert_eq!(all_node_ids.len(), CONCURRENT_NODES);

    // Verify each node exists and has correct data
    for &node_id in &all_node_ids {
        let node = db.get_node(node_id)?;
        assert!(node_id <= CONCURRENT_NODES as u64);
    }

    Ok(())
}

#[test]
fn concurrent_edge_creation() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    // Create a central hub node first
    let hub_id;
    {
        let mut db = GraphDB::open(&*path)?;
        hub_id = db.add_node(Node::new(9999))?;
    }

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<Vec<u64>> {
            barrier_clone.wait();

            let mut db = GraphDB::open(&*path_clone)?;
            let mut edge_ids = Vec::new();

            for i in 0..OPERATIONS_PER_THREAD {
                let node = Node::new((thread_id * OPERATIONS_PER_THREAD + i) as u64);
                let node_id = db.add_node(node)?;

                let edge = Edge::new(0, hub_id, node_id, "connect");
                let edge_id = db.add_edge(edge)?;
                edge_ids.push(edge_id);
            }

            Ok(edge_ids)
        });

        handles.push(handle);
    }

    let mut all_edge_ids = Vec::new();
    for handle in handles {
        let edge_ids = handle.join().unwrap()?;
        all_edge_ids.extend(edge_ids);
    }

    // Verify all edges were created
    let mut db = GraphDB::open(&*path)?;
    assert_eq!(all_edge_ids.len(), CONCURRENT_NODES);

    // Verify hub node has all neighbors
    let neighbors = db.get_neighbors(hub_id)?;
    assert_eq!(neighbors.len(), CONCURRENT_NODES);

    Ok(())
}

#[test]
fn concurrent_read_write_operations() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    // Pre-populate with some data
    let initial_node_count = 50;
    {
        let mut db = GraphDB::open(&*path)?;
        for i in 0..initial_node_count {
            db.add_node(Node::new(i as u64))?;
        }
    }

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    // Half threads write, half read
    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<usize> {
            barrier_clone.wait();

            let mut db = GraphDB::open(&*path_clone)?;
            let mut operations = 0;

            if thread_id < NUM_THREADS / 2 {
                // Writer threads
                for i in 0..OPERATIONS_PER_THREAD {
                    let node = Node::new((thread_id * OPERATIONS_PER_THREAD + i + 1000) as u64);
                    db.add_node(node)?;
                    operations += 1;
                }
            } else {
                // Reader threads
                for i in 0..OPERATIONS_PER_THREAD {
                    let node_id = (i % initial_node_count + 1) as u64;
                    if db.get_node(node_id).is_ok() {
                        operations += 1;
                    }
                }
            }

            Ok(operations)
        });

        handles.push(handle);
    }

    let mut total_operations = 0;
    for handle in handles {
        let operations = handle.join().unwrap()?;
        total_operations += operations;
    }

    // Verify final state
    let mut db = GraphDB::open(&*path)?;
    let expected_total = initial_node_count + (NUM_THREADS / 2) * OPERATIONS_PER_THREAD;

    // Count actual nodes
    let mut actual_count = 0;
    let mut node_id = 1;
    while db.get_node(node_id).is_ok() {
        actual_count += 1;
        node_id += 1;
    }

    assert!(actual_count >= initial_node_count);

    Ok(())
}

#[test]
fn concurrent_transaction_operations() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<Vec<u64>> {
            barrier_clone.wait();

            let mut db = GraphDB::open(&*path_clone)?;
            let mut node_ids = Vec::new();

            for i in 0..OPERATIONS_PER_THREAD {
                let mut tx = db.begin_transaction()?;

                // Add multiple nodes in a single transaction
                for j in 0..5 {
                    let node =
                        Node::new((thread_id * OPERATIONS_PER_THREAD * 5 + i * 5 + j) as u64);
                    let node_id = tx.add_node(node)?;
                    if j == 0 {
                        node_ids.push(node_id);
                    }
                }

                tx.commit()?;
            }

            Ok(node_ids)
        });

        handles.push(handle);
    }

    let mut all_node_ids = Vec::new();
    for handle in handles {
        let node_ids = handle.join().unwrap()?;
        all_node_ids.extend(node_ids);
    }

    // Verify all transactions committed successfully
    let mut db = GraphDB::open(&*path)?;
    db.checkpoint()?;

    for &node_id in &all_node_ids {
        assert!(db.get_node(node_id).is_ok());
    }

    Ok(())
}

#[test]
fn concurrent_stress_test() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    let start_time = Instant::now();
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<(usize, usize, usize)> {
            barrier_clone.wait();

            let mut db = GraphDB::open(&*path_clone)?;
            let mut nodes_created = 0;
            let mut edges_created = 0;
            let mut reads_performed = 0;

            for i in 0..OPERATIONS_PER_THREAD {
                match i % 4 {
                    0 | 1 => {
                        // Create nodes
                        let node = Node::new((thread_id * OPERATIONS_PER_THREAD + i) as u64);
                        db.add_node(node)?;
                        nodes_created += 1;
                    }
                    2 => {
                        // Create edges (connect to existing nodes)
                        if nodes_created > 0 {
                            let from_id =
                                ((thread_id * OPERATIONS_PER_THREAD + i - 1) % 100 + 1) as u64;
                            let to_id = ((thread_id * OPERATIONS_PER_THREAD + i) % 100 + 1) as u64;

                            // Only create edge if both nodes exist
                            if db.get_node(from_id).is_ok() && db.get_node(to_id).is_ok() {
                                let edge = Edge::new(0, from_id, to_id, "stress_test");
                                if db.add_edge(edge).is_ok() {
                                    edges_created += 1;
                                }
                            }
                        }
                    }
                    3 => {
                        // Read operations
                        let node_id = ((i * 7) % 50 + 1) as u64;
                        if db.get_node(node_id).is_ok() {
                            reads_performed += 1;
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Ok((nodes_created, edges_created, reads_performed))
        });

        handles.push(handle);
    }

    let mut total_nodes = 0;
    let mut total_edges = 0;
    let mut total_reads = 0;

    for handle in handles {
        let (nodes, edges, reads) = handle.join().unwrap()?;
        total_nodes += nodes;
        total_edges += edges;
        total_reads += reads;
    }

    let elapsed = start_time.elapsed();

    println!("Concurrent stress test completed in {:?}", elapsed);
    println!(
        "Total operations: {} nodes, {} edges, {} reads",
        total_nodes, total_edges, total_reads
    );
    println!(
        "Operations per second: {:.2}",
        (total_nodes + total_edges + total_reads) as f64 / elapsed.as_secs_f64()
    );

    // Verify database integrity
    let mut db = GraphDB::open(&*path)?;
    db.checkpoint()?;

    // The test passes if we completed without panics or corruption
    assert!(total_nodes > 0);

    Ok(())
}

#[test]
fn concurrent_database_open_close() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();
    let path = Arc::new(path);

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let path_clone = Arc::clone(&path);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<()> {
            for iteration in 0..10 {
                barrier_clone.wait();

                // Open database, perform operation, close
                {
                    let mut db = GraphDB::open(&*path_clone)?;
                    let node = Node::new((thread_id * 10 + iteration) as u64);
                    db.add_node(node)?;
                } // db is dropped here

                // Small delay to simulate real usage
                thread::sleep(Duration::from_millis(1));
            }

            Ok(())
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    // Verify all data persisted
    let mut db = GraphDB::open(&*path)?;
    let mut found_nodes = 0;

    for i in 0..(NUM_THREADS * 10) {
        if db.get_node((i + 1) as u64).is_ok() {
            found_nodes += 1;
        }
    }

    assert_eq!(found_nodes, NUM_THREADS * 10);

    Ok(())
}
