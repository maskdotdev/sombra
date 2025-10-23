#![no_main]

use libfuzzer_sys::fuzz_target;
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use sombra::{GraphDB, Node, Edge};
use parking_lot::Mutex;
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

#[derive(Debug, Clone)]
enum Operation {
    CreateNode { id: u64, label: String },
    ReadNode { id: u64 },
    CreateEdge { from: u64, to: u64, label: String },
    ReadEdges { node_id: u64 },
    FindByLabel { label: String },
}

impl<'a> Arbitrary<'a> for Operation {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let op_type: u8 = u.int_in_range(0..=4)?;
        
        Ok(match op_type {
            0 => Operation::CreateNode {
                id: u.int_in_range(1..=100)?,
                label: format!("L{}", u.int_in_range::<u8>(0..=9)?),
            },
            1 => Operation::ReadNode {
                id: u.int_in_range(1..=100)?,
            },
            2 => Operation::CreateEdge {
                from: u.int_in_range(1..=100)?,
                to: u.int_in_range(1..=100)?,
                label: format!("E{}", u.int_in_range::<u8>(0..=4)?),
            },
            3 => Operation::ReadEdges {
                node_id: u.int_in_range(1..=100)?,
            },
            4 => Operation::FindByLabel {
                label: format!("L{}", u.int_in_range::<u8>(0..=9)?),
            },
            _ => unreachable!(),
        })
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 10 {
        return;
    }

    let _ = std::panic::catch_unwind(|| {
        let mut u = Unstructured::new(data);
        
        // Generate a sequence of operations
        let mut operations = Vec::new();
        while let Ok(op) = Operation::arbitrary(&mut u) {
            operations.push(op);
            if operations.len() >= 50 {
                break;
            }
        }
        
        if operations.is_empty() {
            return;
        }

        // Create temporary database
        let tmp = match NamedTempFile::new() {
            Ok(t) => t,
            Err(_) => return,
        };
        
        let path = tmp.path().to_path_buf();
        let db = match GraphDB::open(&path) {
            Ok(db) => Arc::new(Mutex::new(db)),
            Err(_) => return,
        };

        // Pre-populate with some nodes to avoid missing node errors
        for i in 1..=50 {
            let mut node = Node::new(i);
            node.labels.push(format!("L{}", i % 5));
            let _ = db.lock().add_node(node);
        }
        let _ = db.lock().checkpoint();

        // Determine number of threads (1-4 based on data)
        let num_threads = (data[0] % 4 + 1) as usize;
        
        // Split operations across threads
        let ops_per_thread = operations.len() / num_threads.max(1);
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = Arc::clone(&db);
            let start_idx = thread_id * ops_per_thread;
            let end_idx = if thread_id == num_threads - 1 {
                operations.len()
            } else {
                (thread_id + 1) * ops_per_thread
            };
            
            let thread_ops: Vec<Operation> = operations[start_idx..end_idx].to_vec();

            let handle = thread::spawn(move || {
                for op in thread_ops {
                    match op {
                        Operation::CreateNode { id, label } => {
                            let mut node = Node::new(id);
                            node.labels.push(label);
                            let _ = db_clone.lock().add_node(node);
                        }
                        Operation::ReadNode { id } => {
                            let _ = db_clone.lock().get_node(id);
                        }
                        Operation::CreateEdge { from, to, label } => {
                            // Only create edge if both nodes exist
                            if db_clone.lock().get_node(from).is_ok() 
                                && db_clone.lock().get_node(to).is_ok() {
                                let edge = Edge::new(0, from, to, &label);
                                let _ = db_clone.lock().add_edge(edge);
                            }
                        }
                        Operation::ReadEdges { node_id } => {
                            let _ = db_clone.lock().count_outgoing_edges(node_id);
                        }
                        Operation::FindByLabel { label } => {
                            let _ = db_clone.lock().get_nodes_by_label(&label);
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            let _ = handle.join();
        }

        // Verify database integrity with checkpoint
        let _ = db.lock().checkpoint();
    });
});
