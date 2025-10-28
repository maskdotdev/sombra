// Minimal test to reproduce page corruption bug

use sombra::db::{Config, GraphDB};
use sombra::model::{Node, PropertyValue};
use tempfile::TempDir;

#[test]
fn test_page_corruption_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("corruption_test.db");
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;
    
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();
    
    // Create initial graph
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties.insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }
    
    db.checkpoint().unwrap();
    eprintln!("Initial checkpoint done");
    
    // Create version chains - this is where corruption happens at depth=5
    for depth in 0..=5 {
        eprintln!("\nCreating version chain depth {}", depth);
        
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("counter".to_string(), PropertyValue::Int(depth as i64));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
        
        eprintln!("Committed version chain depth {}", depth);
    }
    
    eprintln!("\nReading nodes after version chains created (1000 iterations)");
    
    // Try to read - this is where corruption is detected
    // Match the benchmark: 1000 iterations of 100 reads
    for iteration in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            let result = tx.get_node(node_id);
            if result.is_err() {
                eprintln!("ERROR at iteration {} reading node {}: {:?}", iteration, node_id, result);
                panic!("Corruption detected!");
            }
        }
        tx.commit().unwrap();
    }
    
    eprintln!("Test completed successfully - no corruption");
}
