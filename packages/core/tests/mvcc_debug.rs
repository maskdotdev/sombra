//! Debug test for MVCC snapshot visibility issue

use sombra::{Config, GraphDB, Node, PropertyValue};
use std::fs;

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_simple_snapshot_isolation() {
    let path = "test_simple_snapshot.db";
    cleanup_test_db(path);
    
    let mut config = Config::default();
    config.mvcc_enabled = true;
    let db = GraphDB::open_with_config(path, config).unwrap();

    // Transaction 1: Create node with value=100
    let node_id = {
        let mut tx1 = db.begin_transaction().unwrap();
        let snapshot1 = tx1.snapshot_ts();
        println!("TX1: snapshot_ts={}", snapshot1);
        
        let mut node = Node::new(0);
        node.properties.insert("value".to_string(), PropertyValue::Int(100));
        let id = tx1.add_node(node).unwrap();
        println!("TX1: Created node {} with value=100", id);
        
        tx1.commit().unwrap();
        println!("TX1: Committed");
        id
    };

    // Transaction 2: Start BEFORE update (snapshot at T2)
    let mut tx2 = db.begin_transaction().unwrap();
    let snapshot2 = tx2.snapshot_ts();
    println!("\nTX2: snapshot_ts={}", snapshot2);
    
    // Transaction 3: Update node to value=200
    {
        let mut tx3 = db.begin_transaction().unwrap();
        let snapshot3 = tx3.snapshot_ts();
        println!("\nTX3: snapshot_ts={}", snapshot3);
        
        let mut node = tx3.get_node(node_id).unwrap().unwrap();
        println!("TX3: Read node {}, value={:?}", node_id, node.properties.get("value"));
        
        node.properties.insert("value".to_string(), PropertyValue::Int(200));
        let returned_id = tx3.add_node(node).unwrap();
        println!("TX3: Updated node {} to value=200 (returned_id={})", node_id, returned_id);
        
        tx3.commit().unwrap();
        println!("TX3: Committed");
    }

    // Now TX2 tries to read - should see value=100 (snapshot isolation)
    println!("\nTX2: Attempting to read node {}", node_id);
    let result = tx2.get_node(node_id);
    
    match result {
        Ok(Some(node)) => {
            println!("TX2: Successfully read node, value={:?}", node.properties.get("value"));
            assert_eq!(
                node.properties.get("value"),
                Some(&PropertyValue::Int(100)),
                "TX2 should see old value (100) due to snapshot isolation"
            );
        }
        Ok(None) => {
            panic!("TX2: Node not found (shouldn't happen!)");
        }
        Err(e) => {
            panic!("TX2: Error reading node: {:?}", e);
        }
    }

    tx2.commit().unwrap();
    println!("TX2: Committed");

    cleanup_test_db(path);
}
