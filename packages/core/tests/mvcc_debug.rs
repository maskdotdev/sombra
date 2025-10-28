//! Debug test to understand how add_node works with MVCC

use sombra::{Config, GraphDB, Node, PropertyValue};

#[test]
fn debug_add_node_behavior() {
    let path = "test_mvcc_debug.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
    
    let mut config = Config::default();
    config.mvcc_enabled = true;
    let mut db = GraphDB::open_with_config(path, config).unwrap();
    
    // Create initial node
    let node_id = {
        let mut tx1 = db.begin_transaction().unwrap();
        println!("TX1 snapshot_ts: {}", tx1.snapshot_ts());
        let mut node = Node::new(1);
        node.properties.insert("version".to_string(), PropertyValue::Int(1));
        let id = tx1.add_node(node).unwrap();
        println!("TX1 added node {}", id);
        tx1.commit().unwrap();
        id
    };
    
    // Try to update it by calling add_node again
    {
        let mut tx2 = db.begin_transaction().unwrap();
        println!("\nTX2 snapshot_ts: {}", tx2.snapshot_ts());
        
        // Read existing node
        let mut node = tx2.get_node(node_id).unwrap().unwrap();
        println!("TX2 read node {:?} with version={:?}", node_id, node.properties.get("version"));
        
        // Update it
        node.properties.insert("version".to_string(), PropertyValue::Int(2));
        
        // Add it again - does this create a new version or overwrite?
        let id2 = tx2.add_node(node).unwrap();
        println!("TX2 add_node returned id {}", id2);
        
        tx2.commit().unwrap();
    }
    
    // Read in new transaction
    {
        let mut tx3 = db.begin_transaction().unwrap();
        println!("\nTX3 snapshot_ts: {}", tx3.snapshot_ts());
        let node = tx3.get_node(node_id).unwrap().unwrap();
        println!("TX3 read node {:?} with version={:?}", node_id, node.properties.get("version"));
        
        // What version do we see?
        if let Some(PropertyValue::Int(v)) = node.properties.get("version") {
            println!("Found version: {}", v);
            // This will fail if MVCC isn't working
            // assert_eq!(*v, 2, "Should see updated version");
        }
        
        tx3.commit().unwrap();
    }
    
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
}
