use sombra::{Config, GraphDB, Node, PropertyValue};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("monitor_db");
    
    let mut db = GraphDB::open_with_config(&db_path, Config::default())?;
    
    println!("Starting metrics monitoring demo...\n");
    
    for i in 0..10 {
        let mut tx = db.begin_transaction()?;
        let mut props = HashMap::new();
        props.insert("name".to_string(), PropertyValue::String(format!("Node{}", i)));
        props.insert("value".to_string(), PropertyValue::Int(i as i64));
        let node = Node {
            id: 0,
            label: "TestNode".to_string(),
            properties: props,
        };
        tx.add_node(node)?;
        tx.commit()?;
        
        thread::sleep(Duration::from_millis(100));
    }
    
    println!("=== Metrics Report ===");
    db.metrics.print_report();
    
    println!("\n=== Health Check ===");
    let health = db.health_check()?;
    println!("Status: {:?}", health.status);
    for check in &health.checks {
        println!("  {:?}", check);
    }
    
    println!("\n=== Prometheus Format ===");
    println!("{}", db.metrics.to_prometheus_format());
    
    println!("\n=== JSON Format ===");
    println!("{}", db.metrics.to_json()?);
    
    println!("\n=== StatsD Format ===");
    for metric in db.metrics.to_statsd("sombra") {
        println!("{}", metric);
    }
    
    Ok(())
}
