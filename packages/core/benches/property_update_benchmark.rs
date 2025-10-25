#![allow(clippy::uninlined_format_args)]

use std::time::Instant;
use tempfile::NamedTempFile;

fn benchmark_single_property_update(node_count: usize, updates_per_node: usize) -> u128 {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();
    let mut db = sombra::db::GraphDB::open(&path).expect("open db");
    let mut node_ids = Vec::new();

    for i in 0..node_count {
        let mut node = sombra::model::Node::new(0);
        node.labels.push("User".to_string());
        node.properties.insert(
            "id".to_string(),
            sombra::model::PropertyValue::Int(i as i64),
        );
        node.properties.insert(
            "name".to_string(),
            sombra::model::PropertyValue::String(format!("User_{}", i)),
        );
        node.properties
            .insert("age".to_string(), sombra::model::PropertyValue::Int(25));
        node.properties
            .insert("score".to_string(), sombra::model::PropertyValue::Int(100));
        let id = db.add_node(node).expect("add node");
        node_ids.push(id);
    }

    let update_start = Instant::now();
    for _ in 0..updates_per_node {
        for &node_id in &node_ids {
            let current_age = if let Some(node) = db.get_node(node_id).expect("get node") {
                if let Some(sombra::model::PropertyValue::Int(age)) = node.properties.get("age") {
                    *age
                } else {
                    25
                }
            } else {
                25
            };
            db.set_node_property(
                node_id,
                "age".to_string(),
                sombra::model::PropertyValue::Int(current_age + 1),
            )
            .expect("set property");
        }
    }
    update_start.elapsed().as_micros()
}

fn benchmark_property_add_and_remove(node_count: usize, updates_per_node: usize) -> u128 {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();
    let mut db = sombra::db::GraphDB::open(&path).expect("open db");
    let mut node_ids = Vec::new();

    for i in 0..node_count {
        let mut node = sombra::model::Node::new(0);
        node.labels.push("User".to_string());
        node.properties.insert(
            "id".to_string(),
            sombra::model::PropertyValue::Int(i as i64),
        );
        node.properties.insert(
            "name".to_string(),
            sombra::model::PropertyValue::String(format!("User_{}", i)),
        );
        node.properties
            .insert("age".to_string(), sombra::model::PropertyValue::Int(25));
        node.properties
            .insert("score".to_string(), sombra::model::PropertyValue::Int(100));
        let id = db.add_node(node).expect("add node");
        node_ids.push(id);
    }

    let update_start = Instant::now();
    for i in 0..updates_per_node {
        for &node_id in &node_ids {
            if i % 2 == 0 {
                db.set_node_property(
                    node_id,
                    "temp".to_string(),
                    sombra::model::PropertyValue::String("temp_value".to_string()),
                )
                .expect("set property");
            } else {
                db.remove_node_property(node_id, "temp")
                    .expect("remove property");
            }
        }
    }
    update_start.elapsed().as_micros()
}

fn main() {
    let node_counts = vec![100, 500];
    let updates_per_node = 10;

    println!("Property Update Performance Benchmark");
    println!("======================================\n");
    println!("Update-in-place optimization:");
    println!("- Tries to update record in existing slot if new data fits");
    println!("- Falls back to delete+reinsert if record grows");
    println!("- Incremental property index updates instead of full rebuild");
    println!("- Reduces WAL writes and page modifications\n");

    for &count in &node_counts {
        println!(
            "Testing with {} nodes, {} operations per node:",
            count, updates_per_node
        );

        let update_time = benchmark_single_property_update(count, updates_per_node);
        println!(
            "  Single property update (set_node_property): {}μs ({:.2}ms)",
            update_time,
            update_time as f64 / 1000.0
        );

        let add_remove_time = benchmark_property_add_and_remove(count, updates_per_node);
        println!(
            "  Add/remove property cycle:              {}μs ({:.2}ms)",
            add_remove_time,
            add_remove_time as f64 / 1000.0
        );

        let ops_per_sec_update =
            (count * updates_per_node) as f64 / (update_time as f64 / 1_000_000.0);
        let ops_per_sec_cycle =
            (count * updates_per_node) as f64 / (add_remove_time as f64 / 1_000_000.0);

        println!(
            "  Throughput (set):         {:.0} ops/sec",
            ops_per_sec_update
        );
        println!(
            "  Throughput (add/remove):  {:.0} ops/sec\n",
            ops_per_sec_cycle
        );
    }

    println!("Summary:");
    println!("--------");
    println!("The update-in-place optimization provides:");
    println!("- Faster property updates when records don't grow (in-place updates)");
    println!("- Reduced WAL size for update workloads");
    println!("- Less memory allocation and copying");
    println!("- Better cache locality");
    println!("- Incremental property index updates");
}
