use sombra::{data_generator::DataGenerator, Edge, GraphDB, Node};
use tempfile::TempDir;

fn main() {
    println!("=== Sombra Performance Metrics Demo ===\n");

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("metrics_demo.db");

    let mut generator = DataGenerator::new();
    let (nodes, edges) = generator.generate_medium_dataset();

    println!(
        "Creating database with {} nodes and {} edges...",
        nodes.len(),
        edges.len()
    );

    {
        let mut db = GraphDB::open(&db_path).unwrap();
        let mut tx = db.begin_transaction().unwrap();

        for node in &nodes {
            tx.add_node(node.clone()).unwrap();
        }

        for edge in &edges {
            tx.add_edge(edge.clone()).unwrap();
        }

        tx.commit().unwrap();
    }

    println!("\n--- Test 1: Cold Cache Reads ---");
    {
        let mut db = GraphDB::open(&db_path).unwrap();

        for i in 1..=100 {
            let _node = db.get_node(i).unwrap();
        }

        println!("Read 100 nodes (cold cache):");
        db.metrics.print_report();
    }

    println!("\n--- Test 2: Warm Cache Reads ---");
    {
        let mut db = GraphDB::open(&db_path).unwrap();

        for _ in 0..10 {
            for i in 1..=100 {
                let _node = db.get_node(i).unwrap();
            }
        }

        println!("Read 100 nodes 10 times (warm cache):");
        db.metrics.print_report();
    }

    println!("\n--- Test 3: Label Index Queries ---");
    {
        let mut db = GraphDB::open(&db_path).unwrap();
        db.metrics.reset();

        for _ in 0..100 {
            let _users = db.get_nodes_by_label("User").unwrap();
        }

        println!("Performed 100 label queries:");
        db.metrics.print_report();
    }

    println!("\n--- Test 4: Graph Traversal ---");
    {
        let mut db = GraphDB::open(&db_path).unwrap();
        db.metrics.reset();

        for i in 1..=50 {
            let _neighbors = db.get_neighbors(i).unwrap();
        }

        println!("Traversed neighbors for 50 nodes:");
        db.metrics.print_report();
    }

    println!("\n--- Test 5: Two-Hop Traversal ---");
    {
        let mut db = GraphDB::open(&db_path).unwrap();
        db.metrics.reset();

        for i in 1..=10 {
            let _neighbors = db.get_neighbors_two_hops(i).unwrap();
        }

        println!("Performed two-hop traversal for 10 nodes:");
        db.metrics.print_report();
    }

    println!("\n✓ Performance metrics demo completed!");
}
