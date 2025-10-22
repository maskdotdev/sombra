#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::expect_fun_call)]

use sombra::{GraphDB, Node, PropertyValue};
use tempfile::NamedTempFile;

#[test]
fn test_property_index_compat_100_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..100 {
            let mut node = Node::new(0);
            node.labels.push("User".to_string());
            node.properties
                .insert("age".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("User", "age")
            .expect("create index");
        db.checkpoint().expect("checkpoint");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query");
        assert_eq!(results.len(), 1);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query after reopen");
        assert_eq!(results.len(), 1);
    }
}

#[test]
fn test_property_index_compat_200_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..200 {
            let mut node = Node::new(0);
            node.labels.push("User".to_string());
            node.properties
                .insert("age".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("User", "age")
            .expect("create index");
        db.checkpoint().expect("checkpoint");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query");
        assert_eq!(results.len(), 1);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query after reopen");
        assert_eq!(results.len(), 1);

        let results2 = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(150))
            .expect("query 150");
        assert_eq!(results2.len(), 1);
    }
}

#[test]
fn test_property_index_compat_300_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..300 {
            let mut node = Node::new(0);
            node.labels.push("User".to_string());
            node.properties
                .insert("age".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("User", "age")
            .expect("create index");
        db.checkpoint().expect("checkpoint");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query");
        assert_eq!(results.len(), 1);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query after reopen");
        assert_eq!(results.len(), 1);

        let results2 = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(250))
            .expect("query 250");
        assert_eq!(results2.len(), 1);
    }
}

#[test]
fn test_nodes_persist_after_reopen_and_add() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut node_ids = Vec::new();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..10 {
            let mut node = Node::new(0);
            node.labels.push("Test".to_string());
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            let id = tx.add_node(node).expect("add node");
            node_ids.push(id);
        }

        tx.commit().expect("commit");
        db.checkpoint().expect("checkpoint");

        println!("Session 1: Added 10 nodes with IDs {:?}", &node_ids[0..10]);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen 1");

        // First verify we can read all nodes from session 1
        for &node_id in &node_ids[0..10] {
            let node = db
                .get_node(node_id)
                .expect(&format!("get node {} after reopen 1", node_id));
            println!("Session 2 (before add): Can read node {}", node.id);
        }

        let mut tx = db.begin_transaction().expect("begin tx");
        for i in 10..20 {
            let mut node = Node::new(0);
            node.labels.push("Test".to_string());
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            let id = tx.add_node(node).expect("add node");
            node_ids.push(id);
        }
        tx.commit().expect("commit");

        // Verify we can still read nodes from session 1 BEFORE checkpoint
        for &node_id in &node_ids[0..10] {
            let node = db
                .get_node(node_id)
                .expect(&format!("get node {} before checkpoint", node_id));
            println!(
                "Session 2 (after add, before checkpoint): Can read node {}",
                node.id
            );
        }

        db.checkpoint().expect("checkpoint");

        // Verify we can still read nodes from session 1 AFTER checkpoint
        for &node_id in &node_ids[0..10] {
            let node = db
                .get_node(node_id)
                .expect(&format!("get node {} after checkpoint", node_id));
            println!("Session 2 (after checkpoint): Can read node {}", node.id);
        }

        println!(
            "Session 2: Added 10 more nodes with IDs {:?}",
            &node_ids[10..20]
        );
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen 2");

        println!("Session 3: Reopened database");

        for &node_id in &node_ids {
            match db.get_node(node_id) {
                Ok(node) => println!("Session 3: Can read node {}", node.id),
                Err(e) => panic!("Session 3: FAILED to read node {}: {:?}", node_id, e),
            }
        }
    }
}

#[test]
fn test_property_index_compat_500_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..500 {
            let mut node = Node::new(0);
            node.labels.push("User".to_string());
            node.properties
                .insert("age".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("User", "age")
            .expect("create index");
        db.checkpoint().expect("checkpoint");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query");
        assert_eq!(results.len(), 1);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query after reopen");
        assert_eq!(results.len(), 1);

        let results2 = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(400))
            .expect("query 400");
        assert_eq!(results2.len(), 1);
    }
}

#[test]
fn test_property_index_compat_multiple_restarts() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..500 {
            let mut node = Node::new(0);
            node.labels.push("Item".to_string());
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");
        db.create_property_index("Item", "value")
            .expect("create index");
        db.checkpoint().expect("checkpoint");

        println!("First session: Added 500 nodes and created index");
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen 1");

        let test_query = db.find_nodes_by_property("Item", "value", &PropertyValue::Int(0));
        println!("After reopen 1, can query node 0: {:?}", test_query.is_ok());

        let mut tx = db.begin_transaction().expect("begin tx");
        for i in 500..1000 {
            let mut node = Node::new(0);
            node.labels.push("Item".to_string());
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            tx.add_node(node).expect("add node");
        }
        tx.commit().expect("commit");
        db.checkpoint().expect("checkpoint");

        println!("Second session: Added 500 more nodes");

        let test_query2 = db.find_nodes_by_property("Item", "value", &PropertyValue::Int(0));
        println!(
            "After adding more nodes, can query node 0: {:?}",
            test_query2.is_ok()
        );
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen 2");

        println!("Third session: Reopened database");

        let results_first = db
            .find_nodes_by_property("Item", "value", &PropertyValue::Int(0))
            .expect("query first");
        assert_eq!(results_first.len(), 1);

        let results_mid = db
            .find_nodes_by_property("Item", "value", &PropertyValue::Int(500))
            .expect("query mid");
        assert_eq!(results_mid.len(), 1);

        let results_last = db
            .find_nodes_by_property("Item", "value", &PropertyValue::Int(999))
            .expect("query last");
        assert_eq!(results_last.len(), 1);
    }
}

#[test]
fn test_property_index_stress_string_values() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..2000 {
            let mut node = Node::new(0);
            node.labels.push("Doc".to_string());
            node.properties.insert(
                "title".to_string(),
                PropertyValue::String(format!("Title_{:05}", i)),
            );
            node.properties.insert(
                "category".to_string(),
                PropertyValue::String(format!("Cat_{}", i % 20)),
            );
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("Doc", "title")
            .expect("create title index");
        db.create_property_index("Doc", "category")
            .expect("create category index");
        db.checkpoint().expect("checkpoint");

        let results = db
            .find_nodes_by_property(
                "Doc",
                "title",
                &PropertyValue::String("Title_01000".to_string()),
            )
            .expect("query title");
        assert_eq!(results.len(), 1);

        let results_cat = db
            .find_nodes_by_property(
                "Doc",
                "category",
                &PropertyValue::String("Cat_5".to_string()),
            )
            .expect("query category");
        assert_eq!(results_cat.len(), 100);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results = db
            .find_nodes_by_property(
                "Doc",
                "title",
                &PropertyValue::String("Title_01999".to_string()),
            )
            .expect("query after reopen");
        assert_eq!(results.len(), 1);

        let results_cat = db
            .find_nodes_by_property(
                "Doc",
                "category",
                &PropertyValue::String("Cat_10".to_string()),
            )
            .expect("query category after reopen");
        assert_eq!(results_cat.len(), 100);
    }
}

#[test]
fn test_property_index_stress_multiple_value_types() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin tx");

        for i in 0..1000 {
            let mut node = Node::new(0);
            node.labels.push("Record".to_string());
            node.properties
                .insert("id".to_string(), PropertyValue::Int(i));
            node.properties
                .insert("active".to_string(), PropertyValue::Bool(i % 3 == 0));
            node.properties.insert(
                "name".to_string(),
                PropertyValue::String(format!("Record_{}", i)),
            );
            tx.add_node(node).expect("add node");
        }

        tx.commit().expect("commit");

        db.create_property_index("Record", "id")
            .expect("create id index");
        db.create_property_index("Record", "active")
            .expect("create active index");
        db.create_property_index("Record", "name")
            .expect("create name index");
        db.checkpoint().expect("checkpoint");

        let results_int = db
            .find_nodes_by_property("Record", "id", &PropertyValue::Int(500))
            .expect("query int");
        assert_eq!(results_int.len(), 1);

        let results_bool = db
            .find_nodes_by_property("Record", "active", &PropertyValue::Bool(true))
            .expect("query bool");
        assert_eq!(results_bool.len(), 334);

        let mut results_bool_false = db
            .find_nodes_by_property("Record", "active", &PropertyValue::Bool(false))
            .expect("query bool false");
        assert_eq!(results_bool_false.len(), 666);
        results_bool_false.sort_unstable();

        let results_string = db
            .find_nodes_by_property(
                "Record",
                "name",
                &PropertyValue::String("Record_250".to_string()),
            )
            .expect("query string");
        assert_eq!(results_string.len(), 1);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let results_int = db
            .find_nodes_by_property("Record", "id", &PropertyValue::Int(999))
            .expect("query int after reopen");
        assert_eq!(results_int.len(), 1);

        let mut results_bool = db
            .find_nodes_by_property("Record", "active", &PropertyValue::Bool(false))
            .expect("query bool after reopen");
        results_bool.sort_unstable();
        assert_eq!(results_bool.len(), 666);

        let results_string = db
            .find_nodes_by_property(
                "Record",
                "name",
                &PropertyValue::String("Record_0".to_string()),
            )
            .expect("query string after reopen");
        assert_eq!(results_string.len(), 1);
    }
}

#[test]
#[ignore]
fn test_property_index_stress_100k_entries() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    const TOTAL_NODES: usize = 100_000;
    const BATCH_SIZE: usize = 1000;

    {
        let mut db = GraphDB::open(&path).expect("open db");

        for batch in 0..(TOTAL_NODES / BATCH_SIZE) {
            let mut tx = db.begin_transaction().expect("begin tx");

            for i in 0..BATCH_SIZE {
                let node_idx = batch * BATCH_SIZE + i;
                let mut node = Node::new(0);
                node.labels.push("User".to_string());
                node.properties
                    .insert("id".to_string(), PropertyValue::Int(node_idx as i64));
                node.properties.insert(
                    "age".to_string(),
                    PropertyValue::Int((node_idx % 100) as i64),
                );
                node.properties
                    .insert("active".to_string(), PropertyValue::Bool(node_idx % 3 == 0));
                tx.add_node(node).expect("add node");
            }

            tx.commit().expect(&format!("commit batch {}", batch));

            if batch % 10 == 0 {
                println!("Inserted {} nodes", batch * BATCH_SIZE);
            }
        }

        println!("Creating property indexes...");
        db.create_property_index("User", "id")
            .expect("create id index");
        db.create_property_index("User", "age")
            .expect("create age index");
        db.create_property_index("User", "active")
            .expect("create active index");

        println!("Checkpointing...");
        db.checkpoint().expect("checkpoint");

        println!("Testing queries...");
        let results = db
            .find_nodes_by_property("User", "id", &PropertyValue::Int(50000))
            .expect("query id");
        assert_eq!(results.len(), 1);

        let results_age = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query age");
        assert_eq!(results_age.len(), 1000);

        let results_active = db
            .find_nodes_by_property("User", "active", &PropertyValue::Bool(true))
            .expect("query active");
        assert_eq!(results_active.len(), 33334);
    }

    {
        println!("Reopening database...");
        let mut db = GraphDB::open(&path).expect("reopen db");

        println!("Verifying indexes persisted...");
        let results = db
            .find_nodes_by_property("User", "id", &PropertyValue::Int(75000))
            .expect("query id after reopen");
        assert_eq!(results.len(), 1);

        let results_age = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(25))
            .expect("query age after reopen");
        assert_eq!(results_age.len(), 1000);

        println!("Testing boundary queries...");
        let first = db
            .find_nodes_by_property("User", "id", &PropertyValue::Int(0))
            .expect("query first");
        assert_eq!(first.len(), 1);

        let last = db
            .find_nodes_by_property("User", "id", &PropertyValue::Int((TOTAL_NODES - 1) as i64))
            .expect("query last");
        assert_eq!(last.len(), 1);
    }

    println!("Stress test completed successfully!");
}

#[test]
fn minimal_node_persistence() {
    let tmp = NamedTempFile::new().expect("temp");
    let path = tmp.path().to_path_buf();

    let node0_id;
    {
        let mut db = GraphDB::open(&path).expect("open");
        let mut tx = db.begin_transaction().expect("tx");
        node0_id = tx.add_node(Node::new(0)).expect("add");
        tx.commit().expect("commit");
        db.checkpoint().expect("checkpoint");
        println!("Session 1: Added node {}", node0_id);
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen");
        let node = db.get_node(node0_id).expect("get node");
        println!("Session 2: Got node {}", node.id);
    }
}
