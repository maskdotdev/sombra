use sombra::db::{ConcurrentGraphDB, Config};
use sombra::model::{Node, PropertyValue};
use std::fs;

#[test]
fn test_mvcc_label_index_snapshot_isolation() {
    let test_db_path = "/tmp/sombra_test_mvcc_label_index_snapshot";
    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_property_index_snapshot_isolation() {
    let test_db_path = "/tmp/sombra_test_mvcc_property_index_snapshot";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // Create property index for (Person, age)
    db.create_property_index("Person", "age").unwrap();

    // T1: Create a node with property "age" = 30
    let mut tx1 = db.begin_transaction().unwrap();
    let mut node1 = Node::new(0);
    node1.labels.push("Person".to_string());
    node1
        .properties
        .insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    node1
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));
    let node1_id = tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // T2: Start a snapshot before T3 modifies properties
    let tx2 = db.begin_transaction().unwrap();

    // T2: Find nodes by property "age" = 30 - should find node1
    let nodes_age_30 = tx2
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .unwrap();
    assert_eq!(nodes_age_30.len(), 1);
    assert_eq!(nodes_age_30[0], node1_id);

    // T3: Update node1 to change age from 30 to 31
    let mut tx3 = db.begin_transaction().unwrap();
    let mut node = tx3.get_node(node1_id).unwrap().unwrap();
    node.properties
        .insert("age".to_string(), PropertyValue::Int(31));
    tx3.update_node(node).unwrap();
    tx3.commit().unwrap();

    // T2: Still reading with old snapshot - should still find node1 with age=30
    let nodes_age_30_after = tx2
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .unwrap();
    assert_eq!(
        nodes_age_30_after.len(),
        1,
        "T2 should still see node with age=30 (snapshot isolation)"
    );
    assert_eq!(nodes_age_30_after[0], node1_id);

    // T2: Should not see any nodes with age=31
    let nodes_age_31 = tx2
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(31))
        .unwrap();
    assert_eq!(
        nodes_age_31.len(),
        0,
        "T2 should not see node with age=31 (snapshot isolation)"
    );

    tx2.commit().unwrap();

    // T4: New transaction should see the updated property
    let tx4 = db.begin_transaction().unwrap();
    let nodes_age_30_new = tx4
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .unwrap();
    assert_eq!(
        nodes_age_30_new.len(),
        0,
        "T4 should not see node with age=30"
    );

    let nodes_age_31_new = tx4
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(31))
        .unwrap();
    assert_eq!(
        nodes_age_31_new.len(),
        1,
        "T4 should see node with age=31"
    );
    assert_eq!(nodes_age_31_new[0], node1_id);

    tx4.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_property_index_concurrent_updates() {
    let test_db_path = "/tmp/sombra_test_mvcc_property_index_concurrent";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // Create property index for (Person, age)
    db.create_property_index("Person", "age").unwrap();

    // Create multiple nodes with different properties concurrently
    std::thread::scope(|s| {
        for i in 0..10 {
            let db = db.clone();
            s.spawn(move || {
                let mut tx = db.begin_transaction().unwrap();
                let mut node = Node::new(0);
                node.labels.push("Person".to_string());
                node.properties.insert(
                    "age".to_string(),
                    PropertyValue::Int((20 + (i % 5)) as i64), // Ages 20-24
                );
                node.properties.insert(
                    "index".to_string(),
                    PropertyValue::Int(i as i64),
                );
                tx.add_node(node).unwrap();
                tx.commit().unwrap();
            });
        }
    });

    // Verify the property index has correct counts
    let tx = db.begin_transaction().unwrap();

    // Each age should have 2 nodes (10 nodes / 5 different ages = 2 each)
    for age in 20..25 {
        let nodes = tx
            .find_nodes_by_property("Person", "age", &PropertyValue::Int(age))
            .unwrap();
        assert_eq!(
            nodes.len(),
            2,
            "Should have 2 nodes with age={}",
            age
        );
    }

    tx.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_property_index_add_remove_properties() {
    let test_db_path = "/tmp/sombra_test_mvcc_property_index_add_remove";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // Create property indexes for name, age, city, and country
    db.create_property_index("Person", "name").unwrap();
    db.create_property_index("Person", "age").unwrap();
    db.create_property_index("Person", "city").unwrap();
    db.create_property_index("Person", "country").unwrap();

    // T1: Create a node with multiple properties
    let mut tx1 = db.begin_transaction().unwrap();
    let mut node1 = Node::new(0);
    node1.labels.push("Person".to_string());
    node1
        .properties
        .insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    node1
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));
    node1
        .properties
        .insert("city".to_string(), PropertyValue::String("NYC".to_string()));
    let node1_id = tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // T2: Start a snapshot
    let tx2 = db.begin_transaction().unwrap();

    // T2: Verify node appears in all property indexes
    assert_eq!(
        tx2.find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string())
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        tx2.find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        tx2.find_nodes_by_property(
            "Person",
            "city",
            &PropertyValue::String("NYC".to_string())
        )
        .unwrap()
        .len(),
        1
    );

    // T3: Update properties - remove "city", change "age", keep "name"
    let mut tx3 = db.begin_transaction().unwrap();
    let mut node = tx3.get_node(node1_id).unwrap().unwrap();
    node.properties.remove("city");
    node.properties
        .insert("age".to_string(), PropertyValue::Int(31));
    node.properties.insert(
        "country".to_string(),
        PropertyValue::String("USA".to_string()),
    );
    tx3.update_node(node).unwrap();
    tx3.commit().unwrap();

    // T2: Should still see old properties (snapshot isolation)
    assert_eq!(
        tx2.find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string())
        )
        .unwrap()
        .len(),
        1,
        "T2 should still see name=Alice"
    );
    assert_eq!(
        tx2.find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
            .unwrap()
            .len(),
        1,
        "T2 should still see age=30"
    );
    assert_eq!(
        tx2.find_nodes_by_property(
            "Person",
            "city",
            &PropertyValue::String("NYC".to_string())
        )
        .unwrap()
        .len(),
        1,
        "T2 should still see city=NYC"
    );
    assert_eq!(
        tx2.find_nodes_by_property(
            "Person",
            "country",
            &PropertyValue::String("USA".to_string())
        )
        .unwrap()
        .len(),
        0,
        "T2 should not see country=USA"
    );

    tx2.commit().unwrap();

    // T4: New transaction should see updated properties
    let tx4 = db.begin_transaction().unwrap();
    assert_eq!(
        tx4.find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string())
        )
        .unwrap()
        .len(),
        1,
        "T4 should see name=Alice"
    );
    assert_eq!(
        tx4.find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
            .unwrap()
            .len(),
        0,
        "T4 should not see age=30"
    );
    assert_eq!(
        tx4.find_nodes_by_property("Person", "age", &PropertyValue::Int(31))
            .unwrap()
            .len(),
        1,
        "T4 should see age=31"
    );
    assert_eq!(
        tx4.find_nodes_by_property(
            "Person",
            "city",
            &PropertyValue::String("NYC".to_string())
        )
        .unwrap()
        .len(),
        0,
        "T4 should not see city=NYC"
    );
    assert_eq!(
        tx4.find_nodes_by_property(
            "Person",
            "country",
            &PropertyValue::String("USA".to_string())
        )
        .unwrap()
        .len(),
        1,
        "T4 should see country=USA"
    );

    tx4.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_property_index_node_deletion() {
    let test_db_path = "/tmp/sombra_test_mvcc_property_index_deletion";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // Create property indexes
    db.create_property_index("Person", "name").unwrap();
    db.create_property_index("Person", "age").unwrap();

    // T1: Create a node with properties
    let mut tx1 = db.begin_transaction().unwrap();
    let mut node1 = Node::new(0);
    node1.labels.push("Person".to_string());
    node1
        .properties
        .insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    node1
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));
    let node1_id = tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // T2: Start a snapshot
    let tx2 = db.begin_transaction().unwrap();

    // T2: Should find the node by property
    let nodes_by_name = tx2
        .find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string()),
        )
        .unwrap();
    assert_eq!(nodes_by_name.len(), 1);

    // T3: Delete the node
    let mut tx3 = db.begin_transaction().unwrap();
    tx3.delete_node(node1_id).unwrap();
    tx3.commit().unwrap();

    // T2: Should still find the node (snapshot isolation)
    let nodes_by_name_after = tx2
        .find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string()),
        )
        .unwrap();
    assert_eq!(
        nodes_by_name_after.len(),
        1,
        "T2 should still find deleted node by property (snapshot isolation)"
    );

    tx2.commit().unwrap();

    // T4: New transaction should not find the deleted node
    let tx4 = db.begin_transaction().unwrap();
    let nodes_by_name_new = tx4
        .find_nodes_by_property(
            "Person",
            "name",
            &PropertyValue::String("Alice".to_string()),
        )
        .unwrap();
    assert_eq!(
        nodes_by_name_new.len(),
        0,
        "T4 should not find deleted node by property"
    );

    tx4.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}


#[test]
fn test_mvcc_label_index_concurrent_updates() {
    let test_db_path = "/tmp/sombra_test_mvcc_label_index_concurrent";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // Create multiple nodes with different labels concurrently
    std::thread::scope(|s| {
        for i in 0..5 {
            let db = db.clone();
            s.spawn(move || {
                let mut tx = db.begin_transaction().unwrap();
                let mut node = Node::new(0);
                node.labels.push(format!("Label{}", i % 2)); // Alternate between Label0 and Label1
                node.properties.insert(
                    "index".to_string(),
                    PropertyValue::Int(i as i64),
                );
                tx.add_node(node).unwrap();
                tx.commit().unwrap();
            });
        }
    });

    // Verify the label index has correct counts
    let tx = db.begin_transaction().unwrap();
    let label0_nodes = tx.get_nodes_by_label("Label0").unwrap();
    let label1_nodes = tx.get_nodes_by_label("Label1").unwrap();

    assert_eq!(label0_nodes.len(), 3, "Should have 3 nodes with Label0");
    assert_eq!(label1_nodes.len(), 2, "Should have 2 nodes with Label1");

    tx.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_label_index_add_remove_labels() {
    let test_db_path = "/tmp/sombra_test_mvcc_label_index_add_remove";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // T1: Create a node with multiple labels
    let mut tx1 = db.begin_transaction().unwrap();
    let mut node1 = Node::new(0);
    node1.labels.push("Person".to_string());
    node1.labels.push("Employee".to_string());
    node1.labels.push("Manager".to_string());
    let node1_id = tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // T2: Start a snapshot
    let tx2 = db.begin_transaction().unwrap();

    // T2: Verify node appears in all three labels
    assert_eq!(tx2.get_nodes_by_label("Person").unwrap().len(), 1);
    assert_eq!(tx2.get_nodes_by_label("Employee").unwrap().len(), 1);
    assert_eq!(tx2.get_nodes_by_label("Manager").unwrap().len(), 1);

    // T3: Remove some labels and add a new one
    let mut tx3 = db.begin_transaction().unwrap();
    let mut node = tx3.get_node(node1_id).unwrap().unwrap();
    node.labels.clear();
    node.labels.push("Person".to_string()); // Keep Person
    node.labels.push("Executive".to_string()); // Add Executive
                                                // Remove Employee and Manager
    tx3.update_node(node).unwrap();
    tx3.commit().unwrap();

    // T2: Should still see old labels (snapshot isolation)
    assert_eq!(
        tx2.get_nodes_by_label("Person").unwrap().len(),
        1,
        "T2 should still see Person label"
    );
    assert_eq!(
        tx2.get_nodes_by_label("Employee").unwrap().len(),
        1,
        "T2 should still see Employee label"
    );
    assert_eq!(
        tx2.get_nodes_by_label("Manager").unwrap().len(),
        1,
        "T2 should still see Manager label"
    );
    assert_eq!(
        tx2.get_nodes_by_label("Executive").unwrap().len(),
        0,
        "T2 should not see Executive label"
    );

    tx2.commit().unwrap();

    // T4: New transaction should see updated labels
    let tx4 = db.begin_transaction().unwrap();
    assert_eq!(
        tx4.get_nodes_by_label("Person").unwrap().len(),
        1,
        "T4 should see Person label"
    );
    assert_eq!(
        tx4.get_nodes_by_label("Employee").unwrap().len(),
        0,
        "T4 should not see Employee label"
    );
    assert_eq!(
        tx4.get_nodes_by_label("Manager").unwrap().len(),
        0,
        "T4 should not see Manager label"
    );
    assert_eq!(
        tx4.get_nodes_by_label("Executive").unwrap().len(),
        1,
        "T4 should see Executive label"
    );

    tx4.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}

#[test]
fn test_mvcc_label_index_node_deletion() {
    let test_db_path = "/tmp/sombra_test_mvcc_label_index_deletion";
    let _ = fs::remove_file(test_db_path);

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let db = ConcurrentGraphDB::open_with_config(test_db_path, config).unwrap();

    // T1: Create a node with label "Person"
    let mut tx1 = db.begin_transaction().unwrap();
    let mut node1 = Node::new(0);
    node1.labels.push("Person".to_string());
    let node1_id = tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // T2: Start a snapshot
    let tx2 = db.begin_transaction().unwrap();

    // T2: Should see the node
    let person_nodes = tx2.get_nodes_by_label("Person").unwrap();
    assert_eq!(person_nodes.len(), 1);

    // T3: Delete the node
    let mut tx3 = db.begin_transaction().unwrap();
    tx3.delete_node(node1_id).unwrap();
    tx3.commit().unwrap();

    // T2: Should still see the node (snapshot isolation)
    let person_nodes_after = tx2.get_nodes_by_label("Person").unwrap();
    assert_eq!(
        person_nodes_after.len(),
        1,
        "T2 should still see deleted node (snapshot isolation)"
    );

    tx2.commit().unwrap();

    // T4: New transaction should not see the deleted node
    let tx4 = db.begin_transaction().unwrap();
    let person_nodes_new = tx4.get_nodes_by_label("Person").unwrap();
    assert_eq!(
        person_nodes_new.len(),
        0,
        "T4 should not see deleted node"
    );

    tx4.commit().unwrap();

    let _ = fs::remove_file(test_db_path);
}
