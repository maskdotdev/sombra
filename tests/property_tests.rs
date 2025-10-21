use proptest::prelude::*;
use sombra::{GraphDB, PropertyValue, Node, Edge};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
enum Operation {
    CreateNode { labels: Vec<String>, props: BTreeMap<String, PropertyValue> },
    GetNode { node_id: u64 },
    CreateEdge { from: u64, to: u64, rel_type: String },
    GetNeighbors { node_id: u64 },
}

fn arb_property_value() -> impl Strategy<Value = PropertyValue> {
    prop_oneof![
        any::<i64>().prop_map(PropertyValue::Int),
        any::<f64>().prop_map(|f| PropertyValue::Float(if f.is_nan() { 0.0 } else { f })),
        any::<bool>().prop_map(PropertyValue::Bool),
        "[a-z]{1,10}".prop_map(PropertyValue::String),
    ]
}

fn arb_operation() -> impl Strategy<Value = Operation> {
    prop_oneof![
        (
            prop::collection::vec("[A-Z][a-z]{2,8}", 1..=3),
            prop::collection::btree_map("[a-z]{1,8}", arb_property_value(), 0..=3)
        ).prop_map(|(labels, props)| Operation::CreateNode { labels, props }),
        (1u64..=100).prop_map(|node_id| Operation::GetNode { node_id }),
        (1u64..=50, 1u64..=50, "[A-Z]{3,10}").prop_map(|(from, to, rel_type)| {
            Operation::CreateEdge { from, to, rel_type }
        }),
        (1u64..=50).prop_map(|node_id| Operation::GetNeighbors { node_id }),
    ]
}

proptest! {
    #[test]
    fn prop_any_sequence_is_serializable(ops in prop::collection::vec(arb_operation(), 1..100)) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut db = GraphDB::open(temp.path()).unwrap();

        let mut created_nodes = Vec::new();
        let mut tx = db.begin_transaction().unwrap();

        for op in ops {
            match op {
                Operation::CreateNode { labels, props } => {
                    let mut node = Node::new(0);
                    node.labels = labels;
                    node.properties = props;
                    if let Ok(node_id) = tx.add_node(node) {
                        created_nodes.push(node_id);
                    }
                }
                Operation::GetNode { node_id } => {
                    let _ = tx.get_node(node_id);
                }
                Operation::CreateEdge { from, to, rel_type } => {
                    if created_nodes.contains(&from) && created_nodes.contains(&to) {
                        let edge = Edge::new(0, from, to, rel_type);
                        let _ = tx.add_edge(edge);
                    }
                }
                Operation::GetNeighbors { node_id } => {
                    if created_nodes.contains(&node_id) {
                        let _ = tx.get_neighbors(node_id);
                    }
                }
            }
        }

        prop_assert!(tx.commit().is_ok());
    }

    #[test]
    fn prop_commit_then_read_is_consistent(
        nodes in prop::collection::vec((
            prop::collection::vec("[A-Z][a-z]{2,5}", 1..=2),
            prop::collection::btree_map("[a-z]{1,5}", any::<i64>().prop_map(PropertyValue::Int), 0..=2)
        ), 1..50)
    ) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut db = GraphDB::open(temp.path()).unwrap();

        let mut node_ids = Vec::new();
        {
            let mut tx = db.begin_transaction().unwrap();
            for (labels, props) in &nodes {
                let mut node = Node::new(0);
                node.labels = labels.clone();
                node.properties = props.clone();
                let node_id = tx.add_node(node).unwrap();
                node_ids.push(node_id);
            }
            tx.commit().unwrap();
        }

        {
            let mut tx = db.begin_transaction().unwrap();
            for (idx, node_id) in node_ids.iter().enumerate() {
                let node = tx.get_node(*node_id).unwrap();
                prop_assert!(node.id > 0);
                prop_assert_eq!(&node.labels, &nodes[idx].0);
            }
            tx.commit().unwrap();
        }
    }

    #[test]
    fn prop_rollback_leaves_no_trace(
        committed in prop::collection::vec(any::<i64>(), 1..20),
        rolled_back in prop::collection::vec(any::<i64>(), 1..20)
    ) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut db = GraphDB::open(temp.path()).unwrap();

        let mut committed_ids = Vec::new();
        {
            let mut tx = db.begin_transaction().unwrap();
            for value in &committed {
                let mut props = BTreeMap::new();
                props.insert("value".to_string(), PropertyValue::Int(*value));
                
                let mut node = Node::new(0);
                node.labels.push("Committed".to_string());
                node.properties = props;
                
                let node_id = tx.add_node(node).unwrap();
                committed_ids.push(node_id);
            }
            tx.commit().unwrap();
        }

        let mut rolled_back_ids = Vec::new();
        {
            let mut tx = db.begin_transaction().unwrap();
            for value in &rolled_back {
                let mut props = BTreeMap::new();
                props.insert("value".to_string(), PropertyValue::Int(*value));
                
                let mut node = Node::new(0);
                node.labels.push("RolledBack".to_string());
                node.properties = props;
                
                let node_id = tx.add_node(node).unwrap();
                rolled_back_ids.push(node_id);
            }
            tx.rollback().unwrap();
        }

        {
            let mut tx = db.begin_transaction().unwrap();
            
            for node_id in &committed_ids {
                let node = tx.get_node(*node_id).unwrap();
                prop_assert!(node.id > 0, "Committed node {} should exist", node_id);
            }
            
            for node_id in &rolled_back_ids {
                let result = tx.get_node(*node_id);
                prop_assert!(result.is_err(), "Rolled back node {} should not exist", node_id);
            }
            
            tx.commit().unwrap();
        }
    }

    #[test]
    fn prop_edges_respect_node_existence(
        nodes in prop::collection::vec(any::<i64>(), 5..20),
        edges in prop::collection::vec((0usize..10, 0usize..10), 0..30)
    ) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut db = GraphDB::open(temp.path()).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        
        let mut node_ids = Vec::new();
        for value in &nodes {
            let mut props = BTreeMap::new();
            props.insert("value".to_string(), PropertyValue::Int(*value));
            
            let mut node = Node::new(0);
            node.labels.push("Node".to_string());
            node.properties = props;
            
            let node_id = tx.add_node(node).unwrap();
            node_ids.push(node_id);
        }

        for (from_idx, to_idx) in &edges {
            if *from_idx < node_ids.len() && *to_idx < node_ids.len() {
                let from = node_ids[*from_idx];
                let to = node_ids[*to_idx];
                let edge = Edge::new(0, from, to, "CONNECTS");
                let _ = tx.add_edge(edge);
            }
        }

        prop_assert!(tx.commit().is_ok());
    }

    #[test]
    fn prop_node_properties_preserved(
        props in prop::collection::btree_map(
            "[a-z]{1,10}",
            prop_oneof![
                any::<i64>().prop_map(PropertyValue::Int),
                any::<bool>().prop_map(PropertyValue::Bool),
                "[a-z]{1,20}".prop_map(PropertyValue::String),
            ],
            0..=10
        )
    ) {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut db = GraphDB::open(temp.path()).unwrap();

        let mut node = Node::new(0);
        node.labels.push("Test".to_string());
        node.properties = props.clone();

        let mut tx = db.begin_transaction().unwrap();
        let node_id = tx.add_node(node).unwrap();
        tx.commit().unwrap();

        let mut tx = db.begin_transaction().unwrap();
        let retrieved_node = tx.get_node(node_id).unwrap();
        prop_assert!(retrieved_node.id > 0);
        
        for (key, value) in &props {
            prop_assert_eq!(retrieved_node.properties.get(key), Some(value));
        }
        
        tx.commit().unwrap();
    }
}

#[test]
fn property_test_idempotent_reads() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("Test".to_string());
    node.properties.insert("test".to_string(), PropertyValue::Int(42));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    for _ in 0..100 {
        let mut tx = db.begin_transaction().unwrap();
        let node1 = tx.get_node(node_id).unwrap();
        let node2 = tx.get_node(node_id).unwrap();
        
        assert_eq!(node1, node2);
        tx.commit().unwrap();
    }
}

#[test]
fn property_test_commutative_node_creation() {
    let temp1 = tempfile::NamedTempFile::new().unwrap();
    let temp2 = tempfile::NamedTempFile::new().unwrap();
    
    let mut db1 = GraphDB::open(temp1.path()).unwrap();
    let mut db2 = GraphDB::open(temp2.path()).unwrap();

    let nodes = vec![
        ("A", 1i64),
        ("B", 2i64),
        ("C", 3i64),
    ];

    {
        let mut tx = db1.begin_transaction().unwrap();
        for (label, value) in &nodes {
            let mut props = BTreeMap::new();
            props.insert("value".to_string(), PropertyValue::Int(*value));
            
            let mut node = Node::new(0);
            node.labels.push(label.to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    {
        let mut tx = db2.begin_transaction().unwrap();
        for (label, value) in nodes.iter().rev() {
            let mut props = BTreeMap::new();
            props.insert("value".to_string(), PropertyValue::Int(*value));
            
            let mut node = Node::new(0);
            node.labels.push(label.to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    let mut tx1 = db1.begin_transaction().unwrap();
    let mut tx2 = db2.begin_transaction().unwrap();
    
    let mut count1 = 0;
    let mut count2 = 0;
    
    for i in 1..=10 {
        if tx1.get_node(i).is_ok() {
            count1 += 1;
        }
        if tx2.get_node(i).is_ok() {
            count2 += 1;
        }
    }
    
    assert_eq!(count1, count2);
    
    tx1.commit().unwrap();
    tx2.commit().unwrap();
}

#[test]
fn test_set_node_property_in_place() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("User".to_string());
    node.properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    node.properties.insert("age".to_string(), PropertyValue::Int(30));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    db.set_node_property(node_id, "age".to_string(), PropertyValue::Int(31)).unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let updated_node = tx.get_node(node_id).unwrap();
    assert_eq!(updated_node.properties.get("age"), Some(&PropertyValue::Int(31)));
    assert_eq!(updated_node.properties.get("name"), Some(&PropertyValue::String("Alice".to_string())));
    tx.commit().unwrap();
}

#[test]
fn test_set_node_property_with_growth() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("User".to_string());
    node.properties.insert("name".to_string(), PropertyValue::String("Bob".to_string()));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    let long_bio = "a".repeat(1000);
    db.set_node_property(node_id, "bio".to_string(), PropertyValue::String(long_bio.clone())).unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let updated_node = tx.get_node(node_id).unwrap();
    assert_eq!(updated_node.properties.get("bio"), Some(&PropertyValue::String(long_bio)));
    assert_eq!(updated_node.properties.get("name"), Some(&PropertyValue::String("Bob".to_string())));
    tx.commit().unwrap();
}

#[test]
fn test_remove_node_property() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("User".to_string());
    node.properties.insert("name".to_string(), PropertyValue::String("Charlie".to_string()));
    node.properties.insert("age".to_string(), PropertyValue::Int(25));
    node.properties.insert("email".to_string(), PropertyValue::String("charlie@example.com".to_string()));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    db.remove_node_property(node_id, "email").unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let updated_node = tx.get_node(node_id).unwrap();
    assert_eq!(updated_node.properties.get("email"), None);
    assert_eq!(updated_node.properties.get("name"), Some(&PropertyValue::String("Charlie".to_string())));
    assert_eq!(updated_node.properties.get("age"), Some(&PropertyValue::Int(25)));
    tx.commit().unwrap();
}

#[test]
fn test_remove_nonexistent_property() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("User".to_string());
    node.properties.insert("name".to_string(), PropertyValue::String("Dave".to_string()));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    db.remove_node_property(node_id, "nonexistent").unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(node_id).unwrap();
    assert_eq!(node.properties.get("name"), Some(&PropertyValue::String("Dave".to_string())));
    tx.commit().unwrap();
}

#[test]
fn test_property_update_persistence() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.path().to_path_buf();
    
    let node_id = {
        let mut db = GraphDB::open(&path).unwrap();
        let mut node = Node::new(0);
        node.labels.push("User".to_string());
        node.properties.insert("count".to_string(), PropertyValue::Int(0));

        let mut tx = db.begin_transaction().unwrap();
        let node_id = tx.add_node(node).unwrap();
        tx.commit().unwrap();

        db.set_node_property(node_id, "count".to_string(), PropertyValue::Int(42)).unwrap();
        node_id
    };

    {
        let mut db = GraphDB::open(&path).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap();
        assert_eq!(node.properties.get("count"), Some(&PropertyValue::Int(42)));
        tx.commit().unwrap();
    }
}

#[test]
fn test_property_update_multiple_times() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("Counter".to_string());
    node.properties.insert("value".to_string(), PropertyValue::Int(0));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    for i in 1..=10 {
        db.set_node_property(node_id, "value".to_string(), PropertyValue::Int(i)).unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(node_id).unwrap();
    assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(10)));
    tx.commit().unwrap();
}

#[test]
fn test_property_update_index_consistency() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("User".to_string());
    node.properties.insert("age".to_string(), PropertyValue::Int(25));
    node.properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    db.set_node_property(node_id, "age".to_string(), PropertyValue::Int(30)).unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let results = tx.find_nodes_by_property("User", "age", &PropertyValue::Int(30)).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], node_id);

    let old_results = tx.find_nodes_by_property("User", "age", &PropertyValue::Int(25)).unwrap();
    assert_eq!(old_results.len(), 0);
    
    tx.commit().unwrap();
}

#[test]
fn test_property_removal_updates_index() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut node = Node::new(0);
    node.labels.push("Product".to_string());
    node.properties.insert("price".to_string(), PropertyValue::Int(100));
    node.properties.insert("name".to_string(), PropertyValue::String("Widget".to_string()));

    let mut tx = db.begin_transaction().unwrap();
    let node_id = tx.add_node(node).unwrap();
    tx.commit().unwrap();

    db.remove_node_property(node_id, "price").unwrap();

    let mut tx = db.begin_transaction().unwrap();
    let results = tx.find_nodes_by_property("Product", "price", &PropertyValue::Int(100)).unwrap();
    assert_eq!(results.len(), 0);

    let node = tx.get_node(node_id).unwrap();
    assert_eq!(node.properties.get("name"), Some(&PropertyValue::String("Widget".to_string())));
    assert_eq!(node.properties.get("price"), None);
    
    tx.commit().unwrap();
}
