#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{GraphDB, Node, PropertyValue};
use tempfile::NamedTempFile;

#[test]
fn property_index_persists_across_restart() {
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

        let nodes = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query");
        assert_eq!(nodes.len(), 1, "Should find one node with age=42");
    }

    {
        let mut db = GraphDB::open(&path).expect("reopen db");

        let nodes = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(42))
            .expect("query after reopen");
        assert_eq!(
            nodes.len(),
            1,
            "Should still find one node with age=42 after reopen"
        );

        let nodes_50 = db
            .find_nodes_by_property("User", "age", &PropertyValue::Int(50))
            .expect("query another value");
        assert_eq!(nodes_50.len(), 1, "Should find one node with age=50");
    }
}
