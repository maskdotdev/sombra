#![allow(clippy::uninlined_format_args)]

use sombra::{Edge, GraphDB, Node, PropertyValue};
use std::collections::BTreeMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 Transaction Handling Example\n");

    let mut db = GraphDB::open("transaction_example.db")?;

    println!("Example 1: Successful Transaction");
    {
        let mut tx = db.begin_transaction()?;

        let mut alice = Node::new(0);
        alice.labels.push("User".to_string());
        alice.properties.insert(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );
        let alice_id = tx.add_node(alice)?;

        let mut bob = Node::new(0);
        bob.labels.push("User".to_string());
        bob.properties
            .insert("name".to_string(), PropertyValue::String("Bob".to_string()));
        let bob_id = tx.add_node(bob)?;

        tx.add_edge(Edge::new(0, alice_id, bob_id, "FOLLOWS"))?;

        tx.commit()?;
        println!("✓ Transaction committed successfully");
        println!("  Created users Alice and Bob with a FOLLOWS relationship\n");
    }

    println!("Example 2: Transaction Rollback");
    {
        let mut tx = db.begin_transaction()?;

        let mut charlie = Node::new(0);
        charlie.labels.push("User".to_string());
        charlie.properties.insert(
            "name".to_string(),
            PropertyValue::String("Charlie".to_string()),
        );
        let _charlie_id = tx.add_node(charlie)?;

        println!("  Added Charlie (transaction not committed yet)");

        tx.rollback()?;
        println!("✓ Transaction rolled back");
        println!("  Charlie was not persisted to the database\n");
    }

    println!("Example 3: Multiple Operations in Transaction");
    {
        let mut tx = db.begin_transaction()?;

        let mut properties = BTreeMap::new();
        properties.insert(
            "title".to_string(),
            PropertyValue::String("Graph Databases 101".to_string()),
        );
        properties.insert("views".to_string(), PropertyValue::Int(1250));

        let mut post = Node::new(0);
        post.labels.push("Post".to_string());
        post.properties = properties;
        let post_id = tx.add_node(post)?;

        let alice_nodes = tx.get_nodes_by_label("User")?;
        if let Some(&alice_id) = alice_nodes.first() {
            tx.add_edge(Edge::new(0, alice_id, post_id, "AUTHORED"))?;
        }

        tx.commit()?;
        println!("✓ Multi-operation transaction committed");
        println!("  Created post and linked to Alice\n");
    }

    println!("Example 4: Error Handling with Transactions");
    {
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let mut tx = db.begin_transaction()?;

            let mut node = Node::new(0);
            node.labels.push("TestNode".to_string());
            let node_id = tx.add_node(node)?;

            let invalid_node_id = 999999;
            match tx.add_edge(Edge::new(0, node_id, invalid_node_id, "TEST")) {
                Ok(_) => tx.commit()?,
                Err(e) => {
                    println!("  Error detected: {}", e);
                    tx.rollback()?;
                    println!("✓ Transaction rolled back due to error");
                    return Err(e.into());
                }
            }

            Ok(())
        })();

        if result.is_err() {
            println!("  Changes were not persisted\n");
        }
    }

    println!("Example 5: Read Consistency");
    {
        let mut tx = db.begin_transaction()?;

        let users = tx.get_nodes_by_label("User")?;
        println!("✓ Read {} users within transaction", users.len());

        for user_id in users.iter().take(3) {
            let user = tx.get_node(*user_id)?;
            if let Some(PropertyValue::String(name)) = user.properties.get("name") {
                println!("  - User: {}", name);
            }
        }

        tx.commit()?;
    }

    println!("\n🎉 Transaction handling examples completed!");
    println!("Database saved to: transaction_example.db");

    Ok(())
}
