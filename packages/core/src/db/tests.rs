use super::*;
use crate::model::{Edge, Node, PropertyValue};
use tempfile::NamedTempFile;

fn create_temp_db(name: &str) -> std::path::PathBuf {
    let tmp = tempfile::Builder::new()
        .prefix(name)
        .suffix(".db")
        .tempfile()
        .expect("create temp file");
    tmp.path().to_path_buf()
}

#[test]
fn graphdb_round_trip() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin transaction");

        let mut node_a = Node::new(0);
        node_a.labels.push("Alpha".into());
        let mut node_b = Node::new(0);
        node_b.labels.push("Beta".into());

        let a_id = tx.add_node(node_a).expect("add node a");
        let b_id = tx.add_node(node_b).expect("add node b");

        let edge = Edge::new(0, a_id, b_id, "LINKS");
        tx.add_edge(edge).expect("add edge");

        tx.commit().expect("commit transaction");
        db.checkpoint().expect("checkpoint");
    }

    let mut db = GraphDB::open(&path).expect("reopen db");
    let node_a = db.get_node(1).expect("get node a").expect("node exists");
    assert_eq!(node_a.labels, vec!["Alpha".to_string()]);
    let neighbors = db.get_neighbors(1).expect("neighbors");
    assert_eq!(neighbors, vec![2]);
}

#[test]
fn delete_edge_updates_adjacency_lists() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let a = tx.add_node(Node::new(0)).expect("add node A");
    let b = tx.add_node(Node::new(0)).expect("add node B");
    let c = tx.add_node(Node::new(0)).expect("add node C");

    let edge_ab = Edge::new(0, a, b, "link");
    let edge_ac = Edge::new(0, a, c, "link");
    let ab_id = tx.add_edge(edge_ab).expect("add edge ab");
    tx.add_edge(edge_ac).expect("add edge ac");

    tx.delete_edge(ab_id).expect("delete edge");
    tx.commit().expect("commit");
    db.checkpoint().expect("checkpoint");
    let mut neighbors = db.get_neighbors(a).expect("neighbors");
    neighbors.sort_unstable();
    assert_eq!(neighbors, vec![c]);

    {
        let mut tx = db.begin_transaction().expect("begin transaction");
        tx.add_edge(Edge::new(0, a, b, "link"))
            .expect("re-add edge");
        tx.commit().expect("commit");
        db.checkpoint().expect("checkpoint");
    }
    let mut neighbors = db.get_neighbors(a).expect("neighbors after reinsert");
    neighbors.sort_unstable();
    assert_eq!(neighbors, vec![b, c]);
}

#[test]
fn delete_node_cascades_edge_removal() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let a = tx.add_node(Node::new(0)).expect("add node A");
    let b = tx.add_node(Node::new(0)).expect("add node B");
    let c = tx.add_node(Node::new(0)).expect("add node C");

    tx.add_edge(Edge::new(0, a, b, "link"))
        .expect("add edge a->b");
    tx.add_edge(Edge::new(0, c, a, "link"))
        .expect("add edge c->a");

    tx.delete_node(a).expect("delete node a");
    tx.commit().expect("commit");
    db.checkpoint().expect("checkpoint");
    assert!(
        db.get_node(a).expect("get_node succeeds").is_none(),
        "deleted node should not exist"
    );
    assert!(db.get_neighbors(c).expect("neighbors of c").is_empty());
    {
        let mut tx = db.begin_transaction().expect("begin transaction");
        tx.add_edge(Edge::new(0, c, b, "link"))
            .expect("add new edge without deleted node");
        tx.commit().expect("commit");
        db.checkpoint().expect("checkpoint");
    }
}

#[test]
fn transaction_commit_persists_changes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).expect("open db");
        let mut tx = db.begin_transaction().expect("begin transaction");
        let node_id = tx.add_node(Node::new(0)).expect("add node in tx");
        assert_eq!(node_id, 1);
        tx.commit().expect("commit transaction");
    }

    let mut db = GraphDB::open(&path).expect("reopen db");
    let node = db
        .get_node(1)
        .expect("read committed node")
        .expect("node exists");
    assert_eq!(node.id, 1);
}

#[test]
fn dropping_active_transaction_panics() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();
    let result = std::panic::catch_unwind(|| {
        let mut db = GraphDB::open(&path).expect("open db");
        let _tx = db.begin_transaction().expect("begin transaction");
    });
    assert!(result.is_err(), "dropping active transaction should panic");
}

#[test]
fn rollback_restores_state() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();
    let mut db = GraphDB::open(&path).expect("open db");
    {
        let mut tx = db.begin_transaction().expect("begin tx");
        let node_id = tx.add_node(Node::new(0)).expect("add node");
        assert_eq!(node_id, 1);
        tx.rollback().expect("rollback should succeed");
    }

    assert!(
        db.get_node(1).expect("get_node succeeds").is_none(),
        "rolled back node should not exist"
    );

    {
        let mut tx = db.begin_transaction().expect("begin second tx");
        let node_id = tx.add_node(Node::new(0)).expect("add node after rollback");
        assert_eq!(
            node_id, 1,
            "node IDs should reset after rollback to previous state"
        );
        dbg!(&tx.dirty_pages);
        assert!(
            tx.dirty_pages.iter().any(|&page_id| page_id > 0),
            "expected data pages to be tracked as dirty"
        );
        tx.commit().expect("commit second tx");
    }

    drop(db);
    let mut reopened = GraphDB::open(&path).expect("reopen db");
    let node = reopened
        .get_node(1)
        .expect("node committed after rollback")
        .expect("node exists");
    assert_eq!(node.id, 1);
}

#[test]
fn rollback_prevents_eviction_corruption() {
    use crate::db::config::Config;

    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let config = Config {
        page_cache_size: 3,
        ..Config::default()
    };
    let mut db = GraphDB::open_with_config(&path, config).expect("open db");

    let initial_node_id = {
        let mut tx = db.begin_transaction().expect("begin tx");
        let node_id = tx.add_node(Node::new(0)).expect("add initial node");
        tx.commit().expect("commit initial node");
        node_id
    };

    {
        let mut tx = db.begin_transaction().expect("begin tx");
        let node = tx
            .get_node(initial_node_id)
            .expect("get initial node")
            .expect("node exists");
        assert_eq!(node.id, initial_node_id);

        for i in 0..5 {
            let mut node = Node::new(0);
            node.labels.push(format!("Label{}", i));
            tx.add_node(node).expect("add node");
        }

        tx.rollback().expect("rollback should succeed");
    }

    let node = db
        .get_node(initial_node_id)
        .expect("initial node should still exist")
        .expect("node exists");
    assert_eq!(node.id, initial_node_id);

    for i in 0..5 {
        let result = db
            .get_node(initial_node_id + 1 + i as u64)
            .expect("get_node succeeds");
        assert!(
            result.is_none(),
            "rolled back node {} should not exist",
            initial_node_id + 1 + i as u64
        );
    }
}

#[test]
fn transaction_tracks_dirty_pages_for_mutations() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin tx");
    tx.add_node(Node::new(0)).expect("add node");
    assert!(
        !tx.dirty_pages.is_empty(),
        "expected dirty pages after node insertion"
    );
    assert!(
        tx.dirty_pages.iter().all(|&page_id| page_id > 0),
        "record pages should have positive IDs"
    );
    tx.commit().expect("commit tx");
}

#[test]
fn transaction_tracking_resets_between_transactions() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    {
        let mut tx = db.begin_transaction().expect("begin tx");
        tx.add_node(Node::new(0)).expect("add node");
        tx.commit().expect("commit tx");
    }

    let tx = db.begin_transaction().expect("begin second tx");
    assert!(
        tx.dirty_pages.is_empty(),
        "dirty page list should be empty for new transaction"
    );
    tx.commit().expect("commit empty tx");
}

#[test]
fn get_nodes_by_label_returns_matching_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let mut person1 = Node::new(0);
    person1.labels.push("Person".into());
    let p1_id = tx.add_node(person1).expect("add person1");

    let mut person2 = Node::new(0);
    person2.labels.push("Person".into());
    let p2_id = tx.add_node(person2).expect("add person2");

    let mut company = Node::new(0);
    company.labels.push("Company".into());
    let _c_id = tx.add_node(company).expect("add company");

    tx.commit().expect("commit transaction");
    db.checkpoint().expect("checkpoint");

    let person_nodes = db.get_nodes_by_label("Person").expect("get Person nodes");
    assert_eq!(person_nodes.len(), 2);
    assert!(person_nodes.contains(&p1_id));
    assert!(person_nodes.contains(&p2_id));

    let company_nodes = db.get_nodes_by_label("Company").expect("get Company nodes");
    assert_eq!(company_nodes.len(), 1);

    let nonexistent = db
        .get_nodes_by_label("NonExistent")
        .expect("get nonexistent label");
    assert_eq!(nonexistent.len(), 0);
}

#[test]
fn get_nodes_by_label_works_across_transactions() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");

    {
        let mut tx = db.begin_transaction().expect("begin transaction");
        let mut user = Node::new(0);
        user.labels.push("User".into());
        tx.add_node(user).expect("add user");
        tx.commit().expect("commit transaction");
        db.checkpoint().expect("checkpoint");
    }

    {
        let mut tx = db.begin_transaction().expect("begin transaction");
        let mut user = Node::new(0);
        user.labels.push("User".into());
        tx.add_node(user).expect("add user");
        tx.commit().expect("commit transaction");
        db.checkpoint().expect("checkpoint");
    }

    let users = db.get_nodes_by_label("User").expect("get User nodes");
    assert_eq!(users.len(), 2);
}

#[test]
fn count_edges_returns_correct_counts() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let a = tx.add_node(Node::new(0)).expect("add node A");
    let b = tx.add_node(Node::new(0)).expect("add node B");
    let c = tx.add_node(Node::new(0)).expect("add node C");
    let d = tx.add_node(Node::new(0)).expect("add node D");

    tx.add_edge(Edge::new(0, a, b, "link"))
        .expect("add edge a->b");
    tx.add_edge(Edge::new(0, a, c, "link"))
        .expect("add edge a->c");
    tx.add_edge(Edge::new(0, a, d, "link"))
        .expect("add edge a->d");
    tx.add_edge(Edge::new(0, b, a, "link"))
        .expect("add edge b->a");

    tx.commit().expect("commit");
    db.checkpoint().expect("checkpoint");

    let outgoing_a = db.count_outgoing_edges(a).expect("count outgoing for a");
    assert_eq!(outgoing_a, 3);

    let incoming_a = db.count_incoming_edges(a).expect("count incoming for a");
    assert_eq!(incoming_a, 1);

    let outgoing_b = db.count_outgoing_edges(b).expect("count outgoing for b");
    assert_eq!(outgoing_b, 1);

    let incoming_b = db.count_incoming_edges(b).expect("count incoming for b");
    assert_eq!(incoming_b, 1);

    let outgoing_c = db.count_outgoing_edges(c).expect("count outgoing for c");
    assert_eq!(outgoing_c, 0);

    let incoming_c = db.count_incoming_edges(c).expect("count incoming for c");
    assert_eq!(incoming_c, 1);
}

#[test]
fn get_incoming_neighbors_returns_correct_nodes() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let hub = tx.add_node(Node::new(0)).expect("add hub");
    let n1 = tx.add_node(Node::new(0)).expect("add n1");
    let n2 = tx.add_node(Node::new(0)).expect("add n2");
    let n3 = tx.add_node(Node::new(0)).expect("add n3");

    tx.add_edge(Edge::new(0, n1, hub, "points_to"))
        .expect("add edge n1->hub");
    tx.add_edge(Edge::new(0, n2, hub, "points_to"))
        .expect("add edge n2->hub");
    tx.add_edge(Edge::new(0, n3, hub, "points_to"))
        .expect("add edge n3->hub");
    tx.add_edge(Edge::new(0, hub, n1, "points_to"))
        .expect("add edge hub->n1");

    tx.commit().expect("commit");
    db.checkpoint().expect("checkpoint");

    let mut incoming = db
        .get_incoming_neighbors(hub)
        .expect("get incoming neighbors");
    incoming.sort_unstable();
    let mut expected = vec![n1, n2, n3];
    expected.sort_unstable();
    assert_eq!(incoming, expected);

    let incoming_n1 = db
        .get_incoming_neighbors(n1)
        .expect("get incoming neighbors for n1");
    assert_eq!(incoming_n1.len(), 1);
    assert_eq!(incoming_n1[0], hub);
}

#[test]
fn bfs_traversal_explores_graph_by_depth() {
    let tmp = NamedTempFile::new().expect("temp file");
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path).expect("open db");
    let mut tx = db.begin_transaction().expect("begin transaction");

    let root = tx.add_node(Node::new(0)).expect("add root");
    let l1_a = tx.add_node(Node::new(0)).expect("add l1_a");
    let l1_b = tx.add_node(Node::new(0)).expect("add l1_b");
    let l2_a = tx.add_node(Node::new(0)).expect("add l2_a");
    let l2_b = tx.add_node(Node::new(0)).expect("add l2_b");

    tx.add_edge(Edge::new(0, root, l1_a, "link"))
        .expect("root->l1_a");
    tx.add_edge(Edge::new(0, root, l1_b, "link"))
        .expect("root->l1_b");
    tx.add_edge(Edge::new(0, l1_a, l2_a, "link"))
        .expect("l1_a->l2_a");
    tx.add_edge(Edge::new(0, l1_b, l2_b, "link"))
        .expect("l1_b->l2_b");

    tx.commit().expect("commit");
    db.checkpoint().expect("checkpoint");

    let results = db.bfs_traversal(root, 2).expect("bfs traversal");

    assert_eq!(results.len(), 5);

    let root_result = results
        .iter()
        .find(|(id, _)| *id == root)
        .expect("find root");
    assert_eq!(root_result.1, 0);

    let l1_a_result = results
        .iter()
        .find(|(id, _)| *id == l1_a)
        .expect("find l1_a");
    assert_eq!(l1_a_result.1, 1);

    let l1_b_result = results
        .iter()
        .find(|(id, _)| *id == l1_b)
        .expect("find l1_b");
    assert_eq!(l1_b_result.1, 1);

    let l2_a_result = results
        .iter()
        .find(|(id, _)| *id == l2_a)
        .expect("find l2_a");
    assert_eq!(l2_a_result.1, 2);

    let l2_b_result = results
        .iter()
        .find(|(id, _)| *id == l2_b)
        .expect("find l2_b");
    assert_eq!(l2_b_result.1, 2);

    let shallow_results = db.bfs_traversal(root, 0).expect("shallow bfs");
    assert_eq!(shallow_results.len(), 1);
}

#[test]
fn property_index_basic_operations() {
    let temp_file = create_temp_db("property_index_basic");
    let mut db = GraphDB::open(&temp_file).expect("open db");

    let mut alice = Node::new(0);
    alice.labels.push("Person".to_string());
    alice.properties.insert(
        "name".to_string(),
        PropertyValue::String("Alice".to_string()),
    );
    alice
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));

    let mut bob = Node::new(0);
    bob.labels.push("Person".to_string());
    bob.properties
        .insert("name".to_string(), PropertyValue::String("Bob".to_string()));
    bob.properties
        .insert("age".to_string(), PropertyValue::Int(25));

    let mut charlie = Node::new(0);
    charlie.labels.push("Person".to_string());
    charlie.properties.insert(
        "name".to_string(),
        PropertyValue::String("Charlie".to_string()),
    );
    charlie
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));

    let alice_id = db.add_node(alice).expect("add alice");
    let bob_id = db.add_node(bob).expect("add bob");
    let charlie_id = db.add_node(charlie).expect("add charlie");

    db.create_property_index("Person", "age")
        .expect("create property index");

    let age_30_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .expect("find nodes with age 30");

    assert_eq!(age_30_nodes.len(), 2);
    assert!(age_30_nodes.contains(&alice_id));
    assert!(age_30_nodes.contains(&charlie_id));

    let age_25_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(25))
        .expect("find nodes with age 25");

    assert_eq!(age_25_nodes.len(), 1);
    assert_eq!(age_25_nodes[0], bob_id);

    let age_40_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(40))
        .expect("find nodes with age 40");

    assert_eq!(age_40_nodes.len(), 0);
}

#[test]
fn property_index_survives_reopen() {
    let temp_file = create_temp_db("property_index_reopen");

    let (alice_id, bob_id) = {
        let mut db = GraphDB::open(&temp_file).expect("open db");

        let mut alice = Node::new(0);
        alice.labels.push("Person".to_string());
        alice
            .properties
            .insert("age".to_string(), PropertyValue::Int(30));

        let mut bob = Node::new(0);
        bob.labels.push("Person".to_string());
        bob.properties
            .insert("age".to_string(), PropertyValue::Int(25));

        let alice_id = db.add_node(alice).expect("add alice");
        let bob_id = db.add_node(bob).expect("add bob");

        db.create_property_index("Person", "age")
            .expect("create property index");

        db.checkpoint().expect("checkpoint");

        (alice_id, bob_id)
    };

    let mut db = GraphDB::open(&temp_file).expect("reopen db");

    db.create_property_index("Person", "age")
        .expect("recreate property index");

    let age_30_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .expect("find nodes with age 30");

    assert_eq!(age_30_nodes.len(), 1);
    assert_eq!(age_30_nodes[0], alice_id);

    let age_25_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(25))
        .expect("find nodes with age 25");

    assert_eq!(age_25_nodes.len(), 1);
    assert_eq!(age_25_nodes[0], bob_id);
}

#[test]
fn property_index_with_string_values() {
    let temp_file = create_temp_db("property_index_string");
    let mut db = GraphDB::open(&temp_file).expect("open db");

    let mut alice = Node::new(0);
    alice.labels.push("Person".to_string());
    alice
        .properties
        .insert("city".to_string(), PropertyValue::String("NYC".to_string()));

    let mut bob = Node::new(0);
    bob.labels.push("Person".to_string());
    bob.properties
        .insert("city".to_string(), PropertyValue::String("SF".to_string()));

    let mut charlie = Node::new(0);
    charlie.labels.push("Person".to_string());
    charlie
        .properties
        .insert("city".to_string(), PropertyValue::String("NYC".to_string()));

    let alice_id = db.add_node(alice).expect("add alice");
    let _bob_id = db.add_node(bob).expect("add bob");
    let charlie_id = db.add_node(charlie).expect("add charlie");

    db.create_property_index("Person", "city")
        .expect("create property index");

    let nyc_nodes = db
        .find_nodes_by_property("Person", "city", &PropertyValue::String("NYC".to_string()))
        .expect("find nodes in NYC");

    assert_eq!(nyc_nodes.len(), 2);
    assert!(nyc_nodes.contains(&alice_id));
    assert!(nyc_nodes.contains(&charlie_id));
}

#[test]
fn property_index_updated_on_node_deletion() {
    let temp_file = create_temp_db("property_index_deletion");
    let mut db = GraphDB::open(&temp_file).expect("open db");

    let mut alice = Node::new(0);
    alice.labels.push("Person".to_string());
    alice
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));

    let mut bob = Node::new(0);
    bob.labels.push("Person".to_string());
    bob.properties
        .insert("age".to_string(), PropertyValue::Int(30));

    let alice_id = db.add_node(alice).expect("add alice");
    let bob_id = db.add_node(bob).expect("add bob");

    db.create_property_index("Person", "age")
        .expect("create property index");

    let age_30_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .expect("find nodes with age 30");
    assert_eq!(age_30_nodes.len(), 2);

    db.delete_node(alice_id).expect("delete alice");

    let age_30_nodes_after = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .expect("find nodes with age 30 after deletion");

    assert_eq!(age_30_nodes_after.len(), 1);
    assert_eq!(age_30_nodes_after[0], bob_id);
}

#[test]
fn property_index_falls_back_to_scan_when_not_indexed() {
    let temp_file = create_temp_db("property_index_fallback");
    let mut db = GraphDB::open(&temp_file).expect("open db");

    let mut alice = Node::new(0);
    alice.labels.push("Person".to_string());
    alice
        .properties
        .insert("age".to_string(), PropertyValue::Int(30));

    let alice_id = db.add_node(alice).expect("add alice");

    let age_30_nodes = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
        .expect("find nodes with age 30 without index");

    assert_eq!(age_30_nodes.len(), 1);
    assert_eq!(age_30_nodes[0], alice_id);
}

#[test]
fn property_index_persists_across_checkpoint_and_reopen() {
    let temp_file = create_temp_db("property_index_persistence");

    {
        let mut db = GraphDB::open(&temp_file).expect("open db");

        let mut alice = Node::new(0);
        alice.labels.push("Person".to_string());
        alice.properties.insert(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );
        alice
            .properties
            .insert("age".to_string(), PropertyValue::Int(30));

        let mut bob = Node::new(0);
        bob.labels.push("Person".to_string());
        bob.properties
            .insert("name".to_string(), PropertyValue::String("Bob".to_string()));
        bob.properties
            .insert("age".to_string(), PropertyValue::Int(25));

        let mut charlie = Node::new(0);
        charlie.labels.push("Person".to_string());
        charlie.properties.insert(
            "name".to_string(),
            PropertyValue::String("Charlie".to_string()),
        );
        charlie
            .properties
            .insert("age".to_string(), PropertyValue::Int(30));

        let alice_id = db.add_node(alice).expect("add alice");
        let _bob_id = db.add_node(bob).expect("add bob");
        let charlie_id = db.add_node(charlie).expect("add charlie");

        db.create_property_index("Person", "age")
            .expect("create age index");
        db.create_property_index("Person", "name")
            .expect("create name index");

        let age_30_nodes = db
            .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
            .expect("find age 30");
        assert_eq!(age_30_nodes.len(), 2);
        assert!(age_30_nodes.contains(&alice_id));
        assert!(age_30_nodes.contains(&charlie_id));

        db.checkpoint().expect("checkpoint");
    }

    {
        let mut db = GraphDB::open(&temp_file).expect("reopen db");

        let age_30_nodes = db
            .find_nodes_by_property("Person", "age", &PropertyValue::Int(30))
            .expect("find age 30 after reopen");
        assert_eq!(age_30_nodes.len(), 2);

        let age_25_nodes = db
            .find_nodes_by_property("Person", "age", &PropertyValue::Int(25))
            .expect("find age 25 after reopen");
        assert_eq!(age_25_nodes.len(), 1);

        let alice_nodes = db
            .find_nodes_by_property(
                "Person",
                "name",
                &PropertyValue::String("Alice".to_string()),
            )
            .expect("find alice by name");
        assert_eq!(alice_nodes.len(), 1);

        let nonexistent_nodes = db
            .find_nodes_by_property("Person", "age", &PropertyValue::Int(99))
            .expect("find nonexistent age");
        assert_eq!(nonexistent_nodes.len(), 0);
    }
}
