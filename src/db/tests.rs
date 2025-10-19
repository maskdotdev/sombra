use super::*;
use crate::error::GraphError;
use crate::model::{Edge, Node};
use tempfile::NamedTempFile;

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
    let node_a = db.get_node(1).expect("get node a");
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
    assert!(matches!(db.get_node(a), Err(GraphError::NotFound("node"))));
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
    let node = db.get_node(1).expect("read committed node");
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
        matches!(db.get_node(1), Err(GraphError::NotFound("node"))),
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
    let node = reopened.get_node(1).expect("node committed after rollback");
    assert_eq!(node.id, 1);
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
