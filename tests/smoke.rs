#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::pager::Pager;
use sombra::storage::header::Header;
use sombra::{Edge, GraphDB, Node, Result};
use tempfile::NamedTempFile;

#[test]
fn deletion_recycles_pages_and_updates_header() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        let first = tx.add_node(Node::new(0))?;
        let second = tx.add_node(Node::new(0))?;
        tx.add_edge(Edge::new(0, first, second, "link"))?;
        tx.delete_node(first)?;
        tx.delete_node(second)?;
        tx.commit()?;
        db.checkpoint()?;
    }

    let mut pager = Pager::open(&path)?;
    let page = pager.fetch_page(0)?;
    let header = Header::read(&page.data)?.expect("graph header");
    assert!(
        header.free_page_head.is_some(),
        "expected free-page head to be populated after deletions"
    );
    Ok(())
}

#[test]
fn reopen_and_traverse_multi_edge_chains() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let root_id;
    let mut expected_neighbors = Vec::new();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        root_id = tx.add_node(Node::new(0))?;
        for _ in 0..4 {
            let id = tx.add_node(Node::new(0))?;
            tx.add_edge(Edge::new(0, root_id, id, "link"))?;
            expected_neighbors.push(id);
        }
        tx.commit()?;
        db.checkpoint()?;
    }

    {
        let mut db = GraphDB::open(&path)?;
        let mut neighbors = db.get_neighbors(root_id)?;
        neighbors.sort_unstable();
        let mut expected = expected_neighbors.clone();
        expected.sort_unstable();
        assert_eq!(neighbors, expected);

        {
            let mut tx = db.begin_transaction()?;
            for _ in 0..3 {
                let id = tx.add_node(Node::new(0))?;
                tx.add_edge(Edge::new(0, root_id, id, "link"))?;
                expected_neighbors.push(id);
            }
            tx.commit()?;
            db.checkpoint()?;
        }
    }

    {
        let mut db = GraphDB::open(&path)?;
        let mut neighbors = db.get_neighbors(root_id)?;
        neighbors.sort_unstable();
        let mut expected = expected_neighbors.clone();
        expected.sort_unstable();
        assert_eq!(neighbors, expected);
    }

    Ok(())
}
