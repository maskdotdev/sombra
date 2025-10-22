#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::pager::Pager;
use sombra::{GraphDB, Node, Result};
use tempfile::NamedTempFile;

#[test]
fn slot_reuse_prevents_page_growth() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut node_ids = Vec::new();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        for _ in 0..100 {
            let node_id = tx.add_node(Node::new(0))?;
            node_ids.push(node_id);
        }
        tx.commit()?;
        db.checkpoint()?;
    }

    let page_count_after_insert = Pager::open(&path)?.page_count();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        for node_id in &node_ids {
            tx.delete_node(*node_id)?;
        }
        tx.commit()?;
        db.checkpoint()?;
    }

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        for _ in 0..100 {
            tx.add_node(Node::new(0))?;
        }
        tx.commit()?;
        db.checkpoint()?;
    }

    let final_page_count = Pager::open(&path)?.page_count();

    assert!(
        final_page_count <= page_count_after_insert + 1,
        "After delete+reinsert cycle, page count should not grow significantly. Before: {}, After: {}",
        page_count_after_insert,
        final_page_count
    );

    Ok(())
}
