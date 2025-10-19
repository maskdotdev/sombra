use sombra::pager::Pager;
use sombra::storage::header::Header;
use sombra::{Edge, GraphDB, Node, Result};
use tempfile::NamedTempFile;

const STRESS_NEIGHBORS: usize = 200;

#[test]
fn mixed_insert_delete_cycles_preserve_structure() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let root_id;
    let mut neighbor_ids = Vec::with_capacity(STRESS_NEIGHBORS);

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        root_id = tx.add_node(Node::new(0))?;
        for _ in 0..STRESS_NEIGHBORS {
            let node_id = tx.add_node(Node::new(0))?;
            tx.add_edge(Edge::new(0, root_id, node_id, "connect"))?;
            neighbor_ids.push(node_id);
        }
        tx.commit()?;
        db.checkpoint()?;
    }

    let baseline_page_count = Pager::open(&path)?.page_count();

    {
        let mut db = GraphDB::open(&path)?;
        let neighbors = db.get_neighbors(root_id)?;
        assert_eq!(neighbors.len(), STRESS_NEIGHBORS);

        {
            let mut tx = db.begin_transaction()?;
            for (idx, node_id) in neighbor_ids.iter().enumerate() {
                if idx % 2 == 0 {
                    tx.delete_node(*node_id)?;
                }
            }
            tx.commit()?;
            db.checkpoint()?;
        }
    }

    let (_after_delete_pages, _free_list_after_delete) = {
        let mut pager = Pager::open(&path)?;
        let page = pager.fetch_page(0)?;
        let header = Header::read(&page.data)?.expect("graph header");
        (pager.page_count(), header.free_page_head)
    };

    {
        let mut db = GraphDB::open(&path)?;
        let neighbors = db.get_neighbors(root_id)?;
        assert_eq!(neighbors.len(), STRESS_NEIGHBORS / 2);

        {
            let mut tx = db.begin_transaction()?;
            for _ in 0..(STRESS_NEIGHBORS / 2) {
                let node_id = tx.add_node(Node::new(0))?;
                tx.add_edge(Edge::new(0, root_id, node_id, "connect"))?;
            }
            tx.commit()?;
            db.checkpoint()?;
        }
    }

    let final_neighbors = {
        let mut db = GraphDB::open(&path)?;
        let neighbors = db.get_neighbors(root_id)?;
        assert_eq!(
            neighbors.len(),
            STRESS_NEIGHBORS,
            "neighbor traversal should recover all edges after reinsertion"
        );
        db.checkpoint()?;
        neighbors.len()
    };

    assert_eq!(
        final_neighbors, STRESS_NEIGHBORS,
        "neighbor traversal should recover all edges after reinsertion"
    );

    let final_page_count = {
        let mut pager = Pager::open(&path)?;
        let _ = pager.fetch_page(0)?;
        pager.page_count()
    };

    assert!(
        final_page_count <= baseline_page_count + 1,
        "stress cycle should not allocate significantly more pages: baseline={baseline_page_count}, final={final_page_count}"
    );

    Ok(())
}
