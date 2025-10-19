use sombra::{Edge, GraphDB, Node, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;

    let center = db.add_node(Node::new(0))?;
    let a = db.add_node(Node::new(0))?;
    let b = db.add_node(Node::new(0))?;
    let c = db.add_node(Node::new(0))?;

    db.add_edge(Edge::new(0, center, a, "connect"))?;
    db.add_edge(Edge::new(0, center, b, "connect"))?;
    let edge_c = db.add_edge(Edge::new(0, center, c, "connect"))?;
    db.flush()?;

    let mut neighbors = db.get_neighbors(center)?;
    neighbors.sort_unstable();
    println!("Initial neighbors: {neighbors:?}");

    db.delete_edge(edge_c)?;
    db.delete_node(a)?;
    db.flush()?;

    let mut after_delete = db.get_neighbors(center)?;
    after_delete.sort_unstable();
    println!("After deletions: {after_delete:?}");

    let d = db.add_node(Node::new(0))?;
    db.add_edge(Edge::new(0, center, d, "connect"))?;
    db.flush()?;

    let mut final_neighbors = db.get_neighbors(center)?;
    final_neighbors.sort_unstable();
    println!("After reinsertion: {final_neighbors:?}");

    Ok(())
}
