#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use std::sync::Arc;

use sombra::{
    primitives::pager::{PageStore, Pager, PagerOptions},
    storage::{
        Graph, GraphOptions, DEFAULT_INLINE_PROP_BLOB, DEFAULT_INLINE_PROP_VALUE,
        STORAGE_FLAG_DEGREE_CACHE,
    },
    types::Result,
};
use tempfile::tempdir;

#[test]
fn graph_open_initializes_metadata_defaults() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("graph.db");

    let store_arc = Arc::new(Pager::create(&db_path, PagerOptions::default())?);
    let store_trait: Arc<dyn PageStore> = store_arc.clone();
    let graph = Graph::open(GraphOptions::new(store_trait.clone()))?;
    drop(graph);

    let meta = store_arc.meta()?;
    assert_ne!(meta.storage_nodes_root.0, 0, "nodes tree root recorded");
    assert_ne!(meta.storage_edges_root.0, 0, "edges tree root recorded");
    assert_ne!(meta.storage_adj_fwd_root.0, 0, "adj_fwd tree root recorded");
    assert_ne!(meta.storage_adj_rev_root.0, 0, "adj_rev tree root recorded");
    assert_ne!(
        meta.storage_index_catalog_root.0, 0,
        "index catalog root recorded"
    );
    assert_ne!(
        meta.storage_label_index_root.0, 0,
        "label index root recorded"
    );
    assert_eq!(
        meta.storage_prop_chunk_root.0, 0,
        "chunked property index root unset by default"
    );
    assert_eq!(
        meta.storage_prop_btree_root.0, 0,
        "btree property index root unset by default"
    );
    assert!(meta.storage_next_node_id >= 1, "next node id initialized");
    assert!(meta.storage_next_edge_id >= 1, "next edge id initialized");
    assert_eq!(
        meta.storage_inline_prop_blob, DEFAULT_INLINE_PROP_BLOB,
        "default inline prop blob persisted"
    );
    assert_eq!(
        meta.storage_inline_prop_value, DEFAULT_INLINE_PROP_VALUE,
        "default inline prop value persisted"
    );
    #[cfg(feature = "degree-cache")]
    {
        assert_ne!(meta.storage_degree_root.0, 0, "degree cache root recorded");
        assert!(
            (meta.storage_flags & STORAGE_FLAG_DEGREE_CACHE) != 0,
            "degree cache flag set when feature enabled"
        );
    }
    #[cfg(not(feature = "degree-cache"))]
    {
        assert_eq!(meta.storage_degree_root.0, 0, "degree cache root cleared");
        assert_eq!(
            meta.storage_flags & STORAGE_FLAG_DEGREE_CACHE,
            0,
            "degree cache flag cleared without feature"
        );
    }
    Ok(())
}

#[test]
fn graph_open_respects_custom_inline_thresholds() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("graph_custom.db");
    let pager = Arc::new(Pager::create(&db_path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();

    let custom_blob = 256;
    let custom_value = 80;
    let graph = Graph::open(
        GraphOptions::new(store.clone())
            .inline_prop_blob(custom_blob)
            .inline_prop_value(custom_value)
            .degree_cache(cfg!(feature = "degree-cache")),
    )?;
    drop(graph);

    let meta = pager.meta()?;
    assert_eq!(
        meta.storage_inline_prop_blob, custom_blob,
        "custom inline blob threshold persisted"
    );
    assert_eq!(
        meta.storage_inline_prop_value, custom_value,
        "custom inline value threshold persisted"
    );
    Ok(())
}
