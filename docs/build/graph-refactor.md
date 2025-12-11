# Graph.rs Decomposition Plan

Goal: reduce `src/storage/graph.rs` (5,470 lines, ~210 methods) to under 2,000 lines in `mod.rs` by moving cohesive domains into dedicated modules while preserving API behavior.

## Target Layout
```
src/storage/graph/
├── mod.rs              # Graph struct, open(), core wiring, re-exports
├── types.rs            # Public types, enums, constants
├── snapshot.rs         # SnapshotPool, SnapshotLease, PooledSnapshot
├── version_cache.rs    # VersionCache, VersionChainRecord trait + impls
├── vacuum.rs           # VacuumSched, vacuum_mvcc, micro_gc, metrics/logging
├── mvcc_ops.rs         # Version visibility, headers, version_log accessors
├── node_ops.rs         # Node CRUD and helpers
├── edge_ops.rs         # Edge CRUD and helpers
├── adjacency_ops.rs    # Adjacency insert/remove/scan, neighbors, BFS, degree
├── index_ops.rs        # Label/property index creation, scans, stats
├── deferred_ops.rs     # Deferred adjacency/index staging + flush
├── props.rs            # Prop encoding/materialization/free helpers
├── writer.rs           # GraphWriter + BulkEdgeValidator
├── helpers.rs          # Shared free functions (encoding, patch ops, utils)
└── tests.rs            # All #[cfg(test)] modules consolidated
```

## Extraction Phases (order)
1) **types.rs**: constants; UnitValue; VacuumSched; AdjacencyBuffer; IndexBuffer; BfsOptions/BfsVisit; VersionVacuumStats/AdjacencyVacuumStats/GraphVacuumStats/GraphMvccStatus/SnapshotPoolStatus; VacuumTrigger/VacuumMode/MicroGcTrigger/MicroGcOutcome/VacuumBudget; RootKind; PropStats; CreateEdgeOptions; GraphWriterStats; GraphTxnState; PropDelta; PropertyPredicate.

2) **helpers.rs**: now_millis; tree open helpers; apply_patch_ops; encode_value_key_*; encode_range_bound/clone_owned_bound; prop_stats_key/update_min_max/compare_prop_values/value_rank/prop_value_to_owned; collect_posting_stream/instrument_posting_stream + ProfilingPostingStream; normalize_labels; encode_bytes helpers; ensure_node_exists.

3) **snapshot.rs**: PooledSnapshot; SnapshotPool; SnapshotLease (RAII, Deref, Drop).

4) **version_cache.rs**: VersionCache; VersionChainRecord trait; impls for node::VersionedNodeRow and edge::VersionedEdgeRow.

5) **props.rs**: materialize_raw_prop_value; node_property_value; node_matches_property_eq/range; bound_allows; encode_property_map; free_prop_values_from_bytes/free_node_props; read_node/edge prop bytes (read + write); materialize_props_owned (+ with_write); build_prop_delta.

6) **mvcc_ops.rs**: log_version_entry; load_version_entry; commit_table accessor; version_log_bytes/count accessors; publish_version_log_usage_metrics; oldest_reader_commit; begin_read_guard/begin_write_guard/commit_with_metrics; vacuum_retention_window; mvcc_status; tx_version_header/tx_pending_version_header; adjacency_value_for_commit; finalize_version_header; reader_snapshot_commit; version_visible/visible_version; retire_version_resources; prune_versioned_vec_tree; maybe_publish_mvcc_metrics.

7) **vacuum.rs**: request_micro_gc/drive_micro_gc; vacuum_version_log/vacuum_adjacency/vacuum_indexes; last_vacuum_stats; trigger_vacuum; micro_gc; maybe_background_vacuum; vacuum_mvcc; vacuum_version_log_with_write; maybe_signal_high_water; compute_vacuum_horizon; select_vacuum_mode; recompute_version_log_bytes; record_vacuum_stats; publish_vacuum_metrics; log_vacuum_stats; enforce_reader_timeouts; wal_health.

8) **deferred_ops.rs**: stage_adjacency_inserts/removals; flush_deferred_writes; flush_deferred_adjacency; stage_label_inserts/removals; stage_prop_index_op; flush_deferred_indexes.

9) **index_ops.rs**: create/drop/has label index; create/drop/has property index; index_catalog_root/property_index/all_property_indexes; property_scan_eq/range (+ streams); label_scan/label_scan_stream; index_cache_stats; insert_indexed_props; update_indexed_props_for_node; index_defs_for_label; bump_ddl_epoch; sync_index_roots; property_stats; PropertyFilterStream; FallbackLabelScan.

10) **node_ops.rs**: create_node; get_node/get_node_in_write; scan_all_nodes; delete_node; update_node; count_nodes_with_label; nodes_with_label; sample_node_labels; node_exists/node_exists_with_write; visible_node_from_bytes/visible_node; finalize_node_head.

11) **edge_ops.rs**: create_edge/insert_edge_unchecked/insert_edge_unchecked_inner; get_edge; scan_all_edges; update_edge; delete_edge; count_edges_with_type; visible_edge_from_bytes/visible_edge; finalize_edge_head; free_edge_props.

12) **adjacency_ops.rs**: neighbors; bfs; degree; insert_adjacencies; remove_adjacency; collect_incident_edges/collect_adjacent_edges; collect_neighbors; enqueue_bfs_neighbors; degree_single/degree_has_cache_entry; count_adjacent_edges; count_loop_edges; rollback_adjacency_batch; debug_collect_adj_fwd/debug_collect_adj_rev; adjacency_bounds_for_node.

13) **writer.rs**: constants; BulkEdgeValidator; GraphWriter struct + impl (try_new, options, stats, validate_trusted_batch, create_edge, ensure_endpoint).

14) **tests.rs**: move all #[cfg(test)] modules: vacuum_background_tests; adjacency_commit_tests; wal_recovery_tests; wal_alert_tests.

## What stays in `mod.rs` (~1.5–1.8k lines)
- Imports, module declarations, re-exports.
- Graph struct definition.
- Graph::open() and initialization wiring (roots, meta updates, caches, options).
- Drop impl; BackgroundMaintainer impl.
- Transaction-state plumbling: take/store/invalidate_txn_cache; GraphTxnState creation.
- Root persistence helpers; degree-cache root persistence.
- Degree-cache helpers that wire to adjacency where needed (or move if preferred).
- lease_latest_snapshot and other thin wrappers that coordinate modules.

## Execution Steps
1) Create module files per layout.
2) Move code in phase order above to minimize dependency churn.
3) Update `src/storage/mod.rs` to use the directory module.
4) Re-export needed items from `graph/mod.rs` to preserve public API.
5) Run `cargo test --all-features` after major chunks (types/helpers/snapshot/version_cache/props/mvcc/vacuum/deferred/index/node/edge/adjacency/writer/tests).

## Open Decisions
- Visibility: default to `pub(crate)` for internal helpers; public types re-exported from `mod.rs` to match existing API surface.
- Tests: consolidate in `graph/tests.rs` (simplifies search); ensure module cfg stays intact.
- Feature flag handling: keep `#[cfg(feature = "degree-cache")]` guards where they are; consider isolating degree-cache helpers if separation improves clarity.
- Commit cadence: prefer incremental commits per 2–3 phases to reduce risk.
