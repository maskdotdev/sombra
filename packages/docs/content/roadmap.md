# Graphite Roadmap

This backlog captures the next set of milestones now that the MVP graph store is in place. Each section groups actionable work items that can be lifted into an issue tracker unchanged. Status uses the `TODO`/`IN PROGRESS`/`DONE` vocabulary to keep tooling-friendly greps simple.

## Milestone: Write-Ahead Logging (WAL)

- **Goal:** Guarantee durability across crashes by journaling page mutations before they reach the main database file.
- **Status:** DONE
- **Owner:** Unassigned
- **Tasks:**
  - [x] Specify WAL file layout (magic, versioning, page image framing, checksum).
  - [x] Extend the pager to stage dirty pages behind a `WalWriter` abstraction that can fsync independently.
  - [x] Implement WAL checkpointing that replays committed entries back into the main file.
  - [x] Add power-failure simulation tests that interrupt writes mid-flight and verify recovery.
- **Notes:** Depends on stable page identifiers and deterministic serialization (already established).

## Milestone: Free Space Management & Compaction

- **Goal:** Reclaim fragmented space and recycle fully free pages without manual intervention.
- **Status:** TODO
- **Owner:** Unassigned
- **Tasks:**
  - [ ] Add fragmentation accounting to `RecordPage` so the compactor can prioritize messy pages.
  - [ ] Build a `CompactionRunner` that copies live records into fresh pages and returns emptied pages to the free list.
  - [ ] Teach the header to persist compaction heuristics (last run LSN, fragmentation watermark).
  - [ ] Stress-test workloads with heavy churn to ensure adjacency pointers remain correct after compaction.
- **Notes:** Shares plumbing with WAL checkpointing; plan execution order accordingly.

## Milestone: Indexing & Query Ergonomics

- **Goal:** Provide indexed access paths for common lookups and expressive traversal APIs.
- **Status:** TODO
- **Owner:** Unassigned
- **Tasks:**
  - [ ] Prototype a label index that maps string labels to node ID sets stored as sorted vectors per page.
  - [ ] Design a property index format (likely hash buckets pointing at record offsets) with pluggable backends.
  - [ ] Extend `GraphDB` with traversal builders supporting label/property predicates and edge direction filters.
  - [ ] Benchmark traversal throughput with and without indexes; record baselines in `docs/benchmarks.md`.
- **Notes:** Requires deterministic node/edge serialization; optional dependency on WAL if concurrent writers are enabled first.

## Milestone: Concurrency & Session Management

- **Goal:** Allow multiple clients to interact with the database safely, starting with reader/writer coordination.
- **Status:** TODO
- **Owner:** Unassigned
- **Tasks:**
  - [ ] Introduce a lightweight lock manager that supports shared (read) and exclusive (write) locks per page.
  - [ ] Add session handles that cache cursors/pagers per connection to reduce contention.
  - [ ] Define transaction boundaries (begin/commit/rollback) layered on top of WAL infrastructure.
  - [ ] Develop integration tests that spawn concurrent readers/writers and assert absence of deadlocks.
- **Notes:** Depends on WAL foundations; staging plan is to land WAL before enabling write concurrency.
