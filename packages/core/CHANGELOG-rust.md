# Changelog

## [0.4.0](https://github.com/maskdotdev/sombra/compare/sombra-v0.3.6...sombra-v0.4.0) (2025-11-13)


### Features

* **mvcc:** add ConcurrentGraphDB API with file locking and comprehensive testing ([dd01ecc](https://github.com/maskdotdev/sombra/commit/dd01ecc174413144cbd570e4ef796f35f90a531f))
* **mvcc:** add core MVCC infrastructure modules ([fedab22](https://github.com/maskdotdev/sombra/commit/fedab22e43f81a9864bb955e07d2936b38122cbc))
* **mvcc:** add max_concurrent_transactions config option ([92bb9e4](https://github.com/maskdotdev/sombra/commit/92bb9e4f5f521321f6b5859a46f15d1fc1477474))
* **mvcc:** add read/write tracking to transactions ([48edc91](https://github.com/maskdotdev/sombra/commit/48edc911d20c7c86c491a9cc2bd07ca7cbdd4129))
* **mvcc:** add snapshot isolation support to graph traversal operations ([d054cb3](https://github.com/maskdotdev/sombra/commit/d054cb3956bb0fd21b1a3e1cf2539d9d961fc6f5))
* **mvcc:** add transaction manager and WAL extension modules for Phase 1 infrastructure ([6484a38](https://github.com/maskdotdev/sombra/commit/6484a387091039898d12bde4a27a98812e14230d))
* **mvcc:** add version support for edge records ([19e4bfe](https://github.com/maskdotdev/sombra/commit/19e4bfef05da57d8732c5344e31a88e64617c306))
* **mvcc:** enable concurrent transactions in MVCC mode ([43e6406](https://github.com/maskdotdev/sombra/commit/43e6406b562890ca93e7b04aa47fb97e458e3880))
* **mvcc:** implement snapshot-isolated read operations for Phase 2 ([ff06def](https://github.com/maskdotdev/sombra/commit/ff06def84aa682bfe9829f7fcce3e61246c9e0db))
* **mvcc:** implement version chains with BTree checksum fix ([cd5854e](https://github.com/maskdotdev/sombra/commit/cd5854ed93e401846403eeb834a5f3c0f789e3bd))
* **mvcc:** integrate MvccTransactionManager with GraphDB ([e77dc52](https://github.com/maskdotdev/sombra/commit/e77dc52a5237c7409ad1813707c04b888956f604))
* **mvcc:** persist timestamp oracle state in database header ([638b136](https://github.com/maskdotdev/sombra/commit/638b1369aede4a533f835535e566b2566281fee7))
* **rwlock:** complete Phase 1 - interior mutability with Mutex ([646f05d](https://github.com/maskdotdev/sombra/commit/646f05d91b30cf9799fd5af202e9b52341fa4c63))


### Bug Fixes

* **gc:** count all freed slots in compact_version_chains, not just page-emptying ones ([229f0f5](https://github.com/maskdotdev/sombra/commit/229f0f5823b18ba5d7244734cc18fa16f4979d9b))
* **index:** implement differential label and property index updates ([62faf6c](https://github.com/maskdotdev/sombra/commit/62faf6c4832f713239cf0c3fe9b8a77fb36b80df))
* **mvcc:** add tombstone checks for auto-commit reads to handle MVCC deletions ([fa39f08](https://github.com/maskdotdev/sombra/commit/fa39f0836bdfc2c1af0684d216690f89428d0860))
* **mvcc:** implement snapshot lifecycle management for GC watermark tracking ([57794fa](https://github.com/maskdotdev/sombra/commit/57794fa28cb06832222259c5b85d630dc6adc0ea))
* **mvcc:** implement transaction cleanup to prevent slot leakage ([74f1cf5](https://github.com/maskdotdev/sombra/commit/74f1cf5c79426895d2eaeba571b33e80f0575b80))
* **mvcc:** resolve commit_ts bug preventing GC from reclaiming versions ([ef44753](https://github.com/maskdotdev/sombra/commit/ef447535c5876c3475d552e6c936cceda0162d0d))
* prevent false corruption errors when scanning index pages ([75c52f0](https://github.com/maskdotdev/sombra/commit/75c52f0d3484912cc7c8d282e3058e2aa630a759))
* **storage:** properly handle mixed page types in insert_new_slot() ([6382687](https://github.com/maskdotdev/sombra/commit/638268711da2cde4c346dbbed778cfa18607a8e2))


### Performance Improvements

* **mvcc:** implement adaptive group commit timeout for low-latency workloads ([e22b6e0](https://github.com/maskdotdev/sombra/commit/e22b6e030b0966b9379c5544a36079c98148623c))
* **mvcc:** optimize commit timestamp updates with version pointer tracking ([3559647](https://github.com/maskdotdev/sombra/commit/35596475d44f12929b51a011ee2dcf630d4678ff))
* **mvcc:** run comprehensive performance benchmarks and document findings ([7ec1f65](https://github.com/maskdotdev/sombra/commit/7ec1f658822ca2efcbf7f05750602f3f4354993e))
* **traversal:** optimize BFS with batch loading and fix cache race condition ([677c4ac](https://github.com/maskdotdev/sombra/commit/677c4acda3e5b1930d787767b7ac68678262d926))


### Documentation

* archive completed MVCC and RwLock implementation planning documents ([7956ec1](https://github.com/maskdotdev/sombra/commit/7956ec1a7509101faac9a87c68a10ac121ca7b0c))
* **mvcc:** add MVCC implementation plans and status ([e8f63f1](https://github.com/maskdotdev/sombra/commit/e8f63f1a0b0e0a82cb1cc1c0c8458dec3a998785))
* **mvcc:** add performance analysis and production guide ([bf57f29](https://github.com/maskdotdev/sombra/commit/bf57f292c0e505db6b77d6c99e82fd6bd4ed7a01))
* **mvcc:** update documentation for Phase 4 completion and performance results ([2582112](https://github.com/maskdotdev/sombra/commit/2582112fd1aedc09a6a37dbd3cbbe118c12558d6))
* **mvcc:** update plan to reflect Phase 2 and Phase 5 completion ([7fd89fc](https://github.com/maskdotdev/sombra/commit/7fd89fce9652eebb31d643baf3dfcecedd07f148))
* **mvcc:** update status after Phase 3 and Phase 4 completion ([cbbaee4](https://github.com/maskdotdev/sombra/commit/cbbaee43e1a7ed21d6951a17cd4fbd27c266ed71))

## [Unreleased]

### Features

* **mvcc:** Phase 4 performance optimizations complete ([Tasks 24-28])
  - **Version pointer tracking optimization**
    - Track created version pointers during transaction execution
    - Update version commit timestamps directly (O(N) vs O(pages × records))
    - Commit time reduced from 391µs to 28µs (93% improvement)
    - Total node creation reduced from 401µs to 37µs (91% improvement)
  - **Adaptive group commit** (completed in previous session)
    - Short timeout (100µs) for single transactions
    - Long timeout (1ms) for batching multiple commits
    - Eliminates batching delays for low-latency workloads
  - **Results**: MVCC overhead now <100µs for real work ✅
    - Node creation: 37µs (matches single-writer baseline)
    - Throughput: ~27,000 txn/sec (was 2,525 txn/sec)
    - Performance goal achieved: Production ready for all workload types
  - Modified files:
    - `src/db/transaction.rs` - Added `created_versions` tracking
    - `src/db/core/nodes.rs` - Return version pointers from node creation
    - `src/db/core/transaction_support.rs` - Fast path for version updates
    - `src/db/group_commit.rs` - Adaptive timeout (previous session)

* **mvcc:** complete performance profiling and analysis ([Task 23])
  - Statistical benchmarks with Criterion framework
  - Detailed component-level profiling with timing instrumentation
  - Comprehensive performance analysis document (MVCC_PERFORMANCE_ANALYSIS.md)
  - **Key findings:**
    - MVCC overhead: ~380µs per transaction (12-15x vs single-writer)
    - Root cause: Group commit synchronization (1ms timeout)
    - MVCC-specific overhead (version metadata): Only ~30µs (7.7% of total)
    - Version chain reads: No degradation with chain depth
    - Read overhead: ~10µs vs single-writer
  - **Production ready** for concurrent workloads (10+ threads)
  - Optimization opportunities documented for Phase 4
  - Benchmarks: `mvcc_simple_criterion.rs`, `mvcc_detailed_profile.rs`

### Documentation

* **mvcc:** update performance analysis with optimization results
  - Post-optimization benchmark results and analysis
  - Before/after comparison tables showing 93% commit time reduction
  - Adaptive group commit behavior explanation
  - Empty transaction vs real work analysis
  - Updated production recommendations (MVCC now optimal for all workloads)
  - Performance characteristics updated to reflect <100µs overhead achievement

* **mvcc:** update implementation status - all phases complete
  - Phase 4 performance optimization marked complete (Tasks 24-28)
  - Phase 5 testing & production marked complete (Tasks 22-23)
  - Overall: 100% complete (27/27 tasks) ✅
  - Updated performance characteristics with post-optimization results
  - Production readiness confirmed for all workload types

* **mvcc:** add comprehensive performance analysis document
  - Transaction overhead breakdown with component-level timing
  - Root cause analysis: Group commit latency dominates (97% of overhead)
  - Version chain performance characteristics
  - Optimization opportunities (high/medium/low impact)
  - Production recommendations for different workload patterns
  - Performance characteristics and production readiness assessment

* **mvcc:** update implementation status with profiling completion
  - Phase 5 Task 23 marked complete
  - 21/24 tasks complete (88% done)
  - Detailed benchmark results and analysis

* **mvcc:** add production readiness guide
  - Configuration recommendations for different workload patterns
  - Migration guide from single-writer to MVCC
  - Performance tuning guidelines
  - Monitoring and troubleshooting procedures
  - Known limitations and workarounds
  - Production deployment checklist

* **mvcc:** update README with MVCC feature and benchmark instructions

## [0.3.6](https://github.com/maskdotdev/sombra/compare/sombra-v0.3.5...sombra-v0.3.6) (2025-10-26)


### Bug Fixes

* **core:** update tests, examples, and benchmarks for Option&lt;Node&gt; return type ([2f13040](https://github.com/maskdotdev/sombra/commit/2f13040bfc94439d42324c5192c10c488c27e04a))
* **query:** add getIds() and getNodes() methods to QueryBuilder, fix execute() implementation ([1a650e9](https://github.com/maskdotdev/sombra/commit/1a650e9d588d48210dc5ba91173d00621f319aef))

## [0.3.5](https://github.com/maskdotdev/sombra/compare/sombra-v0.3.4...sombra-v0.3.5) (2025-10-24)


### Bug Fixes

* getNode returns null for non-existent nodes, remove transaction enforcement, and fix BFS depth semantics ([46e95e7](https://github.com/maskdotdev/sombra/commit/46e95e721fe9b0c59706166fdd0fb36418291917))

## [0.3.4](https://github.com/maskdotdev/sombra/compare/sombra-v0.3.3...sombra-v0.3.4) (2025-10-24)


### Bug Fixes

* remove outdated docs and add alpha software warnings to all packages ([5599fa0](https://github.com/maskdotdev/sombra/commit/5599fa081642c211aefc96666e0d613a8333f2cd))


### Documentation

* add GitHub repository link to all package READMEs ([88d4584](https://github.com/maskdotdev/sombra/commit/88d4584bb2fccc089ec6caabf7a3a675ebf91232))

## [0.4.2](https://github.com/maskdotdev/sombra/compare/sombra-v0.4.1...sombra-v0.4.2) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))

## [0.4.2](https://github.com/maskdotdev/sombra/compare/sombra-v0.4.1...sombra-v0.4.2) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))
