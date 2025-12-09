# Changelog

## [0.4.0](https://github.com/maskdotdev/sombra/compare/sombra-v0.3.6...sombra-v0.4.0) (2025-12-09)


### Features

* add MVCC tombstones and filtering to adjacency/indexes ([2c34e39](https://github.com/maskdotdev/sombra/commit/2c34e399cc8f2e1d3cfeca7d04427b11473e21bf))
* add realistic benchmarks for Rust, Node.js, and Python ([15d4caa](https://github.com/maskdotdev/sombra/commit/15d4caa36e6d53b1a7f885ab72be54328bfae298))
* **admin:** surface MVCC reader metrics ([654daa5](https://github.com/maskdotdev/sombra/commit/654daa506dd6f105e255711e4cabd3dc24a40a27))
* **api:** add /api/graph/full endpoint for complete graph retrieval ([c659ba7](https://github.com/maskdotdev/sombra/commit/c659ba74ebce93fb9ec7e31e785e7ea7a590d8fd))
* **bindings:** add stream close API for Node.js and Python ([e508c67](https://github.com/maskdotdev/sombra/commit/e508c67fb4369ea6778cf75d913db99915d3226c))
* **dashboard:** add fetchFullGraph API and graph types ([560e024](https://github.com/maskdotdev/sombra/commit/560e0241f65672d020598c5c3edbf3e64bc656b2))
* **dashboard:** add PropertyValue to graph canvas and query utils ([068f12b](https://github.com/maskdotdev/sombra/commit/068f12b7745c76bdd4f5930a27d68505735948c4))
* **dashboard:** auto-load full graph and add visual query builder ([528de2a](https://github.com/maskdotdev/sombra/commit/528de2a0f28edf8e14ec149f797d35c4d682f4eb))
* **dashboard:** enhance NodeDetails with categorized properties ([b9ea963](https://github.com/maskdotdev/sombra/commit/b9ea9635f43bffc015d7e65544c79ccf2702b9c7))
* document release workflow and repo overview ([4d45080](https://github.com/maskdotdev/sombra/commit/4d45080544aad929c048b9c617bb2c36e07dbcd0))
* **ffi:** add checkpoint on close and explicit checkpoint method ([b191605](https://github.com/maskdotdev/sombra/commit/b191605d5a41ea0a702b7f7d8b8b7281858c9bb1))
* **graph:** add Reagraph explorer and embed dashboard bundle ([3d333c8](https://github.com/maskdotdev/sombra/commit/3d333c8136d8d566c953b03b9c0171d89529cc7f))
* **node:** add stream close API and enhance QueryStream ([849d3b1](https://github.com/maskdotdev/sombra/commit/849d3b164dfdede49a86bbc455f3fca1417f3480))
* **python:** enhance QueryStream with context manager and close support ([6111bfd](https://github.com/maskdotdev/sombra/commit/6111bfde0b7e5af4eb581b983621a9b934417086))
* ship dashboard stack and typed import tooling ([436c9ff](https://github.com/maskdotdev/sombra/commit/436c9fff97592a5e07d8ef6df7be5a93ee4355f8))
* **storage:** add async fsync durability + wal segmentation ([b738329](https://github.com/maskdotdev/sombra/commit/b738329217ec47b967cf67fe4d71d70ea9d503c3))
* **storage:** add GraphWriter bulk writer ([bd8aec9](https://github.com/maskdotdev/sombra/commit/bd8aec982da95204d4b10b300e05ca2c9d4f6e70))
* **storage:** add MVCC cleanup helpers ([eec2917](https://github.com/maskdotdev/sombra/commit/eec29178fd22c4e1feccd5ca1a5e0d4e822f1deb))
* **storage:** track oldest MVCC reader horizons ([1b46fcd](https://github.com/maskdotdev/sombra/commit/1b46fcd624a9f1135e8ed7e5cabd3cc00fca9095))
* store MVCC headers in adjacency and index entries ([9a29ba4](https://github.com/maskdotdev/sombra/commit/9a29ba4f41c76baeba280ce10c8051c7bf54e7cf))


### Bug Fixes

* default begin_read to LatestCommitted for backward compatibility ([4c946ad](https://github.com/maskdotdev/sombra/commit/4c946ad5ea61387ed26d0d83105805010105a79b))
* **mvcc:** prevent readers from seeing corrupted pages after rollback ([c0dde68](https://github.com/maskdotdev/sombra/commit/c0dde68b4733585f8220a9c5f8adf2c92a18c0b6))
* **pager:** improve eviction algorithm and dynamic cache expansion ([1692b64](https://github.com/maskdotdev/sombra/commit/1692b648c0777f03a830600f1d2dc58edfec9dbb))
* resolve clippy warnings in integration tests ([450cfe5](https://github.com/maskdotdev/sombra/commit/450cfe5198a1dbb34b0500cbe7ff6deda96afc2e))
* resolve clippy warnings in library and benchmarks ([7c13b83](https://github.com/maskdotdev/sombra/commit/7c13b8372da32e2bc4c04d17e363f4bcf006e9c2))
* resolve compilation errors from new-mvcc merge ([5454f36](https://github.com/maskdotdev/sombra/commit/5454f362cc2573dba619f6a1e1a7ebe48a88c643))
* restore missing pager methods and resolve compilation errors after monolithic migration ([167830c](https://github.com/maskdotdev/sombra/commit/167830c3639053e10ecb57b995634f27bddfabe9))
* **storage:** correct PostingStream next_batch return value ([46c1dfb](https://github.com/maskdotdev/sombra/commit/46c1dfbcec8050b4e767b50f130cb8273eb28e06))


### Performance Improvements

* **storage:** tune B-tree inserts and pager/WAL ([e6f8413](https://github.com/maskdotdev/sombra/commit/e6f841387e87477bc9f5365e4dd2a60227e3a231))


### Documentation

* add bindings production readiness and MVCC hardening plans ([46a2699](https://github.com/maskdotdev/sombra/commit/46a2699314d3eca4b8c33e06d7d6d7a9c614937c))
* add comprehensive documentation to query and storage modules ([c67049b](https://github.com/maskdotdev/sombra/commit/c67049ba3e3d39cc5562eabd016212a529fdcd2f))
* add documentation to admin and CLI modules ([d4e6020](https://github.com/maskdotdev/sombra/commit/d4e6020879d1dfa616b79656cab079beac099834))
* add documentation to pager, query AST, and admin verify modules ([53fc174](https://github.com/maskdotdev/sombra/commit/53fc174f47deccb9ce1951fe25b429423683760c))
* add documentation to pager, query profile, storage metrics and catalog ([ff67475](https://github.com/maskdotdev/sombra/commit/ff67475415dd18eac4ff14407d915c0cdb068a23))
* add isolation guarantees documentation for MVCC implementation ([41dacf7](https://github.com/maskdotdev/sombra/commit/41dacf79fd8154081372b580bd2d09c0db2c4177))
* add performance section to Node.js and Python READMEs ([0ead1a1](https://github.com/maskdotdev/sombra/commit/0ead1a1db38c119128f3b1186037fe99cdcbd4a2))
* capture current leaf allocator performance ([b718070](https://github.com/maskdotdev/sombra/commit/b71807081a4a2e7184b8ab856974281be21bd5ad))
* capture MVCC baseline and commit table ([ba962b2](https://github.com/maskdotdev/sombra/commit/ba962b29e8094f7e481471f7894cb214a2224bd0))
* capture mvcc baseline and wal replay ([f578342](https://github.com/maskdotdev/sombra/commit/f578342e9a1b734fb1a45cabbf855079624ab92e))
* describe async fsync durability telemetry ([9eddc1d](https://github.com/maskdotdev/sombra/commit/9eddc1dd7e10731b3860ccb0ad16422ccf0905d8))
* **js:** refresh node bindings README ([4915fb2](https://github.com/maskdotdev/sombra/commit/4915fb2ca28772eddb852ce5fdbbc12fc8e5b37c))
* log allocator cache follow-up run ([7893671](https://github.com/maskdotdev/sombra/commit/789367119feb41f4fda31b1f81443aa01addf93f))
* remove obsolete migration guides and update build plans ([8471d97](https://github.com/maskdotdev/sombra/commit/8471d976e3dbfa16526ecb7b1213d7d6c6a3ee09))
* update changelogs with stream close and error handling improvements ([fc8ba88](https://github.com/maskdotdev/sombra/commit/fc8ba88b4ec442b83f6d1c2340fe708a99e80454))
* update READMEs with stream close and error handling examples ([84462a0](https://github.com/maskdotdev/sombra/commit/84462a0194008b3a3cca289a243f4ee9df46c115))

## Changelog (Rust crate)

All notable changes to the Rust crate will be documented in this file by Release Please.
