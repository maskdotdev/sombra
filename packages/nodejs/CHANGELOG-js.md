# Changelog

## [0.6.0](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.5.4...sombrajs-v0.6.0) (2025-10-30)


### Features

* **mvcc:** implement version chains with BTree checksum fix ([cd5854e](https://github.com/maskdotdev/sombra/commit/cd5854ed93e401846403eeb834a5f3c0f789e3bd))

## [0.5.4](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.5.3...sombrajs-v0.5.4) (2025-10-27)


### Bug Fixes

* **js:** trying to fix npm publish pipeline ([edd113b](https://github.com/maskdotdev/sombra/commit/edd113bb4fc01c392e961c0ba59ae85237f3153d))
* **js:** trying to fix npm publish pipeline ([9c727a3](https://github.com/maskdotdev/sombra/commit/9c727a32edf2a5f60c0caa1c82ec0099c8d58b06))

## [0.5.3](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.5.2...sombrajs-v0.5.3) (2025-10-27)


### Bug Fixes

* scope npm packages under [@unyth](https://github.com/unyth) ([c5fb560](https://github.com/maskdotdev/sombra/commit/c5fb560b3d72cc45e82a2e03c0469318b5b61775))

## [0.5.2](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.5.1...sombrajs-v0.5.2) (2025-10-27)


### Bug Fixes

* **js:** adding platform builds ([c1e0b19](https://github.com/maskdotdev/sombra/commit/c1e0b19da2bf6fba9cb476ab194634fd0839869f))

## [0.5.1](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.5.0...sombrajs-v0.5.1) (2025-10-27)


### Bug Fixes

* **js:** align optional package manifests with binary names ([5cd21b3](https://github.com/maskdotdev/sombra/commit/5cd21b3a9615ac13701cd9395cc0ba4c8e6fb13f))

## [0.5.0](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.15...sombrajs-v0.5.0) (2025-10-27)


### Features

* **node:** add N-API bindings for database inspection and verification ([e9624eb](https://github.com/maskdotdev/sombra/commit/e9624eb9273fc6e28b9e1deb0e9091b85bcb109f))

## [0.4.15](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.14...sombrajs-v0.4.15) (2025-10-26)


### Bug Fixes

* **query:** add getIds() and getNodes() methods to QueryBuilder, fix execute() implementation ([1a650e9](https://github.com/maskdotdev/sombra/commit/1a650e9d588d48210dc5ba91173d00621f319aef))

## [0.4.14](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.13...sombrajs-v0.4.14) (2025-10-25)


### Bug Fixes

* **js:** add multi-label node support with union type semantics and IDE autocomplete ([f585129](https://github.com/maskdotdev/sombra/commit/f5851293a343967a9b587de181faf362d822f573))

## [0.4.13](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.12...sombrajs-v0.4.13) (2025-10-25)


### Bug Fixes

* **js:** implement unified SombraDB API with optional TypeScript generics ([8c1317e](https://github.com/maskdotdev/sombra/commit/8c1317e631861a8f883456704835c5ae6dc9ae77))

## [Unreleased]

### Features

* **Unified API**: Single `SombraDB` class now works with or without TypeScript generics
  - Use `new SombraDB<Schema>('./db')` for full type safety with autocomplete
  - Use `new SombraDB('./db')` for backwards-compatible raw API
  - Auto-detects input format (typed vs raw properties)
  - Always returns plain JavaScript values (no manual `{type, value}` objects in typed mode)
  - Removed separate `TypedSombraDB` class (replaced by generic parameter)
  - All examples and tests updated to use unified API

### Documentation

* Updated README with unified API examples showing both usage patterns
* Added comprehensive JSDoc comments to all methods in `typed.js`

### Breaking Changes

* Removed `createTypedDB()` helper function (use `new SombraDB<Schema>()` instead)
* Removed separate `typed-wrapper.js` and `typed-wrapper.d.ts` files
* Main entry point now uses `typed.js` and `typed.d.ts` for the unified API

### Migration Guide

**Before (v0.4.11 and earlier):**
```typescript
import { createTypedDB } from 'sombradb/typed';
const db = createTypedDB<MySchema>('./db');
```

**After (v0.5.0+):**
```typescript
import { SombraDB } from 'sombradb';
const db = new SombraDB<MySchema>('./db');
```

The raw/backwards-compatible API remains unchanged.

## [0.4.12](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.11...sombrajs-v0.4.12) (2025-10-25)


### Bug Fixes

* **js:** fixing dx of types/functions etc ([a497034](https://github.com/maskdotdev/sombra/commit/a4970340703c58bccd31a6c49877b92e19d540d5))

## [0.4.11](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.10...sombrajs-v0.4.11) (2025-10-24)


### Bug Fixes

* getNode returns null for non-existent nodes, remove transaction enforcement, and fix BFS depth semantics ([46e95e7](https://github.com/maskdotdev/sombra/commit/46e95e721fe9b0c59706166fdd0fb36418291917))

## [0.4.10](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.9...sombrajs-v0.4.10) (2025-10-24)


### Bug Fixes

* **js:** nodejs example formatting ([6e6a4bc](https://github.com/maskdotdev/sombra/commit/6e6a4bc175a7e68151c2588aaa048848c78af3e4))

## [0.4.9](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.8...sombrajs-v0.4.9) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** add alpha software warning to README ([b53ca22](https://github.com/maskdotdev/sombra/commit/b53ca223fda6e7b627d6f5dc9fcdabc0716ccafd))
* **js:** add bin/sombra.js to nodejs package ([9e693de](https://github.com/maskdotdev/sombra/commit/9e693de426b5297497efa7cf1bd6f5baab4f1490))
* **js:** example format ([5226a51](https://github.com/maskdotdev/sombra/commit/5226a51f34f7ca5df258f94de78fa2c561208316))
* **js:** examples cleanup ([d6c5f2a](https://github.com/maskdotdev/sombra/commit/d6c5f2a76ef104d7e5c3198d9ef229646c171d18))
* **js:** improve TypeScript example formatting and type safety ([6b0645e](https://github.com/maskdotdev/sombra/commit/6b0645e39c2862ef7b2ef2ea983074379ed39665))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))
* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))
* **js:** remove outdated note from README ([9e4b3b4](https://github.com/maskdotdev/sombra/commit/9e4b3b48098b11ac622afcdd7bbd5245bd751987))
* **js:** skip optional package publish during prepublish ([903afc5](https://github.com/maskdotdev/sombra/commit/903afc5af56a3a6f41602183fe8641cacfa004a5))
* **js:** update ci and agent ([9b7f8d4](https://github.com/maskdotdev/sombra/commit/9b7f8d476fea64cd7886a236d4484f44d43bca41))
* **js:** use correct --no-gh-release flag in prepublishOnly script ([3427881](https://github.com/maskdotdev/sombra/commit/34278811e0ea2b7a07bfb3a0cacb9f5d8b41c01a))
* remove unused root sombra.d.ts and correct TypeScript definitions path ([9dd6a91](https://github.com/maskdotdev/sombra/commit/9dd6a91318f984e2440ca401be40c3105331f96b))


### Documentation

* add GitHub repository link to all package READMEs ([88d4584](https://github.com/maskdotdev/sombra/commit/88d4584bb2fccc089ec6caabf7a3a675ebf91232))
* **js:** add automated release note to README ([6cc0c68](https://github.com/maskdotdev/sombra/commit/6cc0c684a5e5ccfd3f17c371cc061254ad51ad3c))

## [0.4.8](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.7...sombrajs-v0.4.8) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** add alpha software warning to README ([b53ca22](https://github.com/maskdotdev/sombra/commit/b53ca223fda6e7b627d6f5dc9fcdabc0716ccafd))
* **js:** add bin/sombra.js to nodejs package ([9e693de](https://github.com/maskdotdev/sombra/commit/9e693de426b5297497efa7cf1bd6f5baab4f1490))
* **js:** example format ([5226a51](https://github.com/maskdotdev/sombra/commit/5226a51f34f7ca5df258f94de78fa2c561208316))
* **js:** examples cleanup ([d6c5f2a](https://github.com/maskdotdev/sombra/commit/d6c5f2a76ef104d7e5c3198d9ef229646c171d18))
* **js:** improve TypeScript example formatting and type safety ([6b0645e](https://github.com/maskdotdev/sombra/commit/6b0645e39c2862ef7b2ef2ea983074379ed39665))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))
* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))
* **js:** remove outdated note from README ([9e4b3b4](https://github.com/maskdotdev/sombra/commit/9e4b3b48098b11ac622afcdd7bbd5245bd751987))
* **js:** skip optional package publish during prepublish ([903afc5](https://github.com/maskdotdev/sombra/commit/903afc5af56a3a6f41602183fe8641cacfa004a5))
* **js:** update ci and agent ([9b7f8d4](https://github.com/maskdotdev/sombra/commit/9b7f8d476fea64cd7886a236d4484f44d43bca41))
* **js:** use correct --no-gh-release flag in prepublishOnly script ([3427881](https://github.com/maskdotdev/sombra/commit/34278811e0ea2b7a07bfb3a0cacb9f5d8b41c01a))
* remove unused root sombra.d.ts and correct TypeScript definitions path ([9dd6a91](https://github.com/maskdotdev/sombra/commit/9dd6a91318f984e2440ca401be40c3105331f96b))


### Documentation

* **js:** add automated release note to README ([6cc0c68](https://github.com/maskdotdev/sombra/commit/6cc0c684a5e5ccfd3f17c371cc061254ad51ad3c))

## [0.4.7](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.6...sombrajs-v0.4.7) (2025-10-24)


### Bug Fixes

* remove unused root sombra.d.ts and correct TypeScript definitions path ([9dd6a91](https://github.com/maskdotdev/sombra/commit/9dd6a91318f984e2440ca401be40c3105331f96b))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** add alpha software warning to README ([b53ca22](https://github.com/maskdotdev/sombra/commit/b53ca223fda6e7b627d6f5dc9fcdabc0716ccafd))
* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))
* **js:** remove outdated note from README ([9e4b3b4](https://github.com/maskdotdev/sombra/commit/9e4b3b48098b11ac622afcdd7bbd5245bd751987))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))
* **js:** remove outdated note from README ([9e4b3b4](https://github.com/maskdotdev/sombra/commit/9e4b3b48098b11ac622afcdd7bbd5245bd751987))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))
* **js:** remove outdated note from README ([9e4b3b4](https://github.com/maskdotdev/sombra/commit/9e4b3b48098b11ac622afcdd7bbd5245bd751987))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** remove obsolete optionalDependencies causing 404s ([9f62ac7](https://github.com/maskdotdev/sombra/commit/9f62ac7d364f6b44a3bd9fa46e70657e2cfa273f))

## [0.4.5](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.4...sombrajs-v0.4.5) (2025-10-24)


### Bug Fixes

* **js:** examples cleanup ([d6c5f2a](https://github.com/maskdotdev/sombra/commit/d6c5f2a76ef104d7e5c3198d9ef229646c171d18))
* **js:** improve TypeScript example formatting and type safety ([6b0645e](https://github.com/maskdotdev/sombra/commit/6b0645e39c2862ef7b2ef2ea983074379ed39665))


### Documentation

* **js:** add automated release note to README ([6cc0c68](https://github.com/maskdotdev/sombra/commit/6cc0c684a5e5ccfd3f17c371cc061254ad51ad3c))

## [0.4.8](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.7...sombrajs-v0.4.8) (2025-10-24)


### Bug Fixes

* **js:** improve TypeScript example formatting and type safety ([6b0645e](https://github.com/maskdotdev/sombra/commit/6b0645e39c2862ef7b2ef2ea983074379ed39665))

## [0.4.7](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.6...sombrajs-v0.4.7) (2025-10-24)


### Documentation

* **js:** add automated release note to README ([6cc0c68](https://github.com/maskdotdev/sombra/commit/6cc0c684a5e5ccfd3f17c371cc061254ad51ad3c))

## [0.4.6](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.5...sombrajs-v0.4.6) (2025-10-24)


### Bug Fixes

* **js:** add bin/sombra.js to nodejs package ([9e693de](https://github.com/maskdotdev/sombra/commit/9e693de426b5297497efa7cf1bd6f5baab4f1490))
* **js:** examples cleanup ([d6c5f2a](https://github.com/maskdotdev/sombra/commit/d6c5f2a76ef104d7e5c3198d9ef229646c171d18))
* **js:** skip optional package publish during prepublish ([903afc5](https://github.com/maskdotdev/sombra/commit/903afc5af56a3a6f41602183fe8641cacfa004a5))
* **js:** update ci and agent ([9b7f8d4](https://github.com/maskdotdev/sombra/commit/9b7f8d476fea64cd7886a236d4484f44d43bca41))
* **js:** use correct --no-gh-release flag in prepublishOnly script ([3427881](https://github.com/maskdotdev/sombra/commit/34278811e0ea2b7a07bfb3a0cacb9f5d8b41c01a))

## [0.4.5](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.4...sombrajs-v0.4.5) (2025-10-24)


### Bug Fixes

* **js:** update ci and agent ([9b7f8d4](https://github.com/maskdotdev/sombra/commit/9b7f8d476fea64cd7886a236d4484f44d43bca41))

## [0.4.4](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.3...sombrajs-v0.4.4) (2025-10-24)


### Bug Fixes

* **js:** update ci and agent ([9b7f8d4](https://github.com/maskdotdev/sombra/commit/9b7f8d476fea64cd7886a236d4484f44d43bca41))

## [0.4.3](https://github.com/maskdotdev/sombra/compare/sombrajs-v0.4.2...sombrajs-v0.4.3) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** example format ([5226a51](https://github.com/maskdotdev/sombra/commit/5226a51f34f7ca5df258f94de78fa2c561208316))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))

## [0.4.2](https://github.com/maskdotdev/sombra/compare/v0.4.1...v0.4.2) (2025-10-24)


### Bug Fixes

* **core:** align all packages to v0.4.1 after monorepo restructure ([66e9a69](https://github.com/maskdotdev/sombra/commit/66e9a69fc433064a43c8dd50ef2bac25e49fdf02))
* **js:** example format ([5226a51](https://github.com/maskdotdev/sombra/commit/5226a51f34f7ca5df258f94de78fa2c561208316))
* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))
