# Changelog

All notable changes to the Sombra CLI will be documented in this file.

## [0.3.5](https://github.com/maskdotdev/sombra/compare/cli-v0.3.4...cli-v0.3.5) (2025-10-27)


### Bug Fixes

* **cli:** test and deps path resolution ([5169ed4](https://github.com/maskdotdev/sombra/commit/5169ed45eee7c3bfd3fa10093a2f1d1a6f40f103))

## [0.3.5] - 2025-10-27

### Bug Fixes

* **cli:** properly handle sombradb native binding errors instead of treating them as "not found"
  - Previously, when sombradb was found but failed to load (e.g., native binding mismatch), the CLI would treat it as "not found" and continue searching
  - Now the CLI properly detects when a package exists but has a load error, and displays a helpful error message with solutions
  - Improved error messages for native binding compatibility issues with actionable solutions
* **cli:** support Bun installs by auto-selecting an available package manager
  - Detect Bun-based environments and use `bun add` when npm is not present
  - Allow overriding the installer with `SOMBRA_PACKAGE_MANAGER`
  - Prefer Node.js for running web/seeding scripts while falling back to Bun when necessary

## [0.3.4](https://github.com/maskdotdev/sombra/compare/cli-v0.3.3...cli-v0.3.4) (2025-10-27)


### Bug Fixes

* **cli:** test and deps path resolution ([60473d5](https://github.com/maskdotdev/sombra/commit/60473d50c341e50ddcadda7b927b0d169c4da0a3))

## [0.3.3](https://github.com/maskdotdev/sombra/compare/cli-v0.3.2...cli-v0.3.3) (2025-10-27)


### Bug Fixes

* **cli:** fixing sombradb path resolve ([aad3621](https://github.com/maskdotdev/sombra/commit/aad3621fd5635c733f60b90e25118362432a0f58))

## [0.3.2](https://github.com/maskdotdev/sombra/compare/cli-v0.3.1...cli-v0.3.2) (2025-10-27)


### Bug Fixes

* **cli:** resolve sombradb from local/global roots and correct global install hint ([b7d05ec](https://github.com/maskdotdev/sombra/commit/b7d05ecd4dd2f184ad2967d0856f2b36cfb76653))

## [0.3.1](https://github.com/maskdotdev/sombra/compare/cli-v0.3.0...cli-v0.3.1) (2025-10-27)


### Bug Fixes

* **js:** fixing package name for cli ([8e0e5a4](https://github.com/maskdotdev/sombra/commit/8e0e5a48ea77a9c5a67e9c341a5098b6a0b7f394))

## [0.3.0](https://github.com/maskdotdev/sombra/compare/cli-v0.2.0...cli-v0.3.0) (2025-10-27)


### Features

* **cli:** implement Node-only CLI without cargo requirement ([73b3154](https://github.com/maskdotdev/sombra/commit/73b3154b72f4f88b86768e7a1ddb8defa9a8fb38))


### Bug Fixes

* **cli:** add --force flag to npm install for native bindings ([e119e66](https://github.com/maskdotdev/sombra/commit/e119e6645bc86d6cd3afaf21e7fd08ee685e1d12))

## [0.2.0](https://github.com/maskdotdev/sombra/compare/cli-v0.1.0...cli-v0.2.0) (2025-10-26)


### Features

* **cli:** add sombra-cli package with web UI and database tools orchestration ([dadccf3](https://github.com/maskdotdev/sombra/commit/dadccf3850ad5cf05f734ef99825ac9940f0ab69))
* **web:** add sombra-web package with Next.js UI and demo seeding ([dadccf3](https://github.com/maskdotdev/sombra/commit/dadccf3850ad5cf05f734ef99825ac9940f0ab69))


### Bug Fixes

* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))

## [0.1.0] - Unreleased

### Added
- Initial release of sombra-cli
- `sombra web` command - Launch web UI with auto-installation of sombra-web
- `sombra seed` command - Create demo database with sample data
- `sombra inspect` command - Database inspection (delegates to Rust binary)
- `sombra repair` command - Database maintenance (delegates to Rust binary)
- `sombra verify` command - Integrity verification (delegates to Rust binary)
- `sombra version` command - Version information
- Smart binary discovery for Rust binary (PATH, ~/.cargo/bin, dev directory)
- Automatic sombra-web caching and updates
- Cross-platform support (macOS, Linux, Windows)

### Features
- Auto-installs and caches sombra-web package
- Delegates database commands to Rust binary for native performance
- Helpful error messages when dependencies are missing
- Support for version pinning of sombra-web
- Browser auto-open for web UI
