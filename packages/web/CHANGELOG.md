# Changelog

All notable changes to the Sombra Web UI will be documented in this file.

## [0.2.0](https://github.com/maskdotdev/sombra/compare/web-v0.1.0...web-v0.2.0) (2025-10-26)


### Features

* **cli:** add sombra-cli package with web UI and database tools orchestration ([dadccf3](https://github.com/maskdotdev/sombra/commit/dadccf3850ad5cf05f734ef99825ac9940f0ab69))
* **web:** add sombra-web package with Next.js UI and demo seeding ([dadccf3](https://github.com/maskdotdev/sombra/commit/dadccf3850ad5cf05f734ef99825ac9940f0ab69))


### Bug Fixes

* **js:** publish multiplatform npm packages with corrected version ([cf92b27](https://github.com/maskdotdev/sombra/commit/cf92b27badd31c06b35189a292ce5fbd6ff96e26))

## [0.1.0] - Unreleased

### Added
- Initial release of sombra-web
- Next.js 16 standalone server for web UI
- Interactive graph visualization with reagraph
- Node and edge inspection UI
- Database statistics dashboard
- Query interface for graph exploration
- Seed script for creating demo databases
- Support for database path configuration via CLI or environment variable
- Responsive design with Tailwind CSS
- Real-time graph rendering with D3.js

### Features
- Standalone Next.js runtime (no build required for users)
- Auto-discovery of database via SOMBRA_DB_PATH environment variable
- Command-line arguments for port and database path
- Demo seed script with realistic graph data (people, projects, teams, files)
- Cross-platform support (macOS, Linux, Windows)
