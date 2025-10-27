# Changelog

All notable changes to the Sombra Web UI will be documented in this file.

## [0.2.5](https://github.com/maskdotdev/sombra/compare/web-v0.2.4...web-v0.2.5) (2025-10-27)


### Bug Fixes

* **web:** package-lock in sync ([fcb9c28](https://github.com/maskdotdev/sombra/commit/fcb9c28b01e6df2b158257dc201ff8ca942c85b1))

## [0.2.4](https://github.com/maskdotdev/sombra/compare/web-v0.2.3...web-v0.2.4) (2025-10-27)


### Bug Fixes

* **web:** bump sombradb version ([527cafe](https://github.com/maskdotdev/sombra/commit/527cafed36dee31f6408d117a6d9e561c2244168))

## [0.2.3](https://github.com/maskdotdev/sombra/compare/web-v0.2.2...web-v0.2.3) (2025-10-27)


### Bug Fixes

* **web:** bump sombradb version ([3e9e461](https://github.com/maskdotdev/sombra/commit/3e9e4615c39ff644595b0f29fcf272c713958e32))

## [0.2.2](https://github.com/maskdotdev/sombra/compare/web-v0.2.1...web-v0.2.2) (2025-10-27)


### Bug Fixes

* scope npm packages under [@unyth](https://github.com/unyth) ([c5fb560](https://github.com/maskdotdev/sombra/commit/c5fb560b3d72cc45e82a2e03c0469318b5b61775))
* **web:** bump sombradb version ([06e428c](https://github.com/maskdotdev/sombra/commit/06e428ccfced91619c009c4e1bfa329141769088))

## [0.2.1](https://github.com/maskdotdev/sombra/compare/web-v0.2.0...web-v0.2.1) (2025-10-26)


### Bug Fixes

* **web:** remove platform-specific lightningcss dependency ([737fe05](https://github.com/maskdotdev/sombra/commit/737fe057ecc8012ba0c65734f37f9350df9b7d1d))

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
