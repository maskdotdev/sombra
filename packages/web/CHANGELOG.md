# Changelog

All notable changes to the Sombra Web UI will be documented in this file.

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

