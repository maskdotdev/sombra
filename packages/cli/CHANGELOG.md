# Changelog

All notable changes to the Sombra CLI will be documented in this file.

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

