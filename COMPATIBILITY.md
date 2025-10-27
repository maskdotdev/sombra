# 📦 Sombra Version Compatibility Matrix

The Sombra ecosystem consists of three independently-versioned packages:

- **`sombra`** (Rust crate on crates.io)
- **`@unyth/sombra`** (Node.js/TypeScript on npm)
- **`sombra`** (Python on PyPI)

## Current Versions

| Rust (`sombra`) | Node.js (`@unyth/sombra`) | Python (`sombra`) | Release Date | Notes |
|:----------------|:---------------------|:------------------|:-------------|:------|
| `0.3.3`         | `0.4.5`              | `0.3.3`           | 2024-10-24   | Independent versioning - packages evolve separately |

## Compatibility Rules

- **Breaking changes in Core (Rust)** require updates to BOTH bindings
- **Non-breaking Core changes** do NOT require binding updates
- **Binding-specific features** can be released independently
- Each package follows semantic versioning independently

## Finding Compatible Versions

1. **If using Rust directly:** Use the latest `sombra` version from crates.io
2. **If using Node.js:** Use the latest `@unyth/sombra` version from npm
3. **If using Python:** Use the latest `sombra` version from PyPI

All packages are compatible with each other within the same major version (0.x.x). Minor and patch versions may differ as packages evolve independently.

## Version History

### `sombra` (Rust Core)
- `0.3.3` - Current stable release

### `@unyth/sombra` (Node.js)
- `0.4.5` - Current stable release
- `0.4.4` - Previous release
- `0.4.3` - Previous release
- `0.3.3` - Initial independent versioning baseline

### `sombra` (Python)
- `0.3.3` - Current stable release
