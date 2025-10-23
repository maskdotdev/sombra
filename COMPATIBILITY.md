# 📦 Sombra Version Compatibility Matrix

The Sombra ecosystem consists of three independently-versioned packages:

- **`sombra`** (Rust crate on crates.io)
- **`sombradb`** (Node.js/TypeScript on npm)
- **`sombra`** (Python on PyPI)

## Current Versions

| Rust (`sombra`) | Node.js (`sombradb`) | Python (`sombra`) | Release Date | Notes |
|:----------------|:---------------------|:------------------|:-------------|:------|
| `0.3.3`         | `0.3.3`              | `0.3.3`           | 2024-10-23   | Initial independent versioning |

## Compatibility Rules

- **Breaking changes in Core (Rust)** require updates to BOTH bindings
- **Non-breaking Core changes** do NOT require binding updates
- **Binding-specific features** can be released independently

## Finding Compatible Versions

1. **If using Rust directly:** Use the latest `sombra` version from crates.io
2. **If using Node.js:** Check the "Supported Core Version" in the npm package README
3. **If using Python:** Check the "Supported Core Version" in the PyPI package description

## Version History

### `sombra` (Rust Core)
- `0.3.3` - Current stable release

### `sombradb` (Node.js)
- `0.3.3` - Matches core `0.3.3`

### `sombra` (Python)
- `0.3.3` - Matches core `0.3.3`
