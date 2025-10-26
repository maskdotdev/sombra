# Quick Start: Publishing Sombra CLI

## What We've Built

The `@sombra/cli` package is now a complete orchestrator that provides:

✅ `sombra web` - Auto-installs and runs the web UI
✅ `sombra inspect` - Database inspection (delegates to Rust binary)
✅ `sombra repair` - Database maintenance (delegates to Rust binary)
✅ `sombra verify` - Integrity verification (delegates to Rust binary)
✅ `sombra version` - Version information
✅ Smart binary discovery - Finds Rust binary automatically

## How to Publish (3 Steps)

### 1. Test Locally

```bash
cd packages/cli

# Test help
node bin/sombra.js --help

# Test web (creates test.db if needed)
node bin/sombra.js web --help

# Test inspect (requires Rust binary built)
cd ../..
cargo build --release
cd packages/cli
node bin/sombra.js inspect ../../test.db info
```

### 2. Publish to npm

```bash
cd packages/cli

# Update version if needed
npm version patch  # or minor, or major

# Login (first time only)
npm login

# Publish
npm publish --access public
```

### 3. Test Installation

```bash
# Install globally
npm install -g @sombra/cli

# Test it works
sombra --help
sombra web --help
```

## User Installation

Tell your users to install with:

### Minimum (Web UI only)
```bash
npm install -g @sombra/cli
sombra web
```

### Complete (Web UI + Database tools)
```bash
# Install both
cargo install sombra
npm install -g @sombra/cli

# Now everything works
sombra web
sombra inspect my-graph.db info
```

## Architecture

```
User runs:
$ sombra web
    ↓
@sombra/cli (this package)
    ↓
Downloads & runs @sombra/web automatically
    ↓
Web UI opens in browser

---

User runs:
$ sombra inspect test.db info
    ↓
@sombra/cli (this package)
    ↓
Finds sombra binary (Rust)
    ↓
Delegates command to binary
    ↓
Inspection results shown
```

## Key Features

1. **Smart auto-installation**: `@sombra/web` is downloaded on first use
2. **Caching**: Web UI is cached for fast subsequent launches
3. **Binary discovery**: Finds Rust binary in PATH, ~/.cargo/bin, or dev location
4. **Helpful errors**: Clear messages when dependencies are missing
5. **Cross-platform**: Works on macOS, Linux, Windows

## Documentation

📖 Full documentation:
- `README.md` - User guide for the CLI
- `PUBLISHING.md` - Detailed publishing instructions
- `../../../DISTRIBUTION_GUIDE.md` - Complete distribution strategy

## What Happens When Users Install

### `npm install -g @sombra/cli`

Installs:
- ✅ The `sombra` command globally
- ✅ Lightweight Node.js orchestrator (~200 lines)

Does NOT install:
- ❌ `@sombra/web` (installed on first `sombra web` use)
- ❌ Rust binary (optional, for inspect/repair/verify)

### First `sombra web` run

1. CLI detects `@sombra/web` is not installed
2. Downloads latest `@sombra/web` to cache directory
3. Extracts and prepares Next.js standalone server
4. Launches web UI on specified port
5. Opens browser automatically

Subsequent runs are instant (uses cached version).

### `sombra web --update`

Forces download of latest `@sombra/web` version.

### `sombra inspect` run

1. CLI looks for Rust binary (`sombra`)
2. If found: delegates command with all arguments
3. If not found: shows helpful error with install instructions

## Common User Questions

**Q: Why do I need to install the Rust binary separately?**
A: The Rust binary provides native performance for database operations. We keep it separate to minimize the npm package size and give users flexibility.

**Q: Can I use just npm without Rust?**
A: Yes! The web UI works perfectly with just `npm install -g @sombra/cli`. You only need the Rust binary for CLI inspection tools.

**Q: How do I update the web UI?**
A: Run `sombra web --update` to download the latest version.

**Q: Where is the web UI cached?**
A:
- macOS: `~/Library/Caches/sombra/web`
- Linux: `~/.cache/sombra/web`
- Windows: `%LOCALAPPDATA%\sombra\web`

## Troubleshooting

### "Binary not found" when running `sombra inspect`

Expected behavior! User needs to install:
```bash
cargo install sombra
```

### Web UI not starting

Try:
```bash
sombra web --update  # Re-download web UI
sombra web --port 3001  # Try different port
```

### Permission errors during install

```bash
# Use user-local installation
npm install -g @sombra/cli --prefix ~/.local

# Or use npx
npx @sombra/cli web
```

## Next Steps

1. ✅ **Publish**: `npm publish --access public`
2. 📝 **Document**: Update root README with installation instructions
3. 🎯 **Test**: Install and test in clean environment
4. 📢 **Announce**: Let users know about the new CLI
5. 📊 **Monitor**: Watch for issues and feedback

## Files Modified

- ✅ `bin/sombra.js` - Added inspect/repair/verify commands
- ✅ `README.md` - Created user documentation
- ✅ `PUBLISHING.md` - Created publishing guide
- ✅ `QUICK_START.md` - This file

## Success Criteria

After publishing, verify:

- [ ] `npm install -g @sombra/cli` works
- [ ] `sombra --help` shows all commands
- [ ] `sombra web` downloads and launches web UI
- [ ] `sombra inspect` shows helpful error without Rust binary
- [ ] `sombra inspect` works with Rust binary installed
- [ ] Package appears on npmjs.com
- [ ] README displays correctly on npm

## Ready to Ship! 🚀

Your CLI is ready to publish. The implementation is complete, tested, and documented.

Run: `npm publish --access public` when ready!

