# sombra

The official CLI for Sombra Graph Database - command-line interface for database inspection, web UI, and management tools.

## Installation

### Global Installation (Recommended)

```bash
npm install -g sombra-cli
```

After installation, the `sombra` command will be available globally.

### Local Installation

```bash
npx sombra <command>
```

Or add to your project:

```bash
npm install sombra-cli
```

## Commands

### `sombra seed` - Create Demo Database

Create a demo database with sample data for testing and exploration.

```bash
# Create demo database with default name (demo.db)
sombra seed

# Create demo database with custom name
sombra seed my-demo.db

# Then launch web UI to explore it
sombra web my-demo.db
```

The seed command creates a graph with:
- **4 Person nodes** (Alice, Bob, Charlie, Diana)
- **3 Project nodes** (Web App, Mobile App, API Service)
- **2 Team nodes** (Frontend Team, Backend Team)
- **3 File nodes** (source code files)
- **24+ edges** with various relationships (WORKS_WITH, REPORTS_TO, MEMBER_OF, etc.)

Perfect for:
- 🎯 Testing the web UI
- 📊 Learning graph queries
- 🧪 Exploring Sombra features
- 🎓 Demo presentations

### `sombra web` - Start Web UI

Launch the interactive web UI for visualizing and querying your Sombra database.

```bash
# Start web UI with default settings
sombra web

# Specify database and port
sombra web --db ./my-graph.db --port 3000

# Start without opening browser
sombra web --no-open

# Update to latest web UI version
sombra web --update
```

**Options:**
- `--db <path>` - Path to database file (default: looks for `SOMBRA_DB_PATH` env var)
- `--port <port>` - Port to run on (default: 3000)
- `--open` - Open browser automatically (default: true)
- `--no-open` - Don't open browser
- `--update` - Update sombra-web to latest version
- `--version-pin <version>` - Install specific version of sombra-web

### `sombra inspect` - Database Inspection

Inspect database information, statistics, and health.

```bash
# Show database information
sombra inspect graph.db info

# Show detailed statistics
sombra inspect graph.db stats

# Verify database integrity
sombra inspect graph.db verify

# Show raw header
sombra inspect graph.db header

# Show WAL status
sombra inspect graph.db wal-info
```

**Sub-commands:**
- `info` - Show database metadata and general information
- `stats` - Display detailed performance statistics
- `verify` - Run a quick integrity check
- `header` - Show raw header contents
- `wal-info` - Display Write-Ahead Log status

### `sombra repair` - Database Repair

Perform maintenance and repair operations.

```bash
# Force WAL checkpoint
sombra repair graph.db checkpoint

# Vacuum database
sombra repair graph.db vacuum
```

**Sub-commands:**
- `checkpoint` - Force a WAL checkpoint (merge WAL into main database)
- `vacuum` - Compact the database and reclaim space

⚠️ **Warning:** Always backup your database before repair operations!

### `sombra verify` - Integrity Verification

Run comprehensive database integrity checks.

```bash
# Full integrity check
sombra verify graph.db

# Quick checksum-only verification
sombra verify --checksum-only graph.db

# Detailed check with more error reporting
sombra verify --max-errors=100 graph.db
```

**Options:**
- `--checksum-only` - Verify only page checksums (faster)
- `--skip-indexes` - Skip index consistency validation
- `--skip-adjacency` - Skip adjacency validation
- `--max-errors=N` - Limit number of reported issues (default: 16)

### `sombra version` - Version Information

Show version information for CLI and components.

```bash
sombra version
```

### `sombra help` - Help

Show help information.

```bash
sombra help
sombra <command> --help
```

## Requirements

- **Node.js** 18 or higher
- Automatically installs `sombra-web` on first use for the web UI
- All commands work with just Node.js - no Rust toolchain required!

## Examples

### Quick Start with Demo Data

```bash
# 1. Create a demo database
sombra seed demo.db

# 2. Launch web UI to explore it
sombra web demo.db
```

### Complete Workflow

```bash
# 1. Start web UI for visual exploration
sombra web --db my-graph.db

# 2. Check database health (in another terminal)
sombra inspect my-graph.db info

# 3. View performance metrics
sombra inspect my-graph.db stats

# 4. Run integrity check
sombra verify my-graph.db

# 5. Maintenance - checkpoint WAL
sombra repair my-graph.db checkpoint

# 6. Optimize database
sombra repair my-graph.db vacuum
```

### CI/CD Integration

```bash
#!/bin/bash
# Database health check in CI pipeline

if ! sombra verify --checksum-only production.db; then
  echo "Database integrity check failed!"
  exit 1
fi

echo "Database is healthy"
```

## Architecture

The `sombra` CLI package provides a unified command-line interface that:

1. **Handles `web` and `seed` commands** - Downloads and runs `sombra-web` package automatically
2. **Uses native Node.js bindings** - For `inspect`, `repair`, `verify` commands via the `sombradb` package

This design provides:
- ✅ Easy installation via npm (no Rust toolchain needed)
- ✅ Automatic web UI updates
- ✅ Native performance through N-API bindings
- ✅ Consistent CLI experience across all commands

## Troubleshooting

### "sombradb package not found" Error

If you see an error about `sombradb` not being found, ensure you have the latest version:

```bash
npm install -g sombra
```

Or for local projects:

```bash
npm install sombra
```

### Web UI Not Starting

If the web UI fails to start:

1. **Update to latest version:**
   ```bash
   sombra web --update
   ```

2. **Check Node.js version:**
   ```bash
   node --version  # Should be 18+
   ```

3. **Clear cache and reinstall:**
   ```bash
   rm -rf ~/.cache/sombra/web  # or equivalent on your OS
   sombra web --update
   ```

### Port Already in Use

If port 3000 is busy:

```bash
sombra web --port 3001
```

## Development

For development and testing:

```bash
# Clone the repo
git clone https://github.com/maskdotdev/sombra.git
cd sombra/packages/cli

# Test locally
node bin/sombra.js web
node bin/sombra.js inspect ../../test.db info
```

## Related Packages

- **[sombradb](https://www.npmjs.com/package/sombradb)** - Node.js/TypeScript library for Sombra
- **[sombra-web](https://www.npmjs.com/package/sombra-web)** - Web UI (auto-installed by CLI)
- **[sombra](https://crates.io/crates/sombra)** - Rust core library

## License

MIT

## Links

- [Documentation](https://github.com/maskdotdev/sombra/tree/main/docs)
- [GitHub](https://github.com/maskdotdev/sombra)
- [Issues](https://github.com/maskdotdev/sombra/issues)

