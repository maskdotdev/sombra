# Sombra CLI - Quick Start Guide

Get up and running with Sombra CLI in 5 minutes!

## 📦 Installation

### Option 1: Global Installation (Recommended)

```bash
npm install -g sombra
```

### Option 2: Use with npx (No Installation)

```bash
npx sombra <command>
```

### Option 3: Local Project

```bash
npm install sombra
# Then use: npx sombra <command>
```

## 🚀 Quick Start

### 1. Create a Demo Database

```bash
sombra seed demo.db
```

This creates a sample graph database with:
- 4 Person nodes (Alice, Bob, Charlie, Diana)
- 3 Project nodes (Web App, Mobile App, API Service)
- 2 Team nodes (Frontend Team, Backend Team)
- 3 File nodes (source code files)
- 24+ edges with various relationships

### 2. Launch the Web UI

```bash
sombra web --db demo.db
```

The web UI will automatically open in your browser at `http://localhost:3000`.

### 3. Inspect Your Database

```bash
# Show database information
sombra inspect demo.db info

# Show performance statistics
sombra inspect demo.db stats

# Check database integrity
sombra verify demo.db
```

## 📚 All Commands

### `sombra web` - Web UI

Start the interactive web interface:

```bash
# Basic usage
sombra web --db my-graph.db

# Custom port
sombra web --db my-graph.db --port 8080

# Don't open browser automatically
sombra web --db my-graph.db --no-open

# Update to latest web UI version
sombra web --update
```

### `sombra seed` - Create Demo Data

```bash
# Create demo database
sombra seed demo.db

# Default creates ./demo.db
sombra seed
```

### `sombra inspect` - Database Inspection

```bash
# Show general information
sombra inspect my-graph.db info

# Show performance statistics
sombra inspect my-graph.db stats

# Show raw header contents
sombra inspect my-graph.db header

# Check WAL status
sombra inspect my-graph.db wal-info

# Quick integrity check
sombra inspect my-graph.db verify
```

### `sombra verify` - Integrity Verification

```bash
# Full integrity check
sombra verify my-graph.db

# Quick checksum-only verification
sombra verify --checksum-only my-graph.db

# Skip index validation
sombra verify --skip-indexes my-graph.db

# Show more errors (default: 16)
sombra verify --max-errors=100 my-graph.db
```

### `sombra repair` - Database Maintenance

```bash
# Force WAL checkpoint
sombra repair my-graph.db checkpoint

# Vacuum (compact) database
sombra repair my-graph.db vacuum

# Skip confirmation prompt
sombra repair my-graph.db checkpoint --yes
```

⚠️ **Always backup your database before repair operations!**

### `sombra version` - Version Info

```bash
sombra version
```

## 💡 Common Workflows

### Development Workflow

```bash
# 1. Create a test database
sombra seed test.db

# 2. Start web UI for development
sombra web --db test.db --port 3000

# 3. Check database health periodically
sombra inspect test.db stats
```

### Production Health Check

```bash
# Quick integrity check
sombra verify --checksum-only production.db

# Full verification
sombra verify production.db

# Check WAL status
sombra inspect production.db wal-info

# Checkpoint if needed
sombra repair production.db checkpoint --yes
```

### Database Maintenance

```bash
# 1. Backup first!
cp my-graph.db my-graph.db.backup

# 2. Checkpoint WAL
sombra repair my-graph.db checkpoint

# 3. Vacuum to reclaim space
sombra repair my-graph.db vacuum

# 4. Verify integrity
sombra verify my-graph.db
```

## 🔧 Requirements

- **Node.js** 18 or higher
- That's it! No Rust toolchain required.

All commands work with just Node.js through native bindings.

## 🐛 Troubleshooting

### "sombradb package not found"

Make sure you have the latest version:

```bash
npm install -g sombra
```

### Web UI not starting

```bash
# Update web UI
sombra web --update

# Check Node.js version (must be 18+)
node --version
```

### Port already in use

```bash
# Use a different port
sombra web --db my-graph.db --port 3001
```

## 📖 Learn More

- [Full CLI Documentation](./README.md)
- [Sombra Documentation](https://github.com/maskdotdev/sombra/tree/main/docs)
- [Node.js API Guide](https://github.com/maskdotdev/sombra/tree/main/docs/nodejs-guide.md)

## 🎯 Next Steps

1. **Explore the Web UI** - Visual graph exploration and querying
2. **Try the Node.js API** - Install `sombradb` for programmatic access
3. **Build your graph** - Import your own data and relationships
4. **Monitor performance** - Use `inspect stats` to track metrics

Happy graphing! 🚀
