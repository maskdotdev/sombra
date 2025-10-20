# Sombra CLI Tools

Sombra provides a unified command-line interface for database inspection, repair, and verification operations.

## Installation

### Via Cargo (Recommended)

```bash
cargo install sombra
```

This installs the `sombra` binary globally, making it available system-wide.

### Via npm

The CLI is bundled with the npm package:

```bash
npm install -g sombradb
```

The `sombra` command will be available after installation. Note: This requires the Rust binary to be installed separately via `cargo install sombra`.

### Via pip

The CLI is bundled with the Python package:

```bash
pip install sombra
```

The `sombra` command will be available after installation. Note: This requires the Rust binary to be installed separately via `cargo install sombra`.

### From Source

```bash
git clone https://github.com/maskdotdev/sombra.git
cd sombra
cargo build --release
# Binary will be at target/release/sombra
```

## Usage

The CLI provides three main commands: `inspect`, `repair`, and `verify`.

### Inspect Command

Inspect database information and statistics.

```bash
sombra inspect <database> <command>
```

**Available commands:**

- `info` - Show database metadata and general information
- `stats` - Display detailed performance statistics
- `verify` - Run a quick integrity check
- `header` - Show raw header contents
- `wal-info` - Display Write-Ahead Log status

**Examples:**

```bash
# Show database information
sombra inspect graph.db info

# View performance statistics
sombra inspect graph.db stats

# Check database integrity
sombra inspect graph.db verify

# Show WAL status
sombra inspect graph.db wal-info
```

### Repair Command

Perform maintenance and repair operations on the database.

```bash
sombra repair <database> <command>
```

**Available commands:**

- `checkpoint` - Force a WAL checkpoint (merge WAL into main database)
- `vacuum` - Compact the database and reclaim space

**Examples:**

```bash
# Checkpoint the WAL
sombra repair graph.db checkpoint

# Vacuum the database
sombra repair graph.db vacuum
```

**⚠️ Warning:** Always backup your database before running repair operations! The CLI will prompt for confirmation before proceeding.

### Verify Command

Comprehensive database integrity verification.

```bash
sombra verify [OPTIONS] <database>
```

**Options:**

- `--checksum-only` - Verify only page checksums (faster)
- `--skip-indexes` - Skip index consistency validation
- `--skip-adjacency` - Skip adjacency validation
- `--max-errors=N` - Limit the number of reported issues (default: 16)
- `-h, --help` - Show help message

**Examples:**

```bash
# Full integrity check
sombra verify graph.db

# Quick checksum-only verification
sombra verify --checksum-only graph.db

# Detailed check with more error reporting
sombra verify --max-errors=100 graph.db

# Skip expensive checks
sombra verify --skip-adjacency --skip-indexes graph.db
```

## Other Commands

```bash
# Show version
sombra version

# Show help
sombra help

# Show help for a specific command
sombra inspect --help
sombra repair --help
sombra verify --help
```

## Integration with Package Managers

### npm

When you install `sombradb` via npm, the CLI wrapper is automatically available:

```bash
npm install sombradb
npx sombra inspect mydb.db info
```

For global installation:

```bash
npm install -g sombradb
sombra inspect mydb.db info
```

### pip

When you install `sombra` via pip, the CLI entry point is automatically configured:

```bash
pip install sombra
sombra inspect mydb.db info
```

## Exit Codes

The CLI uses standard exit codes:

- `0` - Success
- `1` - Error (invalid arguments, database error, integrity violations, etc.)
- `130` - Interrupted by user (Ctrl+C)

## Examples

### Complete Workflow

```bash
# 1. Check database health
sombra inspect mydb.db info

# 2. View performance metrics
sombra inspect mydb.db stats

# 3. Run integrity check
sombra verify mydb.db

# 4. If issues found, backup and repair
cp mydb.db mydb.db.backup
sombra repair mydb.db checkpoint

# 5. Verify the repair worked
sombra verify mydb.db

# 6. Optimize the database
sombra repair mydb.db vacuum
```

### CI/CD Integration

```bash
#!/bin/bash
set -e

# Verify database integrity in CI
if ! sombra verify --checksum-only production.db; then
  echo "Database integrity check failed!"
  exit 1
fi

echo "Database is healthy"
```

### Monitoring Script

```bash
#!/bin/bash

# Regular health check script
DB_PATH="/var/lib/myapp/data.db"

echo "=== Database Health Report ==="
sombra inspect "$DB_PATH" info
sombra inspect "$DB_PATH" wal-info

# Run verification weekly
if [ "$(date +%u)" -eq 1 ]; then
  echo "=== Weekly Integrity Check ==="
  sombra verify "$DB_PATH"
fi
```

## Compatibility

The CLI tools work with Sombra database versions 0.2.0 and later. The unified `sombra` command provides all functionality previously available through separate `sombra-inspect`, `sombra-repair`, and `sombra-verify` binaries.

## Troubleshooting

### "Binary not found" errors

If you get errors about the binary not being found after installing via npm or pip, ensure you have the Rust binary installed:

```bash
cargo install sombra
```

The npm and pip packages include wrappers that call the Rust binary, which must be installed separately.

### Permission errors

If you get permission errors when installing globally:

```bash
# For npm
npm install -g sombradb --unsafe-perm

# For pip
pip install --user sombra

# For cargo (installs to ~/.cargo/bin by default)
cargo install sombra
```

### Building from source

If you need to build from source:

```bash
git clone https://github.com/maskdotdev/sombra.git
cd sombra
cargo build --release

# The binary will be at target/release/sombra
# Copy it to your PATH
cp target/release/sombra ~/.cargo/bin/
```
