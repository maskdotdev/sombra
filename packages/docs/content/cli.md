# Sombra CLI Tools

Sombra provides a unified command-line interface for database administration, data import/export, and interactive exploration.

## Installation

### Via npm (Recommended)

```bash
npm install -g sombradb
```

This installs the `sombra` CLI globally. The npm package bundles native binaries for supported platforms.

### Via npx (No Installation)

```bash
npx sombradb <command>
```

Use the CLI without installing it globally.

### Via pip (Python)

```bash
pip install sombra
sombra <command>
```

### Via Cargo (For Rust Users)

```bash
cargo install sombra
```

Installs the standalone Rust binary directly.

### From Source

```bash
git clone https://github.com/maskdotdev/sombra.git
cd sombra
cargo build --release
# Binary at target/release/sombra
```

## Global Options

All subcommands share these flags (must appear before the subcommand):

| Flag                                 | Description                                                       |
| ------------------------------------ | ----------------------------------------------------------------- |
| `--config <file>`                    | Path to CLI config file (defaults to `~/.config/sombra/cli.toml`) |
| `--database <path>`                  | Default database path. Also via `SOMBRA_DATABASE` env var         |
| `--profile <name>`                   | Load defaults from named profile. Also via `SOMBRA_PROFILE`       |
| `--page-size <bytes>`                | Override pager page size when creating a database                 |
| `--cache-pages <count>`              | Override pager cache size (in pages)                              |
| `--synchronous {full\|normal\|off}`  | Override pager synchronous mode                                   |
| `--pager-group-commit-max-writers <writers>` | Maximum writers batched per WAL group commit.     |
| `--pager-async-fsync {on\|off}`              | Enable async fsync handling.                      |
| `--version-codec {none\|snappy}`             | Codec to apply to historical versions.            |
| `--format {text\|json}`              | Output format (text by default)                                   |
| `--theme {auto\|dark\|light\|plain}` | Color theme for text output                                       |
| `--quiet`                            | Suppress decorative output; useful when piping                    |

## Configuration File

The CLI looks for `~/.config/sombra/cli.toml` (or `SOMBRA_CONFIG`):

```toml
[database]
default = "/path/to/my-graph.sombra"

[profiles.dev]
database = "/path/to/dev-db.sombra"
page_size = 16384
cache_pages = 8192
synchronous = "normal"
# Advanced WAL/MVCC settings
pager_group_commit_max_writers = 8
pager_async_fsync = true
version_codec = "snappy"
```

Set `default_profile = "dev"` at the top to apply profile defaults automatically.

## Commands

### Quick Start Commands

#### init

Create a new database, seed demo data, and optionally launch the dashboard:

```bash
sombra init [DB] [--open-dashboard] [--skip-demo]
```

Options:

- `--open-dashboard` - Launch the web dashboard after init
- `--skip-demo` - Skip seeding demo data

#### doctor

Run diagnostics (stats + verify) and print a health report:

```bash
sombra doctor [DB] [--verify-level {fast|full}] [--json]
```

Exits with status 2 if verification fails, making it suitable for CI checks.

### Admin Commands

All admin commands accept an optional `[DB]` positional argument. When omitted, falls back to `--database`, `SOMBRA_DATABASE`, or `cli.toml`.

#### stats

Print pager, WAL, and storage metadata:

```bash
sombra stats [DB]
```

Use `--format json` for machine-readable output.

#### mvcc-status

Display MVCC diagnostics including version-log usage, commit-table state, and active readers:

```bash
sombra mvcc-status [DB]
```

#### checkpoint

Force or attempt a WAL checkpoint:

```bash
sombra checkpoint [DB] [--mode {force|best-effort}]
```

#### vacuum

Copy the database to a new file, forcing a checkpoint first:

```bash
sombra vacuum [DB] --into <PATH> [--analyze]
```

The `--analyze` flag gathers label-cardinality statistics.

#### verify

Perform structural verification:

```bash
sombra verify [DB] [--level {fast|full}]
```

- `fast` - Validates pager metadata only
- `full` - Scans nodes, edges, and adjacency tables; checks symmetry, endpoint existence, duplicates

Exits with status 2 when invariants fail.

### Data Import/Export

#### import

Import nodes and edges from CSV files:

```bash
sombra import [DB] \
  --nodes <FILE> \
  [--node-id-column <col>] \
  [--node-labels <label>|...] \
  [--node-label-column <col>] \
  [--node-props <col1,col2,...>] \
  [--node-prop-types <col:type,...>] \
  [--edges <FILE>] \
  [--edge-src-column <col>] \
  [--edge-dst-column <col>] \
  [--edge-type <TYPE> | --edge-type-column <col>] \
  [--edge-props <col1,col2,...>] \
  [--edge-prop-types <col:type,...>] \
  [--create]
```

**Node file** (required): CSV with an `id` column (default name `id`). Labels can come from `--node-labels` (pipe-separated) and/or `--node-label-column`.

**Edge file** (optional): CSV with source/destination columns (`src`/`dst` by default). Edge type can be a constant (`--edge-type`) or from `--edge-type-column`.

**Property types**: Use `--node-prop-types` or `--edge-prop-types` to override auto-detection:

- `auto` (default), `string`, `bool`, `int`, `float`, `date`, `datetime`, `bytes`
- Dates: ISO-8601 (`YYYY-MM-DD`, RFC3339)
- Bytes: `0x`-prefixed hex string

Example:

```bash
sombra import graph.sombra \
  --nodes people.csv --node-id-column person_id --node-labels Person \
  --node-props name,age,email \
  --node-prop-types age:int,birth_date:date \
  --edges follows.csv --edge-src-column src --edge-dst-column dst \
  --edge-type FOLLOWS \
  --create
```

#### export

Export nodes and edges to CSV files:

```bash
sombra export [DB] \
  [--nodes <FILE> [--node-props <col1,col2,...>]] \
  [--edges <FILE> [--edge-props <col1,col2,...>]]
```

Node output includes `id` + `labels`; edge output includes `src`, `dst`, `type`.

Example:

```bash
sombra export graph.sombra \
  --nodes nodes_out.csv --node-props name,age \
  --edges edges_out.csv --edge-props since,weight
```

### Dashboard

Launch the experimental web dashboard:

```bash
sombra dashboard [DB] [--host <HOST>] [--port <PORT>] [--open-browser] [--read-only]
```

Options:

- `--host` - Bind address (default: `127.0.0.1`)
- `--port` - Bind port (default: `7654`)
- `--open-browser` - Open dashboard in default browser
- `--read-only` - Disable mutating/admin endpoints
- `--assets <DIR>` - Custom dashboard assets directory
- `--allow-origin <ORIGIN>` - Additional CORS origins (repeatable)

### Demo Data

Populate a database with demo nodes and edges:

```bash
sombra seed-demo [DB] [--create]
```

Creates three `User` nodes (Ada, Grace, Alan) with `FOLLOWS` edges.

### Profile Management

Profiles bundle pager/cache/database defaults for different environments.

```bash
# List profiles
sombra profile list

# Create or update a profile
sombra profile save dev \
  --database ~/projects/dev-db.sombra \
  --page-size 16384 \
  --cache-pages 8192 \
  --synchronous full \
  --pager-async-fsync on \
  --set-default
```

### Shell Completions

Generate completions for your shell:

```bash
sombra completions <shell> > /path/to/completions
```

Supported shells: `bash`, `zsh`, `fish`, `powershell`, `elvish`.

## Exit Codes

- `0` - Success
- `1` - Error (invalid arguments, database error, etc.)
- `2` - Verification failure (integrity violations)
- `130` - Interrupted by user (Ctrl+C)

## Examples

### Complete Workflow

```bash
# 1. Initialize a new database with demo data
sombra init mydb.sombra --open-dashboard

# 2. Import your own data
sombra import mydb.sombra \
  --nodes users.csv --node-labels User \
  --edges friendships.csv --edge-type FRIENDS

# 3. Check database health
sombra doctor mydb.sombra

# 4. View statistics
sombra stats mydb.sombra

# 5. Run full integrity check
sombra verify mydb.sombra --level full

# 6. Compact the database
sombra vacuum mydb.sombra --into mydb-compacted.sombra --analyze
```

### CI/CD Integration

```bash
#!/bin/bash
set -e

# Verify database integrity before deployment
if ! sombra verify --level fast production.sombra; then
  echo "Database integrity check failed!"
  exit 1
fi

echo "Database is healthy"
```

### Monitoring Script

```bash
#!/bin/bash

DB_PATH="/var/lib/myapp/data.sombra"

echo "=== Database Health Report ==="
sombra stats "$DB_PATH"
sombra mvcc-status "$DB_PATH"

# Weekly full verification
if [ "$(date +%u)" -eq 1 ]; then
  echo "=== Weekly Integrity Check ==="
  sombra verify "$DB_PATH" --level full
fi
```

## Troubleshooting

### Permission Errors

```bash
# For npm global install
npm install -g sombradb --unsafe-perm

# For pip user install
pip install --user sombra

# For cargo (installs to ~/.cargo/bin)
cargo install sombra
```

### Binary Not Found

Ensure the installation directory is in your PATH:

- npm: Check `npm bin -g`
- pip: Check `python -m site --user-base`/bin
- cargo: Ensure `~/.cargo/bin` is in PATH
