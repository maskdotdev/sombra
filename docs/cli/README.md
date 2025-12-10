# Sombra CLI

The `sombra` binary is the primary administrative interface for Sombra databases. It bundles the core admin commands (stats/checkpoint/vacuum/verify/import/export), demo helpers, the dashboard server, and DX niceties like profiles, shell completions, and JSON telemetry events.

This README distills the essentials so you can operate the CLI without digging through the entire repository.

## Installation

```bash
cargo install sombra
# or build from source inside this repo:
cargo build --bin cli
```

The binary is named `sombra` and lives under `target/debug/` or `target/release/` depending on your build mode.

## Global Options

Every subcommand accepts the following flags before the command name:

| Flag | Description |
| ---- | ----------- |
| `--config <file>` | Optional path to `cli.toml` (defaults to `~/.config/sombra/cli.toml` when present). |
| `--database <path>` | Sets the default database. Also supported via `SOMBRA_DATABASE`. |
| `--profile <name>` | Loads pager/cache/database defaults from the named profile (or `SOMBRA_PROFILE`). |
| `--events <file>` | Appends newline-delimited JSON telemetry (start/finish, durations, metadata) to the file. |
| `--page-size <bytes>` / `--cache-pages <pages>` | Override pager layout per command. |
| `--synchronous {full\|normal\|off}` | Override pager sync mode. |
| `--distinct-neighbors-default` | Sets the default neighbor-behavior bit for graphs. |
| `--pager-group-commit-max-writers <writers>` | Limit concurrent writers per group commit. |
| `--pager-async-fsync {on\|off}` | Enable async fsync handling. |
| `--version-codec {none\|snappy}` | Compression codec for historical versions. |
| `--format {text\|json}` | Switch between human output and machine-friendly JSON. |
| `--theme {auto\|dark\|light\|plain}` | Control ANSI coloring. |
| `--quiet` | Suppress decorative output/icons (spinners become silent). |

## Quickstart

```bash
# Seed a demo DB and launch the dashboard UI
sombra init demo.sombra --open-dashboard

# Run structural diagnostics
sombra doctor demo.sombra --verify-level full

# Import CSV data
sombra import demo.sombra \
  --nodes data/nodes.csv \
  --edges data/edges.csv \
  --edge-type FOLLOWS \
  --create
```

Most commands accept `[DB]` as an optional positional argument. If omitted, the CLI falls back to `--database`, `SOMBRA_DATABASE`, or the default in `cli.toml`.

## WAL layout (segmented only)

Sombra now requires a segmented WAL directory named `db-wal/` next to your database file. The legacy single-file `db-wal` is rejected: if one exists, remove it (after checkpointing) or rewrite into a fresh database so the WAL directory can be created.

## Profiles (`cli.toml`)

Profiles remove the need to repeat pager/default arguments:

```toml
[database]
default = "/Users/me/dev-db.sombra"

[profiles.dev]
database = "/Users/me/dev-db.sombra"
page_size = 16384
cache_pages = 8192
synchronous = "normal"
distinct_neighbors_default = true

default_profile = "dev"
```

Manage profiles from the CLI:

```bash
sombra profile list
sombra profile show dev
sombra profile save staging \
  --database prod.sombra \
  --page-size 65536 \
  --pager-async-fsync on \
  --version-codec snappy
sombra profile delete staging
```

`--profile <name>` (or `SOMBRA_PROFILE`) selects a profile for the current invocation. Command-line flags always override profile defaults.

## Telemetry Events

Every command can emit JSON telemetry when `--events <file>` (or `SOMBRA_EVENTS`) is set. Events look like:

```json
{"timestamp":"2024-11-17T10:00:00Z","command":"import","phase":"finish","metadata":{"db_path":"graph.sombra","duration_ms":5234.2,"nodes":12000,"edges":32000,"success":true}}
```

Because the log is append-only, you can `tail -f` it locally, ship it to CI artifacts, or point dashboards/`jq` at it for live visibility.

Example consumers:

```bash
# Follow events in real time
SOMBRA_EVENTS=~/sombra.events sombra import demo.sombra --nodes nodes.csv &
tail -f ~/sombra.events | jq

# Summarize doctor durations in CI
jq -s '[.[] | select(.command=="doctor" and .phase=="finish") | .metadata.duration_ms ] | {runs: length, avg_ms: (add / length)}'   ~/sombra.events
```

Because the file is newline-delimited JSON, it plays nicely with tools like `jq`, `rg`, `sed`, log forwarders, or custom telemetry daemons.

## Admin Commands

| Command | Purpose |
| ------- | ------- |
| `sombra stats [DB]` | Pager/WAL/storage statistics. |
| `sombra checkpoint [DB] [--mode {force\|best-effort}]` | Forces/attempts a checkpoint. |
| `sombra vacuum [DB] [--into <path> \| --replace [--backup <path>]] [--analyze]` | Produces a compacted database copy, optionally swapping it into place (with a backup) and collecting label stats. |
| `sombra verify [DB] [--level {fast\|full}]` | Structural verification (exit code 2 on failure). |
| `sombra import [DB] --nodes <file> [--edges <file>] [...]` | Typed CSV ingest with property/type overrides and index controls. |
| `sombra export [DB] [--nodes <file>] [--edges <file>]` | CSV export of nodes/edges with optional property subsets. |
| `sombra seed-demo [DB] [--create]` | Seeds the Stage 8 demo graph. |
| `sombra init [DB] [--skip-demo] [--open-dashboard]` | Bootstraps a database (seeds demo data by default). |
| `sombra doctor [DB] [--verify-level ...] [--json]` | Runs stats + verify and prints a health report. |
| `sombra dashboard [DB] [--host ... --port ...]` | Serves the bundled dashboard/API server. |

Every admin command supports `--format json` for machine-friendly output in addition to the default themed text.

## Shell Completions

Generate completions with:

```bash
sombra completions bash > ~/.local/share/bash-completion/sombra
```

Supported shells: `bash`, `zsh`, `fish`, `powershell`, `elvish`.

## Further Reading

The canonical, deep-dive documentation (CSV schemas, binding integrations, advanced pragmas) still lives in [`docs/cli.md`](../cli.md). This README is a friendlier entry point; when in doubt, consult the upstream doc for exhaustive option matrices.
