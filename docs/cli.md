# Sombra CLI Guide

`sombra` is the command-line interface for administering a Sombra database. The binary is provided by the `sombra-cli` crate and exposes operational commands (stats, checkpoint, verify, vacuum) plus CSV import/export tooling.

## Global Options

All subcommands share the following flags (must appear before the subcommand):

| Flag | Description |
| ---- | ----------- |
| `--config <file>` | Optional path to a CLI config file (defaults to `~/.config/sombra/cli.toml` when present). |
| `--database <path>` | Sets the default database path. Can also come from `SOMBRA_DATABASE` or the config file. |
| `--profile <name>` | Loads defaults from the named profile. Can also come from `SOMBRA_PROFILE` or `cli.toml`. |
| `--page-size <bytes>` | Override pager page size when creating a database. |
| `--cache-pages <count>` | Override pager cache size (in pages). |
| `--synchronous {full\|normal\|off}` | Override the pager synchronous mode. |
| `--distinct-neighbors-default` | Sets the default for storage neighbor queries (matches Stage 7/8 behavior). |
| `--pager-group-commit-max-writers <writers>` | Maximum writers batched per WAL group commit. |
| `--pager-group-commit-max-frames <frames>` | Maximum WAL frames per group commit. |
| `--pager-group-commit-max-wait-ms <ms>` | Time window in milliseconds to wait for group commits. |
| `--pager-async-fsync {on\|off}` | Enable async fsync handling. |
| `--pager-async-fsync-max-wait-ms <ms>` | Maximum coalesce delay for async fsync batching. |
| `--pager-wal-segment-bytes <bytes>` | Preferred WAL segment size. |
| `--pager-wal-preallocate-segments <count>` | Number of WAL segments to preallocate. |
| `--inline-history {on\|off}` | Embed newest historical version inline on page heads. |
| `--inline-history-max-bytes <bytes>` | Maximum inline history payload size. |
| `--version-codec {none\|snappy}` | Codec to apply to historical versions. |
| `--version-codec-min-bytes <bytes>` | Minimum payload size before compressing historical versions. |
| `--version-codec-min-savings-bytes <bytes>` | Minimum bytes saved to keep compression output. |
| `--snapshot-pool-size <count>` | Cached snapshots to reuse for reads (0 disables). |
| `--snapshot-pool-max-age-ms <ms>` | Maximum age for pooled read snapshots. |
| `--format {text\|json}` | Controls output formatting (text by default). |
| `--theme {auto\|dark\|light\|plain}` | Controls ANSI coloring for text output (auto-detects TTYs by default). |
| `--quiet` | Suppresses decorative output/icons; useful when piping logs. |

### Configuration File

The CLI looks for `~/.config/sombra/cli.toml` (or the path supplied by `--config` /
`SOMBRA_CONFIG`). When present it provides defaults shared across all invocations:

```toml
[database]
default = "/Users/me/projects/demo-db/graph.sombra"
```

The `[database].default` entry mirrors the `--database` / `SOMBRA_DATABASE` environment
variable and lets you skip repeating the `<DB>` argument on every command. Additional
sections (pager defaults, profiles, etc.) will be added over time; the CLI ignores
unknown keys so it is safe to stash future-ready settings today.

Profiles live under `[profiles.<name>]` and can override pager/cache/database settings. Example:

```toml
[profiles.dev]
database = "/Users/me/projects/dev-db.sombra"
page_size = 16384            # bytes
cache_pages = 8192
synchronous = "normal"       # full | normal | off
distinct_neighbors_default = true
# Advanced Pager/WAL settings
pager_group_commit_max_writers = 10
pager_group_commit_max_frames = 64
pager_group_commit_max_wait_ms = 5
pager_async_fsync = true
pager_async_fsync_max_wait_ms = 2
pager_wal_segment_bytes = 4194304 # 4MB
pager_wal_preallocate_segments = 2
# MVCC / History settings
inline_history = true
inline_history_max_bytes = 128
version_codec = "snappy"
version_codec_min_bytes = 64
version_codec_min_savings_bytes = 10
snapshot_pool_size = 4
snapshot_pool_max_age_ms = 5000
```

Set `default_profile = "dev"` at the top of the file (or pass `--profile dev`) to apply these
defaults automatically. Per-command flags always override profile values.

### Progress Feedback

Import/export/vacuum/verify/seed operations display an animated spinner plus a completion summary
when stdout is a TTY. Use `--quiet` (or pipe the command) to disable the spinner while keeping the
final success/error lines.

### Shell Completions

Generate completions for your shell with:

```
sombra completions <shell> > /path/to/completions
```

Supported shells include `bash`, `zsh`, `fish`, `powershell`, and `elvish`. Follow your shell’s
standard instructions to source/install the file (e.g., place Bash completions in
`/etc/bash_completion.d/sombra` or `~/.local/share/bash-completion/sombra` and reload your shell).

## Profile Management

Profiles bundle pager/cache/database defaults so you can jump between environments without repeating flags.

List configured profiles (the default is marked):

```
sombra profile list
```

Create or update a profile:

```
sombra profile save dev \
  --database ~/projects/dev-db.sombra \
  --page-size 16384 \
  --cache-pages 8192 \
  --synchronous full \
  --distinct-neighbors-default \
  --pager-group-commit-max-writers 16 \
  --pager-async-fsync on \
  --version-codec snappy \
  --set-default
```

Use `--profile dev` (or `SOMBRA_PROFILE=dev`) to run commands with those defaults. Flags provided on
the command line always override the profile values.

## Quickstart Commands

### Init

```
sombra init [DB] [--open-dashboard] [--skip-demo]
```

Creates a new database (if needed), seeds the demo graph, and optionally launches the dashboard
locally. Use `--skip-demo` when you have your own data already; pass `--open-dashboard` to
automatically open the dashboard UI once seeding finishes.

### Doctor

```
sombra doctor [DB] [--verify-level {fast|full}] [--json]
```

Runs `stats` + `verify` and prints a formatted report (or JSON when `--json` is provided). The command
exits with status 2 when verification fails so CI scripts can gate deployments on doctor health.

## Admin Commands

Every admin command accepts an optional `[DB]` positional argument. When omitted, the CLI
falls back to `--database`, the `SOMBRA_DATABASE` environment variable, or the path stored
in `cli.toml`.

```
sombra stats [DB]
```

Prints pager/WAL/storage metadata. Use `--format json` for machine-readable output.

```
sombra mvcc-status [DB]
```

Displays MVCC diagnostics, including version-log usage, the commit-table state,
and currently active readers. Text output summarizes outstanding commits and
slow readers; `--format json` emits the same data for automation.

```
sombra checkpoint [DB] [--mode {force|best-effort}]
```

Forces or attempts a checkpoint against the target database. Reports elapsed time and resulting LSN.

```
sombra vacuum [DB] --into <PATH> [--analyze]
```

Copies the database file to `PATH`, forcing a checkpoint first. When `--analyze` is set the command gathers label-cardinality statistics and emits them in JSON/text output.

```
sombra verify [DB] [--level {fast|full}]
```

Performs structural verification. `fast` validates pager metadata; `full` scans nodes, edges, and adjacency tables (ensuring symmetry, endpoint existence, duplicate detection) and exits with status code 2 when invariants fail.

## CSV Import

```
sombra import [DB] \
  --nodes <FILE> \
  [--node-id-column <col>] \
  [--node-labels <label>|…] \
  [--node-label-column <col>] \
  [--node-props <col1,col2,…>] \
  [--edges <FILE>] \
  [--edge-src-column <col>] \
  [--edge-dst-column <col>] \
  [--edge-type <TYPE> | --edge-type-column <col>] \
  [--edge-props <col1,col2,…>] \
  [--create]
```

* **Nodes file** (required): CSV with an `id` column (default name `id`). Labels can come from `--node-labels` (pipe-separated literal list) and/or `--node-label-column`. Property columns default to “everything except ID/label columns” but you can pass `--node-props` with a comma-separated list.
* **Edges file** (optional): CSV with source/destination columns (`src`/`dst` by default). Edge type can be a constant (`--edge-type`) or read from `--edge-type-column`. You must import nodes first so the importer can map external IDs.
* Values are parsed as bool/int/float/string/null automatically, with optional type overrides via `--node-prop-types col:type` / `--edge-prop-types col:type`. Accepted types: `auto` (default), `string`, `bool`, `int`, `float`, `date`, `datetime`, `bytes`. Date/datetime values accept ISO-8601 strings (`YYYY-MM-DD`, RFC3339 datetimes) or pre-computed epoch days/milliseconds; byte values expect a `0x`-prefixed hex string. Use the overrides to force `string` when you need to disable the built-in heuristics.
* `--disable-indexes` drops existing Stage 7 property indexes before importing so writes can skip index maintenance. Pair it with `--build-indexes` to rebuild every dropped index offline after the load completes (the command enforces this pairing automatically).
* Sample CSV fixtures live under `tests/fixtures/import/` (e.g., `people_nodes.csv`, `follows_edges.csv`) and cover typed columns, dates/datetimes, and byte payloads.
* Import batches are executed through the storage layer (no `MutationSpec` indirection) and a final checkpoint is issued so data is durable immediately.

Example:

```
sombra import graph.sombra \
  --nodes people.csv --node-id-column person_id --node-labels Person \
  --node-props name,age,email \
  --node-prop-types age:int,birth_date:date \
  --edges follows.csv --edge-src-column src --edge-dst-column dst \
  --edge-type FOLLOWS \
  --edge-prop-types weight:float,created_at:datetime \
  --create
```

## CSV Export

```
sombra export [DB] \
  [--nodes <FILE> [--node-props <col1,col2,…>]] \
  [--edges <FILE> [--edge-props <col1,col2,…>]]
```

Exports the requested tables to CSV. Node output always includes `id` + `labels`; edge output includes `src`, `dst`, `type`. Property columns are optional lists via `--node-props` / `--edge-props` (defaults to none to keep files lean). Identifiers match the numeric IDs stored in the engine; labels/types are resolved through the dictionary (fallbacks like `LABEL#123` appear if a name is missing).

Example:

```
sombra export graph.sombra \
  --nodes nodes_out.csv --node-props name,age \
  --edges edges_out.csv --edge-props since,weight
```

## Demo Seeding

```
sombra seed-demo [DB] [--create]
```

Populates the database with a tiny Stage 8 demo graph (three `User` nodes and
`FOLLOWS` edges). Pass `--create` the first time so the CLI can create the file
if it does not exist yet. Once seeded you can immediately run queries such as
`MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a,b` from the dashboard or bindings.

## JSON Output

All admin commands support `--format json`. Import/export currently emit plain text summaries (JSON payloads land in future revisions); use shell redirection to capture CSV artifacts.
