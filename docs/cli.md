# Sombra CLI Guide

`sombra` is the command-line interface for administering a Sombra database. The binary is provided by the `sombra-cli` crate and exposes operational commands (stats, checkpoint, verify, vacuum) plus CSV import/export tooling.

## Global Options

All subcommands share the following flags (must appear before the subcommand):

| Flag | Description |
| ---- | ----------- |
| `--page-size <bytes>` | Override pager page size when creating a database. |
| `--cache-pages <count>` | Override pager cache size (in pages). |
| `--synchronous {full\|normal\|off}` | Override the pager synchronous mode. |
| `--distinct-neighbors-default` | Sets the default for storage neighbor queries (matches Stage 7/8 behavior). |
| `--format {text\|json}` | Controls output formatting (text by default). |

## Admin Commands

```
sombra stats <DB>
```

Prints pager/WAL/storage metadata. Use `--format json` for machine-readable output.

```
sombra mvcc-status <DB>
```

Displays MVCC diagnostics, including version-log usage, the commit-table state,
and currently active readers. Text output summarizes outstanding commits and
slow readers; `--format json` emits the same data for automation.

```
sombra checkpoint <DB> [--mode {force|best-effort}]
```

Forces or attempts a checkpoint against the target database. Reports elapsed time and resulting LSN.

```
sombra vacuum <DB> [--into <PATH> | --replace [--backup <PATH>]] [--analyze]
```

Copies the database file to `PATH`, forcing a checkpoint first. When `--replace` is provided the compacted copy is swapped into place automatically (default backup `<DB>.bak`, configurable via `--backup`). When `--analyze` is set the command gathers label-cardinality statistics and emits them in JSON/text output.

```
sombra verify <DB> [--level {fast|full}]
```

Performs structural verification. `fast` validates pager metadata; `full` scans nodes, edges, and adjacency tables (ensuring symmetry, endpoint existence, duplicate detection) and exits with status code 2 when invariants fail.

## CSV Import

```
sombra import <DB> \
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
sombra export <DB> \
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
sombra seed-demo <DB> [--create]
```

Populates the database with a tiny Stage 8 demo graph (three `User` nodes and
`FOLLOWS` edges). Pass `--create` the first time so the CLI can create the file
if it does not exist yet. Once seeded you can immediately run queries such as
`MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a,b` from the dashboard or bindings.

## JSON Output

All admin commands support `--format json`. Import/export currently emit plain text summaries (JSON payloads land in future revisions); use shell redirection to capture CSV artifacts.
