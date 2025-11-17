# Sombra CLI DX & Visual Polish Plan

This document lays out the concrete steps for turning the `sombra` CLI into a tool with premium developer experience and delightful terminal visuals.

## Goals

1. **Readable output at a glance** – commands highlight success/failure, key metrics, and next actions through consistent formatting and color.
2. **Low-friction workflows** – repeated flags are replaced by profiles/config, inline validation catches mistakes early, and long tasks show meaningful progress.
3. **Confidence to share** – composable modules, snapshots, and telemetry hooks make future CLI work predictable, tested, and demo-friendly.

## Current Issues

- Ad-hoc print statements (`src/bin/cli.rs`) make every command look different; no colors, units, or hierarchy.
- Clap validation is incomplete (`ImportCmd.nodes` optional but required at runtime, `--edge-type` vs `--edge-type-column` conflict handled manually).
- Long operations (import/export/vacuum/verify) have zero progress feedback.
- Errors surface as `error: …` lines without context or remediation suggestions.
- Users must repeat `--db`, pager, cache, and synchronous flags for every invocation.

## Visual System

1. **Presentation module** (`src/bin/cli/ui.rs`)
   - Shared `Formatter` that renders titled blocks, multi-column key/value tables, and bullet findings.
   - Auto-detect TTY; fallback to plain text/JSON when piping.
   - Theme struct (default/dim/high-contrast) with `--theme auto|light|dark|plain`.
2. **Styled command printers**
   - `stats`, `checkpoint`, `vacuum`, `verify` use the module for consistent layout, human units, emoji (✅/⚠️) for health.
   - Provide JSON output parity via existing `OutputFormat`.
3. **Helpful errors**
   - Wrap `CliError` with `Context` so we can output `Problem / Why / Try this` sections.
4. **TTY niceties**
   - Optional `--quiet` flag.
   - Align numbers, highlight key warnings (e.g., `distinct_neighbors_default=false`).

## DX Enhancements

1. **Argument ergonomics**
   - Mark required inputs directly in Clap (e.g., `#[arg(required = true)]`).
   - Use `conflicts_with_all` and `requires` for combos (`--nodes` + `--edges`).
   - Add global `--database <path>` shorthand so subcommands inherit a default.
2. **Profiles/configuration**
   - Read `~/.config/sombra/config.toml` (and `$SOMBRA_CONFIG` override) for default DB + pager settings.
   - Support `sombra profile save <name>` + `sombra --profile <name>`.
3. **Progress + guidance**
   - Integrate `indicatif` progress bars for Import/Export/Vacuum/Verify with rate, ETA, and stage name.
   - Emit success summaries plus follow-up suggestions (e.g., `Run 'sombra stats --db …' to inspect sizing`).
4. **New developer commands**
   - `sombra init`: creates DB, seeds demo, prints dashboard URL.
   - `sombra doctor`: runs verify + stats + disk checks, outputs colored report.
5. **Shell completions & docs**
   - Add `sombra completions <shell>` generator; include instructions in README.
   - Update docs to explain CLI workflows and share gifs/screenshots.

## Implementation Phases

| Phase | Scope | Deliverables |
| --- | --- | --- |
| **0. Baseline** | Snapshot current output, add integration tests for `--help` and error cases. | `cargo insta` tests or golden text fixtures. |
| **1. Foundation** | Presentation module, Clap validation fixes, new `--theme/--quiet`, config loader scaffolding. | Refactored `print_*` functions using new UI helpers. |
| **2. Progress & Profiles** | Indicatif bars + telemetry hooks, profile/config commands, helpful error contexts. | `import/export/vacuum/verify` show progress; config documented. |
| **3. DX Extras** | `init`, `doctor`, shell completions, README/docs updates, marketing-ready demo script. | New subcommands, documented flows, GIF/screenshots of CLI. |

### Recent Progress

- Shared UI formatter with theming/quiet support is live plus the structured stats/checkpoint/vacuum/verify printers.
- Global `--theme`, `--quiet`, `--config`, and `--database` flags are hooked up, and commands can omit `[DB]` when a default is configured.
- Config loader reads `~/.config/sombra/cli.toml` (or `SOMBRA_CONFIG`) so DX teams can store project defaults without wrapper scripts.
- Spinner-based progress feedback now wraps import/export/vacuum/verify/seed, giving long-running commands visible activity and duration summaries.
- `sombra completions <shell>` generates Bash/Zsh/Fish/etc. scripts so developers can wire autocomplete quickly.
- Profile management (`--profile`, `default_profile`, `sombra profile list/save`) is implemented, letting developers persist pager/cache/db defaults per environment.
- New DX-oriented commands: `sombra init` bootstraps a database + dashboard, and `sombra doctor` runs verify/stats diagnostics with JSON/text output.

## Risks & Open Questions

- **Config format**: TOML proposed for familiarity; confirm whether CLI should also read `.env` / project-local files for compatibility with Node/Python bindings.
- **Crate size**: Adding UI/progress deps (e.g., `indicatif`, `nu-ansi-term`, `comfy-table`) increases binary size slightly; ensure release builds remain within distribution targets.
- **Async dashboard**: Dashboard server already async; ensure progress rendering doesn’t fight with tokio runtime (use blocking tasks or multi-thread runtime as needed).
