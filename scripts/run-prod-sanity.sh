#!/usr/bin/env bash
set -euo pipefail

# Run core prod-readiness sanity suites. Intended for local/staging validation,
# not a replacement for full nightly/soak tests.
#
# Usage:
#   scripts/run-prod-sanity.sh [--release]
#
# Flags:
#   --release  Build and run tests in release mode (longer build, faster tests)

MODE="debug"
MODE_FLAG=""
if [[ "${1:-}" == "--release" ]]; then
  MODE="release"
  MODE_FLAG="--release"
fi

echo "Running prod sanity tests (mode: ${MODE})"

# Core integration suites covering storage, admin, WAL replay, and stress.
cargo test ${MODE_FLAG:+$MODE_FLAG} --tests

# WAL crash replay specifics.
cargo test ${MODE_FLAG:+$MODE_FLAG} --test wal_crash_replay

# Graph churn/stress.
cargo test ${MODE_FLAG:+$MODE_FLAG} --test storage_stress

# CLI/admin end-to-end (import/export/checkpoint/vacuum).
cargo test ${MODE_FLAG:+$MODE_FLAG} --test cli_admin_commands
cargo test ${MODE_FLAG:+$MODE_FLAG} --test admin_phase1

echo "Prod sanity suites complete."
