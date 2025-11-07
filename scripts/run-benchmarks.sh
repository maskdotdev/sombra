#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-bench-results}"
STAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
OUT_DIR="${ROOT}/${STAMP}"

echo "running criterion benches…"
cargo test --release -p sombra-bench --benches --features bench

echo "collecting artifacts into ${OUT_DIR}"
cargo run --release -p sombra-bench --bin bench-collector -- --criterion target/criterion --out-dir "${OUT_DIR}"

if [[ -n "${LDBC_NODES:-}" && -n "${LDBC_EDGES:-}" ]]; then
  echo "running LDBC baseline…"
  cargo run --release -p sombra-bench --bin ldbc-baseline -- \
    --nodes "${LDBC_NODES}" \
    --edges "${LDBC_EDGES}" \
    --db "${OUT_DIR}/ldbc-baseline.sombra" \
    --out-dir "${OUT_DIR}"
else
  echo "LDBC_NODES/EDGES not set; skipping baseline run"
fi

echo "bench results available under ${OUT_DIR}"
