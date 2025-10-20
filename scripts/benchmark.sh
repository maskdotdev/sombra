#!/bin/bash
set -e

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║             Sombra Performance Benchmarks                  ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

OUTPUT_DIR="target/benchmark-results-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "Results will be saved to: $OUTPUT_DIR"
echo ""

echo "━━━ Building in release mode ━━━"
cargo build --release --benches
echo "✓ Build complete"
echo ""

echo "━━━ Running benchmark suite ━━━"
echo ""

cargo bench --bench benchmark_main 2>&1 | tee "$OUTPUT_DIR/main.txt"
echo ""

cargo bench --bench read_benchmark 2>&1 | tee "$OUTPUT_DIR/reads.txt"
echo ""

cargo bench --bench index_benchmark 2>&1 | tee "$OUTPUT_DIR/indexes.txt"
echo ""

cargo bench --bench scalability_benchmark 2>&1 | tee "$OUTPUT_DIR/scalability.txt"
echo ""

echo "━━━ Benchmark Summary ━━━"
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo ""

if [ -d "target/criterion" ]; then
    echo "HTML reports available at:"
    echo "  file://$(pwd)/target/criterion/report/index.html"
    echo ""
fi

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║           ✓ Benchmarks Completed Successfully!            ║"
echo "╚═══════════════════════════════════════════════════════════╝"
