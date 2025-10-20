#!/bin/bash
set -e

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║              Sombra Comprehensive Test Suite              ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

echo "━━━ Step 1: Code Format Check ━━━"
cargo fmt -- --check
echo "✓ Code formatting check passed"
echo ""

echo "━━━ Step 2: Clippy Lints ━━━"
cargo clippy --all-targets --all-features -- -D warnings
echo "✓ Clippy passed with no warnings"
echo ""

echo "━━━ Step 3: Unit Tests ━━━"
cargo test --lib --all-features
echo "✓ Unit tests passed"
echo ""

echo "━━━ Step 4: Integration Tests ━━━"
cargo test --tests --all-features
echo "✓ Integration tests passed"
echo ""

echo "━━━ Step 5: Doc Tests ━━━"
cargo test --doc --all-features
echo "✓ Doc tests passed"
echo ""

echo "━━━ Step 6: Build CLI Tools ━━━"
cargo build --bins --release
echo "✓ CLI tools built successfully"
echo ""

if command -v npm &> /dev/null; then
    echo "━━━ Step 7: Node.js Bindings ━━━"
    npm ci
    npm run build
    npm run test:all
    echo "✓ Node.js bindings tests passed"
    echo ""
fi

if command -v python3 &> /dev/null && command -v maturin &> /dev/null; then
    echo "━━━ Step 8: Python Bindings ━━━"
    python3 -m maturin build --release -F python
    echo "✓ Python bindings built successfully"
    echo ""
fi

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║              ✓ All Tests Passed Successfully!             ║"
echo "╚═══════════════════════════════════════════════════════════╝"
