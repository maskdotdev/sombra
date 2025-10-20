#!/bin/bash
set -e

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║          Sombra Multi-Platform Wheel Builder              ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

if ! command -v maturin &> /dev/null; then
    echo "Error: maturin is not installed"
    echo "Install with: pip install maturin"
    exit 1
fi

OUTPUT_DIR="dist"
mkdir -p "$OUTPUT_DIR"

echo "━━━ Building Python Wheel ━━━"
python3 -m maturin build --release -F python --out "$OUTPUT_DIR"
echo "✓ Wheel built successfully"
echo ""

echo "━━━ Building Node.js Bindings ━━━"
if command -v npm &> /dev/null; then
    npm ci
    npm run build
    echo "✓ Node.js bindings built successfully"
    echo ""
fi

echo "━━━ Build Artifacts ━━━"
echo ""
echo "Python wheels:"
ls -lh "$OUTPUT_DIR"/*.whl 2>/dev/null || echo "  (none)"
echo ""
echo "Node.js bindings:"
ls -lh sombra.*.node 2>/dev/null || echo "  (none)"
echo ""

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║             ✓ Wheels Built Successfully!                  ║"
echo "╚═══════════════════════════════════════════════════════════╝"
