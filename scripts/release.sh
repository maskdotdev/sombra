#!/bin/bash
set -e

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║              Sombra Release Preparation                    ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

if [ $# -ne 1 ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.2.0"
    exit 1
fi

VERSION=$1
echo "Preparing release: v$VERSION"
echo ""

echo "━━━ Pre-flight Checks ━━━"

if [ -n "$(git status --porcelain)" ]; then
    echo "✗ Error: Working directory is not clean"
    echo "  Commit or stash your changes first"
    exit 1
fi
echo "✓ Working directory is clean"

CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "⚠ Warning: Not on main branch (currently on $CURRENT_BRANCH)"
    read -p "  Continue anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi
echo "✓ Branch check passed"
echo ""

echo "━━━ Running Test Suite ━━━"
./scripts/test-all.sh || {
    echo "✗ Tests failed. Fix issues before releasing."
    exit 1
}
echo ""

echo "━━━ Updating Version Numbers ━━━"

sed -i.bak "s/^version = \".*\"/version = \"$VERSION\"/" Cargo.toml
rm Cargo.toml.bak
echo "✓ Updated Cargo.toml"

if [ -f "package.json" ]; then
    npm version "$VERSION" --no-git-tag-version
    echo "✓ Updated package.json"
fi

if [ -f "pyproject.toml" ]; then
    sed -i.bak "s/^version = \".*\"/version = \"$VERSION\"/" pyproject.toml
    rm pyproject.toml.bak
    echo "✓ Updated pyproject.toml"
fi
echo ""

echo "━━━ Building Release Artifacts ━━━"
cargo build --release
./scripts/build-wheels.sh
echo "✓ Release artifacts built"
echo ""

echo "━━━ Creating Git Tag ━━━"
git add Cargo.toml Cargo.lock package.json package-lock.json pyproject.toml 2>/dev/null || true
git commit -m "chore: bump version to $VERSION"
git tag -a "v$VERSION" -m "Release version $VERSION"
echo "✓ Created tag v$VERSION"
echo ""

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║          ✓ Release v$VERSION Prepared!                    "
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "  1. Review the changes: git show"
echo "  2. Push to origin: git push && git push --tags"
echo "  3. GitHub Actions will automatically:"
echo "     - Build all platform binaries"
echo "     - Run tests across platforms"
echo "     - Publish to npm (if packages are whitelisted)"
echo ""
echo "⚠️  IMPORTANT: npm Spam Detection"
echo "  First-time releases of platform packages may be blocked by npm."
echo "  If you see '403 Package name triggered spam detection':"
echo "    → See NPM_SPAM_DETECTION.md for resolution steps"
echo "    → Contact npm support (typically resolves in 24-48 hours)"
echo "    → Re-run workflow after approval"
echo ""
echo "Manual publishing (if needed):"
echo "  4. Publish to crates.io: cargo publish"
echo "  5. Publish to npm: npm publish"
echo "  6. Publish to PyPI: twine upload dist/*"
echo ""
