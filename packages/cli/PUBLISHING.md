# Publishing Guide for @sombra/cli

This guide explains how to publish and distribute the Sombra CLI to users.

## Package Structure

The Sombra CLI ecosystem consists of three npm packages:

1. **`@sombra/cli`** - Main CLI orchestrator (this package)
2. **`@sombra/web`** - Web UI runtime
3. **`sombradb`** - Node.js/TypeScript library with native bindings

## Pre-Publishing Checklist

### 1. Update Version

Update version in `package.json`:

```bash
cd packages/cli
npm version patch  # or minor, or major
```

### 2. Test Locally

Test the CLI before publishing:

```bash
# Test web command
node bin/sombra.js web --help
node bin/sombra.js web --db test.db

# Test with Rust binary installed
node bin/sombra.js inspect test.db info
```

### 3. Test Global Installation

Test as if installed globally:

```bash
# Link locally
npm link

# Test commands
sombra web --help
sombra inspect --help

# Unlink
npm unlink -g @sombra/cli
```

## Publishing Steps

### Option 1: Manual Publishing

```bash
cd packages/cli

# Login to npm (first time only)
npm login

# Publish
npm publish --access public
```

### Option 2: Automated Publishing (Recommended)

Use the workspace release script:

```bash
cd /path/to/sombra

# Publish CLI
cd packages/cli
npm publish --access public

# Publish Web UI (if updated)
cd ../web
npm run prepack  # Builds Next.js standalone
npm publish --access public
```

## Post-Publishing

### 1. Verify Package

```bash
# Install from npm
npm install -g @sombra/cli@latest

# Test commands
sombra --help
sombra web --help
```

### 2. Test Installation Flow

Test the complete user installation experience:

```bash
# Clean environment test
docker run -it node:18 bash

# Inside container
npm install -g @sombra/cli
sombra web --help
sombra web --install  # Pre-download web UI

# Test with Rust binary
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
cargo install sombra
sombra inspect --help
```

### 3. Update Documentation

Update version references in:
- `README.md` (root)
- `docs/cli.md`
- `docs/getting-started.md`
- `COMPATIBILITY.md`

## Distribution Strategy

### For npm Users

Users install with:

```bash
npm install -g @sombra/cli
```

This gives them:
- ✅ `sombra web` command (works immediately)
- ⚠️ `sombra inspect/repair/verify` (requires Rust binary)

### For Rust Users

Users install with:

```bash
cargo install sombra
```

This gives them:
- ✅ `sombra inspect/repair/verify` commands (works immediately)
- ⚠️ `sombra web` command (requires Node.js and npm)

### Recommended Installation (Best UX)

Document this installation flow for users who want everything:

```bash
# 1. Install Rust binary (for inspect/repair/verify)
cargo install sombra

# 2. Install CLI orchestrator (for web UI)
npm install -g @sombra/cli

# Now all commands work!
sombra web          # Web UI
sombra inspect      # Database inspection
sombra repair       # Database repair
sombra verify       # Integrity checks
```

## Version Compatibility

Maintain compatibility between packages:

| Package | Version | Notes |
|---------|---------|-------|
| `@sombra/cli` | 0.1.x | CLI orchestrator |
| `@sombra/web` | 0.1.x | Web UI (auto-installed) |
| `sombradb` | 0.4.x | Node.js library |
| `sombra` (Rust) | 0.3.x | Core library + binary |

Update `COMPATIBILITY.md` when publishing new versions.

## Release Workflow

### For Patch Releases (Bug Fixes)

```bash
cd packages/cli
npm version patch
npm publish --access public

# Tag in git
git add .
git commit -m "chore(cli): release v0.1.1"
git tag cli-v0.1.1
git push origin main --tags
```

### For Minor Releases (New Features)

```bash
cd packages/cli
npm version minor
npm publish --access public

# Update changelog
echo "## [0.2.0] - $(date +%Y-%m-%d)" >> CHANGELOG.md
echo "### Added" >> CHANGELOG.md
echo "- New feature description" >> CHANGELOG.md

git add .
git commit -m "chore(cli): release v0.2.0"
git tag cli-v0.2.0
git push origin main --tags
```

### For Major Releases (Breaking Changes)

```bash
cd packages/cli
npm version major
npm publish --access public

# Update all documentation
# Update migration guide
# Announce breaking changes

git add .
git commit -m "chore(cli): release v1.0.0"
git tag cli-v1.0.0
git push origin main --tags
```

## Publishing @sombra/web

The web package requires special handling:

```bash
cd packages/web

# 1. Build Next.js standalone
npm run build

# 2. Package for npm
node scripts/package-web-runtime.js

# 3. Publish
npm publish --access public
```

The `@sombra/web` package:
- Contains a full Next.js standalone server
- Is auto-installed by `@sombra/cli` on first `sombra web` use
- Can be pinned to specific versions with `--version-pin`

## Troubleshooting

### Package Not Found After Publishing

Wait a few minutes for npm CDN propagation, then:

```bash
npm view @sombra/cli
```

### Permission Errors

Ensure you have access to the `@sombra` npm organization:

```bash
npm owner ls @sombra/cli
```

If you don't have access, request it from the organization owner.

### File Not Included

Check `package.json` files array:

```json
{
  "files": [
    "bin/**",
    "README.md",
    "LICENSE"
  ]
}
```

Test what will be included:

```bash
npm pack --dry-run
```

## CI/CD Integration

### GitHub Actions Workflow

Create `.github/workflows/publish-cli.yml`:

```yaml
name: Publish CLI

on:
  push:
    tags:
      - 'cli-v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
          registry-url: 'https://registry.npmjs.org'
      
      - name: Install dependencies
        run: |
          cd packages/cli
          npm ci || npm install
      
      - name: Publish to npm
        run: |
          cd packages/cli
          npm publish --access public
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

### Manual Trigger

For manual releases without tags:

```bash
cd packages/cli

# Publish with specific tag
npm publish --access public --tag beta
npm publish --access public --tag latest
```

## User Communication

### Announce New Version

When publishing a new version:

1. **GitHub Release** - Create release notes
2. **npm Registry** - Automatically shows README
3. **Documentation** - Update version in docs
4. **Social Media** - Announce major features
5. **Discord/Slack** - Notify community

### Breaking Changes

For breaking changes:

1. Update `CHANGELOG.md` with migration guide
2. Bump major version
3. Keep old version available
4. Provide clear migration instructions

## Monitoring

After publishing, monitor:

1. **npm downloads** - `npm view @sombra/cli`
2. **GitHub issues** - Installation problems
3. **User feedback** - Feature requests
4. **Error reports** - Runtime issues

## Support

Common user issues:

### "Binary not found"

Guide users to install Rust binary:
```bash
cargo install sombra
```

### "@sombra/web not starting"

Guide users to update:
```bash
sombra web --update
```

### "Permission denied"

Guide users to use --user flag or npx:
```bash
npm install --user -g @sombra/cli
# or
npx @sombra/cli web
```

## Security

### npm Token Security

- Use 2FA for npm account
- Store `NPM_TOKEN` in GitHub secrets
- Use automation tokens (not user tokens)
- Rotate tokens regularly

### Package Integrity

- Enable npm 2FA
- Sign Git tags
- Use lock files
- Verify published package contents

## Rollback Procedure

If a bad version is published:

```bash
# Deprecate the bad version
npm deprecate @sombra/cli@0.1.5 "Known issues, use 0.1.6"

# Or unpublish (within 72 hours only)
npm unpublish @sombra/cli@0.1.5

# Publish fixed version
npm version patch
npm publish --access public
```

## Resources

- [npm Publishing Docs](https://docs.npmjs.com/cli/v9/commands/npm-publish)
- [Semantic Versioning](https://semver.org/)
- [npm Organization Management](https://docs.npmjs.com/orgs/)
- [GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github)

