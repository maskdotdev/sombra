# GitHub Actions Workflows

This directory contains the CI/CD workflows for the Sombra monorepo.

## Overview

The repository uses **release-please** for automated releases across 5 packages:
- `sombra` (Rust) - Core library
- `sombradb` (Node.js) - Node.js bindings with native code
- `sombra` (Python) - Python bindings
- `sombra-cli` (Node.js) - CLI orchestrator
- `sombra-web` (Node.js) - Web UI

## Workflows

### 📋 Main Workflows

#### `release-please.yml`
**Orchestrator workflow** that manages releases for all packages.

**Triggers:** Push to `main` branch

**What it does:**
1. Runs release-please to detect conventional commits
2. Creates/updates release PRs for each package
3. When release PRs are merged, invokes appropriate publish workflows

**Outputs for each package:**
- `{package}_release` - Boolean if release was created
- `{package}_tag` - Git tag name (e.g., `cli-v0.1.0`)

#### `ci.yml`
**Continuous Integration** - Runs tests and checks on PRs.

### 📦 Publish Workflows (Reusable)

Each package has its own publish workflow that can be called by `release-please.yml` or triggered manually.

#### `publish-cli.yml`
Publishes the `sombra-cli` package to npm.

**Triggers:**
- Workflow call from `release-please.yml` (primary)
- Push to tag `cli-v*` (fallback)

**Steps:**
1. Checkout code at tag
2. Setup Bun runtime (required for `bun run build`)
3. Setup Node.js with npm registry auth
4. Build distributable bundle with `bun run build`
5. Verify package contents
6. Publish to npm with `--access public`

**Secrets required:** `NPM_TOKEN`

#### `publish-web.yml`
Publishes the `sombra-web` package to npm after building Next.js standalone.

**Triggers:**
- Workflow call from `release-please.yml` (primary)
- Push to tag `web-v*` (fallback)

**Steps:**
1. Checkout code at tag
2. Setup Node.js
3. Install dependencies
4. Build Next.js standalone (`npm run build`)
5. Package for npm distribution
6. Verify dist-npm/ output
7. Publish to npm with `--access public`

**Secrets required:** `NPM_TOKEN`

#### `publish-npm.yml`
Publishes the `sombradb` package (Node.js bindings with native code).

**Triggers:**
- Workflow call from `release-please.yml` (primary)
- Push to tag `sombrajs-v*` (fallback)

**Steps:**
1. Build native binaries for all platforms (macOS, Windows, Linux)
2. Collect artifacts
3. Publish to npm

**Secrets required:** `NPM_TOKEN`

#### `publish-rust.yml`
Publishes the `sombra` Rust crate to crates.io.

**Triggers:**
- Workflow call from `release-please.yml` (primary)
- Push to tag `sombra-v*` (fallback)

**Secrets required:** `CARGO_REGISTRY_TOKEN`

#### `publish-python.yml`
Publishes the `sombra` Python package to PyPI.

**Triggers:**
- Workflow call from `release-please.yml` (primary)
- Push to tag `sombrapy-v*` (fallback)

**Secrets required:** `PYPI_API_TOKEN`

## Release Process

### 1. Make Changes with Conventional Commits

Use the appropriate scope for your changes:

```bash
# CLI changes
git commit -m "feat(cli): add new seed command"
git commit -m "fix(cli): resolve binary discovery issue"

# Web changes
git commit -m "feat(web): add graph filtering"
git commit -m "fix(web): resolve rendering bug"

# Core changes
git commit -m "feat(core): add new traversal algorithm"

# Node.js bindings
git commit -m "fix(sombrajs): resolve memory leak"

# Python bindings
git commit -m "feat(sombrapy): add new API method"
```

### 2. Push to Main

Merge your PR to the `main` branch. Release-please will:
- Detect conventional commits
- Calculate appropriate version bump
- Create/update release PR(s)

### 3. Review Release PR

Check the release PR at: https://github.com/maskdotdev/sombra/pulls

Verify:
- ✅ Version bump is correct (major/minor/patch)
- ✅ CHANGELOG entries are accurate
- ✅ All commits are included

### 4. Merge Release PR

When you merge the release PR, release-please will:
- Create GitHub release
- Tag the commit
- Trigger the appropriate publish workflow

### 5. Automatic Publishing

The publish workflow will automatically:
- Build the package (if needed)
- Publish to npm/crates.io/PyPI
- Verify the publish succeeded

### 6. Verify

Check that the package is available:
- npm: `npm view sombra-cli` or `npm view sombra-web`
- crates.io: https://crates.io/crates/sombra
- PyPI: https://pypi.org/project/sombra/

## Version Bumping

Release-please automatically determines version bumps based on commit types:

| Commit Type | Version Bump | Example |
|-------------|--------------|---------|
| `feat(cli):` | Minor | 0.1.0 → 0.2.0 |
| `fix(cli):` | Patch | 0.1.0 → 0.1.1 |
| `feat(cli)!:` or `BREAKING CHANGE:` | Major | 0.1.0 → 1.0.0 |
| `docs(cli):` | None | No release |
| `chore(cli):` | None | No release |

## Required Secrets

The following secrets must be configured in the repository:

### GitHub Secrets

- `NPM_TOKEN` - npm token for publishing packages
  - Used by: `publish-cli.yml`, `publish-web.yml`, `publish-npm.yml`
  - Get from: https://www.npmjs.com/settings/~/tokens
  - Permissions: Publish

- `CARGO_REGISTRY_TOKEN` - crates.io token
  - Used by: `publish-rust.yml`
  - Get from: https://crates.io/settings/tokens
  - Permissions: Publish new versions

- `PYPI_API_TOKEN` - PyPI token
  - Used by: `publish-python.yml`
  - Get from: https://pypi.org/manage/account/token/
  - Scope: Project (sombra)

## Manual Publishing (Fallback)

If automated publishing fails, you can manually trigger workflows by pushing tags:

```bash
# CLI
git tag cli-v0.1.0
git push origin cli-v0.1.0

# Web
git tag web-v0.1.0
git push origin web-v0.1.0

# Rust
git tag sombra-v0.3.6
git push origin sombra-v0.3.6

# Node.js
git tag sombrajs-v0.4.15
git push origin sombrajs-v0.4.15

# Python
git tag sombrapy-v0.3.6
git push origin sombrapy-v0.3.6
```

## Troubleshooting

### Release PR Not Created

**Possible causes:**
1. No conventional commits since last release
2. Commits don't trigger releases (e.g., `docs`, `chore`)
3. Incorrect scope in commit message
4. Previous release PR still open

**Solution:**
- Check commit messages follow format: `type(scope): message`
- Use triggering types: `feat`, `fix`, `perf`
- Close old release PRs if stale

### Publish Workflow Failed

**Common issues:**

1. **Build failure**
   - Check workflow logs
   - Verify dependencies install correctly
   - Test build locally first

2. **npm publish permission denied**
   - Verify `NPM_TOKEN` secret is set
   - Check token hasn't expired
   - Verify token has publish permissions

3. **Version already exists**
   - Check if version was already published
   - May need to bump version manually

**Solution:**
- Check workflow logs in Actions tab
- Fix the issue
- Re-run workflow or manually publish

### Multiple Packages Released at Once

This is expected! The monorepo can release multiple packages independently:

```
feat(cli): add feature    → CLI v0.1.1
feat(web): add feature    → Web v0.1.1
fix(core): fix bug        → Core v0.3.7
```

Each gets its own:
- Release PR
- GitHub release
- Git tag
- Publish workflow

## Testing Changes

Before merging changes that affect workflows:

1. **Create test branch**
2. **Push and create PR** to see CI results
3. **For publish workflows:** Test in fork with dummy npm package
4. **Review logs carefully** before merging

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Developer: git commit -m "feat(cli): new feature"          │
│                                                              │
│  GitHub: Push to main                                       │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│  release-please.yml (orchestrator)                          │
│                                                              │
│  - Detects conventional commits                             │
│  - Creates/updates release PRs                              │
│  - Outputs: cli_tag, web_tag, etc.                         │
└──────────────────────────────────────────────────────────────┘
                              ↓
                    [Developer merges release PR]
                              ↓
┌──────────────────────────────────────────────────────────────┐
│  release-please.yml triggers publish workflows              │
│                                                              │
│  ├─→ publish-cli.yml    (if cli_tag exists)                │
│  ├─→ publish-web.yml    (if web_tag exists)                │
│  ├─→ publish-npm.yml    (if js_tag exists)                 │
│  ├─→ publish-rust.yml   (if rust_tag exists)               │
│  └─→ publish-python.yml (if py_tag exists)                 │
└──────────────────────────────────────────────────────────────┘
                              ↓
┌──────────────────────────────────────────────────────────────┐
│  Packages published to registries                           │
│                                                              │
│  - sombra-cli → npm                                         │
│  - sombra-web → npm                                         │
│  - sombradb → npm                                           │
│  - sombra → crates.io                                       │
│  - sombra → PyPI                                            │
└──────────────────────────────────────────────────────────────┘
```

## Best Practices

1. **Test locally before pushing**
   - Build packages locally
   - Run tests
   - Verify functionality

2. **Use descriptive commit messages**
   - Users read the CHANGELOG
   - Be specific about what changed
   - Include context when needed

3. **One feature per commit**
   - Makes CHANGELOG cleaner
   - Easier to track changes
   - Better for rollbacks

4. **Coordinate releases**
   - If CLI depends on web, release web first
   - Update COMPATIBILITY.md
   - Test integration before release

5. **Monitor workflow runs**
   - Check Actions tab after pushing
   - Verify publish succeeded
   - Test installation from registry

## Related Documentation

- `/release-please-config.json` - Release configuration
- `/.release-please-manifest.json` - Current versions
- `/RELEASE_PLEASE_SETUP.md` - Setup guide
- `/packages/cli/RELEASING.md` - CLI release guide
- `/packages/web/RELEASING.md` - Web release guide

## Support

If you encounter issues with workflows:

1. Check workflow logs in Actions tab
2. Review this documentation
3. Check package-specific RELEASING.md files
4. Open an issue if problem persists

Happy releasing! 🚀
