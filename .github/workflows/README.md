# GitHub Actions Workflows

This directory contains the CI/CD workflows for the Sombra monorepo.

## Workflow Architecture

### Release Automation

The release process is orchestrated through **reusable workflows** that avoid the GitHub Actions limitation where tags created by `GITHUB_TOKEN` don't trigger `push` events.

#### Main Workflow: `release-please.yml`

Triggers on pushes to `main` and coordinates the entire release process:

1. **Release Please Action** - Creates/updates release PRs with version bumps and changelogs
2. **Conditional Publishing** - When a release PR is merged, invokes the appropriate publish workflow(s) via `workflow_call`

#### Reusable Publish Workflows

Each package has its own publish workflow that accepts a `tag` input:

- **`publish-npm.yml`** - Builds and publishes the Node.js package to npm
- **`publish-rust.yml`** - Publishes the Rust crate to crates.io
- **`publish-python.yml`** - Builds wheels and publishes to PyPI

These workflows can be triggered in two ways:
1. **Primary**: Called by `release-please.yml` with the release tag as input
2. **Fallback**: Direct tag push or release creation (for manual intervention)

### Why This Architecture?

When Release Please creates a tag using `GITHUB_TOKEN`, GitHub **does not fire push events** for that tag (to prevent recursive workflow triggers). This means workflows with `on.push.tags` won't run automatically.

Our solution: Use `workflow_call` to explicitly invoke publish workflows from the release-please workflow, passing the tag as an input parameter.

### Required Secrets

Ensure these secrets are configured in the repository settings:

- `NPM_TOKEN` - npm publish token with provenance support
- `CARGO_REGISTRY_TOKEN` - crates.io API token
- `PYPI_API_TOKEN` - PyPI API token for publishing

### CI Workflow: `ci.yml`

Runs on PRs and pushes to `main`:
- Linting (rustfmt, clippy)
- Multi-platform builds (macOS, Windows, Linux)
- Test matrix across Node.js versions
- Conditional npm publishing (only on version tags)

## Debugging Release Issues

### Release doesn't publish

1. Check if the release PR was properly merged (not closed without merging)
2. Verify the workflow outputs in `release-please.yml` show `js_release: true` (or `rust_release`/`py_release`)
3. Ensure the correct secrets are configured
4. Check the Actions tab for workflow run details

### Manual republishing

If you need to manually trigger a publish:

1. For npm: Push a tag matching `sombrajs-v*` pattern
2. For Rust/Python: Create a GitHub release with tag matching `sombra-v*` or `sombrapy-v*`

Or run the workflow manually via GitHub Actions UI (if workflow_dispatch is added).

## Testing Workflows Locally

Use [act](https://github.com/nektos/act) to test workflows locally:

```bash
# Test the release-please workflow (dry-run)
act push -W .github/workflows/release-please.yml -n

# Test a publish workflow with mock inputs
act workflow_call -W .github/workflows/publish-npm.yml \
  -s NPM_TOKEN=test-token \
  --input tag=sombrajs-v0.4.6
```

## Modifying Workflows

When making changes to these workflows:

1. Ensure reusable workflows maintain both `workflow_call` and their fallback triggers
2. Test the entire flow with a draft release PR before merging changes
3. Update this README if the architecture changes
4. Keep `docs/contributing.md` in sync with the release process description

