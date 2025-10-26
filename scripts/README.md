# Sombra Scripts

This directory contains utility scripts for building, testing, and validating Sombra.

## Validation & Testing Scripts

### `validate-cli-web.sh`

Comprehensive validation script for the CLI and web packages. Runs 20 automated tests to ensure everything is working correctly.

**Usage:**
```bash
./scripts/validate-cli-web.sh
```

**Tests Performed:**
- ✅ CLI package structure and permissions
- ✅ CLI help commands
- ✅ Web package structure and dependencies
- ✅ Web build process (Next.js compilation)
- ✅ Standalone server generation
- ✅ Test database creation
- ✅ Web server startup
- ✅ REST API endpoints (/api/graph/*)
- ✅ Component and route structure
- ✅ SombraDB dependency resolution

**Exit Codes:**
- `0` - All tests passed
- `1` - One or more tests failed

### `demo-cli-web.sh`

Interactive demo showing the CLI and web UI in action. Creates a sample social network database and starts the web server.

**Usage:**
```bash
./scripts/demo-cli-web.sh
```

**What it does:**
1. Creates a demo database with people, companies, and cities
2. Tests CLI commands (help, web)
3. Starts the web server on port 13000
4. Tests REST API endpoints
5. Provides URL to access the web UI
6. Waits for user input before cleanup

Press Enter when done to stop the server and clean up.

## Build Scripts

### `build-wheels.sh`

Builds Python wheels for all platforms.

```bash
./scripts/build-wheels.sh
```

### `release.sh`

Prepares and publishes releases.

```bash
./scripts/release.sh
```

## Testing Scripts

### `test-all.sh`

Runs all test suites across the project.

```bash
./scripts/test-all.sh
```

### `benchmark.sh`

Runs performance benchmarks.

```bash
./scripts/benchmark.sh [benchmark-name]
```

## Utility Scripts

### `fix-exports.js`

Fixes npm package exports for the Node.js bindings.

```bash
node scripts/fix-exports.js
```

### `publish-with-delay.sh`

Publishes packages with delays to avoid registry rate limits.

```bash
./scripts/publish-with-delay.sh
```

## For Developers

### Running the Validation Suite

Before submitting a PR or publishing a release:

```bash
# Clean build
cd packages/web
rm -rf node_modules package-lock.json .next
npm install
npm run build

# Run validation
cd ../..
./scripts/validate-cli-web.sh
```

### Testing the CLI Locally

```bash
# Link the CLI locally (for testing without npm publish)
cd packages/cli
npm link

# Use it
sombra web --help
```

### Testing the Web Package Locally

```bash
cd packages/web

# Development mode
npm run dev

# Production build and test
npm run build
SOMBRA_DB_PATH=./test.db node dist-npm/start.js
```

## Troubleshooting

### "Cannot find module 'sombradb'"

The web package depends on the nodejs package. Ensure it's built:

```bash
cd packages/nodejs
npm run build
```

### "Cannot find native binding" (lightningcss/tailwindcss)

This is an npm optional dependencies issue. Fix:

```bash
cd packages/web
rm -rf node_modules package-lock.json
npm install
```

### Port already in use

Kill existing server processes:

```bash
lsof -ti:3000 | xargs kill -9
# or
pkill -f "dist-npm/start.js"
```

### Validation script fails on "Creating test database"

Ensure the nodejs package is built and sombradb is accessible:

```bash
cd packages/nodejs
npm run build
cd ../web
npm install
```

## CI/CD Integration

These scripts can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Validate CLI and Web
  run: ./scripts/validate-cli-web.sh
  
- name: Run benchmarks
  run: ./scripts/benchmark.sh
  
- name: Run all tests
  run: ./scripts/test-all.sh
```

## Script Maintenance

When updating scripts:

1. **Test thoroughly**: Run on different platforms (macOS, Linux, Windows)
2. **Update documentation**: Keep this README in sync
3. **Add validation**: Update `validate-cli-web.sh` for new features
4. **Version check**: Update required dependency versions
5. **Error handling**: Ensure scripts fail gracefully with clear messages

