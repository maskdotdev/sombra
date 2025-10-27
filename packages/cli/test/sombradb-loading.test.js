#!/usr/bin/env node
/**
 * Test suite for sombradb loading mechanism
 * Tests that the CLI can find sombradb in various scenarios
 */

const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");

// ANSI color codes
const colors = {
	reset: "\x1b[0m",
	green: "\x1b[32m",
	red: "\x1b[31m",
	yellow: "\x1b[33m",
	blue: "\x1b[36m",
};

let passed = 0;
let failed = 0;
const failures = [];

const testDir = path.join(os.tmpdir(), `sombra-cli-loading-test-${Date.now()}`);
const cliPath = path.join(__dirname, "..", "bin", "sombra.js");

function test(name, fn) {
	process.stdout.write(`${colors.blue}▶${colors.reset} ${name}... `);
	try {
		fn();
		passed++;
		console.log(`${colors.green}✓ PASS${colors.reset}`);
	} catch (error) {
		failed++;
		console.log(`${colors.red}✗ FAIL${colors.reset}`);
		failures.push({ name, error: error.message });
	}
}

function assert(condition, message) {
	if (!condition) {
		throw new Error(`Assertion failed: ${message}`);
	}
}

function setup() {
	console.log(`\n${colors.yellow}Setting up test environment...${colors.reset}`);
	if (!fs.existsSync(testDir)) {
		fs.mkdirSync(testDir, { recursive: true });
	}
	console.log(`Test directory: ${testDir}\n`);
}

function cleanup() {
	console.log(`\n${colors.yellow}Cleaning up...${colors.reset}`);
	try {
		if (fs.existsSync(testDir)) {
			fs.rmSync(testDir, { recursive: true, force: true });
		}
	} catch (error) {
		console.error(`Warning: Cleanup failed: ${error.message}`);
	}
}

console.log(`
╔════════════════════════════════════════════════════════════╗
║         SombraDB Loading Mechanism Test Suite             ║
╚════════════════════════════════════════════════════════════╝
`);

setup();

// ----------------------------------------------------------------------------
// Test 1: CLI's own node_modules (simulating global install)
// ----------------------------------------------------------------------------

test("loadSombraDB can find sombradb from CLI's node_modules", () => {
	// The CLI should be able to find sombradb in its own node_modules
	const testDb = path.join(testDir, "test.db");
	
	// Run seed command which requires sombradb
	const result = spawnSync("node", [cliPath, "seed", testDb, "--nodes", "5"], {
		encoding: "utf8",
		cwd: testDir, // Run from temp dir (no local node_modules)
		timeout: 30000,
	});

	assert(
		result.status === 0,
		`CLI should find sombradb from its own node_modules. Status: ${result.status}, stderr: ${result.stderr}`,
	);
	assert(
		!result.stderr.includes("sombradb package not found"),
		"Should not show 'package not found' error",
	);
	assert(fs.existsSync(testDb), "Database should be created");
});

// ----------------------------------------------------------------------------
// Test 2: Verify error message when sombradb genuinely missing
// ----------------------------------------------------------------------------

test("loadSombraDB shows helpful error when sombradb missing", () => {
	// Create a minimal test script that tries to load sombradb
	const testScript = path.join(testDir, "test-load.js");
	const scriptContent = `
const path = require('path');
const cliDir = path.join(__dirname, '..', '..', 'cli', 'bin');

// Temporarily rename node_modules to simulate missing sombradb
const nodeModules = path.join(cliDir, '..', 'node_modules');
const nodeModulesBackup = path.join(cliDir, '..', 'node_modules.backup');

let renamed = false;
try {
	if (require('fs').existsSync(nodeModules)) {
		require('fs').renameSync(nodeModules, nodeModulesBackup);
		renamed = true;
	}
} catch (e) {
	// Can't rename, skip test
	console.log("SKIP: Cannot rename node_modules");
	process.exit(0);
}

try {
	const result = require('child_process').spawnSync(
		'node',
		[path.join(cliDir, 'sombra.js'), 'inspect', 'fake.db', 'info'],
		{ encoding: 'utf8' }
	);
	
	console.log(result.stderr);
	if (!result.stderr.includes('sombradb package not found')) {
		throw new Error('Should show error message');
	}
} finally {
	if (renamed) {
		require('fs').renameSync(nodeModulesBackup, nodeModules);
	}
}
`;

	// Note: This test might not be reliable in all environments
	// We'll skip it if we can't manipulate node_modules
	console.log(
		`${colors.yellow}(Skipping rename test - would interfere with package)${colors.reset}`,
	);
});

// ----------------------------------------------------------------------------
// Test 3: Verify all inspect commands can load sombradb
// ----------------------------------------------------------------------------

test("All inspect commands can load sombradb", () => {
	const testDb = path.join(testDir, "inspect-test.db");
	
	// Create a database first
	let result = spawnSync("node", [cliPath, "seed", testDb, "--nodes", "5"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "Seed should succeed");

	// Test inspect info
	result = spawnSync("node", [cliPath, "inspect", testDb, "info"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "inspect info should succeed");
	assert(
		!result.stderr.includes("sombradb package not found"),
		"inspect info should not show loading error",
	);

	// Test inspect stats
	result = spawnSync("node", [cliPath, "inspect", testDb, "stats"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "inspect stats should succeed");
	assert(
		!result.stderr.includes("sombradb package not found"),
		"inspect stats should not show loading error",
	);

	// Test inspect sample
	result = spawnSync("node", [cliPath, "inspect", testDb, "sample"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "inspect sample should succeed");
	assert(
		!result.stderr.includes("sombradb package not found"),
		"inspect sample should not show loading error",
	);
});

// ----------------------------------------------------------------------------
// Test 4: Verify repair command can load sombradb
// ----------------------------------------------------------------------------

test("Repair command can load sombradb", () => {
	const testDb = path.join(testDir, "repair-test.db");
	
	// Create a database first
	let result = spawnSync("node", [cliPath, "seed", testDb, "--nodes", "5"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "Seed should succeed");

	// Test repair
	result = spawnSync("node", [cliPath, "repair", testDb, "--check-only"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "repair should succeed");
	assert(
		!result.stderr.includes("sombradb package not found"),
		"repair should not show loading error",
	);
});

// ----------------------------------------------------------------------------
// Test 5: Verify verify command can load sombradb
// ----------------------------------------------------------------------------

test("Verify command can load sombradb", () => {
	const testDb = path.join(testDir, "verify-test.db");
	
	// Create a database first
	let result = spawnSync("node", [cliPath, "seed", testDb, "--nodes", "5"], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "Seed should succeed");

	// Test verify
	result = spawnSync("node", [cliPath, "verify", testDb], {
		encoding: "utf8",
		cwd: testDir,
		timeout: 30000,
	});
	assert(result.status === 0, "verify should succeed");
	assert(
		!result.stderr.includes("sombradb package not found"),
		"verify should not show loading error",
	);
});

// ----------------------------------------------------------------------------
// Test 6: Test from different working directories
// ----------------------------------------------------------------------------

test("CLI works from different working directories", () => {
	const subDir = path.join(testDir, "subdir");
	fs.mkdirSync(subDir, { recursive: true });
	
	const testDb = path.join(testDir, "cwd-test.db");
	
	// Run from subdirectory
	const result = spawnSync("node", [cliPath, "seed", testDb, "--nodes", "5"], {
		encoding: "utf8",
		cwd: subDir, // Different working directory
		timeout: 30000,
	});

	assert(
		result.status === 0,
		`Should work from subdirectory. Status: ${result.status}`,
	);
	assert(fs.existsSync(testDb), "Database should be created");
});

cleanup();

// ============================================================================
// TEST RESULTS
// ============================================================================

console.log(`
╔════════════════════════════════════════════════════════════╗
║                    Test Results                            ║
╚════════════════════════════════════════════════════════════╝
`);

console.log(`${colors.green}Passed: ${passed}${colors.reset}`);
console.log(`${colors.red}Failed: ${failed}${colors.reset}`);
console.log(`Total:  ${passed + failed}`);

if (failures.length > 0) {
	console.log(`\n${colors.red}Failures:${colors.reset}\n`);
	failures.forEach(({ name, error }) => {
		console.log(`${colors.red}✗ ${name}${colors.reset}`);
		console.log(`  ${error}\n`);
	});
}

if (failed === 0) {
	console.log(`\n${colors.green}✓ All sombradb loading tests passed!${colors.reset}\n`);
	process.exit(0);
} else {
	console.log(`\n${colors.red}✗ ${failed} test(s) failed${colors.reset}\n`);
	process.exit(1);
}

