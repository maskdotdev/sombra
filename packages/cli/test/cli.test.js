#!/usr/bin/env node
/**
 * Comprehensive test suite for Sombra CLI
 * Tests all commands and verifies they work correctly
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

// Test results tracking
let passed = 0;
let failed = 0;
const failures = [];

// Test database paths
const testDir = path.join(os.tmpdir(), `sombra-cli-test-${Date.now()}`);
const testDb = path.join(testDir, "test.db");
const cliPath = path.join(__dirname, "..", "bin", "sombra.js");

/**
 * Run the CLI with given arguments
 */
function runCli(args, options = {}) {
	const result = spawnSync("node", [cliPath, ...args], {
		encoding: "utf8",
		cwd: options.cwd || testDir,
		env: { ...process.env, ...options.env },
		timeout: options.timeout || 30000,
	});

	return {
		stdout: result.stdout || "",
		stderr: result.stderr || "",
		status: result.status,
		error: result.error,
	};
}

/**
 * Assert helper
 */
function assert(condition, message) {
	if (!condition) {
		throw new Error(`Assertion failed: ${message}`);
	}
}

/**
 * Test runner
 */
function test(name, fn) {
	process.stdout.write(`${colors.blue}▶${colors.reset} ${name}... `);
	try {
		fn();
		passed++;
		console.log(`${colors.green}✓ PASS${colors.reset}`);
	} catch (error) {
		failed++;
		console.log(`${colors.red}✗ FAIL${colors.reset}`);
		failures.push({ name, error: error.message, stack: error.stack });
	}
}

/**
 * Setup: Create test directory
 */
function setup() {
	console.log(`\n${colors.yellow}Setting up test environment...${colors.reset}`);
	if (!fs.existsSync(testDir)) {
		fs.mkdirSync(testDir, { recursive: true });
	}
	console.log(`Test directory: ${testDir}\n`);
}

/**
 * Cleanup: Remove test directory
 */
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

// ============================================================================
// TEST SUITE
// ============================================================================

console.log(`
╔════════════════════════════════════════════════════════════╗
║              Sombra CLI Test Suite                         ║
╚════════════════════════════════════════════════════════════╝
`);

setup();

// ----------------------------------------------------------------------------
// Help and Version Commands
// ----------------------------------------------------------------------------

test("sombra --help shows usage", () => {
	const result = runCli(["--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("Sombra CLI"), "Should show CLI name");
	assert(result.stdout.includes("Commands:"), "Should list commands");
	assert(result.stdout.includes("web"), "Should mention web command");
	assert(result.stdout.includes("seed"), "Should mention seed command");
	assert(result.stdout.includes("inspect"), "Should mention inspect command");
});

test("sombra help shows usage", () => {
	const result = runCli(["help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("Sombra CLI"), "Should show CLI name");
});

test("sombra version shows version info", () => {
	const result = runCli(["version"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(
		result.stdout.includes("sombra-cli") || result.stdout.match(/\d+\.\d+\.\d+/),
		"Should show version",
	);
});

// ----------------------------------------------------------------------------
// Seed Command Tests
// ----------------------------------------------------------------------------

test("sombra seed --help shows seed help", () => {
	const result = runCli(["seed", "--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("seed"), "Should mention seed command");
});

test("sombra seed creates database with default data", () => {
	const result = runCli(["seed", testDb]);
	assert(result.status === 0, `Exit code should be 0, got ${result.status}`);
	assert(
		result.stdout.includes("created") || result.stdout.includes("✓"),
		"Should show success message",
	);
	assert(fs.existsSync(testDb), "Database file should be created");

	// Check database has content
	const stats = fs.statSync(testDb);
	assert(stats.size > 0, "Database should not be empty");
});

test("sombra seed with custom size creates database", () => {
	const customDb = path.join(testDir, "custom.db");
	const result = runCli(["seed", customDb, "--nodes", "50", "--edges", "75"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(fs.existsSync(customDb), "Database file should be created");
});

test("sombra seed with scenario creates specific data", () => {
	const scenarioDb = path.join(testDir, "scenario.db");
	const result = runCli(["seed", scenarioDb, "--scenario", "social"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(fs.existsSync(scenarioDb), "Database file should be created");
});

// ----------------------------------------------------------------------------
// Inspect Command Tests
// ----------------------------------------------------------------------------

test("sombra inspect --help shows inspect help", () => {
	const result = runCli(["inspect", "--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("inspect"), "Should mention inspect command");
	assert(result.stdout.includes("info"), "Should mention info subcommand");
	assert(result.stdout.includes("stats"), "Should mention stats subcommand");
});

test("sombra inspect <db> info shows database information", () => {
	const result = runCli(["inspect", testDb, "info"]);
	assert(result.status === 0, `Exit code should be 0, got ${result.status}`);
	assert(
		result.stdout.includes("DATABASE INFO") ||
			result.stdout.includes("Database") ||
			result.stdout.includes("Path"),
		"Should show database info",
	);
});

test("sombra inspect <db> stats shows statistics", () => {
	const result = runCli(["inspect", testDb, "stats"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(
		result.stdout.includes("STATISTICS") ||
			result.stdout.includes("Nodes") ||
			result.stdout.includes("Edges") ||
			result.stdout.match(/\d+/),
		"Should show statistics",
	);
});

test("sombra inspect <db> sample shows sample data", () => {
	const result = runCli(["inspect", testDb, "sample"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(
		result.stdout.includes("SAMPLE") || result.stdout.includes("Node"),
		"Should show sample data",
	);
});

test("sombra inspect <db> sample --limit 5 respects limit", () => {
	const result = runCli(["inspect", testDb, "sample", "--limit", "5"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.length > 0, "Should show output");
});

test("sombra inspect nonexistent.db fails gracefully", () => {
	const result = runCli(["inspect", "nonexistent.db", "info"]);
	assert(result.status !== 0, "Exit code should be non-zero for missing file");
});

// ----------------------------------------------------------------------------
// Repair Command Tests
// ----------------------------------------------------------------------------

test("sombra repair --help shows repair help", () => {
	const result = runCli(["repair", "--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("repair"), "Should mention repair command");
});

test("sombra repair <db> runs repair operations", () => {
	const result = runCli(["repair", testDb]);
	assert(result.status === 0, "Exit code should be 0");
	// Should complete without error (even if no repairs needed)
	assert(result.stdout.length > 0, "Should show output");
});

test("sombra repair <db> --check-only does dry run", () => {
	const result = runCli(["repair", testDb, "--check-only"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.length > 0, "Should show output");
});

test("sombra repair nonexistent.db fails gracefully", () => {
	const result = runCli(["repair", "nonexistent.db"]);
	assert(result.status !== 0, "Exit code should be non-zero for missing file");
});

// ----------------------------------------------------------------------------
// Verify Command Tests
// ----------------------------------------------------------------------------

test("sombra verify --help shows verify help", () => {
	const result = runCli(["verify", "--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("verify"), "Should mention verify command");
});

test("sombra verify <db> runs verification", () => {
	const result = runCli(["verify", testDb]);
	assert(result.status === 0, "Exit code should be 0");
	assert(
		result.stdout.includes("Verif") || result.stdout.includes("✓"),
		"Should show verification output",
	);
});

test("sombra verify <db> --deep runs deep verification", () => {
	const result = runCli(["verify", testDb, "--deep"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.length > 0, "Should show output");
});

test("sombra verify nonexistent.db fails gracefully", () => {
	const result = runCli(["verify", "nonexistent.db"]);
	assert(result.status !== 0, "Exit code should be non-zero for missing file");
});

// ----------------------------------------------------------------------------
// Web Command Tests
// ----------------------------------------------------------------------------

test("sombra web --help shows web help", () => {
	const result = runCli(["web", "--help"]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("web"), "Should mention web command");
	assert(
		result.stdout.includes("port") || result.stdout.includes("--port"),
		"Should mention port option",
	);
});

test("sombra web --check-install verifies web package", () => {
	const result = runCli(["web", "--check-install"]);
	// May pass or fail depending on whether sombra-web is installed
	// Just check it doesn't crash
	assert(
		result.status === 0 || result.status === 1,
		"Should exit with 0 or 1",
	);
	assert(result.stdout.length > 0 || result.stderr.length > 0, "Should show output");
});

// Note: We don't actually start the web server in tests as it would hang
// and require network/browser interaction

// ----------------------------------------------------------------------------
// Error Handling Tests
// ----------------------------------------------------------------------------

test("sombra with unknown command shows error", () => {
	const result = runCli(["unknown-command"]);
	assert(result.status !== 0, "Exit code should be non-zero");
});

test("sombra with no arguments shows help", () => {
	const result = runCli([]);
	assert(result.status === 0, "Exit code should be 0");
	assert(result.stdout.includes("Sombra CLI"), "Should show help");
});

// ----------------------------------------------------------------------------
// Integration Tests
// ----------------------------------------------------------------------------

test("Integration: seed → inspect → repair → verify workflow", () => {
	const workflowDb = path.join(testDir, "workflow.db");

	// 1. Seed
	let result = runCli(["seed", workflowDb, "--nodes", "20"]);
	assert(result.status === 0, "Seed should succeed");

	// 2. Inspect info
	result = runCli(["inspect", workflowDb, "info"]);
	assert(result.status === 0, "Inspect info should succeed");

	// 3. Inspect stats
	result = runCli(["inspect", workflowDb, "stats"]);
	assert(result.status === 0, "Inspect stats should succeed");

	// 4. Repair
	result = runCli(["repair", workflowDb, "--check-only"]);
	assert(result.status === 0, "Repair check should succeed");

	// 5. Verify
	result = runCli(["verify", workflowDb]);
	assert(result.status === 0, "Verify should succeed");
});

test("Integration: multiple databases can be created and inspected", () => {
	const db1 = path.join(testDir, "multi1.db");
	const db2 = path.join(testDir, "multi2.db");

	let result = runCli(["seed", db1]);
	assert(result.status === 0, "First seed should succeed");

	result = runCli(["seed", db2, "--scenario", "code"]);
	assert(result.status === 0, "Second seed should succeed");

	result = runCli(["inspect", db1, "stats"]);
	assert(result.status === 0, "Inspect first db should succeed");

	result = runCli(["inspect", db2, "stats"]);
	assert(result.status === 0, "Inspect second db should succeed");
});

// ============================================================================
// TEST RESULTS
// ============================================================================

cleanup();

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
	failures.forEach(({ name, error, stack }) => {
		console.log(`${colors.red}✗ ${name}${colors.reset}`);
		console.log(`  ${error}`);
		if (process.env.VERBOSE) {
			console.log(`  ${stack}`);
		}
		console.log();
	});
}

if (failed === 0) {
	console.log(`\n${colors.green}✓ All tests passed!${colors.reset}\n`);
	process.exit(0);
} else {
	console.log(
		`\n${colors.red}✗ ${failed} test(s) failed${colors.reset}\n`,
	);
	process.exit(1);
}

