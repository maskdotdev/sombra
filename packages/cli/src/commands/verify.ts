import { existsSync } from "fs";
import { loadSombraDB } from "../runtime/sombradb";
import { printField, printHeader, printSection } from "../utils/display";

function printUsage(): void {
	console.log(`┌─────────────────────────────────────────────┐
│         Sombra Database Verification        │
└─────────────────────────────────────────────┘

USAGE:
    sombra verify [OPTIONS] <database>

OPTIONS:
    --deep                Perform comprehensive deep verification
    --checksum-only       Verify only page checksums
    --skip-indexes        Skip index consistency validation
    --skip-adjacency      Skip adjacency validation
    --max-errors=N        Limit the number of reported issues (default 16)
    -h, --help            Show this help message

EXAMPLES:
    sombra verify graph.db
    sombra verify --deep graph.db
    sombra verify --checksum-only graph.db
    sombra verify --max-errors=100 graph.db
`);
}

export async function runVerifyCommand(argv: string[]): Promise<void> {
	let checksumOnly = false;
	let verifyIndexes = true;
	let verifyAdjacency = true;
	let deepVerify = false;
	let maxErrors = 16;
	let dbPath: string | null = null;

	for (const arg of argv) {
		if (arg === "--help" || arg === "-h") {
			printUsage();
			process.exit(0);
		}
		if (arg === "--deep") {
			deepVerify = true;
			verifyIndexes = true;
			verifyAdjacency = true;
			maxErrors = 100;
			continue;
		}
		if (arg === "--checksum-only") {
			checksumOnly = true;
			continue;
		}
		if (arg === "--skip-indexes") {
			verifyIndexes = false;
			continue;
		}
		if (arg === "--skip-adjacency") {
			verifyAdjacency = false;
			continue;
		}
		if (arg.startsWith("--max-errors=")) {
			const parsed = Number.parseInt(arg.split("=")[1], 10);
			if (Number.isNaN(parsed)) {
				console.error("Error: Invalid max-errors value");
				process.exit(1);
			}
			maxErrors = parsed;
			continue;
		}
		if (!dbPath) {
			dbPath = arg;
		} else {
			console.error(`Error: unexpected extra argument: ${arg}`);
			process.exit(1);
		}
	}

	if (!dbPath) {
		console.error("Error: database path required");
		console.log();
		process.exit(1);
	}

	if (!existsSync(dbPath)) {
		console.error(`Error: Database file not found: ${dbPath}`);
		process.exit(1);
	}

	printHeader("INTEGRITY VERIFICATION");

	if (deepVerify) {
		console.log("  Running deep integrity checks...");
	} else {
		console.log("  Running integrity checks...");
	}
	console.log();

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);

	const options = {
		checksumOnly,
		verifyIndexes,
		verifyAdjacency,
		maxErrors,
	};

	const report = db.verifyIntegrity(options);

	printSection("Verification Results");
	printField("Pages Checked", report.checkedPages);
	printField("Checksum Failures", report.checksumFailures);
	printField("Record Errors", report.recordErrors);
	printField("Index Errors", report.indexErrors);
	printField("Adjacency Errors", report.adjacencyErrors);

	if (report.errors && report.errors.length > 0) {
		printSection("Error Details");
		for (let i = 0; i < report.errors.length; i++) {
			console.log(`  ${i + 1}. ${report.errors[i]}`);
		}
	}

	console.log();

	const totalErrors =
		report.checksumFailures +
		report.recordErrors +
		report.indexErrors +
		report.adjacencyErrors;
	if (totalErrors === 0) {
		console.log("  ✓ No issues found - database is healthy!");
		console.log();
		console.log("  Status: PASS");
		console.log();
		process.exit(0);
	} else {
		console.log("  ✗ Integrity violations detected");
		console.log();
		console.log("  Status: FAIL");
		console.log();
		process.exit(1);
	}
}
