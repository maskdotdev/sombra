import { existsSync, statSync } from "fs";
import { loadSombraDB } from "../runtime/sombradb";
import { formatBytes, printField, printHeader, printSection } from "../utils/display";

function runInfo(dbPath: string): void {
	printHeader("DATABASE INFO");

	const { SombraDB, getDefaultPageSize } = loadSombraDB();
	const db = new SombraDB(dbPath);

	const fileSize = statSync(dbPath).size;
	const pageSize = getDefaultPageSize();
	const header = db.getHeader();

	printSection("General");
	printField("Database Path", dbPath);
	printField("File Size", formatBytes(fileSize));
	printField("Page Size", `${pageSize} bytes`);

	printSection("Graph Statistics");
	printField("Total Nodes", header.nextNodeId);
	printField("Total Edges", header.nextEdgeId);

	printSection("Storage");
	if (header.freePageHead !== undefined && header.freePageHead !== null) {
		printField("Free Page List Head", header.freePageHead);
	} else {
		printField("Free Page List Head", "None");
	}
	if (header.lastRecordPage !== undefined && header.lastRecordPage !== null) {
		printField("Last Record Page", header.lastRecordPage);
	} else {
		printField("Last Record Page", "None");
	}

	printSection("Index");
	if (header.btreeIndexPage !== undefined && header.btreeIndexPage !== null) {
		printField("BTree Index Root Page", header.btreeIndexPage);
		printField("BTree Index Size", `${header.btreeIndexSize} entries`);
	} else {
		printField("BTree Index", "Not initialized");
	}

	printSection("Transactions");
	printField("Last Committed TX ID", header.lastCommittedTxId);

	console.log();
	console.log("✓ Database opened successfully");
	console.log();
}

function runStats(dbPath: string): void {
	printHeader("DATABASE STATISTICS");

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);
	const metrics = db.getMetrics();

	printSection("Performance Metrics");
	printField("Cache Hits", metrics.cacheHits);
	printField("Cache Misses", metrics.cacheMisses);

	const totalAccesses = metrics.cacheHits + metrics.cacheMisses;
	if (totalAccesses > 0) {
		const hitRate = (metrics.cacheHits / totalAccesses) * 100.0;
		printField("Cache Hit Rate", `${hitRate.toFixed(2)}%`);
	}

	printField("Node Lookups", metrics.nodeLookups);
	printField("Edge Traversals", metrics.edgeTraversals);

	printSection("Write-Ahead Log");
	printField("WAL Bytes Written", formatBytes(metrics.walBytesWritten));
	printField("WAL Syncs", metrics.walSyncs);
	printField("Checkpoints", metrics.checkpointsPerformed);
	printField("Page Evictions", metrics.pageEvictions);

	printSection("Transactions");
	printField("Transactions Committed", metrics.transactionsCommitted);
	printField("Transactions Rolled Back", metrics.transactionsRolledBack);

	console.log();
}

function runVerify(dbPath: string): void {
	printHeader("INTEGRITY VERIFICATION");

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);

	console.log("  Running integrity checks...");
	console.log();

	const options = {
		checksumOnly: false,
		maxErrors: 100,
		verifyIndexes: true,
		verifyAdjacency: true,
	};

	const report = db.verifyIntegrity(options);

	printSection("Verification Results");
	printField("Pages Checked", report.checkedPages);
	printField("Checksum Failures", report.checksumFailures);
	printField("Record Errors", report.recordErrors);
	printField("Index Errors", report.indexErrors);
	printField("Adjacency Errors", report.adjacencyErrors);

	const totalErrors =
		report.checksumFailures +
		report.recordErrors +
		report.indexErrors +
		report.adjacencyErrors;

	console.log();

	if (totalErrors === 0) {
		console.log("  ✓ No issues found - database is healthy!");
		console.log();
		console.log("  Status: PASS");
	} else {
		console.log(`  ✗ Found ${totalErrors} issue(s)`);

		if (report.errors && report.errors.length > 0) {
			printSection("Error Details");
			for (let i = 0; i < report.errors.length; i++) {
				console.log(`  ${i + 1}. ${report.errors[i]}`);
			}
		}

		console.log();
		console.log("  Status: FAIL");
	}

	console.log();
}

function runHeader(dbPath: string): void {
	printHeader("RAW HEADER CONTENTS");

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);
	const header = db.getHeader();

	printSection("Header Fields");
	printField("next_node_id", header.nextNodeId);
	printField("next_edge_id", header.nextEdgeId);
	printField(
		"free_page_head",
		header.freePageHead !== undefined ? header.freePageHead : "None",
	);
	printField(
		"last_record_page",
		header.lastRecordPage !== undefined ? header.lastRecordPage : "None",
	);
	printField("last_committed_tx_id", header.lastCommittedTxId);
	printField(
		"btree_index_page",
		header.btreeIndexPage !== undefined ? header.btreeIndexPage : "None",
	);
	printField("btree_index_size", header.btreeIndexSize);

	console.log();
}

function runSample(dbPath: string, args: string[]): void {
	printHeader("SAMPLE DATA");

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);

	let limit = 10;
	const limitIdx = args.indexOf("--limit");
	if (limitIdx !== -1 && args[limitIdx + 1]) {
		const value = Number.parseInt(args[limitIdx + 1], 10);
		if (Number.isNaN(value) || value < 1) {
			console.error("Error: Invalid limit value");
			process.exit(1);
		}
		limit = value;
	}

	printSection(`Sampling ${limit} Nodes`);

	const header = db.getHeader();
	const maxNodes = Math.min(limit, header.nextNodeId - 1);

	for (let i = 1; i <= maxNodes; i++) {
		try {
			const node = db.getNode(i);
			if (node) {
				console.log(`  Node ${i}:`);
				if (node.properties) {
					console.log(`    Properties: ${JSON.stringify(node.properties)}`);
				}
			}
		} catch {
			// Node might not exist, skip
		}
	}

	console.log();
	console.log(`✓ Sampled ${maxNodes} node(s)`);
	console.log();
}

function runWalInfo(dbPath: string): void {
	printHeader("WAL INFORMATION");

	const walPath = `${dbPath}-wal`;

	try {
		const stats = statSync(walPath);
		const size = stats.size;

		printSection("WAL Status");
		printField("WAL File", walPath);
		printField("WAL Size", formatBytes(size));
		printField("Status", "Active");

		if (size === 0) {
			console.log();
			console.log("  ℹ WAL file exists but is empty (clean state)");
		} else {
			const frameSize = 4096 + 24;
			const estimatedFrames = Math.floor(size / frameSize);
			printField("Estimated Frames", estimatedFrames);

			console.log();
			console.log("  ⚠ WAL contains uncommitted changes");
			console.log("    Run checkpoint to merge changes into main database");
		}
	} catch {
		printSection("WAL Status");
		printField("WAL File", "Not found");
		printField("Status", "No active WAL");

		console.log();
		console.log("  ✓ Database is in clean state (no WAL)");
	}

	console.log();
}

function printUsage(): void {
	console.log(`┌─────────────────────────────────────────────┐
│         Sombra Database Inspector           │
└─────────────────────────────────────────────┘

USAGE:
    sombra inspect <database> <command>

COMMANDS:
    info         Show database metadata
    stats        Show detailed statistics
    sample       Show sample data (default limit: 10)
    verify       Run integrity check
    header       Show raw header contents
    wal-info     Show WAL status

OPTIONS:
    --limit N    Limit number of samples (for sample command)

EXAMPLES:
    sombra inspect graph.db info
    sombra inspect graph.db sample
    sombra inspect graph.db sample --limit 5
    sombra inspect graph.db verify
`);
}

export async function runInspectCommand(argv: string[]): Promise<void> {
	if (argv.length < 2 || argv.includes("--help") || argv.includes("-h")) {
		printUsage();
		process.exit(argv.includes("--help") || argv.includes("-h") ? 0 : 1);
	}

	const dbPath = argv[0];
	const subcommand = argv[1];

	if (!existsSync(dbPath)) {
		console.error(`Error: Database file not found: ${dbPath}`);
		process.exit(1);
	}

	switch (subcommand) {
		case "info":
			return runInfo(dbPath);
		case "stats":
			return runStats(dbPath);
		case "sample":
			return runSample(dbPath, argv.slice(2));
		case "verify":
			return runVerify(dbPath);
		case "header":
			return runHeader(dbPath);
		case "wal-info":
			return runWalInfo(dbPath);
		default:
			console.error(`Error: Unknown inspect command '${subcommand}'`);
			console.error();
			process.exit(1);
	}
}
