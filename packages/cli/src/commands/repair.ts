import { existsSync, statSync } from "fs";
import { loadSombraDB } from "../runtime/sombradb";
import { formatBytes, printHeader, printSection } from "../utils/display";
import { askConfirmation } from "../utils/prompt";

function runCheckpoint(dbPath: string): void {
	printHeader("CHECKPOINT WAL");

	const walPath = `${dbPath}-wal`;

	let walSizeBefore: number | null = null;
	try {
		walSizeBefore = statSync(walPath).size;
		console.log("  WAL size before: " + formatBytes(walSizeBefore));
	} catch {
		console.log("  No WAL file found");
	}

	console.log();
	console.log("  Performing checkpoint...");

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);
	db.checkpoint();

	let walSizeAfter: number | null = null;
	try {
		walSizeAfter = statSync(walPath).size;
	} catch {
		// WAL removed
	}

	printSection("Results");

	if (walSizeAfter !== null) {
		console.log("  WAL size after: " + formatBytes(walSizeAfter));

		if (walSizeBefore !== null) {
			const saved = Math.max(0, walSizeBefore - walSizeAfter);
			if (saved > 0) {
				console.log("  Space reclaimed: " + formatBytes(saved));
			}
		}
	} else {
		console.log("  WAL file removed (clean state)");
	}

	console.log();
	console.log("  ✓ Checkpoint completed successfully");
	console.log();
}

function runVacuum(dbPath: string): void {
	printHeader("VACUUM DATABASE");

	const sizeBefore = statSync(dbPath).size;

	console.log("  Database size before: " + formatBytes(sizeBefore));
	console.log();
	console.log("  Compacting database...");
	console.log("  (This may take a while for large databases)");
	console.log();

	const { SombraDB } = loadSombraDB();
	const db = new SombraDB(dbPath);
	db.checkpoint();

	printSection("Results");

	const sizeAfter = statSync(dbPath).size;
	console.log("  Database size after: " + formatBytes(sizeAfter));

	if (sizeBefore > sizeAfter) {
		const saved = sizeBefore - sizeAfter;
		const percent = (saved / sizeBefore) * 100.0;
		console.log(
			`  Space reclaimed: ${formatBytes(saved)} (${percent.toFixed(1)}%)`,
		);
	} else {
		console.log("  No space reclaimed (database already compact)");
	}

	console.log();
	console.log("  ✓ Vacuum completed successfully");
	console.log();
}

function printUsage(): void {
	console.log(`┌─────────────────────────────────────────────┐
│          Sombra Database Repair             │
└─────────────────────────────────────────────┘

USAGE:
    sombra repair <database> [command] [--yes]

COMMANDS:
    checkpoint       Force WAL checkpoint (default)
    vacuum           Compact database

OPTIONS:
    --yes            Skip confirmation prompt
    --check-only     Check what repairs are needed without applying

EXAMPLES:
    sombra repair graph.db
    sombra repair graph.db checkpoint
    sombra repair graph.db vacuum

WARNING:
    Always backup your database before repair!
`);
}

export async function runRepairCommand(argv: string[]): Promise<void> {
	if (argv.length < 1 || argv.includes("--help") || argv.includes("-h")) {
		printUsage();
		process.exit(argv.includes("--help") || argv.includes("-h") ? 0 : 1);
	}

	const dbPath = argv[0];
	let subcommand = argv[1];
	const skipConfirm = argv.includes("--yes");
	const checkOnly = argv.includes("--check-only");

	if (!subcommand || subcommand.startsWith("--")) {
		subcommand = "checkpoint";
	}

	if (!existsSync(dbPath)) {
		console.log();
		console.log("╔══════════════════════════════════════════════════════════╗");
		console.log("║                         ERROR                            ║");
		console.log("╚══════════════════════════════════════════════════════════╝");
		console.log();
		console.log(`  Database file not found: ${dbPath}`);
		console.log();
		process.exit(1);
	}

	const execute = () => {
		if (checkOnly) {
			console.log();
			console.log(`Would perform: ${subcommand}`);
			console.log();
			return;
		}

		switch (subcommand) {
			case "checkpoint":
				return runCheckpoint(dbPath);
			case "vacuum":
				return runVacuum(dbPath);
			default:
				console.log();
				console.error(`Error: Unknown repair command '${subcommand}'`);
				console.log();
				process.exit(1);
		}
	};

	if (skipConfirm || checkOnly) {
		execute();
		return;
	}

	console.log();
	console.log("  ⚠  WARNING: Always backup your database before repair!");
	console.log();

	await new Promise<void>((resolve) => {
		askConfirmation((confirmed) => {
			if (confirmed) {
				execute();
			} else {
				console.log();
				console.log("  Aborted.");
				console.log();
				process.exit(0);
			}
			resolve();
		});
	});
}
