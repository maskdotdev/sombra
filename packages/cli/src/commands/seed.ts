import { existsSync } from "fs";
import { join } from "path";
import { spawnSync } from "child_process";
import {
	ensureSombraWebInstalled,
	resolveLocalSombraWeb,
} from "../runtime/web-install";
import { resolveNodeRuntime } from "../runtime/node";
import { loadSombraDB } from "../runtime/sombradb";

export async function runSeedCommand(argv: string[]): Promise<void> {
	if (argv.includes("--help") || argv.includes("-h")) {
		console.log(`Usage: sombra seed [database-path]

Create a demo database with sample data for testing and exploration.

Arguments:
  database-path    Path for the new database (default: ./demo.db)

Example:
  sombra seed demo.db
  sombra web demo.db
`);
		process.exit(0);
	}

	let webDir = resolveLocalSombraWeb();
	if (!webDir) {
		console.log("Installing sombra-web (needed for seeding)...");
		webDir = ensureSombraWebInstalled();
	}

	const seedScript = join(webDir, "scripts", "seed-demo.js");
	if (!existsSync(seedScript)) {
		console.error("Error: seed-demo.js not found in sombra-web package.");
		console.error("Try updating: sombra web --update");
		process.exit(1);
	}

	const dbPath = argv[0] || "./demo.db";
	console.log(`Creating demo database: ${dbPath}`);

	const runtime = resolveNodeRuntime();
	const result = spawnSync(runtime, [seedScript, dbPath], {
		stdio: "inherit",
	});

	if (result.error) {
		const code = (result.error as NodeJS.ErrnoException).code;
		if (code === "ENOENT") {
			console.error(
				`Unable to locate runtime '${runtime}' required to execute the seed script.`,
			);
			console.error(
				"Install Node.js or set SOMBRA_NODE_RUNTIME to the path of a compatible Node binary.",
			);
		} else {
			console.error("Error running seed script:", result.error);
		}
		process.exit(1);
	}

	if (result.status === 0 && existsSync(dbPath)) {
		try {
			const { SombraDB } = loadSombraDB();
			const db = new SombraDB(dbPath);
			db.checkpoint();
			console.log(`✓ Database created successfully: ${dbPath}`);
		} catch {
			console.log(`✓ Database created successfully: ${dbPath}`);
		}
	}

	process.exit(result.status || 0);
}
