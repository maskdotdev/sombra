import { spawn } from "child_process";
import { existsSync } from "fs";
import { join } from "path";
import { resolveNodeRuntime } from "../runtime/node";
import {
	ensureSombraWebInstalled,
	resolveLocalSombraWeb,
} from "../runtime/web-install";
import { openBrowser } from "../utils/browser";

function printHelp(): void {
	console.log(
		"Usage: sombra web [--db <path>] [--port <port>] [--open] [--no-open] [--update] [--check-install] [--install]\n",
	);
}

export async function runWebCommand(argv: string[]): Promise<void> {
	if (argv.includes("--help") || argv.includes("-h")) {
		printHelp();
		process.exit(0);
	}

	if (argv.includes("--check-install")) {
		const local = resolveLocalSombraWeb();
		if (local) {
			console.log("✓ @unyth/sombra-web is installed");
			console.log(`  Location: ${local}`);
			process.exit(0);
		}
		console.log("✗ @unyth/sombra-web is not installed");
		console.log("  Run 'sombra web --install' to install it");
		process.exit(1);
	}

	const getArg = (name: string): string | undefined => {
		const index = argv.indexOf(name);
		return index !== -1 ? argv[index + 1] : undefined;
	};

	const port = getArg("--port") || process.env.PORT || "3000";
	const db = getArg("--db") || process.env.SOMBRA_DB_PATH;
	const shouldOpen = argv.includes("--open") || !argv.includes("--no-open");
	const versionPin = getArg("--version-pin");
	const update = argv.includes("--update");
	const preinstall = argv.includes("--install");

	let webDir = resolveLocalSombraWeb();
	if (!webDir || update) {
		webDir = ensureSombraWebInstalled(versionPin);
	}

	if (preinstall) {
		console.log("@unyth/sombra-web installed to cache.");
		process.exit(0);
	}

	const entryCandidates = [
		join(webDir, "dist-npm", "start.js"),
		join(webDir, "dist", "start.js"),
	];

	const entry = entryCandidates.find((candidate) => existsSync(candidate));
	if (!entry) {
		console.error("Could not locate @unyth/sombra-web runtime.");
		process.exit(1);
	}

	const env = { ...process.env, PORT: String(port) };
	if (db) env.SOMBRA_DB_PATH = db;
	const runtime = resolveNodeRuntime();
	const args = [entry, "--port", String(port)];
	if (db) {
		args.push("--db", db);
	}

	const child = spawn(runtime, args, { stdio: "inherit", env });
	child.on("error", (err) => {
		console.error(
			"Failed to launch Sombra web runtime:",
			(err as Error).message,
		);
		const code = (err as NodeJS.ErrnoException).code;
		if (code === "ENOENT") {
			console.error(
				`Runtime '${runtime}' not found. Set SOMBRA_NODE_RUNTIME to your Node executable if Node is not on PATH.`,
			);
		}
		process.exit(1);
	});

	child.on("spawn", () => {
		if (shouldOpen) {
			const url = `http://localhost:${port}`;
			openBrowser(url);
			console.log(`Sombra web running at ${url}`);
		}
	});

	child.on("exit", (code) => {
		process.exit(code ?? 0);
	});
}
