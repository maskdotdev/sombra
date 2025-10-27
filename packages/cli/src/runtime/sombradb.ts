import { spawnSync } from "child_process";
import { dirname, join } from "path";
import { homedir } from "os";

type ResolutionAttempt = {
	method: string;
	path?: string;
	error: string;
};

export function loadSombraDB(): any {
	const debug = process.env.SOMBRA_DEBUG_RESOLUTION === "1";
	const attempts: ResolutionAttempt[] = [];

	const tryLoad = (resolved: string, context: string) => {
		try {
			// eslint-disable-next-line @typescript-eslint/no-var-requires
			return require(resolved);
		} catch (loadError: any) {
			if (
				loadError.code === "MODULE_NOT_FOUND" &&
				loadError.message.includes(resolved)
			) {
				return null;
			}

			console.error("");
			console.error("Error: Found @unyth/sombra but failed to load it.");
			console.error("");
			console.error(`  Location: ${resolved}`);
			console.error(`  Context: ${context}`);
			console.error("");
			console.error("  Error details:");
			console.error(`    ${loadError.message}`);
			console.error("");

			if (
				loadError.message.includes("Cannot find native binding") ||
				loadError.message.includes("NODE_MODULE_VERSION")
			) {
				console.error("This appears to be a native binding compatibility issue.");
				console.error("");
				console.error("Solutions:");
				console.error("  1. Reinstall @unyth/sombra in this location:");
				console.error(`     cd ${dirname(dirname(resolved))}`);
				console.error(`     npm install @unyth/sombra --force`);
				console.error("");
				console.error("  2. Or install @unyth/sombra locally in your project:");
				console.error("     npm install @unyth/sombra");
				console.error("");
				console.error(
					"  3. Or reinstall the CLI globally with your current package manager:",
				);
				console.error("     npm install -g @unyth/sombra-cli --force");
				console.error("     # or: pnpm add -g @unyth/sombra-cli");
				console.error("     # or: bun add -g @unyth/sombra-cli");
			}
			console.error("");
			process.exit(1);
		}
	};

	try {
		const packageRoot = dirname(__dirname);
		if (debug) console.error(`[DEBUG] Attempt 1: CLI package root: ${packageRoot}`);
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const resolved = require.resolve("@unyth/sombra", { paths: [packageRoot] });
		if (debug) console.error(`[DEBUG] ✓ Found at: ${resolved}`);
		const loaded = tryLoad(resolved, "CLI package root");
		if (loaded) return loaded;
		if (debug) console.error("[DEBUG] ✗ Path doesn't exist");
	} catch (e: any) {
		if (e.code === "MODULE_NOT_FOUND" && e.message.includes("@unyth/sombra")) {
			if (debug) console.error("[DEBUG] ✗ Not found");
			attempts.push({ method: "CLI package root", error: "Package not found" });
		} else {
			if (debug) console.error(`[DEBUG] ✗ Failed to load: ${e.message}`);
			throw e;
		}
	}

	try {
		if (debug) console.error("[DEBUG] Attempt 2: Regular require.resolve");
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const resolved = require.resolve("@unyth/sombra");
		if (debug) console.error(`[DEBUG] ✓ Found at: ${resolved}`);
		const loaded = tryLoad(resolved, "Regular resolution");
		if (loaded) return loaded;
		if (debug) console.error("[DEBUG] ✗ Path doesn't exist");
	} catch (e: any) {
		if (e.code === "MODULE_NOT_FOUND" && e.message.includes("@unyth/sombra")) {
			if (debug) console.error("[DEBUG] ✗ Not found");
			attempts.push({ method: "Regular resolution", error: "Package not found" });
		} else {
			if (debug) console.error(`[DEBUG] ✗ Failed to load: ${e.message}`);
			throw e;
		}
	}

	try {
		if (debug) console.error(`[DEBUG] Attempt 3: CWD: ${process.cwd()}`);
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const resolved = require.resolve("@unyth/sombra", { paths: [process.cwd()] });
		if (debug) console.error(`[DEBUG] ✓ Found at: ${resolved}`);
		const loaded = tryLoad(resolved, "Current working directory");
		if (loaded) return loaded;
		if (debug) console.error("[DEBUG] ✗ Path doesn't exist");
	} catch (e: any) {
		if (e.code === "MODULE_NOT_FOUND" && e.message.includes("@unyth/sombra")) {
			if (debug) console.error("[DEBUG] ✗ Not found");
			attempts.push({
				method: "Current working directory",
				error: "Package not found",
			});
		} else {
			if (debug) console.error(`[DEBUG] ✗ Failed to load: ${e.message}`);
			throw e;
		}
	}

	const candidateRoots: { manager: string; root: string }[] = [];

	try {
		const r = spawnSync("npm", ["root", "-g"], {
			encoding: "utf8",
			timeout: 5000,
		});
		if (r && r.status === 0) {
			const root = (r.stdout || "").trim();
			if (root) candidateRoots.push({ manager: "npm", root });
		}
	} catch {
		// ignore
	}

	try {
		const r = spawnSync("pnpm", ["root", "-g"], {
			encoding: "utf8",
			timeout: 5000,
		});
		if (r && r.status === 0) {
			const root = (r.stdout || "").trim();
			if (root) candidateRoots.push({ manager: "pnpm", root });
		}
	} catch {
		// ignore
	}

	try {
		const r = spawnSync("yarn", ["global", "dir"], {
			encoding: "utf8",
			timeout: 5000,
		});
		if (r && r.status === 0) {
			const dir = (r.stdout || "").trim();
			if (dir) candidateRoots.push({ manager: "yarn", root: join(dir, "node_modules") });
		}
	} catch {
		// ignore
	}

	try {
		const bunInstall = process.env.BUN_INSTALL || join(homedir(), ".bun");
		const bunGlobalNodeModules = join(
			bunInstall,
			"install",
			"global",
			"node_modules",
		);
		candidateRoots.push({ manager: "bun", root: bunGlobalNodeModules });
	} catch {
		// ignore
	}

	const uniqueRoots: { manager: string; root: string }[] = [];
	const seen = new Set<string>();
	for (const { manager, root } of candidateRoots) {
		if (root && !seen.has(root)) {
			seen.add(root);
			uniqueRoots.push({ manager, root });
		}
	}

	if (debug)
		console.error(
			`[DEBUG] Attempt 4: Global package managers (${uniqueRoots.length} roots)`,
		);

	for (const { manager, root } of uniqueRoots) {
		try {
			if (debug) console.error(`[DEBUG]   Trying ${manager}: ${root}`);
			// eslint-disable-next-line @typescript-eslint/no-var-requires
			const resolved = require.resolve("@unyth/sombra", { paths: [root] });
			if (debug) console.error(`[DEBUG]   ✓ Found at: ${resolved}`);
			const loaded = tryLoad(resolved, `Global ${manager} (${root})`);
			if (loaded) return loaded;
			if (debug) console.error("[DEBUG]   ✗ Path doesn't exist");
		} catch (e: any) {
			if (e.code === "MODULE_NOT_FOUND" && e.message.includes("@unyth/sombra")) {
				if (debug) console.error("[DEBUG]   ✗ Not found");
				attempts.push({
					method: `Global ${manager}`,
					path: root,
					error: "Package not found",
				});
			} else {
				if (debug) console.error(`[DEBUG]   ✗ Failed to load: ${e.message}`);
				throw e;
			}
		}
	}

	console.error("Error: @unyth/sombra package not found or failed to load.");
	console.error("");
	console.error(
		"To use inspect, repair, and verify commands, install @unyth/sombra (project-local):",
	);
	console.error("");
	console.error(
		"  npm install @unyth/sombra     # or: pnpm add @unyth/sombra / bun add @unyth/sombra",
	);
	console.error("");
	console.error("Or install the CLI globally (includes @unyth/sombra):");
	console.error("");
	console.error(
		"  npm install -g @unyth/sombra-cli    # or: pnpm add -g @unyth/sombra-cli / bun add -g @unyth/sombra-cli",
	);
	console.error("");
	console.error(
		"Hint: Run with SOMBRA_DEBUG_RESOLUTION=1 to see detailed resolution attempts",
	);
	console.error("");
	process.exit(1);
}
