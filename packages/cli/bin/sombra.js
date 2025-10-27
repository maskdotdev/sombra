#!/usr/bin/env node
"use strict";

const path = require("path");

function ensureVersionEnv() {
	if (!process.env.SOMBRA_CLI_VERSION) {
		try {
			const pkg = require("../package.json");
			if (pkg && pkg.version) {
				process.env.SOMBRA_CLI_VERSION = pkg.version;
			}
		} catch {
			// Ignore; fallback logic in CLI will handle missing version.
		}
	}
}

function loadCli() {
	const distPath = path.join(__dirname, "..", "dist", "index.js");
	try {
		ensureVersionEnv();
		return require(distPath);
	} catch (error) {
		if (error && error.code === "MODULE_NOT_FOUND") {
			console.error("Sombra CLI build artifacts missing.");
			console.error("Run `bun run build` before using the CLI.");
		} else {
			console.error("Failed to load compiled CLI:");
			console.error(error && error.stack ? error.stack : error);
		}
		process.exit(1);
	}
}

const cli = loadCli();

if (cli && typeof cli.run === "function") {
	Promise.resolve(cli.run(process.argv)).catch((err) => {
		console.error(err instanceof Error ? err.stack || err.message : err);
		process.exit(1);
	});
} else {
	console.error("Invalid CLI build: expected exported run() function.");
	process.exit(1);
}
