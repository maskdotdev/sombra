export function loadSombraDB(): any {
	const debug = process.env.SOMBRA_DEBUG_RESOLUTION === "1";

	try {
		if (debug)
			console.error("[DEBUG] Attempt: load sombradb bundled with sombra-cli");
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const resolved = require.resolve("sombradb");
		if (debug) console.error(`[DEBUG] ✓ Found at: ${resolved}`);
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		return require(resolved);
	} catch (loadError: any) {
		if (debug) console.error(`[DEBUG] ✗ Failed: ${loadError.message}`);

		const messageParts: string[] = [];

		if (
			loadError.code === "MODULE_NOT_FOUND" &&
			loadError.message.includes("sombradb")
		) {
			messageParts.push(
				"Error: sombradb (bundled with the CLI) is missing from this installation.",
			);
		} else if (
			loadError.message.includes("Cannot find native binding") ||
			loadError.message.includes("NODE_MODULE_VERSION")
		) {
			messageParts.push(
				"Error: The bundled sombradb native binding is incompatible with this environment.",
			);
		} else {
			messageParts.push("Error: Failed to load the bundled sombradb dependency.");
		}

		messageParts.push("");
		messageParts.push(
			"This usually means the sombra-cli installation is incomplete or corrupted.",
		);
		messageParts.push("");
		messageParts.push("Try reinstalling sombra-cli:");
		messageParts.push("  npm install -g sombra-cli --force");
		messageParts.push("  # or: pnpm add -g sombra-cli");
		messageParts.push("  # or: bun add -g sombra-cli");
		messageParts.push("");
		messageParts.push("If you're developing from source, reinstall dependencies:");
		messageParts.push("  npm install");
		messageParts.push("");

		if (
			loadError.message.includes("Cannot find native binding") ||
			loadError.message.includes("NODE_MODULE_VERSION")
		) {
			messageParts.push(
				"To rebuild the native binding manually, reinstall sombradb in this workspace:",
			);
			messageParts.push("  npm install sombradb --force");
			messageParts.push("");
		}

		messageParts.push("Original error:");
		messageParts.push(`  ${loadError.message}`);
		messageParts.push("");

		for (const part of messageParts) {
			console.error(part);
		}

		process.exit(1);
	}
}
