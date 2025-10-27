/**
 * Entry point for the Bun-based Sombra CLI.
 * The command implementations are provided in dedicated modules.
 */
import { runCli } from "./runtime/cli-runner";

export async function run(argv = process.argv): Promise<void> {
	await runCli(argv);
}

// Execute immediately when invoked via CLI
if (require.main === module) {
	run().catch((err) => {
		console.error(err instanceof Error ? err.stack ?? err.message : err);
		process.exit(1);
	});
}
