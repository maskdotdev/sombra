import { printUsage } from "../utils/usage";
import { runWebCommand } from "../commands/web";
import { runSeedCommand } from "../commands/seed";
import { runInspectCommand } from "../commands/inspect";
import { runRepairCommand } from "../commands/repair";
import { runVerifyCommand } from "../commands/verify";
import { runVersionCommand } from "../commands/version";

type CommandHandler = (argv: string[]) => Promise<void> | void;

const commands: Record<string, CommandHandler> = {
	web: runWebCommand,
	seed: runSeedCommand,
	inspect: runInspectCommand,
	repair: runRepairCommand,
	verify: runVerifyCommand,
	version: runVersionCommand,
};

export async function runCli(argv: string[]): Promise<void> {
	const [, , maybeCommand, ...commandArgs] = argv;
	if (
		!maybeCommand ||
		maybeCommand === "help" ||
		maybeCommand === "--help" ||
		maybeCommand === "-h"
	) {
		printUsage();
		process.exit(0);
	}

	const handler = commands[maybeCommand];
	if (!handler) {
		console.error(`Unknown command: ${maybeCommand}`);
		printUsage();
		process.exit(1);
	}

	await handler(commandArgs);
}
