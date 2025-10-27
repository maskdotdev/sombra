import readline from "readline";

export function askConfirmation(callback: (confirmed: boolean) => void): void {
	const rl = readline.createInterface({
		input: process.stdin,
		output: process.stdout,
	});

	rl.question("  Continue? [y/N] ", (answer) => {
		rl.close();
		callback(answer.trim().toLowerCase() === "y");
	});
}
