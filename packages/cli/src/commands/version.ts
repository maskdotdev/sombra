export function runVersionCommand(): void {
	const version =
		process.env.SOMBRA_CLI_VERSION || process.env.npm_package_version || null;
	if (version) {
		console.log(`sombra ${version}`);
	} else {
		console.log("sombra (version unknown)");
	}
	process.exit(0);
}
