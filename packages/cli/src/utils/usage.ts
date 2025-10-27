export function printUsage(): void {
	console.log(`Sombra CLI

Usage:
  sombra <command> [options]

Commands:
  web           Start the Sombra web UI
  seed          Create a demo database with sample data
  inspect       Inspect database information and statistics
  repair        Perform maintenance and repair operations
  verify        Run comprehensive integrity verification
  version       Show version information
  help          Show this help

Run 'sombra <command> --help' for more information on a command.`);
}
