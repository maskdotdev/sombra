export function formatBytes(bytes: number): string {
	const KB = 1024;
	const MB = KB * 1024;
	const GB = MB * 1024;

	if (bytes >= GB) {
		return `${(bytes / GB).toFixed(2)} GB`;
	}
	if (bytes >= MB) {
		return `${(bytes / MB).toFixed(2)} MB`;
	}
	if (bytes >= KB) {
		return `${(bytes / KB).toFixed(2)} KB`;
	}
	return `${bytes} B`;
}

export function printHeader(title: string): void {
	const width = 60;
	const padding = Math.floor((width - title.length - 2) / 2);
	console.log();
	console.log("╔" + "═".repeat(width) + "╗");
	console.log(
		"║" +
			" ".repeat(padding) +
			title +
			" ".repeat(width - padding - title.length) +
			"║",
	);
	console.log("╚" + "═".repeat(width) + "╝");
	console.log();
}

export function printSection(title: string): void {
	console.log();
	console.log("─── " + title + " " + "─".repeat(55 - title.length));
}

export function printField(name: string, value: string | number): void {
	const dots = ".".repeat(Math.max(1, 30 - name.length));
	console.log(`  ${name}${dots} ${value}`);
}
