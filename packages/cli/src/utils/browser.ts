import { spawn } from "child_process";

export function openBrowser(url: string): void {
	const platform = process.platform;
	let cmd: string;
	let args: string[];

	if (platform === "darwin") {
		cmd = "open";
		args = [url];
	} else if (platform === "win32") {
		cmd = "cmd";
		args = ["/c", "start", '""', url];
	} else {
		cmd = "xdg-open";
		args = [url];
	}

	const child = spawn(cmd, args, { stdio: "ignore", detached: true });
	child.unref();
}
