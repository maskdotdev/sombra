import { spawnSync } from "child_process";
import { basename } from "path";

export function resolveNodeRuntime(): string {
	const override =
		process.env.SOMBRA_NODE_RUNTIME || process.env.SOMBRA_NODE_PATH;
	if (override) return override;

	const execPath = process.execPath || "";
	const execName = basename(execPath).toLowerCase();
	if (execName === "node" || execName === "node.exe") return execPath;

	const nodeCheck = spawnSync("node", ["--version"], { stdio: "ignore" });
	if (!nodeCheck.error && nodeCheck.status === 0) return "node";

	return execPath;
}
