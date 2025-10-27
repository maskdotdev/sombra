import { spawnSync } from "child_process";
import { join } from "path";
import { existsSync, mkdirSync, writeFileSync } from "fs";
import { isBunRuntime, preferredPackageManager } from "./constants";

function ensureCacheManifest(target: string): void {
	const manifestPath = join(target, "package.json");
	if (!existsSync(manifestPath)) {
		const manifest = {
			name: "sombra-web-cache",
			private: true,
			license: "UNLICENSED",
			description: "Internal cache directory used by sombra CLI",
		};
		writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
	}
}

type PackageManagerName = "bun" | "npm" | "pnpm" | "yarn";

interface PackageManagerDescriptor {
	command: string;
	args: string[];
}

const managerConfig: Record<PackageManagerName, PackageManagerDescriptor> = {
	bun: { command: "bun", args: ["add", ""] },
	npm: { command: "npm", args: ["install", "", "--force"] },
	pnpm: { command: "pnpm", args: ["add", "", "--force"] },
	yarn: { command: "yarn", args: ["add", "", "--force"] },
};

function createExecutionOrder(): PackageManagerName[] {
	const order: PackageManagerName[] = [];
	const seen = new Set<PackageManagerName>();

	const enqueue = (name: PackageManagerName | undefined) => {
		if (!name || seen.has(name)) return;
		seen.add(name);
		order.push(name);
	};

	if (
		preferredPackageManager === "bun" ||
		preferredPackageManager === "npm" ||
		preferredPackageManager === "pnpm" ||
		preferredPackageManager === "yarn"
	) {
		enqueue(preferredPackageManager);
	}
	if (isBunRuntime) enqueue("bun");
	(["npm", "pnpm", "yarn"] as PackageManagerName[]).forEach(enqueue);
	return order;
}

interface InstallAttempt {
	name: PackageManagerName;
	type: "missing" | "failed";
	exitCode?: number | null;
}

export function installPackageToCache(
	target: string,
	installSpec: string,
): string {
	ensureCacheManifest(target);
	mkdirSync(target, { recursive: true });

	const order = createExecutionOrder();
	if (order.length === 0) {
		console.error(
			"Failed to locate a package manager. Install npm, pnpm, yarn, or bun.",
		);
		process.exit(1);
	}

	const attempts: InstallAttempt[] = [];

	for (const managerName of order) {
		const descriptor = managerConfig[managerName];
		const args = descriptor.args.map((arg) =>
			arg === "" ? installSpec : arg,
		);

		console.log(
			`Installing ${installSpec} with ${descriptor.command} ${args.join(" ")}`,
		);

		const result = spawnSync(descriptor.command, args, {
			cwd: target,
			stdio: "inherit",
		});

		if (result.error && result.error.code === "ENOENT") {
			attempts.push({ name: managerName, type: "missing" });
			continue;
		}

		if (result.status === 0) {
			return managerName;
		}

		attempts.push({
			name: managerName,
			type: "failed",
			exitCode: result.status ?? null,
		});
	}

	console.error("Failed to install sombra-web after trying available managers.");
	for (const attempt of attempts) {
		if (attempt.type === "missing") {
			console.error(`  - ${attempt.name}: not found on PATH`);
		} else {
			console.error(
				`  - ${attempt.name}: exited with code ${attempt.exitCode ?? "unknown"}`,
			);
		}
	}
	console.error(
		"Set SOMBRA_PACKAGE_MANAGER to npm, pnpm, yarn, or bun to force a specific manager.",
	);
	process.exit(1);
}
