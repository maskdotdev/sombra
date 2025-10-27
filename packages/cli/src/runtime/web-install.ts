import { existsSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { homedir } from "os";
import { installPackageToCache } from "./package-managers";

function getCacheDir(): string {
	const platform = process.platform;
	const home = homedir();

	if (platform === "darwin") return join(home, "Library", "Caches", "sombra", "web");
	if (platform === "win32") {
		const localAppData = process.env.LOCALAPPDATA || join(home, "AppData", "Local");
		return join(localAppData, "sombra", "web");
	}
	const xdgCache = process.env.XDG_CACHE_HOME || join(home, ".cache");
	return join(xdgCache, "sombra", "web");
}

export function resolveLocalSombraWeb(): string | null {
	try {
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const pkgPath = require.resolve("sombra-web/package.json");
		return dirname(pkgPath);
	} catch {
		return null;
	}
}

export function ensureSombraWebInstalled(version?: string): string {
	const local = resolveLocalSombraWeb();
	if (local) return local;

	const cacheDir = getCacheDir();

	const isFilePath =
		!!version &&
		(version.startsWith("file:") ||
			version.startsWith("/") ||
			version.startsWith("."));

	const targetName = isFilePath
		? version.replace(/[^a-zA-Z0-9.-]/g, "_")
		: version || "latest";
	const target = join(cacheDir, targetName);

	const marker = join(target, "node_modules", "sombra-web", "package.json");
	if (existsSync(marker)) return dirname(marker);

	mkdirSync(target, { recursive: true });

	const installSpec = isFilePath
		? version!
		: `sombra-web@${version || "latest"}`;

	const managerUsed = installPackageToCache(target, installSpec);
	if (
		managerUsed &&
		process.env.SOMBRA_DEBUG_RESOLUTION &&
		process.env.SOMBRA_DEBUG_RESOLUTION !== "0"
	) {
		console.error(`[DEBUG] sombra-web installed with ${managerUsed}`);
	}

	const installedDir = join(target, "node_modules", "sombra-web");
	if (existsSync(installedDir)) return installedDir;
	return target;
}
