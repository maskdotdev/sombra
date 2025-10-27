// Runtime capability detection helpers.
const globalAny = globalThis as typeof globalThis & { Bun?: unknown };

export const isBunRuntime = typeof globalAny.Bun !== "undefined";

export const preferredPackageManager =
	process.env.SOMBRA_PACKAGE_MANAGER?.trim().toLowerCase() || undefined;
