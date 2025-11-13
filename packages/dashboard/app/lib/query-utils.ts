export type QueryRow = Record<string, unknown>;

export function extractRows(value: unknown): QueryRow[] | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const rows = (value as Record<string, unknown>).rows;
  if (!Array.isArray(rows)) {
    return null;
  }
  return rows.filter((row): row is QueryRow => {
    return row !== null && typeof row === "object" && !Array.isArray(row);
  });
}

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return JSON.stringify(value);
}
