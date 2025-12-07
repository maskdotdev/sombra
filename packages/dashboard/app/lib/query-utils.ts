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

/**
 * Returns a smart preview string for complex values
 */
export function formatCellPreview(value: unknown): { preview: string; isComplex: boolean } {
  if (value === null || value === undefined) {
    return { preview: "—", isComplex: false };
  }
  if (typeof value === "boolean" || typeof value === "number") {
    return { preview: String(value), isComplex: false };
  }
  if (typeof value === "string") {
    // Check if it's JSON
    const trimmed = value.trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        JSON.parse(value);
        return { preview: truncate(trimmed, 40), isComplex: true };
      } catch {
        // Not valid JSON
      }
    }
    // Check if it's code or long text
    if (value.length > 60 || value.includes("\n")) {
      return { preview: truncate(value, 40), isComplex: true };
    }
    return { preview: value, isComplex: false };
  }
  // Objects/arrays
  const json = JSON.stringify(value);
  return { preview: truncate(json, 40), isComplex: true };
}

function truncate(str: string, maxLen: number): string {
  const singleLine = str.replace(/\n/g, " ").replace(/\s+/g, " ");
  if (singleLine.length <= maxLen) return singleLine;
  return singleLine.slice(0, maxLen - 1) + "...";
}

