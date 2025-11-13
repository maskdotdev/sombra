const DEFAULT_API_BASE = "";

function makeUrl(path: string, base: string) {
  if (!base) {
    return path;
  }
  try {
    return new URL(path, base).toString();
  } catch {
    return path;
  }
}

const API_BASE =
  import.meta.env.VITE_SOMBRA_API?.trim().replace(/\/$/, "") ??
  DEFAULT_API_BASE;

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(makeUrl(path, API_BASE), {
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Request failed (${response.status} ${response.statusText}): ${text}`,
    );
  }

  return (await response.json()) as T;
}

export type HealthStatus = {
  status: string;
  read_only: boolean;
};

export type StatsReport = {
  pager: {
    page_size: number;
    cache_pages: number;
    hits: number;
    misses: number;
    evictions: number;
    dirty_writebacks: number;
    last_checkpoint_lsn: number;
  };
  wal: {
    path: string;
    exists: boolean;
    size_bytes: number;
    last_checkpoint_lsn: number;
  };
  storage: {
    next_node_id: number;
    next_edge_id: number;
    estimated_node_count: number;
    estimated_edge_count: number;
    inline_prop_blob: number;
    inline_prop_value: number;
    storage_flags: number;
    distinct_neighbors_default: boolean;
  };
  filesystem: {
    db_path: string;
    db_size_bytes: number;
    wal_path: string;
    wal_size_bytes: number;
  };
};

export type LabelSummary = {
  name: string;
  count: number;
};

export async function fetchHealth(): Promise<HealthStatus> {
  return request("/health");
}

export async function fetchStats(): Promise<StatsReport> {
  return request("/api/stats");
}

export async function fetchLabelSamples(): Promise<LabelSummary[]> {
  const payload = await request<{ labels: LabelSummary[] }>("/api/labels");
  return payload.labels;
}

export async function ensureLabelIndexes(labels: string[]): Promise<void> {
  const filtered = Array.from(
    new Set(labels.map((label) => label.trim()).filter((label) => label.length > 0)),
  );
  if (filtered.length === 0) {
    return;
  }
  await request("/api/labels/indexes", {
    method: "POST",
    body: JSON.stringify({ labels: filtered }),
  });
}

type ExecuteQueryOptions = {
  maxRows?: number;
};

export async function executeQuery(
  body: unknown,
  options?: ExecuteQueryOptions,
): Promise<unknown> {
  const params = new URLSearchParams();
  if (
    typeof options?.maxRows === "number" &&
    Number.isFinite(options.maxRows) &&
    options.maxRows > 0
  ) {
    params.set("max_rows", String(Math.round(options.maxRows)));
  }
  const path = params.size > 0 ? `/api/query?${params.toString()}` : "/api/query";
  return request(path, {
    method: "POST",
    body: JSON.stringify(body),
  });
}
