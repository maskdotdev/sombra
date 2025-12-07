import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router";
import { executeQuery, fetchHealth, fetchStats, type HealthStatus, type StatsReport } from "../lib/api";
import { cn } from "../lib/utils";
import { Badge } from "../components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import { Textarea } from "../components/ui/textarea";
import { Button } from "../components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { useForm } from "react-hook-form";
import { DEMO_FOLLOWS_QUERY } from "../lib/query-presets";
import { extractRows } from "../lib/query-utils";
import { PropertyValueCell } from "../components/query/property-value";

export function meta() {
  return [
    { title: "Sombra Dashboard" },
    {
      name: "description",
      content: "Operational overview for Sombra databases",
    },
  ];
}

export default function Home() {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [stats, setStats] = useState<StatsReport | null>(null);
  const [fetchedAt, setFetchedAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([fetchHealth(), fetchStats()])
      .then(([h, s]) => {
        setHealth(h);
        setStats(s);
        setFetchedAt(new Date().toISOString());
      })
      .catch((err) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false));
  }, []);

  const formatter = new Intl.NumberFormat();
  const bytesFormatter = new Intl.NumberFormat(undefined, {
    style: "unit",
    unit: "byte",
    unitDisplay: "narrow",
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <p className="text-muted-foreground">Loading dashboard...</p>
      </div>
    );
  }

  if (error || !health || !stats) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Alert variant="destructive" className="max-w-md">
          <AlertTitle>Failed to load dashboard</AlertTitle>
          <AlertDescription>{error ?? "Unknown error"}</AlertDescription>
        </Alert>
      </div>
    );
  }

  const isEmptyDataset = stats.storage.estimated_node_count === 0;

  return (
    <div className="space-y-6 px-4 py-10 max-w-6xl mx-auto">
      <header className="flex flex-col gap-2">
        <div className="flex items-center gap-3">
          <Badge
            variant={health.status === "ok" ? "default" : "secondary"}
            className={cn(
              "uppercase tracking-wide",
              health.status === "ok"
                ? "bg-emerald-500 text-white hover:bg-emerald-500"
                : "bg-amber-500 text-black hover:bg-amber-500",
            )}
          >
            {health.status}
          </Badge>
          <span className="text-muted-foreground text-sm">
            Last refreshed {fetchedAt ? new Date(fetchedAt).toLocaleTimeString() : "—"}
          </span>
        </div>
        <h1 className="text-3xl font-semibold">Database Overview</h1>
        <p className="text-muted-foreground">
          Real-time signals surfaced from the Sombra CLI dashboard server.
        </p>
      </header>

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader className="space-y-1">
            <CardDescription>Read-only mode</CardDescription>
            <CardTitle className="text-2xl">
              {health.read_only ? "Enabled" : "Disabled"}
            </CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            Mutating admin commands (vacuum, import, checkpoint) are{" "}
            {health.read_only ? "currently disabled." : "available."}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="space-y-1">
            <CardDescription>Pager cache pages</CardDescription>
            <CardTitle className="text-2xl">
              {formatter.format(stats.pager.cache_pages)}
            </CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            Page size {formatter.format(stats.pager.page_size)} bytes
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="space-y-1">
            <CardDescription>Estimated records</CardDescription>
            <CardTitle className="text-2xl">
              {formatter.format(stats.storage.estimated_node_count)} nodes
            </CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            {formatter.format(stats.storage.estimated_edge_count)} edges
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Pager &amp; WAL</CardTitle>
            <CardDescription>
              Cache behaviour and write-ahead log state.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Metric</TableHead>
                  <TableHead className="text-right">Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                <StatRow label="Cache hits" value={stats.pager.hits} />
                <StatRow label="Cache misses" value={stats.pager.misses} />
                <StatRow label="Evictions" value={stats.pager.evictions} />
                <StatRow
                  label="Dirty writebacks"
                  value={stats.pager.dirty_writebacks}
                />
                <StatRow
                  label="Last checkpoint LSN"
                  value={stats.pager.last_checkpoint_lsn}
                />
                <TableRow>
                  <TableCell>WAL size</TableCell>
                  <TableCell className="text-right font-mono">
                    {bytesFormatter.format(stats.wal.size_bytes)}
                  </TableCell>
                </TableRow>
                <TableRow>
                  <TableCell>WAL file</TableCell>
                  <TableCell className="text-right text-sm text-muted-foreground">
                    {stats.wal.exists ? stats.wal.path : "not present"}
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Storage layout</CardTitle>
            <CardDescription>Internals of the current dataset.</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableBody>
                <StatRow
                  label="Next node ID"
                  value={stats.storage.next_node_id}
                />
                <StatRow
                  label="Next edge ID"
                  value={stats.storage.next_edge_id}
                />
                <StatRow
                  label="Inline property blob"
                  value={stats.storage.inline_prop_blob}
                />
                <StatRow
                  label="Inline property value"
                  value={stats.storage.inline_prop_value}
                />
                <TableRow>
                  <TableCell>Distinct neighbors default</TableCell>
                  <TableCell className="text-right font-mono">
                    {stats.storage.distinct_neighbors_default ? "true" : "false"}
                  </TableCell>
                </TableRow>
                <TableRow>
                  <TableCell>Database file</TableCell>
                  <TableCell className="text-right text-sm text-muted-foreground">
                    {stats.filesystem.db_path}
                  </TableCell>
                </TableRow>
                <TableRow>
                  <TableCell>DB size</TableCell>
                  <TableCell className="text-right font-mono">
                    {bytesFormatter.format(stats.filesystem.db_size_bytes)}
                  </TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-6 lg:grid-cols-2">
        <Card className="col-span-full">
          <CardHeader className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <CardTitle>Query console</CardTitle>
              <CardDescription>
                Paste a JSON query specification and run it against the connected
                database. Queries are limited to read-only operations in the MVP.
              </CardDescription>
            </div>
            <Button asChild variant="outline" size="sm">
              <Link to="/graph">Open Graph Explorer</Link>
            </Button>
          </CardHeader>
          <CardContent className="space-y-4">
            {isEmptyDataset && (
              <Alert>
                <AlertTitle>No data detected</AlertTitle>
                <AlertDescription>
                  <p>
                    The connected database has zero nodes, so the sample query
                    below will fail until you seed demo data or import your own
                    graph.
                  </p>
                  <code className="rounded bg-muted px-2 py-1 font-mono text-xs">
                    sombra seed-demo {stats.filesystem.db_path} --create
                  </code>
                  <p className="text-xs text-muted-foreground">
                    (Use <code>cargo run --bin cli -- seed-demo ...</code> if
                    you do not have the <code>sombra</code> binary installed.)
                  </p>
                </AlertDescription>
              </Alert>
            )}
            <QueryConsole />
          </CardContent>
        </Card>
      </section>
    </div>
  );
}

function StatRow({ label, value }: { label: string; value: number }) {
  const formatter = new Intl.NumberFormat();
  return (
    <TableRow>
      <TableCell>{label}</TableCell>
      <TableCell className="text-right font-mono">
        {formatter.format(value)}
      </TableCell>
    </TableRow>
  );
}

type QueryFormValues = {
  payload: string;
};

function QueryConsole() {
  const form = useForm<QueryFormValues>({
    defaultValues: {
      payload: JSON.stringify(DEMO_FOLLOWS_QUERY, null, 2),
    },
  });
  const [result, setResult] = useState<QueryResultState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setSubmitting] = useState(false);
  const [history, setHistory] = useState<QueryHistoryEntry[]>([]);

  const tabularRows = useMemo(() => result?.rows ?? null, [result]);
  const tableColumns = useMemo(() => {
    if (!tabularRows || tabularRows.length === 0) {
      return [] as string[];
    }
    const cols = new Set<string>();
    for (const row of tabularRows) {
      Object.keys(row).forEach((key) => cols.add(key));
    }
    return Array.from(cols);
  }, [tabularRows]);

  return (
    <form
      className="space-y-4"
      onSubmit={form.handleSubmit(async (values) => {
        setSubmitting(true);
        setError(null);
        try {
          const payload = JSON.parse(values.payload);
          const response = await executeQuery(payload);
          const rows = extractRows(response);
          setResult({ raw: response, rows });
          const entry: QueryHistoryEntry = {
            id: crypto.randomUUID?.() ?? `${Date.now()}`,
            payload: values.payload,
            timestamp: new Date().toISOString(),
            rowCount: Array.isArray(rows) ? rows.length : null,
          };
          setHistory((prev) => [entry, ...prev].slice(0, 5));
        } catch (err) {
          setResult(null);
          setError(err instanceof Error ? err.message : String(err));
        } finally {
          setSubmitting(false);
        }
      })}
    >
      <Textarea
        {...form.register("payload")}
        minLength={2}
        rows={10}
        className="font-mono text-sm"
      />
      <div className="flex items-center gap-3">
        <Button type="submit" disabled={isSubmitting}>
          {isSubmitting ? "Running…" : "Run query"}
        </Button>
        {error && (
          <Alert variant="destructive" className="flex-1">
            <AlertTitle>Query failed</AlertTitle>
            <AlertDescription className="font-mono text-xs">
              {error}
            </AlertDescription>
          </Alert>
        )}
      </div>
      {tabularRows && tabularRows.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Rows</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    {tableColumns.map((column) => (
                      <TableHead key={column}>{column}</TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {tabularRows.map((row, idx) => (
                    <TableRow key={idx}>
                      {tableColumns.map((column) => (
                        <TableCell key={column} className="font-mono text-xs">
                          <PropertyValueCell value={row[column]} propertyKey={column} />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}
      {tabularRows && tabularRows.length > 0 && (
        <GraphExplorerTeaser rowCount={tabularRows.length} />
      )}
      {result !== null && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Result</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="text-xs bg-muted p-4 rounded-md overflow-auto">
              <code>{JSON.stringify(result.raw, null, 2)}</code>
            </pre>
          </CardContent>
        </Card>
      )}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Recent queries</CardTitle>
          <CardDescription>
            Stored locally for this session (last 5 entries).
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {history.length === 0 && (
            <p className="text-sm text-muted-foreground">
              Run a query to build history.
            </p>
          )}
          {history.map((entry) => (
            <div
              key={entry.id}
              className="flex items-center justify-between gap-3 rounded-md border p-3"
            >
              <div>
                <p className="text-sm font-medium">
                  {entry.rowCount === null
                    ? "Unknown row count"
                    : `${entry.rowCount} row${entry.rowCount === 1 ? "" : "s"}`}
                </p>
                <p className="text-xs text-muted-foreground">
                  {new Date(entry.timestamp).toLocaleTimeString()}
                </p>
              </div>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => form.setValue("payload", entry.payload, { shouldDirty: true })}
              >
                Load
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>
    </form>
  );
}

function GraphExplorerTeaser({ rowCount }: { rowCount: number }) {
  return (
    <Card className="border-dashed bg-muted/30">
      <CardHeader className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <CardTitle className="text-base">Visualize this result</CardTitle>
          <CardDescription>
            Open the Graph Explorer to map the {rowCount} row{rowCount === 1 ? "" : "s"} that just ran
            without overloading the console view.
          </CardDescription>
        </div>
        <Button asChild size="sm">
          <Link to="/graph">Launch Graph Explorer</Link>
        </Button>
      </CardHeader>
    </Card>
  );
}

type QueryResultState = {
  raw: unknown;
  rows: Record<string, unknown>[] | null;
};

type QueryHistoryEntry = {
  id: string;
  payload: string;
  timestamp: string;
  rowCount: number | null;
};
