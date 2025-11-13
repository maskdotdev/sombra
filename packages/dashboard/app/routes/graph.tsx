import { type FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useLoaderData } from "react-router";
import { ChevronDownIcon } from "lucide-react";
import { GraphView } from "../components/query/graph-view";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../components/ui/select";
import { Slider } from "../components/ui/slider";
import { Textarea } from "../components/ui/textarea";
import { ensureLabelIndexes, executeQuery, fetchLabelSamples } from "../lib/api";
import { createAutoGraphSpec, DEMO_FOLLOWS_QUERY } from "../lib/query-presets";
import { extractRows, type QueryRow } from "../lib/query-utils";

const INITIAL_ROW_LIMIT = 250;
const MIN_ROW_LIMIT = 25;
const MAX_ROW_LIMIT = 1_000;

type QueryRunMetadata = {
  limit: number;
  truncated: boolean;
  rowCount: number;
  executedAt: string;
  mode: "auto" | "manual";
};

export async function loader() {
  const labels = await fetchLabelSamples();
  return { labels };
}

export function meta() {
  return [
    { title: "Graph Explorer · Sombra" },
    {
      name: "description",
      content: "Visualize database rows as a graph without overwhelming the browser.",
    },
  ];
}

export default function GraphExplorer() {
  const { labels: knownLabels } = useLoaderData<typeof loader>();
  const defaultLabel = knownLabels[0]?.name ?? "";
  const hasLabels = knownLabels.length > 0;
  const labelSuggestions = useMemo(() => knownLabels.slice(0, 6), [knownLabels]);
  const [hasBootstrapped, setBootstrapped] = useState(false);
  const [manualPayload, setManualPayload] = useState(() => JSON.stringify(DEMO_FOLLOWS_QUERY, null, 2));
  const [rowLimit, setRowLimit] = useState(INITIAL_ROW_LIMIT);
  const [rows, setRows] = useState<QueryRow[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRun, setLastRun] = useState<QueryRunMetadata | null>(null);
  const [activeMode, setActiveMode] = useState<"auto" | "manual">("auto");
  const [autoFilters, setAutoFilters] = useState({
    sourceLabel: defaultLabel,
    targetLabel: defaultLabel,
    edgeType: "",
    direction: "out" as "out" | "in" | "both",
  });

  useEffect(() => {
    if (!defaultLabel) {
      return;
    }
    setAutoFilters((prev) => {
      if (prev.sourceLabel || prev.targetLabel) {
        return prev;
      }
      return { ...prev, sourceLabel: defaultLabel, targetLabel: defaultLabel };
    });
  }, [defaultLabel]);

  const fetchGraph = useCallback(
    async (spec: unknown, mode: "auto" | "manual") => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await executeQuery(spec, { maxRows: rowLimit });
        const parsedRows = extractRows(response) ?? [];
        const metadata = parseLimitMetadata(response);
        setRows(parsedRows);
        setActiveMode(mode);
        setLastRun({
          limit: metadata.limit ?? rowLimit,
          truncated: metadata.truncated,
          rowCount: parsedRows.length,
          executedAt: new Date().toISOString(),
          mode,
        });
      } catch (err) {
        setRows([]);
        setLastRun(null);
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setIsLoading(false);
      }
    },
    [rowLimit],
  );

  const autoSpec = useMemo(() => {
    if (!autoFilters.sourceLabel.trim() || !autoFilters.targetLabel.trim()) {
      return null;
    }
    try {
      return createAutoGraphSpec(autoFilters);
    } catch {
      return null;
    }
  }, [autoFilters]);

  const selectedLabels = useMemo(() => {
    const trimmed = [autoFilters.sourceLabel, autoFilters.targetLabel]
      .map((label) => label.trim())
      .filter((label) => label.length > 0);
    return Array.from(new Set(trimmed));
  }, [autoFilters.sourceLabel, autoFilters.targetLabel]);

  const runAutoQuery = useCallback(async () => {
    if (!autoSpec || selectedLabels.length === 0) {
      setError("Provide both source and target labels to sample nodes.");
      return;
    }
    try {
      await ensureLabelIndexes(selectedLabels);
    } catch (err) {
      console.warn("Unable to build label indexes (continuing with fallback):", err);
    }
    await fetchGraph(autoSpec, "auto");
  }, [autoSpec, fetchGraph, selectedLabels]);

  const runManualQuery = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      try {
        const parsed = JSON.parse(manualPayload);
        await fetchGraph(parsed, "manual");
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [fetchGraph, manualPayload],
  );

  const canAutoSample = Boolean(autoSpec && selectedLabels.length > 0);
  useEffect(() => {
    if (hasBootstrapped || !canAutoSample) {
      return;
    }
    setBootstrapped(true);
    void runAutoQuery();
  }, [canAutoSample, hasBootstrapped, runAutoQuery]);

  const resetAutoFilters = () => {
    setAutoFilters({ sourceLabel: defaultLabel, targetLabel: defaultLabel, edgeType: "", direction: "out" });
  };

  const resetManual = () => {
    setManualPayload(JSON.stringify(DEMO_FOLLOWS_QUERY, null, 2));
  };

  return (
    <div className="space-y-6 px-4 py-10 max-w-6xl mx-auto">
      <header className="space-y-2">
        <Badge variant="secondary" className="uppercase tracking-wide">
          Graph explorer
        </Badge>
        <h1 className="text-3xl font-semibold">Visualize relationships safely</h1>
        <p className="text-muted-foreground max-w-3xl">
          Pull a manageable slice of the database, render it as a graph, then iterate on the query without
          freezing the browser. Increase the row limit once you are happy with the layout.
        </p>
      </header>

      <section className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Automatic sample</CardTitle>
            <CardDescription>
              Pick simple filters and we&apos;ll stream a capped slice ({MAX_ROW_LIMIT} rows max) the moment you load this page.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-5">
            {!hasLabels && (
              <Alert>
                <AlertTitle>No labels detected</AlertTitle>
                <AlertDescription>
                  Load or import data into the connected database, then refresh this page to visualize the graph.
                </AlertDescription>
              </Alert>
            )}
            <div className="grid gap-4 lg:grid-cols-3">
              <Field label="Source label">
                <Input
                  placeholder="User"
                  value={autoFilters.sourceLabel}
                  onChange={(event) =>
                    setAutoFilters((prev) => ({ ...prev, sourceLabel: event.target.value }))
                  }
                />
              </Field>
              <Field label="Target label">
                <Input
                  placeholder="User"
                  value={autoFilters.targetLabel}
                  onChange={(event) =>
                    setAutoFilters((prev) => ({ ...prev, targetLabel: event.target.value }))
                  }
                />
              </Field>
              <Field label="Edge type">
                <Input
                  placeholder="FOLLOWS"
                  value={autoFilters.edgeType}
                  onChange={(event) =>
                    setAutoFilters((prev) => ({ ...prev, edgeType: event.target.value }))
                  }
                />
              </Field>
            </div>
            {labelSuggestions.length > 0 && (
              <p className="text-xs text-muted-foreground">
                Suggestions:{" "}
                {labelSuggestions.map((label, idx) => (
                  <span key={label.name}>
                    <button
                      type="button"
                      className="underline-offset-2 hover:underline"
                      onClick={() =>
                        setAutoFilters((prev) => ({
                          ...prev,
                          sourceLabel: label.name,
                          targetLabel: label.name,
                        }))
                      }
                    >
                      {label.name} ({label.count})
                    </button>
                    {idx < labelSuggestions.length - 1 ? ", " : ""}
                  </span>
                ))}
              </p>
            )}

            <div className="grid gap-4 lg:grid-cols-3 lg:items-end">
              <Field label="Direction">
                <Select
                  value={autoFilters.direction}
                  onValueChange={(value: "out" | "in" | "both") =>
                    setAutoFilters((prev) => ({ ...prev, direction: value }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Direction" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="out">Outgoing</SelectItem>
                    <SelectItem value="in">Incoming</SelectItem>
                    <SelectItem value="both">Both directions</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label={`Row limit (${rowLimit})`}>
                <Slider
                  value={[rowLimit]}
                  min={MIN_ROW_LIMIT}
                  max={MAX_ROW_LIMIT}
                  step={25}
                  onValueChange={(value) => setRowLimit(value[0] ?? INITIAL_ROW_LIMIT)}
                />
                <p className="text-xs text-muted-foreground pt-1">
                  Higher limits reveal more of the graph but may slow the force simulation.
                </p>
              </Field>
              <div className="flex flex-wrap gap-3">
                <Button onClick={runAutoQuery} disabled={isLoading}>
                  {isLoading && activeMode === "auto" ? "Sampling…" : "Refresh sample"}
                </Button>
                <Button variant="ghost" disabled={isLoading} onClick={resetAutoFilters}>
                  Reset filters
                </Button>
              </div>
            </div>

            <details className="overflow-hidden rounded-lg border bg-muted/30">
              <summary className="flex cursor-pointer items-center justify-between px-4 py-2 text-sm font-medium">
                <span>Need a bespoke query?</span>
                <ChevronDownIcon className="size-4" />
              </summary>
              <div className="space-y-3 border-t px-4 py-4 text-sm">
                <p className="text-muted-foreground">
                  Paste any JSON spec and we&apos;ll still stream a capped subset so your browser stays responsive.
                </p>
                <form className="space-y-3" onSubmit={runManualQuery}>
                  <Textarea
                    value={manualPayload}
                    onChange={(event) => setManualPayload(event.target.value)}
                    rows={8}
                    className="font-mono text-xs"
                    spellCheck={false}
                  />
                  <div className="flex flex-wrap gap-3">
                    <Button type="submit" disabled={isLoading}>
                      {isLoading && activeMode === "manual" ? "Running…" : "Run custom query"}
                    </Button>
                    <Button type="button" variant="ghost" disabled={isLoading} onClick={resetManual}>
                      Reset spec
                    </Button>
                  </div>
                </form>
              </div>
            </details>

            {error && (
              <Alert variant="destructive">
                <AlertTitle>Query failed</AlertTitle>
                <AlertDescription className="font-mono text-xs">{error}</AlertDescription>
              </Alert>
            )}

            {lastRun && (
              <div className="flex flex-col gap-1 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
                <span>
                  {lastRun.mode === "auto" ? "Automatic sample" : "Manual spec"} loaded {lastRun.rowCount} row
                  {lastRun.rowCount === 1 ? "" : "s"} (limit {lastRun.limit}).
                </span>
                <span>Last run {new Date(lastRun.executedAt).toLocaleTimeString()}</span>
              </div>
            )}

            {lastRun?.truncated && (
              <Alert>
                <AlertTitle>Result truncated</AlertTitle>
                <AlertDescription>
                  Only the first {lastRun.limit} rows were streamed to keep the page responsive. Increase the limit when
                  you&apos;re ready for a larger slice.
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>

        {rows.length > 0 ? (
          <GraphView rows={rows} />
        ) : (
          <Card className="border-dashed">
            <CardHeader>
              <CardTitle>No graph rendered yet</CardTitle>
              <CardDescription>Run a query above to fetch a subset of nodes and edges.</CardDescription>
            </CardHeader>
          </Card>
        )}
      </section>
    </div>
  );
}

function parseLimitMetadata(value: unknown): { limit: number | null; truncated: boolean } {
  if (!value || typeof value !== "object") {
    return { limit: null, truncated: false };
  }
  const record = value as Record<string, unknown>;
  const limitCandidate = record["row_limit"] ?? record["rowLimit"];
  const limit =
    typeof limitCandidate === "number" && Number.isFinite(limitCandidate) ? Math.floor(limitCandidate) : null;
  const truncatedValue = record["truncated"];
  const truncated = typeof truncatedValue === "boolean" ? truncatedValue : false;
  return { limit, truncated };
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <Label className="text-sm text-muted-foreground">{label}</Label>
      {children}
    </div>
  );
}
