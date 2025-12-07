import { type ComponentProps, type ComponentType, type FormEvent, useCallback, useEffect, useState } from "react";
import { GraphView } from "../components/query/graph-view";
import type { GraphCanvasProps } from "../components/query/graph-canvas";
import { QueryBuilder } from "../components/query/query-builder";
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
import { Label } from "../components/ui/label";
import { Slider } from "../components/ui/slider";
import { Textarea } from "../components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../components/ui/tabs";
import { executeQuery, fetchFullGraph, type FullGraphResponse } from "../lib/api";
import { DEMO_FOLLOWS_QUERY } from "../lib/query-presets";
import { extractRows, type QueryRow } from "../lib/query-utils";

type GraphViewProps = Omit<ComponentProps<typeof GraphView>, "GraphRenderer">;
type GraphRendererComponent = ComponentType<GraphCanvasProps>;

const INITIAL_ROW_LIMIT = 250;
const MIN_ROW_LIMIT = 25;
const MAX_ROW_LIMIT = 1_000;

type GraphComponentType = ComponentType<{ rows: QueryRow[] }>;

type GraphExplorerPageProps = {
    GraphComponent?: GraphComponentType;
};

type QueryRunMetadata = {
    limit: number;
    truncated: boolean;
    rowCount: number;
    executedAt: string;
};

/**
 * GraphView wrapper that loads reagraph as the default renderer.
 * Falls back to a placeholder while loading.
 */
function GraphViewWithReagraph(props: GraphViewProps) {
    const [GraphRenderer, setGraphRenderer] = useState<GraphRendererComponent | null>(null);
    const [loadError, setLoadError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        void import("../components/query/reagraph-canvas")
            .then((module) => {
                if (!cancelled) {
                    setGraphRenderer(() => module.ReagraphCanvas);
                }
            })
            .catch((error) => {
                console.error("Failed to load Reagraph renderer", error);
                if (!cancelled) {
                    setLoadError(error instanceof Error ? error.message : String(error));
                }
            });
        return () => {
            cancelled = true;
        };
    }, []);

    const ActiveRenderer: GraphRendererComponent =
        GraphRenderer ??
        (({ height = 540 }) => (
            <div
                className="flex h-full w-full items-center justify-center rounded-2xl border bg-muted/30 text-sm text-muted-foreground"
                style={{ height }}
            >
                {loadError ? (
                    <span>Failed to load graph renderer. Please refresh.</span>
                ) : (
                    <span>Loading graph renderer…</span>
                )}
            </div>
        ));

    return <GraphView {...props} GraphRenderer={ActiveRenderer} />;
}

export function meta() {
    return [
        { title: "Graph Explorer · Sombra" },
        {
            name: "description",
            content: "Visualize database rows as a WebGL-powered graph without overwhelming the browser.",
        },
    ];
}

export default function GraphExplorer() {
    return <GraphExplorerPage GraphComponent={GraphViewWithReagraph} />;
}

export function GraphExplorerPage({ GraphComponent = GraphViewWithReagraph }: GraphExplorerPageProps) {
    const [queryMode, setQueryMode] = useState<"visual" | "json">("visual");
    const [queryPayload, setQueryPayload] = useState(() => JSON.stringify(DEMO_FOLLOWS_QUERY, null, 2));
    const [builderQuery, setBuilderQuery] = useState<unknown>(null);
    const [rowLimit, setRowLimit] = useState(INITIAL_ROW_LIMIT);
    const [rows, setRows] = useState<QueryRow[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [lastRun, setLastRun] = useState<QueryRunMetadata | null>(null);
    const [fullGraph, setFullGraph] = useState<FullGraphResponse | null>(null);
    const [isLoadingFullGraph, setIsLoadingFullGraph] = useState(true);

    // Load full graph on mount
    useEffect(() => {
        let cancelled = false;
        setIsLoadingFullGraph(true);
        fetchFullGraph()
            .then((data) => {
                if (!cancelled) {
                    setFullGraph(data);
                    // Convert to rows format for GraphView compatibility
                    const graphRows = convertFullGraphToRows(data);
                    setRows(graphRows);
                    setLastRun({
                        limit: graphRows.length,
                        truncated: false,
                        rowCount: graphRows.length,
                        executedAt: new Date().toISOString(),
                    });
                }
            })
            .catch((err) => {
                if (!cancelled) {
                    console.error("Failed to load full graph:", err);
                    setError(err instanceof Error ? err.message : String(err));
                }
            })
            .finally(() => {
                if (!cancelled) {
                    setIsLoadingFullGraph(false);
                }
            });
        return () => {
            cancelled = true;
        };
    }, []);

    const fetchGraph = useCallback(
        async (spec: unknown) => {
            setIsLoading(true);
            setError(null);
            try {
                const response = await executeQuery(spec, { maxRows: rowLimit });
                const parsedRows = extractRows(response) ?? [];
                const metadata = parseLimitMetadata(response);
                setRows(parsedRows);
                setLastRun({
                    limit: metadata.limit ?? rowLimit,
                    truncated: metadata.truncated,
                    rowCount: parsedRows.length,
                    executedAt: new Date().toISOString(),
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

    const runJsonQuery = useCallback(
        async (event: FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            try {
                const parsed = JSON.parse(queryPayload);
                await fetchGraph(parsed);
            } catch (err) {
                setError(err instanceof Error ? err.message : String(err));
            }
        },
        [fetchGraph, queryPayload],
    );

    const runVisualQuery = useCallback(async () => {
        if (!builderQuery) {
            setError("No query defined. Add at least one node.");
            return;
        }
        await fetchGraph(builderQuery);
    }, [fetchGraph, builderQuery]);

    const handleBuilderChange = useCallback((query: unknown) => {
        setBuilderQuery(query);
    }, []);

    const copyToJson = useCallback(() => {
        if (builderQuery) {
            setQueryPayload(JSON.stringify(builderQuery, null, 2));
            setQueryMode("json");
        }
    }, [builderQuery]);

    const resetQuery = () => {
        setQueryPayload(JSON.stringify(DEMO_FOLLOWS_QUERY, null, 2));
    };

    return (
        <div className="space-y-6 px-4 py-10 max-w-6xl mx-auto">
            <header className="space-y-2">
                <Badge variant="secondary" className="uppercase tracking-wide">
                    Graph explorer
                </Badge>
                <h1 className="text-3xl font-semibold">Visualize relationships</h1>
                <p className="text-muted-foreground max-w-3xl">
                    {fullGraph ? (
                        <>Loaded {fullGraph.nodes.length} nodes and {fullGraph.edges.length} edges. Use the query tools below to filter, or explore the full graph.</>
                    ) : isLoadingFullGraph ? (
                        <>Loading full graph from database...</>
                    ) : (
                        <>Query the database and render the results as an interactive graph.</>
                    )}
                </p>
            </header>

            <section className="space-y-6">
                <Card>
                    <CardHeader>
                        <CardTitle>Query</CardTitle>
                        <CardDescription>
                            Build a query visually or write JSON directly.
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-5">
                        <Tabs value={queryMode} onValueChange={(v) => setQueryMode(v as "visual" | "json")}>
                            <TabsList className="mb-4">
                                <TabsTrigger value="visual">Visual Builder</TabsTrigger>
                                <TabsTrigger value="json">JSON</TabsTrigger>
                            </TabsList>

                            <TabsContent value="visual" className="space-y-4">
                                <QueryBuilder onQueryChange={handleBuilderChange} />
                                
                                {builderQuery && (
                                    <details className="text-sm">
                                        <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                                            Preview generated JSON
                                        </summary>
                                        <pre className="mt-2 p-3 rounded-md bg-muted font-mono text-xs overflow-auto max-h-48">
                                            {JSON.stringify(builderQuery, null, 2)}
                                        </pre>
                                    </details>
                                )}

                                <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                                    <div className="space-y-2 sm:w-64">
                                        <Label className="text-sm text-muted-foreground">
                                            Row limit ({rowLimit})
                                        </Label>
                                        <Slider
                                            value={[rowLimit]}
                                            min={MIN_ROW_LIMIT}
                                            max={MAX_ROW_LIMIT}
                                            step={25}
                                            onValueChange={(value) => setRowLimit(value[0] ?? INITIAL_ROW_LIMIT)}
                                        />
                                    </div>
                                    <div className="flex flex-wrap gap-3">
                                        <Button type="button" onClick={runVisualQuery} disabled={isLoading}>
                                            {isLoading ? "Running..." : "Run query"}
                                        </Button>
                                        <Button type="button" variant="outline" onClick={copyToJson}>
                                            Copy to JSON
                                        </Button>
                                    </div>
                                </div>
                            </TabsContent>

                            <TabsContent value="json" className="space-y-4">
                                <form className="space-y-4" onSubmit={runJsonQuery}>
                                    <Textarea
                                        value={queryPayload}
                                        onChange={(event) => setQueryPayload(event.target.value)}
                                        rows={10}
                                        className="font-mono text-xs"
                                        spellCheck={false}
                                        placeholder='{"match": [{"node": "n", "labels": ["Person"]}], "return": ["n"]}'
                                    />
                                    
                                    <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                                        <div className="space-y-2 sm:w-64">
                                            <Label className="text-sm text-muted-foreground">
                                                Row limit ({rowLimit})
                                            </Label>
                                            <Slider
                                                value={[rowLimit]}
                                                min={MIN_ROW_LIMIT}
                                                max={MAX_ROW_LIMIT}
                                                step={25}
                                                onValueChange={(value) => setRowLimit(value[0] ?? INITIAL_ROW_LIMIT)}
                                            />
                                        </div>
                                        <div className="flex flex-wrap gap-3">
                                            <Button type="submit" disabled={isLoading}>
                                                {isLoading ? "Running..." : "Run query"}
                                            </Button>
                                            <Button type="button" variant="ghost" disabled={isLoading} onClick={resetQuery}>
                                                Reset
                                            </Button>
                                        </div>
                                    </div>
                                </form>
                            </TabsContent>
                        </Tabs>

                        {error && (
                            <Alert variant="destructive">
                                <AlertTitle>Query failed</AlertTitle>
                                <AlertDescription className="font-mono text-xs">{error}</AlertDescription>
                            </Alert>
                        )}

                        {lastRun && (
                            <div className="flex flex-col gap-1 text-sm text-muted-foreground sm:flex-row sm:items-center sm:justify-between">
                                <span>
                                    Loaded {lastRun.rowCount} row{lastRun.rowCount === 1 ? "" : "s"} (limit {lastRun.limit})
                                </span>
                                <span>Last run {new Date(lastRun.executedAt).toLocaleTimeString()}</span>
                            </div>
                        )}

                        {lastRun?.truncated && (
                            <Alert>
                                <AlertTitle>Result truncated</AlertTitle>
                                <AlertDescription>
                                    Only the first {lastRun.limit} rows were returned. Increase the limit to see more.
                                </AlertDescription>
                            </Alert>
                        )}
                    </CardContent>
                </Card>

                {isLoadingFullGraph ? (
                    <Card className="border-dashed">
                        <CardHeader>
                            <CardTitle>Loading graph...</CardTitle>
                            <CardDescription>Fetching all nodes and edges from the database.</CardDescription>
                        </CardHeader>
                    </Card>
                ) : rows.length > 0 ? (
                    <GraphComponent rows={rows} />
                ) : (
                    <Card className="border-dashed">
                        <CardHeader>
                            <CardTitle>No graph rendered yet</CardTitle>
                            <CardDescription>Run a query above to fetch nodes and edges.</CardDescription>
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

/**
 * Convert the full graph response into QueryRow[] format that GraphView expects.
 * Each edge becomes a row with source node (a) and target node (b).
 */
function convertFullGraphToRows(graph: FullGraphResponse): QueryRow[] {
    // Build a map of node IDs to nodes for quick lookup
    const nodeMap = new Map<number, FullGraphResponse["nodes"][0]>();
    for (const node of graph.nodes) {
        nodeMap.set(node._id, node);
    }

    // Convert each edge to a row with source and target nodes
    const rows: QueryRow[] = [];
    for (const edge of graph.edges) {
        const sourceNode = nodeMap.get(edge._source);
        const targetNode = nodeMap.get(edge._target);
        if (sourceNode && targetNode) {
            rows.push({
                a: {
                    _id: sourceNode._id,
                    labels: sourceNode._labels,
                    props: Object.fromEntries(
                        Object.entries(sourceNode).filter(([k]) => !k.startsWith("_"))
                    ),
                },
                b: {
                    _id: targetNode._id,
                    labels: targetNode._labels,
                    props: Object.fromEntries(
                        Object.entries(targetNode).filter(([k]) => !k.startsWith("_"))
                    ),
                },
            });
        }
    }

    // If there are isolated nodes (no edges), add them as single-node rows
    const connectedNodeIds = new Set<number>();
    for (const edge of graph.edges) {
        connectedNodeIds.add(edge._source);
        connectedNodeIds.add(edge._target);
    }
    for (const node of graph.nodes) {
        if (!connectedNodeIds.has(node._id)) {
            rows.push({
                a: {
                    _id: node._id,
                    labels: node._labels,
                    props: Object.fromEntries(
                        Object.entries(node).filter(([k]) => !k.startsWith("_"))
                    ),
                },
            });
        }
    }

    return rows;
}
