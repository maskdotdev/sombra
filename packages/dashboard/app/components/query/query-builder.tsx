import { useCallback, useEffect, useState } from "react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Label } from "../ui/label";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "../ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "../ui/card";
import { Badge } from "../ui/badge";
import { fetchLabelSamples, type LabelSummary } from "../../lib/api";
import { Plus, Trash2, X, Circle, ArrowRight } from "lucide-react";

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

type Direction = "out" | "in" | "both";

type NodeMatch = {
    id: string;
    var: string;
    label: string;
};

type EdgeMatch = {
    id: string;
    from: string;
    to: string;
    edgeType: string;
    direction: Direction;
};

type PropertyFilter = {
    id: string;
    variable: string;
    property: string;
    operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "contains" | "starts_with";
    value: string;
};

type FilterGroup = {
    id: string;
    logic: "and" | "or";
    filters: PropertyFilter[];
};

export type QueryBuilderState = {
    nodes: NodeMatch[];
    edges: EdgeMatch[];
    filterGroups: FilterGroup[];
};

type QueryBuilderProps = {
    onQueryChange: (query: unknown) => void;
    initialState?: QueryBuilderState;
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

let idCounter = 0;
function generateId(prefix: string): string {
    return `${prefix}-${++idCounter}-${Date.now().toString(36)}`;
}

function createEmptyNode(): NodeMatch {
    const varName = `n${idCounter + 1}`;
    return { id: generateId("node"), var: varName, label: "" };
}

function createEmptyEdge(nodes: NodeMatch[]): EdgeMatch {
    return {
        id: generateId("edge"),
        from: nodes[0]?.var ?? "",
        to: nodes[1]?.var ?? nodes[0]?.var ?? "",
        edgeType: "",
        direction: "out",
    };
}

function createEmptyFilter(nodes: NodeMatch[]): PropertyFilter {
    return {
        id: generateId("filter"),
        variable: nodes[0]?.var ?? "",
        property: "",
        operator: "=",
        value: "",
    };
}

function createEmptyFilterGroup(nodes: NodeMatch[]): FilterGroup {
    return {
        id: generateId("group"),
        logic: "and",
        filters: [createEmptyFilter(nodes)],
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Query generation
// ─────────────────────────────────────────────────────────────────────────────

function buildQuerySpec(state: QueryBuilderState): unknown {
    const matches = state.nodes
        .filter((n) => n.var.trim())
        .map((n) => {
            const match: Record<string, string> = { var: n.var.trim() };
            if (n.label.trim()) {
                match.label = n.label.trim();
            }
            return match;
        });

    const edges = state.edges
        .filter((e) => e.from.trim() && e.to.trim())
        .map((e) => {
            const edge: Record<string, string> = {
                from: e.from.trim(),
                to: e.to.trim(),
                direction: e.direction,
            };
            if (e.edgeType.trim()) {
                edge.edge_type = e.edgeType.trim();
            }
            return edge;
        });

    // Build where clause from filter groups
    const whereConditions = buildWhereClause(state.filterGroups);

    // Collect all unique variables for projection
    const varSet = new Set<string>();
    for (const n of state.nodes) {
        if (n.var.trim()) varSet.add(n.var.trim());
    }

    const projections = Array.from(varSet).map((v) => ({
        kind: "var",
        var: v,
    }));

    const spec: Record<string, unknown> = {
        $schemaVersion: 1,
        matches,
        projections,
    };

    if (edges.length > 0) {
        spec.edges = edges;
    }

    if (whereConditions) {
        spec.where = whereConditions;
    }

    return spec;
}

function buildWhereClause(filterGroups: FilterGroup[]): unknown | null {
    const nonEmptyGroups = filterGroups
        .map((group) => {
            const validFilters = group.filters.filter(
                (f) => f.variable.trim() && f.property.trim() && f.value.trim()
            );
            if (validFilters.length === 0) return null;

            const conditions = validFilters.map((f) => ({
                op: mapOperator(f.operator),
                left: { var: f.variable.trim(), prop: f.property.trim() },
                right: parseFilterValue(f.value),
            }));

            if (conditions.length === 1) return conditions[0];
            return { [group.logic]: conditions };
        })
        .filter(Boolean);

    if (nonEmptyGroups.length === 0) return null;
    if (nonEmptyGroups.length === 1) return nonEmptyGroups[0];
    return { and: nonEmptyGroups };
}

function mapOperator(op: PropertyFilter["operator"]): string {
    switch (op) {
        case "=":
            return "eq";
        case "!=":
            return "neq";
        case ">":
            return "gt";
        case "<":
            return "lt";
        case ">=":
            return "gte";
        case "<=":
            return "lte";
        case "contains":
            return "contains";
        case "starts_with":
            return "starts_with";
        default:
            return "eq";
    }
}

function parseFilterValue(value: string): unknown {
    const trimmed = value.trim();
    // Try to parse as number
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
        return Number(trimmed);
    }
    // Try to parse as boolean
    if (trimmed.toLowerCase() === "true") return true;
    if (trimmed.toLowerCase() === "false") return false;
    // Return as string (strip surrounding quotes if present)
    if (
        (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
        (trimmed.startsWith("'") && trimmed.endsWith("'"))
    ) {
        return trimmed.slice(1, -1);
    }
    return trimmed;
}

// ─────────────────────────────────────────────────────────────────────────────
// Component
// ─────────────────────────────────────────────────────────────────────────────

export function QueryBuilder({ onQueryChange, initialState }: QueryBuilderProps) {
    const [labels, setLabels] = useState<LabelSummary[]>([]);
    const [edgeTypes, setEdgeTypes] = useState<string[]>([]);

    const [nodes, setNodes] = useState<NodeMatch[]>(
        initialState?.nodes ?? [createEmptyNode()]
    );
    const [edges, setEdges] = useState<EdgeMatch[]>(initialState?.edges ?? []);
    const [filterGroups, setFilterGroups] = useState<FilterGroup[]>(
        initialState?.filterGroups ?? []
    );

    // Fetch labels on mount
    useEffect(() => {
        fetchLabelSamples()
            .then((samples) => {
                setLabels(samples);
                // Extract edge types (labels that look like edge types - uppercase with underscores)
                const possibleEdgeTypes = samples
                    .filter((s) => /^[A-Z][A-Z_]*$/.test(s.name))
                    .map((s) => s.name);
                setEdgeTypes(possibleEdgeTypes);
            })
            .catch(console.error);
    }, []);

    // Emit query on state change
    useEffect(() => {
        const spec = buildQuerySpec({ nodes, edges, filterGroups });
        onQueryChange(spec);
    }, [nodes, edges, filterGroups, onQueryChange]);

    // ─────────────────────────────────────────────────────────────────────────
    // Node handlers
    // ─────────────────────────────────────────────────────────────────────────

    const addNode = useCallback(() => {
        setNodes((prev) => [...prev, createEmptyNode()]);
    }, []);

    const removeNode = useCallback((id: string) => {
        setNodes((prev) => prev.filter((n) => n.id !== id));
    }, []);

    const updateNode = useCallback(
        (id: string, field: keyof NodeMatch, value: string) => {
            setNodes((prev) =>
                prev.map((n) => (n.id === id ? { ...n, [field]: value } : n))
            );
        },
        []
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Edge handlers
    // ─────────────────────────────────────────────────────────────────────────

    const addEdge = useCallback(() => {
        setEdges((prev) => [...prev, createEmptyEdge(nodes)]);
    }, [nodes]);

    const removeEdge = useCallback((id: string) => {
        setEdges((prev) => prev.filter((e) => e.id !== id));
    }, []);

    const updateEdge = useCallback(
        (id: string, field: keyof EdgeMatch, value: string) => {
            setEdges((prev) =>
                prev.map((e) => (e.id === id ? { ...e, [field]: value } : e))
            );
        },
        []
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Filter handlers
    // ─────────────────────────────────────────────────────────────────────────

    const addFilterGroup = useCallback(() => {
        setFilterGroups((prev) => [...prev, createEmptyFilterGroup(nodes)]);
    }, [nodes]);

    const removeFilterGroup = useCallback((groupId: string) => {
        setFilterGroups((prev) => prev.filter((g) => g.id !== groupId));
    }, []);

    const updateFilterGroupLogic = useCallback(
        (groupId: string, logic: "and" | "or") => {
            setFilterGroups((prev) =>
                prev.map((g) => (g.id === groupId ? { ...g, logic } : g))
            );
        },
        []
    );

    const addFilterToGroup = useCallback(
        (groupId: string) => {
            setFilterGroups((prev) =>
                prev.map((g) =>
                    g.id === groupId
                        ? { ...g, filters: [...g.filters, createEmptyFilter(nodes)] }
                        : g
                )
            );
        },
        [nodes]
    );

    const removeFilter = useCallback((groupId: string, filterId: string) => {
        setFilterGroups((prev) =>
            prev.map((g) =>
                g.id === groupId
                    ? { ...g, filters: g.filters.filter((f) => f.id !== filterId) }
                    : g
            )
        );
    }, []);

    const updateFilter = useCallback(
        (
            groupId: string,
            filterId: string,
            field: keyof PropertyFilter,
            value: string
        ) => {
            setFilterGroups((prev) =>
                prev.map((g) =>
                    g.id === groupId
                        ? {
                              ...g,
                              filters: g.filters.map((f) =>
                                  f.id === filterId ? { ...f, [field]: value } : f
                              ),
                          }
                        : g
                )
            );
        },
        []
    );

    // Available variables for edge/filter selectors
    const availableVars = nodes.filter((n) => n.var.trim()).map((n) => n.var.trim());

    return (
        <div className="space-y-4">
            {/* Nodes Section */}
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <CardTitle className="text-base flex items-center gap-2">
                            <Circle className="h-4 w-4" />
                            Match Nodes
                        </CardTitle>
                        <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={addNode}
                        >
                            <Plus className="h-4 w-4 mr-1" />
                            Add Node
                        </Button>
                    </div>
                </CardHeader>
                <CardContent className="space-y-3">
                    {nodes.map((node, idx) => (
                        <div
                            key={node.id}
                            className="flex items-end gap-3 p-3 rounded-lg border bg-muted/30"
                        >
                            <div className="flex-1 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    Variable
                                </Label>
                                <Input
                                    value={node.var}
                                    onChange={(e) =>
                                        updateNode(node.id, "var", e.target.value)
                                    }
                                    placeholder="n1"
                                    className="h-8 font-mono"
                                />
                            </div>
                            <div className="flex-1 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    Label
                                </Label>
                                <Select
                                    value={node.label}
                                    onValueChange={(v) =>
                                        updateNode(node.id, "label", v === "__any__" ? "" : v)
                                    }
                                >
                                    <SelectTrigger className="h-8">
                                        <SelectValue placeholder="Any label" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="__any__">Any label</SelectItem>
                                        {labels.map((label) => (
                                            <SelectItem key={label.name} value={label.name}>
                                                {label.name}{" "}
                                                <span className="text-muted-foreground">
                                                    ({label.count})
                                                </span>
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>
                            {nodes.length > 1 && (
                                <Button
                                    type="button"
                                    variant="ghost"
                                    size="icon"
                                    className="h-8 w-8 text-muted-foreground hover:text-destructive"
                                    onClick={() => removeNode(node.id)}
                                >
                                    <Trash2 className="h-4 w-4" />
                                </Button>
                            )}
                        </div>
                    ))}
                </CardContent>
            </Card>

            {/* Edges Section */}
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <CardTitle className="text-base flex items-center gap-2">
                            <ArrowRight className="h-4 w-4" />
                            Match Edges
                        </CardTitle>
                        <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={addEdge}
                            disabled={availableVars.length < 1}
                        >
                            <Plus className="h-4 w-4 mr-1" />
                            Add Edge
                        </Button>
                    </div>
                </CardHeader>
                <CardContent className="space-y-3">
                    {edges.length === 0 && (
                        <p className="text-sm text-muted-foreground py-2">
                            No edges defined. Add an edge to match relationships.
                        </p>
                    )}
                    {edges.map((edge) => (
                        <div
                            key={edge.id}
                            className="flex flex-wrap items-end gap-3 p-3 rounded-lg border bg-muted/30"
                        >
                            <div className="w-24 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    From
                                </Label>
                                <Select
                                    value={edge.from}
                                    onValueChange={(v) => updateEdge(edge.id, "from", v)}
                                >
                                    <SelectTrigger className="h-8 font-mono">
                                        <SelectValue placeholder="from" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {availableVars.map((v) => (
                                            <SelectItem key={v} value={v}>
                                                {v}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="w-32 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    Direction
                                </Label>
                                <Select
                                    value={edge.direction}
                                    onValueChange={(v) =>
                                        updateEdge(edge.id, "direction", v as Direction)
                                    }
                                >
                                    <SelectTrigger className="h-8">
                                        <SelectValue />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="out">
                                            Outgoing (&rarr;)
                                        </SelectItem>
                                        <SelectItem value="in">
                                            Incoming (&larr;)
                                        </SelectItem>
                                        <SelectItem value="both">
                                            Both (&harr;)
                                        </SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="w-24 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    To
                                </Label>
                                <Select
                                    value={edge.to}
                                    onValueChange={(v) => updateEdge(edge.id, "to", v)}
                                >
                                    <SelectTrigger className="h-8 font-mono">
                                        <SelectValue placeholder="to" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {availableVars.map((v) => (
                                            <SelectItem key={v} value={v}>
                                                {v}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="flex-1 min-w-32 space-y-1.5">
                                <Label className="text-xs text-muted-foreground">
                                    Edge Type
                                </Label>
                                <Select
                                    value={edge.edgeType}
                                    onValueChange={(v) =>
                                        updateEdge(edge.id, "edgeType", v === "__any__" ? "" : v)
                                    }
                                >
                                    <SelectTrigger className="h-8">
                                        <SelectValue placeholder="Any type" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="__any__">Any type</SelectItem>
                                        {edgeTypes.map((t) => (
                                            <SelectItem key={t} value={t}>
                                                {t}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>

                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-muted-foreground hover:text-destructive"
                                onClick={() => removeEdge(edge.id)}
                            >
                                <Trash2 className="h-4 w-4" />
                            </Button>
                        </div>
                    ))}
                </CardContent>
            </Card>

            {/* Filters Section */}
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <CardTitle className="text-base">Property Filters</CardTitle>
                        <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={addFilterGroup}
                            disabled={availableVars.length < 1}
                        >
                            <Plus className="h-4 w-4 mr-1" />
                            Add Filter Group
                        </Button>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    {filterGroups.length === 0 && (
                        <p className="text-sm text-muted-foreground py-2">
                            No filters defined. Add a filter group to constrain results.
                        </p>
                    )}
                    {filterGroups.map((group, groupIdx) => (
                        <div
                            key={group.id}
                            className="p-3 rounded-lg border bg-muted/30 space-y-3"
                        >
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                    <Badge variant="outline" className="text-xs">
                                        Group {groupIdx + 1}
                                    </Badge>
                                    <Select
                                        value={group.logic}
                                        onValueChange={(v) =>
                                            updateFilterGroupLogic(
                                                group.id,
                                                v as "and" | "or"
                                            )
                                        }
                                    >
                                        <SelectTrigger className="h-7 w-20">
                                            <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="and">AND</SelectItem>
                                            <SelectItem value="or">OR</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>
                                <div className="flex items-center gap-1">
                                    <Button
                                        type="button"
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => addFilterToGroup(group.id)}
                                    >
                                        <Plus className="h-3 w-3 mr-1" />
                                        Add
                                    </Button>
                                    <Button
                                        type="button"
                                        variant="ghost"
                                        size="icon"
                                        className="h-7 w-7 text-muted-foreground hover:text-destructive"
                                        onClick={() => removeFilterGroup(group.id)}
                                    >
                                        <X className="h-4 w-4" />
                                    </Button>
                                </div>
                            </div>

                            {group.filters.map((filter, filterIdx) => (
                                <div
                                    key={filter.id}
                                    className="flex flex-wrap items-end gap-2"
                                >
                                    {filterIdx > 0 && (
                                        <Badge
                                            variant="secondary"
                                            className="h-8 px-2 text-xs uppercase"
                                        >
                                            {group.logic}
                                        </Badge>
                                    )}
                                    <div className="w-20 space-y-1">
                                        <Label className="text-xs text-muted-foreground">
                                            Var
                                        </Label>
                                        <Select
                                            value={filter.variable}
                                            onValueChange={(v) =>
                                                updateFilter(
                                                    group.id,
                                                    filter.id,
                                                    "variable",
                                                    v
                                                )
                                            }
                                        >
                                            <SelectTrigger className="h-8 font-mono">
                                                <SelectValue placeholder="var" />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {availableVars.map((v) => (
                                                    <SelectItem key={v} value={v}>
                                                        {v}
                                                    </SelectItem>
                                                ))}
                                            </SelectContent>
                                        </Select>
                                    </div>
                                    <div className="flex-1 min-w-24 space-y-1">
                                        <Label className="text-xs text-muted-foreground">
                                            Property
                                        </Label>
                                        <Input
                                            value={filter.property}
                                            onChange={(e) =>
                                                updateFilter(
                                                    group.id,
                                                    filter.id,
                                                    "property",
                                                    e.target.value
                                                )
                                            }
                                            placeholder="name"
                                            className="h-8"
                                        />
                                    </div>
                                    <div className="w-24 space-y-1">
                                        <Label className="text-xs text-muted-foreground">
                                            Op
                                        </Label>
                                        <Select
                                            value={filter.operator}
                                            onValueChange={(v) =>
                                                updateFilter(
                                                    group.id,
                                                    filter.id,
                                                    "operator",
                                                    v as PropertyFilter["operator"]
                                                )
                                            }
                                        >
                                            <SelectTrigger className="h-8">
                                                <SelectValue />
                                            </SelectTrigger>
                                            <SelectContent>
                                                <SelectItem value="=">=</SelectItem>
                                                <SelectItem value="!=">!=</SelectItem>
                                                <SelectItem value=">">&gt;</SelectItem>
                                                <SelectItem value="<">&lt;</SelectItem>
                                                <SelectItem value=">=">&gt;=</SelectItem>
                                                <SelectItem value="<=">&lt;=</SelectItem>
                                                <SelectItem value="contains">
                                                    contains
                                                </SelectItem>
                                                <SelectItem value="starts_with">
                                                    starts with
                                                </SelectItem>
                                            </SelectContent>
                                        </Select>
                                    </div>
                                    <div className="flex-1 min-w-24 space-y-1">
                                        <Label className="text-xs text-muted-foreground">
                                            Value
                                        </Label>
                                        <Input
                                            value={filter.value}
                                            onChange={(e) =>
                                                updateFilter(
                                                    group.id,
                                                    filter.id,
                                                    "value",
                                                    e.target.value
                                                )
                                            }
                                            placeholder="value"
                                            className="h-8"
                                        />
                                    </div>
                                    {group.filters.length > 1 && (
                                        <Button
                                            type="button"
                                            variant="ghost"
                                            size="icon"
                                            className="h-8 w-8 text-muted-foreground hover:text-destructive"
                                            onClick={() =>
                                                removeFilter(group.id, filter.id)
                                            }
                                        >
                                            <Trash2 className="h-4 w-4" />
                                        </Button>
                                    )}
                                </div>
                            ))}
                        </div>
                    ))}
                </CardContent>
            </Card>
        </div>
    );
}

export { buildQuerySpec };
