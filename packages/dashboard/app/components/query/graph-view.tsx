import { useEffect, useMemo, useState } from "react"
import { ActivityIcon, Maximize2Icon, Share2Icon, XIcon } from "lucide-react"

import { GraphCanvas, type GraphCanvasProps } from "./graph-canvas"
import { deriveNodeLabel, isGraphEntity } from "./graph-types"
import type { GraphEdgeDatum, GraphEntity, GraphNodeDatum } from "./graph-types"
import { PropertyValue } from "./property-value"
import { Badge } from "../ui/badge"
import { Button } from "../ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../ui/card"
import { Input } from "../ui/input"
import { Label } from "../ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../ui/select"
import { Slider } from "../ui/slider"
import { Switch } from "../ui/switch"
import type { QueryRow } from "~/lib/query-utils"

type ColumnStat = {
  key: string
  count: number
}

type GraphRendererComponent = React.ComponentType<GraphCanvasProps>

type GraphViewProps = {
  rows: QueryRow[]
  GraphRenderer?: GraphRendererComponent
}

export function GraphView({ rows, GraphRenderer = GraphCanvas }: GraphViewProps) {
  const nodeColumns = useMemo(() => detectNodeColumns(rows), [rows])
  const [sourceColumn, setSourceColumn] = useState<string>("")
  const [targetColumn, setTargetColumn] = useState<string>("")
  const [maxEdges, setMaxEdges] = useState(150)
  const [filterText, setFilterText] = useState("")
  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null)
  const [showEdgeLabels, setShowEdgeLabels] = useState(false)
  const [isGraphExpanded, setIsGraphExpanded] = useState(false)

  useEffect(() => {
    if (nodeColumns.length === 0) {
      setSourceColumn("")
      setTargetColumn("")
      return
    }
    setSourceColumn((prev) => {
      if (prev && nodeColumns.some((col) => col.key === prev)) {
        return prev
      }
      return nodeColumns[0]?.key ?? ""
    })
    setTargetColumn((prev) => {
      if (prev && nodeColumns.some((col) => col.key === prev)) {
        return prev
      }
      return nodeColumns[1]?.key ?? nodeColumns[0]?.key ?? ""
    })
  }, [nodeColumns])

  useEffect(() => {
    setSelectedNodeId(null)
  }, [sourceColumn, targetColumn, rows])

  const limitedRows = useMemo(() => {
    if (!filterText.trim()) {
      return rows
    }
    const normalized = filterText.toLowerCase()
    return rows.filter((row) =>
      Object.values(row).some((value) => {
        if (typeof value === "string") {
          return value.toLowerCase().includes(normalized)
        }
        if (isGraphEntity(value)) {
          const label = deriveNodeLabel(value).toLowerCase()
          return label.includes(normalized)
        }
        return false
      })
    )
  }, [rows, filterText])

  const graph = useMemo(() => {
    if (nodeColumns.length === 0) {
      return { nodes: [], edges: [] }
    }
    return buildGraphData(limitedRows, {
      nodeColumns: nodeColumns.map((col) => col.key),
      sourceColumn,
      targetColumn,
      maxEdges,
    })
  }, [limitedRows, nodeColumns, sourceColumn, targetColumn, maxEdges])

  const selectedNode = useMemo(
    () => graph.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [graph.nodes, selectedNodeId]
  )

  if (rows.length === 0 || nodeColumns.length === 0) {
    return null
  }

  const hasDistinctColumns = sourceColumn && targetColumn && sourceColumn !== targetColumn

  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between gap-3">
            <div>
              <CardTitle className="flex items-center gap-2 text-lg">
                <Share2Icon className="size-5 text-primary" />
                Graph view
              </CardTitle>
              <CardDescription>
                Visualize row relationships as a force-directed graph. Choose which columns represent
                the edge endpoints.
              </CardDescription>
            </div>
            <Badge variant="secondary">
              {graph.nodes.length} nodes · {graph.edges.length} edges
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
        <div className="grid gap-4 lg:grid-cols-3">
          <Field label="Source column">
            <Select value={sourceColumn} onValueChange={setSourceColumn}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select column" />
              </SelectTrigger>
              <SelectContent>
                {nodeColumns.map((column) => (
                  <SelectItem key={column.key} value={column.key}>
                    {column.key} ({column.count})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </Field>
          <Field label="Target column">
            <Select value={targetColumn} onValueChange={setTargetColumn}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select column" />
              </SelectTrigger>
              <SelectContent>
                {nodeColumns.map((column) => (
                  <SelectItem key={column.key} value={column.key}>
                    {column.key} ({column.count})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </Field>
          <Field label="Filter rows">
            <Input
              value={filterText}
              placeholder="name contains Ada"
              onChange={(event) => setFilterText(event.target.value)}
            />
          </Field>
        </div>

        <div className="flex flex-col gap-3">
          <Label className="flex items-center gap-2 text-sm text-muted-foreground">
            <ActivityIcon className="size-4" />
            Max edges to render ({graph.edges.length}/{maxEdges})
          </Label>
          <Slider
            min={20}
            max={400}
            step={10}
            value={[maxEdges]}
            onValueChange={(value) => setMaxEdges(value[0] ?? 150)}
          />
        </div>

        <div className="flex flex-wrap items-center justify-between gap-4 rounded-2xl border bg-muted/10 p-4">
          <div>
            <p className="text-sm font-semibold">Edge labels</p>
            <p className="text-xs text-muted-foreground">
              Display column pairs directly on the connections.
            </p>
          </div>
          <Switch checked={showEdgeLabels} onCheckedChange={setShowEdgeLabels} />
        </div>

        {!hasDistinctColumns && (
          <p className="text-sm text-muted-foreground">
            Select two different columns to draw edges. We&apos;ll still scatter the nodes even if
            only one column is available.
          </p>
        )}

        <div className="flex items-center justify-between gap-3 text-sm text-muted-foreground">
          <span>
            Showing up to {maxEdges} relationships from {limitedRows.length} matching rows.
          </span>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setFilterText("")
              if (nodeColumns[0]) setSourceColumn(nodeColumns[0].key)
              if (nodeColumns[1]) {
                setTargetColumn(nodeColumns[1].key)
              } else if (nodeColumns[0]) {
                setTargetColumn(nodeColumns[0].key)
              }
              setMaxEdges(150)
            }}
          >
            Reset view
          </Button>
        </div>

        <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,3.5fr)_minmax(0,1fr)]">
          <div className="relative">
            <Button
              type="button"
              variant="secondary"
              size="sm"
              className="absolute right-4 top-4 z-20"
              disabled={isGraphExpanded}
              onClick={() => setIsGraphExpanded(true)}
            >
              <Maximize2Icon className="mr-1.5 size-4" />
              {isGraphExpanded ? "Expanded" : "Expand"}
            </Button>
            <GraphRenderer
              nodes={graph.nodes}
              edges={graph.edges}
              height={600}
              selectedNodeId={selectedNodeId}
              onNodeSelect={(node) => setSelectedNodeId(node?.id ?? null)}
              showEdgeLabels={showEdgeLabels}
            />
          </div>
          <NodeDetails node={selectedNode} />
        </div>
        </CardContent>
      </Card>

      {isGraphExpanded && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4">
          <div
            className="absolute inset-0"
            aria-hidden="true"
            onClick={() => setIsGraphExpanded(false)}
          />
          <div className="relative z-10 w-full max-w-[1400px]">
            <div className="absolute -right-3 -top-3 flex gap-2">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                className="shadow-lg"
                onClick={() => setIsGraphExpanded(false)}
              >
                <XIcon className="mr-1 size-4" />
                Close
              </Button>
            </div>
            <GraphRenderer
              nodes={graph.nodes}
              edges={graph.edges}
              height={780}
              selectedNodeId={selectedNodeId}
              onNodeSelect={(node) => setSelectedNodeId(node?.id ?? null)}
              showEdgeLabels={showEdgeLabels}
            />
          </div>
        </div>
      )}
    </>
  )
}

function NodeDetails({ node }: { node: GraphNodeDatum | null }) {
  if (!node) {
    return (
      <div className="border rounded-xl bg-muted/20 p-4 text-sm text-muted-foreground h-full">
        Click any node to inspect its properties.
      </div>
    )
  }

  const entries = Object.entries(node.props)
  
  // Categorize properties for better display
  const identityProps: [string, unknown][] = []
  const locationProps: [string, unknown][] = []
  const codeProps: [string, unknown][] = []
  const metadataProps: [string, unknown][] = []
  const otherProps: [string, unknown][] = []
  
  const identityKeys = ['name', 'id', 'kind', 'type', 'visibility', 'language']
  const locationKeys = ['filePath', 'path', 'file', 'startLine', 'endLine', 'start_line', 'end_line', 'line', 'column']
  const codeKeys = ['codeText', 'code', 'source', 'sourceCode', 'content', 'body', 'snippet']
  const metadataKeys = ['metadata', 'props', 'attributes', 'data']
  
  for (const [key, value] of entries) {
    const lowerKey = key.toLowerCase()
    if (identityKeys.some(k => lowerKey === k.toLowerCase())) {
      identityProps.push([key, value])
    } else if (locationKeys.some(k => lowerKey === k.toLowerCase())) {
      locationProps.push([key, value])
    } else if (codeKeys.some(k => lowerKey.includes(k.toLowerCase()))) {
      codeProps.push([key, value])
    } else if (metadataKeys.some(k => lowerKey === k.toLowerCase())) {
      metadataProps.push([key, value])
    } else {
      otherProps.push([key, value])
    }
  }

  // Extract key info for header
  const name = node.props.name as string | undefined
  const filePath = node.props.filePath as string | undefined
  const startLine = node.props.startLine as number | undefined
  const endLine = node.props.endLine as number | undefined
  const language = node.props.language as string | undefined

  return (
    <div className="border rounded-xl bg-muted/10 text-sm h-full overflow-auto max-h-[600px]">
      {/* Header section */}
      <div className="p-4 border-b bg-muted/20">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <p className="text-lg font-semibold leading-tight truncate" title={node.label}>
              {name || node.label}
            </p>
            <p className="text-muted-foreground text-xs">ID: {node.id}</p>
          </div>
          {language && (
            <Badge variant="secondary" className="text-[0.6rem] uppercase shrink-0">
              {language}
            </Badge>
          )}
        </div>
        
        {/* Labels */}
        {node.labels.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            {node.labels.map((label) => (
              <Badge key={label} variant="outline" className="text-xs">
                {label}
              </Badge>
            ))}
          </div>
        )}
        
        {/* File location */}
        {filePath && (
          <div className="mt-2 text-xs text-muted-foreground font-mono truncate" title={filePath}>
            {shortenPath(filePath)}
            {startLine && (
              <span className="text-foreground/70">
                :{startLine}{endLine && endLine !== startLine ? `-${endLine}` : ''}
              </span>
            )}
          </div>
        )}
      </div>
      
      {/* Properties sections */}
      <div className="p-4 space-y-4">
        {entries.length === 0 ? (
          <p className="text-muted-foreground">No properties returned for this node.</p>
        ) : (
          <>
            {/* Code section - prioritized */}
            {codeProps.length > 0 && (
              <PropertySection title="Code">
                {codeProps.map(([key, value]) => (
                  <PropertyItem key={key} propKey={key} value={value} />
                ))}
              </PropertySection>
            )}
            
            {/* Identity properties */}
            {identityProps.length > 0 && (
              <PropertySection title="Details">
                <div className="grid grid-cols-2 gap-x-3 gap-y-1.5">
                  {identityProps.map(([key, value]) => (
                    <PropertyItemCompact key={key} propKey={key} value={value} />
                  ))}
                </div>
              </PropertySection>
            )}
            
            {/* Location properties (if not shown in header) */}
            {locationProps.filter(([k]) => !['filePath', 'startLine', 'endLine'].includes(k)).length > 0 && (
              <PropertySection title="Location">
                <div className="grid grid-cols-2 gap-x-3 gap-y-1.5">
                  {locationProps
                    .filter(([k]) => !['filePath', 'startLine', 'endLine'].includes(k))
                    .map(([key, value]) => (
                      <PropertyItemCompact key={key} propKey={key} value={value} />
                    ))}
                </div>
              </PropertySection>
            )}
            
            {/* Metadata section */}
            {metadataProps.length > 0 && (
              <PropertySection title="Metadata">
                {metadataProps.map(([key, value]) => (
                  <PropertyItem key={key} propKey={key} value={value} />
                ))}
              </PropertySection>
            )}
            
            {/* Other properties */}
            {otherProps.length > 0 && (
              <PropertySection title="Other Properties">
                {otherProps.map(([key, value]) => (
                  <PropertyItem key={key} propKey={key} value={value} />
                ))}
              </PropertySection>
            )}
          </>
        )}
      </div>
    </div>
  )
}

function PropertySection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <p className="text-[0.65rem] uppercase tracking-wide text-muted-foreground font-medium border-b border-border/50 pb-1">
        {title}
      </p>
      {children}
    </div>
  )
}

function PropertyItem({ propKey, value }: { propKey: string; value: unknown }) {
  return (
    <div>
      <p className="text-[0.65rem] uppercase tracking-wide text-muted-foreground mb-1">{formatPropKey(propKey)}</p>
      <PropertyValue value={value} propertyKey={propKey} />
    </div>
  )
}

function PropertyItemCompact({ propKey, value }: { propKey: string; value: unknown }) {
  // For simple values, show inline
  if (value === null || value === undefined) {
    return (
      <div className="flex items-baseline gap-1.5">
        <span className="text-[0.65rem] text-muted-foreground">{formatPropKey(propKey)}:</span>
        <span className="text-xs italic text-muted-foreground">null</span>
      </div>
    )
  }
  
  if (typeof value === 'boolean' || typeof value === 'number' || (typeof value === 'string' && value.length < 50)) {
    return (
      <div className="flex items-baseline gap-1.5 min-w-0">
        <span className="text-[0.65rem] text-muted-foreground shrink-0">{formatPropKey(propKey)}:</span>
        <span className="text-xs font-mono truncate" title={String(value)}>
          {typeof value === 'boolean' ? (value ? 'true' : 'false') : String(value)}
        </span>
      </div>
    )
  }
  
  // For complex values, use full PropertyItem
  return <PropertyItem propKey={propKey} value={value} />
}

function formatPropKey(key: string): string {
  // Convert camelCase to Title Case with spaces
  return key
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, str => str.toUpperCase())
    .trim()
}

function shortenPath(path: string): string {
  // Show last 3 path segments
  const parts = path.split('/')
  if (parts.length <= 4) return path
  return '.../' + parts.slice(-3).join('/')
}

function Field({
  label,
  children,
}: {
  label: string
  children: React.ReactNode
}) {
  return (
    <div className="space-y-2">
      <Label className="text-sm text-muted-foreground">{label}</Label>
      {children}
    </div>
  )
}

function detectNodeColumns(rows: QueryRow[]): ColumnStat[] {
  const counts = new Map<string, number>()
  for (const row of rows) {
    for (const [key, value] of Object.entries(row)) {
      if (isGraphEntity(value)) {
        counts.set(key, (counts.get(key) ?? 0) + 1)
      }
    }
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([key, count]) => ({ key, count }))
}

function buildGraphData(
  rows: QueryRow[],
  options: {
    nodeColumns: string[]
    sourceColumn: string
    targetColumn: string
    maxEdges: number
  }
): { nodes: GraphNodeDatum[]; edges: GraphEdgeDatum[] } {
  const nodeMap = new Map<number, GraphNodeDatum>()
  const edges: GraphEdgeDatum[] = []

  for (const row of rows) {
    for (const column of options.nodeColumns) {
      const value = row[column]
      if (isGraphEntity(value)) {
        upsertNode(nodeMap, value, column)
      }
    }
  }

  let emitted = 0
  for (const row of rows) {
    if (emitted >= options.maxEdges) {
      break
    }
    const source = row[options.sourceColumn]
    const target = row[options.targetColumn]
    if (!isGraphEntity(source) || !isGraphEntity(target)) {
      continue
    }
    const sourceNode = upsertNode(nodeMap, source, options.sourceColumn)
    const targetNode = upsertNode(nodeMap, target, options.targetColumn)
    if (sourceNode.id === targetNode.id) {
      continue
    }
    edges.push({
      id: `${sourceNode.id}-${targetNode.id}-${emitted}`,
      source: sourceNode.id,
      target: targetNode.id,
      sourceColumn: options.sourceColumn,
      targetColumn: options.targetColumn,
    })
    emitted += 1
  }

  return {
    nodes: Array.from(nodeMap.values()),
    edges,
  }
}

function upsertNode(map: Map<number, GraphNodeDatum>, entity: GraphEntity, column: string) {
  const existing = map.get(entity._id)
  if (existing) {
    if (!existing.groups.includes(column)) {
      existing.groups = [...existing.groups, column]
    }
    return existing
  }
  const normalizedProps =
    entity.props && typeof entity.props === "object" && !Array.isArray(entity.props)
      ? (entity.props as Record<string, unknown>)
      : {}
  const node: GraphNodeDatum = {
    id: entity._id,
    label: deriveNodeLabel(entity),
    props: normalizedProps,
    labels: entity.labels ?? [],
    groups: [column],
    original: entity,
  }
  map.set(node.id, node)
  return node
}
