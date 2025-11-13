import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
    GraphCanvas as ReagraphGraphCanvas,
    type GraphCanvasRef,
    type GraphEdge as ReagraphEdge,
    type GraphNode as ReagraphNode,
    type InternalGraphEdge,
    type InternalGraphNode,
    type LabelVisibilityType,
    type Theme,
    type CameraMode,
    type NodeRenderer,
    Label,
    Sphere,
} from "reagraph"
import {
    CrosshairIcon,
    LocateFixedIcon,
    LockIcon,
    TagIcon,
    Maximize2Icon,
    RotateCcwIcon,
    UnlockIcon,
    ZoomInIcon,
    ZoomOutIcon,
} from "lucide-react"

import type { GraphCanvasProps } from "./graph-canvas"
import type { GraphEdgeDatum, GraphNodeDatum } from "./graph-types"

const GROUP_COLORS = ["#60a5fa", "#34d399", "#f472b6", "#fbbf24", "#a78bfa", "#38bdf8", "#fb7185"]
const MAX_NODE_LABEL_LENGTH = 28
const MAX_SUB_LABEL_LENGTH = 24

const REAGRAPH_THEME: Theme = {
    canvas: {
        background: "#020617",
        fog: null,
    },
    node: {
        fill: "#60a5fa",
        activeFill: "#60a5fa",
        opacity: 0.9,
        selectedOpacity: 1,
        inactiveOpacity: 0.1,
        label: {
            color: "#e2e8f0",
            activeColor: "#ffffff",
            backgroundColor: "#000000",
            backgroundOpacity: 0.6,
            padding: 3,
            radius: 2,
        },
        subLabel: {
            color: "#94a3b8",
            activeColor: "#f1f5f9",
        },
    },
    ring: {
        fill: "#0f172a",
        activeFill: "#1e293b",
    },
    edge: {
        fill: "#8ea6d5",
        activeFill: "#8ea6d5",
        opacity: 0.8,
        selectedOpacity: 1,
        inactiveOpacity: 0.1,
        label: {
            color: "#cbd5f5",
            activeColor: "#ffffff",
            fontSize: 8,
        },
    },
    arrow: {
        fill: "#8ea6d5",
        activeFill: "#8ea6d5",
    },
    lasso: {
        background: "rgba(59,130,246,0.25)",
        border: "#3b82f6",
    },
}

const CAMERA_MODES: { id: CameraMode; label: string }[] = [
    { id: "pan", label: "Pan" },
    { id: "orbit", label: "Orbit" },
    { id: "rotate", label: "Rotate" },
]

export function ReagraphCanvas({
    nodes,
    edges,
    height = 540,
    onNodeSelect,
    selectedNodeId,
    showEdgeLabels = false,
}: GraphCanvasProps) {
    const canvasRef = useRef<GraphCanvasRef | null>(null)
    const [cameraMode, setCameraMode] = useState<CameraMode>("pan")
    const [cameraFrozen, setCameraFrozen] = useState(false)
    const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null)
    const [hoveredEdgeId, setHoveredEdgeId] = useState<string | null>(null)
    const [edgeLabelsEnabled, setEdgeLabelsEnabled] = useState(showEdgeLabels)
    const groupColors = useMemo(() => buildGroupColorMap(nodes), [nodes])
    useEffect(() => {
        setEdgeLabelsEnabled(showEdgeLabels)
    }, [showEdgeLabels])

    const reagraphNodes = useMemo<ReagraphNode[]>(() => {
        return nodes.map((node) => {
            const primaryGroup = node.groups[0]
            const fallbackColor = primaryGroup ? groupColors.get(primaryGroup) : null
            const size = Math.min(32, 16 + node.groups.length * 2)
            const formattedLabel = formatNodeLabel(node.label)
            const formattedSubLabel = formatNodeSubLabel(node.labels, node.groups)
            return {
                id: String(node.id),
                label: formattedLabel,
                subLabel: formattedSubLabel ?? undefined,
                data: node,
                fill: fallbackColor ?? "#60a5fa",
                labelVisible: false,
                size,
            }
        })
    }, [nodes, groupColors])

    const { edgeEndpointMap, nodeEdgeMap } = useMemo(() => {
        const edgeEndpoint = new Map<string, { source: string; target: string }>()
        const nodeEdges = new Map<string, Set<string>>()
        for (const edge of edges) {
            const edgeId = edge.id
            const sourceId = String(edge.source)
            const targetId = String(edge.target)
            edgeEndpoint.set(edgeId, { source: sourceId, target: targetId })
            if (!nodeEdges.has(sourceId)) {
                nodeEdges.set(sourceId, new Set())
            }
            if (!nodeEdges.has(targetId)) {
                nodeEdges.set(targetId, new Set())
            }
            nodeEdges.get(sourceId)!.add(edgeId)
            nodeEdges.get(targetId)!.add(edgeId)
        }
        return {
            edgeEndpointMap: edgeEndpoint,
            nodeEdgeMap: nodeEdges,
        }
    }, [edges])

    const selectedIds = useMemo(() => {
        if (!selectedNodeId) {
            return []
        }
        return [String(selectedNodeId)]
    }, [selectedNodeId])
    const selectedNodeKey = selectedNodeId != null ? String(selectedNodeId) : null

    const gatherNodeNeighborhood = useCallback(
        (nodeId: string) => {
            const ids = new Set<string>([nodeId])
            const connectedEdges = nodeEdgeMap.get(nodeId)
            connectedEdges?.forEach((edgeId) => {
                const endpoints = edgeEndpointMap.get(edgeId)
                if (endpoints) {
                    ids.add(endpoints.source)
                    ids.add(endpoints.target)
                }
            })
            return ids
        },
        [nodeEdgeMap, edgeEndpointMap],
    )

    const focusNodes = useCallback((nodeIds: Iterable<string>) => {
        const unique = Array.from(new Set(nodeIds))
        if (unique.length === 0) {
            return
        }
        canvasRef.current?.fitNodesInView(unique, { animated: true })
    }, [])

    const focusNode = useCallback(
        (nodeId: string | null | undefined) => {
            if (!nodeId) {
                return
            }
            focusNodes(gatherNodeNeighborhood(nodeId))
        },
        [focusNodes, gatherNodeNeighborhood],
    )

    const focusEdge = useCallback(
        (edge: InternalGraphEdge | null | undefined) => {
            if (!edge) {
                return
            }
            const sourceNeighborhood = gatherNodeNeighborhood(String(edge.source))
            const targetNeighborhood = gatherNodeNeighborhood(String(edge.target))
            const combined = new Set<string>(sourceNeighborhood)
            targetNeighborhood.forEach((id) => combined.add(id))
            focusNodes(combined)
        },
        [focusNodes, gatherNodeNeighborhood],
    )

    const handleNodeClick = useCallback(
        (node?: InternalGraphNode | null) => {
            if (!node) {
                onNodeSelect?.(null)
                return
            }
            const original = (node.data as GraphNodeDatum | undefined) ?? null
            onNodeSelect?.(original)
            focusNode(node.id)
        },
        [focusNode, onNodeSelect],
    )

    const handleNodePointerOver = useCallback((node: InternalGraphNode) => {
        setHoveredNodeId(node.id)
    }, [])

    const handleNodePointerOut = useCallback((node: InternalGraphNode) => {
        setHoveredNodeId((current) => (current === node.id ? null : current))
    }, [])

    const handleEdgeClick = useCallback(
        (edge: InternalGraphEdge) => {
            focusEdge(edge)
        },
        [focusEdge],
    )

    const handleEdgePointerOver = useCallback((edge: InternalGraphEdge) => {
        setHoveredEdgeId(edge.id)
    }, [])

    const handleEdgePointerOut = useCallback((edge: InternalGraphEdge) => {
        setHoveredEdgeId((current) => (current === edge.id ? null : current))
    }, [])

    const handleCanvasClick = useCallback(() => {
        onNodeSelect?.(null)
        setHoveredNodeId(null)
        setHoveredEdgeId(null)
    }, [onNodeSelect])

    const labelType = useMemo<LabelVisibilityType>(() => {
        return edgeLabelsEnabled ? "edges" : "none"
    }, [edgeLabelsEnabled])
    const layoutType = cameraMode === "pan" ? "forceDirected2d" : "forceDirected3d"

    const activeNodeIds = useMemo(() => {
        const ids = new Set<string>(selectedIds)
        const includeNode = (nodeId: string | null) => {
            if (!nodeId) {
                return
            }
            ids.add(nodeId)
            const connectedEdges = nodeEdgeMap.get(nodeId)
            connectedEdges?.forEach((edgeId) => {
                const endpoints = edgeEndpointMap.get(edgeId)
                if (endpoints) {
                    ids.add(endpoints.source)
                    ids.add(endpoints.target)
                }
            })
        }
        includeNode(hoveredNodeId)
        includeNode(selectedNodeKey)
        if (hoveredEdgeId) {
            const endpoints = edgeEndpointMap.get(hoveredEdgeId)
            if (endpoints) {
                ids.add(endpoints.source)
                ids.add(endpoints.target)
            }
        }
        return Array.from(ids)
    }, [selectedIds, hoveredNodeId, selectedNodeKey, hoveredEdgeId, nodeEdgeMap, edgeEndpointMap])

    const activeEdgeIds = useMemo(() => {
        const ids = new Set<string>()
        if (hoveredEdgeId) {
            ids.add(hoveredEdgeId)
        }
        const includeNodeEdges = (nodeId: string | null) => {
            if (!nodeId) {
                return
            }
            nodeEdgeMap.get(nodeId)?.forEach((edgeId) => ids.add(edgeId))
        }
        includeNodeEdges(hoveredNodeId)
        includeNodeEdges(selectedNodeKey)
        return Array.from(ids)
    }, [hoveredEdgeId, hoveredNodeId, selectedNodeKey, nodeEdgeMap])

    const reagraphEdges = useMemo<ReagraphEdge[]>(() => {
        const activeSet = new Set(activeEdgeIds)
        return edges.map((edge) => {
            const isActive = activeSet.has(edge.id)
            return {
                id: edge.id,
                source: String(edge.source),
                target: String(edge.target),
                label: edgeLabelsEnabled && isActive ? formatEdgeLabel(edge) : undefined,
                data: edge,
                arrowPlacement: "end",
            }
        })
    }, [edges, edgeLabelsEnabled, activeEdgeIds])

    const activeGraphIds = useMemo(() => {
        const combined = new Set<string>([...activeNodeIds, ...activeEdgeIds])
        return Array.from(combined)
    }, [activeNodeIds, activeEdgeIds])
    const highlightSelections = activeNodeIds.length > 0 ? activeNodeIds : selectedIds
    const hasGraphFocus = activeGraphIds.length > 0

    const renderNode = useMemo<NodeRenderer>(() => {
        return ({ color, size, opacity, animated, id, selected, node, active }) => {
            const text = node.label ?? ""
            const fontSize = Math.min(10, Math.max(4, size * 0.4))
            const isEmphasized = active || selected
            const labelOpacity = hasGraphFocus ? (isEmphasized ? 1 : 0.25) : 1
            const strokeWidth = isEmphasized ? 2.25 : 1.25
            const labelYOffset = -(size + fontSize * 0.6)
            return (
                <group>
                    <Sphere
                        color={color}
                        id={id}
                        size={size}
                        opacity={opacity}
                        animated={animated}
                        selected={selected}
                        node={node}
                        active={active}
                    />
                    {text && (
                        <group position={[0, labelYOffset, 2]}>
                            <Label
                                text={text}
                                fontSize={fontSize}
                                backgroundColor="transparent"
                                backgroundOpacity={0}
                                padding={0}
                                radius={0}
                                strokeColor={color}
                                strokeWidth={strokeWidth}
                                color={color}
                                opacity={labelOpacity}
                                ellipsis={MAX_NODE_LABEL_LENGTH}
                            />
                        </group>
                    )}
                </group>
            )
        }
    }, [hasGraphFocus])

    const centerAllNodes = useCallback(() => {
        canvasRef.current?.centerGraph(undefined, { animated: true })
    }, [])

    const centerSelectedNode = useCallback(() => {
        if (selectedNodeId == null) {
            return
        }
        canvasRef.current?.centerGraph([String(selectedNodeId)], { animated: true })
    }, [selectedNodeId])

    const fitGraphInView = useCallback(() => {
        canvasRef.current?.fitNodesInView(undefined, { animated: true })
    }, [])

    const resetCamera = useCallback(() => {
        canvasRef.current?.resetControls(true)
    }, [])

    const zoomIn = useCallback(() => {
        canvasRef.current?.zoomIn()
    }, [])

    const zoomOut = useCallback(() => {
        canvasRef.current?.zoomOut()
    }, [])

    const toggleCameraFreeze = useCallback(() => {
        if (!canvasRef.current) {
            return
        }
        if (cameraFrozen) {
            canvasRef.current.unFreeze()
            setCameraFrozen(false)
            return
        }
        canvasRef.current.freeze()
        setCameraFrozen(true)
    }, [cameraFrozen])

    const hasSelection = selectedNodeId != null

    useEffect(() => {
        const canvas = canvasRef.current
        if (!canvas) {
            return
        }
        if (cameraMode === "pan") {
            canvas.centerGraph(undefined, { animated: true })
            return
        }
        canvas.fitNodesInView(undefined, { animated: true })
    }, [cameraMode])

    return (
        <div className="relative h-full w-full rounded-2xl border bg-background" style={{ height }}>
            <ReagraphGraphCanvas
                ref={canvasRef}
                nodes={reagraphNodes}
                edges={reagraphEdges}
                selections={highlightSelections}
                actives={activeGraphIds}
                animated
                layoutType={layoutType}
                aggregateEdges
                cameraMode={cameraMode}
                labelType={labelType}
                edgeLabelPosition={edgeLabelsEnabled ? "above" : "natural"}
                edgeArrowPosition="end"
                theme={REAGRAPH_THEME}
                onNodeClick={handleNodeClick}
                onCanvasClick={handleCanvasClick}
                onNodePointerOver={handleNodePointerOver}
                onNodePointerOut={handleNodePointerOut}
                onEdgeClick={handleEdgeClick}
                onEdgePointerOver={handleEdgePointerOver}
                onEdgePointerOut={handleEdgePointerOut}
                renderNode={renderNode}
            />
            <CameraToolbar
                cameraMode={cameraMode}
                onModeChange={setCameraMode}
                onReset={resetCamera}
                onFit={fitGraphInView}
                onCenterAll={centerAllNodes}
                onCenterSelection={centerSelectedNode}
                hasSelection={hasSelection}
                onZoomIn={zoomIn}
                onZoomOut={zoomOut}
                onToggleFreeze={toggleCameraFreeze}
                isFrozen={cameraFrozen}
                edgeLabelsEnabled={edgeLabelsEnabled}
                onToggleEdgeLabels={() => setEdgeLabelsEnabled((prev) => !prev)}
            />
        </div>
    )
}

function buildGroupColorMap(nodes: GraphNodeDatum[]) {
    const map = new Map<string, string>()
    const seen = new Set<string>()
    for (const node of nodes) {
        for (const group of node.groups) {
            if (!seen.has(group)) {
                const color = GROUP_COLORS[seen.size % GROUP_COLORS.length]
                map.set(group, color)
                seen.add(group)
            }
        }
    }
    return map
}

function formatEdgeLabel(edge: GraphEdgeDatum) {
    if (edge.sourceColumn === edge.targetColumn) {
        return edge.sourceColumn
    }
    return `${edge.sourceColumn} → ${edge.targetColumn}`
}

function formatNodeLabel(value: string) {
    const trimmed = value?.trim() ?? ""
    if (!trimmed) {
        return "Untitled node"
    }
    if (trimmed.length <= MAX_NODE_LABEL_LENGTH) {
        return trimmed
    }
    return `${trimmed.slice(0, MAX_NODE_LABEL_LENGTH - 1)}…`
}

function formatNodeSubLabel(labels: string[], groups: string[]) {
    const candidate = labels[0] ?? groups[0] ?? ""
    if (!candidate) {
        return null
    }
    if (candidate.length <= MAX_SUB_LABEL_LENGTH) {
        return candidate
    }
    return `${candidate.slice(0, MAX_SUB_LABEL_LENGTH - 1)}…`
}

type CameraToolbarProps = {
    cameraMode: CameraMode
    onModeChange: (mode: CameraMode) => void
    onReset: () => void
    onFit: () => void
    onCenterAll: () => void
    onCenterSelection: () => void
    hasSelection: boolean
    onZoomIn: () => void
    onZoomOut: () => void
    onToggleFreeze: () => void
    isFrozen: boolean
    edgeLabelsEnabled: boolean
    onToggleEdgeLabels: () => void
}

function CameraToolbar({
    cameraMode,
    onModeChange,
    onReset,
    onFit,
    onCenterAll,
    onCenterSelection,
    hasSelection,
    onZoomIn,
    onZoomOut,
    onToggleFreeze,
    isFrozen,
    edgeLabelsEnabled,
    onToggleEdgeLabels,
}: CameraToolbarProps) {
    return (
        <div className="pointer-events-none absolute inset-0 flex flex-col justify-between p-4">
            <div className="pointer-events-auto ml-auto flex flex-wrap gap-2">
                <IconButton label="Reset camera" onClick={onReset}>
                    <RotateCcwIcon className="size-4" />
                </IconButton>
                <IconButton label="Center graph" onClick={onCenterAll}>
                    <CrosshairIcon className="size-4" />
                </IconButton>
                <IconButton label="Focus selection" onClick={onCenterSelection} disabled={!hasSelection}>
                    <LocateFixedIcon className="size-4" />
                </IconButton>
                <IconButton label="Fit graph to view" onClick={onFit}>
                    <Maximize2Icon className="size-4" />
                </IconButton>
                <IconButton label="Zoom in" onClick={onZoomIn}>
                    <ZoomInIcon className="size-4" />
                </IconButton>
                <IconButton label="Zoom out" onClick={onZoomOut}>
                    <ZoomOutIcon className="size-4" />
                </IconButton>
                <IconButton label={isFrozen ? "Unlock camera" : "Lock camera"} onClick={onToggleFreeze}>
                    {isFrozen ? <UnlockIcon className="size-4" /> : <LockIcon className="size-4" />}
                </IconButton>
                <IconButton
                    label={edgeLabelsEnabled ? "Hide edge labels" : "Show edge labels"}
                    onClick={onToggleEdgeLabels}
                >
                    <TagIcon className={`size-4 ${edgeLabelsEnabled ? "text-primary" : ""}`} />
                </IconButton>
            </div>
            <div className="pointer-events-auto flex flex-wrap gap-1 rounded-full border bg-background/80 p-1 text-xs shadow-sm">
                {CAMERA_MODES.map((mode) => (
                    <button
                        key={mode.id}
                        type="button"
                        onClick={() => onModeChange(mode.id)}
                        className={`rounded-full px-3 py-1 transition ${
                            cameraMode === mode.id
                                ? "bg-primary text-primary-foreground shadow-sm"
                                : "text-muted-foreground hover:text-foreground"
                        }`}
                    >
                        {mode.label}
                    </button>
                ))}
            </div>
        </div>
    )
}

type IconButtonProps = {
    onClick: () => void
    label: string
    disabled?: boolean
    children: ReactNode
}

function IconButton({ onClick, label, disabled, children }: IconButtonProps) {
    return (
        <button
            type="button"
            onClick={onClick}
            disabled={disabled}
            className={`rounded-full border bg-background/90 p-2 text-muted-foreground shadow-sm transition hover:text-foreground ${
                disabled
                    ? "cursor-not-allowed opacity-50"
                    : "hover:border-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/50"
            }`}
            aria-label={label}
            title={label}
        >
            {children}
        </button>
    )
}
