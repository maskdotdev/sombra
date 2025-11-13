import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react"
import {
  easeCubicInOut,
  easeCubicOut,
  drag,
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceX,
  forceY,
  select,
  zoom,
  zoomIdentity,
  zoomTransform,
  type D3DragEvent,
  type D3ZoomEvent,
  type Selection,
  type ZoomBehavior,
} from "d3"

import type { GraphEdgeDatum, GraphNodeDatum } from "./graph-types"

type ForceNode = GraphNodeDatum & {
  x?: number
  y?: number
  vx?: number
  vy?: number
  fx?: number | null
  fy?: number | null
}

type ForceLink = Omit<GraphEdgeDatum, "source" | "target"> & {
  source: number | ForceNode
  target: number | ForceNode
}

type HoveredEdgeInfo = {
  id: string
  sourceId: number
  targetId: number
}

type HighlightContext =
  | {
      focusNodeIds: Set<number>
      primaryNodeIds: Set<number>
      hoveredEdgeId: string | null
    }
  | null

const GROUP_COLORS = [
  "#60a5fa",
  "#34d399",
  "#f472b6",
  "#fbbf24",
  "#a78bfa",
  "#38bdf8",
  "#fb7185",
]

const EDGE_COLOR = "#8ea6d5"
const NODE_STROKE = "#0f172a"
const MIN_ZOOM = 0.08
const MAX_ZOOM = 4.5

export type GraphCanvasProps = {
  nodes: GraphNodeDatum[]
  edges: GraphEdgeDatum[]
  height?: number
  onNodeSelect?: (node: GraphNodeDatum | null) => void
  selectedNodeId?: number | null
  showEdgeLabels?: boolean
}

export function GraphCanvas({
  nodes,
  edges,
  height = 540,
  onNodeSelect,
  selectedNodeId,
  showEdgeLabels = false,
}: GraphCanvasProps) {
  const svgRef = useRef<SVGSVGElement | null>(null)
  const wrapperRef = useRef<HTMLDivElement | null>(null)
  const markerId = useId()
  const glowId = useId()
  const gradientId = useId()
  const [dimensions, setDimensions] = useState({ width: 720, height })
  const [zoomLevel, setZoomLevel] = useState(1)
  const nodeSelectionRef = useRef<Selection<SVGGElement, ForceNode, SVGGElement, unknown> | null>(null)
  const linkSelectionRef = useRef<Selection<SVGLineElement, ForceLink, SVGGElement, unknown> | null>(null)
  const edgeLabelGroupRef = useRef<Selection<SVGGElement, unknown, null, undefined> | null>(null)
  const edgeLabelSelectionRef = useRef<
    Selection<SVGTextElement, ForceLink, SVGGElement, unknown> | null
  >(null)
  const zoomBehaviorRef = useRef<ZoomBehavior<SVGSVGElement, unknown> | null>(null)
  const nodePositionsRef = useRef<Map<number, ForceNode>>(new Map())
  const selectedNodeIdRef = useRef<number | null>(null)
  const pendingFocusRef = useRef(false)
  const zoomFrameRef = useRef<number | null>(null)
  const showEdgeLabelsRef = useRef(showEdgeLabels)
  const [tooltip, setTooltip] = useState<{ node: ForceNode; x: number; y: number } | null>(null)
  const [hoveredNodeId, setHoveredNodeId] = useState<number | null>(null)
  const [hoveredEdge, setHoveredEdge] = useState<HoveredEdgeInfo | null>(null)
  const ids = useMemo(() => {
    const sanitize = (value: string) => value.replace(/:/g, "-")
    return {
      marker: sanitize(markerId),
      glow: sanitize(glowId),
      gradient: sanitize(gradientId),
    }
  }, [markerId, glowId, gradientId])
  useEffect(() => {
    selectedNodeIdRef.current = selectedNodeId ?? null
  }, [selectedNodeId])
  const updateTooltip = useCallback(
    (event: MouseEvent, node: ForceNode | null) => {
      if (!wrapperRef.current) {
        return
      }
      if (!node) {
        setTooltip(null)
        return
      }
      const rect = wrapperRef.current.getBoundingClientRect()
      setTooltip({
        node,
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      })
    },
    []
  )

  const adjacencyMap = useMemo(() => {
    const map = new Map<number, Set<number>>()
    for (const edge of edges) {
      const sourceId = edge.source
      const targetId = edge.target
      if (typeof sourceId !== "number" || typeof targetId !== "number" || sourceId === targetId) {
        continue
      }
      if (!map.has(sourceId)) {
        map.set(sourceId, new Set())
      }
      if (!map.has(targetId)) {
        map.set(targetId, new Set())
      }
      map.get(sourceId)!.add(targetId)
      map.get(targetId)!.add(sourceId)
    }
    return map
  }, [edges])
  const groupColors = useMemo(() => buildGroupColorMap(nodes), [nodes])
  const legendEntries = useMemo(
    () => Array.from(groupColors.entries()).sort((a, b) => a[0].localeCompare(b[0])),
    [groupColors]
  )
  const highlightContext = useMemo<HighlightContext>(() => {
    if (hoveredNodeId) {
      const focusNodes = new Set<number>([hoveredNodeId])
      const neighbors = adjacencyMap.get(hoveredNodeId)
      neighbors?.forEach((id) => focusNodes.add(id))
      return {
        focusNodeIds: focusNodes,
        primaryNodeIds: new Set<number>([hoveredNodeId]),
        hoveredEdgeId: null,
      }
    }
    if (hoveredEdge) {
      const primary = new Set<number>([hoveredEdge.sourceId, hoveredEdge.targetId])
      return {
        focusNodeIds: new Set(primary),
        primaryNodeIds: primary,
        hoveredEdgeId: hoveredEdge.id,
      }
    }
    if (selectedNodeId) {
      const focusNodes = new Set<number>([selectedNodeId])
      const neighbors = adjacencyMap.get(selectedNodeId)
      neighbors?.forEach((id) => focusNodes.add(id))
      return {
        focusNodeIds: focusNodes,
        primaryNodeIds: new Set<number>([selectedNodeId]),
        hoveredEdgeId: null,
      }
    }
    return null
  }, [hoveredNodeId, hoveredEdge, selectedNodeId, adjacencyMap])

  const focusNodes = useCallback(
    (idsToFocus: number[]) => {
      if (idsToFocus.length === 0) {
        return false
      }
      const svgElement = svgRef.current
      const zoomBehavior = zoomBehaviorRef.current
      if (!svgElement || !zoomBehavior) {
        return false
      }
      const nodesToInspect = idsToFocus
        .map((id) => nodePositionsRef.current.get(id))
        .filter((node): node is ForceNode => typeof node?.x === "number" && typeof node?.y === "number")
      if (nodesToInspect.length === 0) {
        return false
      }
      let minX = Infinity
      let minY = Infinity
      let maxX = -Infinity
      let maxY = -Infinity
      for (const node of nodesToInspect) {
        const x = node.x ?? 0
        const y = node.y ?? 0
        if (x < minX) minX = x
        if (x > maxX) maxX = x
        if (y < minY) minY = y
        if (y > maxY) maxY = y
      }
      if (!Number.isFinite(minX) || !Number.isFinite(minY)) {
        return false
      }
      const padding = 80
      minX -= padding
      minY -= padding
      maxX += padding
      maxY += padding
      const boundsWidth = Math.max(maxX - minX, 120)
      const boundsHeight = Math.max(maxY - minY, 120)
      const currentTransform = zoomTransform(svgElement)
      const viewLeft = (-currentTransform.x) / currentTransform.k
      const viewTop = (-currentTransform.y) / currentTransform.k
      const viewRight = viewLeft + dimensions.width / currentTransform.k
      const viewBottom = viewTop + dimensions.height / currentTransform.k
      const margin = 48
      const fitsHoriz = minX >= viewLeft + margin && maxX <= viewRight - margin
      const fitsVert = minY >= viewTop + margin && maxY <= viewBottom - margin
      if (fitsHoriz && fitsVert) {
        return true
      }
      const scaleX = dimensions.width / boundsWidth
      const scaleY = dimensions.height / boundsHeight
      const scale = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, 0.85 * Math.min(scaleX, scaleY)))
      const centerX = (minX + maxX) / 2
      const centerY = (minY + maxY) / 2
      const target = zoomIdentity
        .translate(dimensions.width / 2, dimensions.height / 2)
        .scale(scale)
        .translate(-centerX, -centerY)
      const svgSelection = select(svgElement)
      svgSelection.interrupt()
      svgSelection
        .transition()
        .duration(750)
        .ease(easeCubicInOut)
        .call(zoomBehavior.transform as any, target)
      return true
    },
    [dimensions]
  )

  const focusSelection = useCallback(
    (nodeId: number | null) => {
      if (!nodeId) {
        return true
      }
      const focusIds = new Set<number>([nodeId])
      const neighbors = adjacencyMap.get(nodeId)
      neighbors?.forEach((id) => focusIds.add(id))
      return focusNodes(Array.from(focusIds))
    },
    [adjacencyMap, focusNodes]
  )

  useEffect(() => {
    setDimensions((prev) => ({ ...prev, height }))
  }, [height])

  useEffect(() => {
    return () => {
      if (typeof window !== "undefined" && zoomFrameRef.current !== null) {
        window.cancelAnimationFrame(zoomFrameRef.current)
        zoomFrameRef.current = null
      }
    }
  }, [])

  useEffect(() => {
    if (!wrapperRef.current) {
      return
    }
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setDimensions((prev) => ({
          ...prev,
          width: entry.contentRect.width,
        }))
      }
    })
    observer.observe(wrapperRef.current)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    const svgEl = svgRef.current
    if (!svgEl) {
      return
    }

    const svg = select(svgEl)
    svg.selectAll("*").remove()

    if (nodes.length === 0) {
      nodeSelectionRef.current = null
      linkSelectionRef.current = null
      nodePositionsRef.current = new Map()
      edgeLabelGroupRef.current = null
      edgeLabelSelectionRef.current = null
      setTooltip(null)
      setZoomLevel((prev) => (prev === 1 ? prev : 1))
      return
    }

    const updateZoomLevel = (value: number) => {
      const next = Number(value.toFixed(2))
      if (typeof window === "undefined") {
        setZoomLevel(next)
        return
      }
      if (zoomFrameRef.current !== null) {
        window.cancelAnimationFrame(zoomFrameRef.current)
      }
      zoomFrameRef.current = window.requestAnimationFrame(() => {
        setZoomLevel(next)
        zoomFrameRef.current = null
      })
    }

    const simNodes: ForceNode[] = nodes.map((node) => ({ ...node }))
    const linkData: ForceLink[] = edges.map((edge) => ({ ...edge }))

    const defs = svg.append("defs")
    const gradient = defs
      .append("radialGradient")
      .attr("id", ids.gradient)
      .attr("cx", "50%")
      .attr("cy", "50%")
      .attr("r", "75%")
    gradient.append("stop").attr("offset", "0%").attr("stop-color", "#1f2937").attr("stop-opacity", 1)
    gradient.append("stop").attr("offset", "100%").attr("stop-color", "#020617").attr("stop-opacity", 1)

    const glowFilter = defs
      .append("filter")
      .attr("id", ids.glow)
      .attr("x", "-50%")
      .attr("y", "-50%")
      .attr("width", "200%")
      .attr("height", "200%")
    glowFilter
      .append("feDropShadow")
      .attr("dx", 0)
      .attr("dy", 3)
      .attr("stdDeviation", 6)
      .attr("flood-color", "#38bdf8")
      .attr("flood-opacity", 0.35)

    defs
      .append("marker")
      .attr("id", ids.marker)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 14)
      .attr("refY", 0)
      .attr("markerWidth", 8)
      .attr("markerHeight", 8)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", EDGE_COLOR)

    svg
      .append("rect")
      .attr("width", dimensions.width)
      .attr("height", dimensions.height)
      .attr("fill", `url(#${ids.gradient})`)

    const container = svg.append("g")

    const zoomBehavior = zoom<SVGSVGElement, unknown>()
      .scaleExtent([MIN_ZOOM, MAX_ZOOM])
      .on("zoom", (event: D3ZoomEvent<SVGSVGElement, unknown>) => {
        container.attr("transform", event.transform.toString())
        updateZoomLevel(event.transform.k)
      })

    svg.call(zoomBehavior as any)
    svg.call(zoomBehavior.transform as any, zoomIdentity)
    zoomBehaviorRef.current = zoomBehavior

    const linkGroup = container
      .append("g")
      .attr("stroke", EDGE_COLOR)
      .attr("stroke-opacity", 0.55)
      .attr("stroke-linecap", "round")
    const linkElements = linkGroup
      .selectAll<SVGLineElement, ForceLink>("line")
      .data(linkData, (d) => d.id)
      .join("line")
      .attr("stroke-width", 1.4)
      .attr("marker-end", edges.length ? `url(#${ids.marker})` : null)
      .style("cursor", "pointer")
      .on("mouseenter", (_event: MouseEvent, link: ForceLink) => {
        setHoveredNodeId(null)
        setHoveredEdge({
          id: link.id,
          sourceId: getLinkNodeId(link.source),
          targetId: getLinkNodeId(link.target),
        })
      })
      .on("mouseleave", (_event: MouseEvent, link: ForceLink) => {
        setHoveredEdge((current) => {
          if (current && current.id === link.id) {
            return null
          }
          return current
        })
      })

    linkSelectionRef.current = linkElements

    const labelGroup = container
      .append<SVGGElement>("g")
      .attr("class", "edge-labels")
      .attr("pointer-events", "none")
    edgeLabelGroupRef.current = labelGroup
    const labelElements = labelGroup
      .selectAll<SVGTextElement, ForceLink>("text")
      .data(linkData, (d) => d.id)
      .join("text")
      .attr("fill", "#fefefe")
      .attr("font-size", 10)
      .attr("font-weight", 500)
      .attr("text-anchor", "middle")
      .attr("dy", -4)
      .attr("opacity", 0.95)
      .attr("paint-order", "stroke")
      .attr("stroke", "rgba(2,6,23,0.85)")
      .attr("stroke-width", 2)
      .attr("stroke-linejoin", "round")
      .text((d) => formatEdgeLabel(d))
    edgeLabelSelectionRef.current = labelElements
    labelGroup.attr("display", showEdgeLabelsRef.current ? null : "none")

    const nodeGroup = container.append("g")
    const nodeElements = nodeGroup
      .selectAll<SVGGElement, ForceNode>("g")
      .data(simNodes, (d) => d.id)
      .join((enter) => {
        const nodeEnter = enter
          .append("g")
          .attr("role", "button")
          .attr("tabindex", 0)
          .attr("aria-label", (d) => d.label)
          .style("cursor", "pointer")
        nodeEnter
          .append("circle")
          .attr("data-circle-role", "halo")
          .attr("r", 26)
          .attr("fill", (d) => hexToRgba(groupColors.get(d.groups[0] ?? "") ?? "#38bdf8", 0.18))
          .attr("stroke", hexToRgba("#ffffff", 0.12))
          .attr("stroke-width", 1)
        nodeEnter
          .append("circle")
          .attr("data-circle-role", "core")
          .attr("r", 16)
          .attr("fill", (d) => groupColors.get(d.groups[0] ?? "") ?? "#38bdf8")
          .attr("stroke", NODE_STROKE)
          .attr("stroke-width", 1.4)
          .attr("filter", `url(#${ids.glow})`)
          .attr("opacity", 0.96)
        nodeEnter
          .append("text")
          .attr("fill", "#fdfdfd")
          .attr("text-anchor", "middle")
          .attr("dy", 4)
          .attr("font-size", 11)
          .attr("font-weight", 600)
          .attr("paint-order", "stroke")
          .attr("stroke", "rgba(2,6,23,0.85)")
          .attr("stroke-width", 1)
          .attr("stroke-linejoin", "round")
          .text((d) => truncateLabel(d.label))
        return nodeEnter
      })

    nodeElements
      .on("click", (_event: MouseEvent, node: ForceNode) => {
        if (selectedNodeIdRef.current === node.id) {
          onNodeSelect?.(null)
        } else {
          onNodeSelect?.(node)
        }
      })
      .on("mouseenter", (event: MouseEvent, node: ForceNode) => {
        setHoveredNodeId(node.id)
        setHoveredEdge(null)
        updateTooltip(event, node)
      })
      .on("mousemove", (event: MouseEvent, node: ForceNode) => {
        updateTooltip(event, node)
      })
      .on("mouseleave", (event: MouseEvent, node: ForceNode) => {
        setHoveredNodeId((current) => (current === node.id ? null : current))
        updateTooltip(event, null)
      })

    const dragBehavior = drag<SVGGElement, ForceNode>()
      .on("start", (event: D3DragEvent<SVGGElement, ForceNode, ForceNode>, node) => {
        if (!event.active) {
          simulation.alphaTarget(0.3).restart()
        }
        node.fx = node.x ?? 0
        node.fy = node.y ?? 0
      })
      .on("drag", (event: D3DragEvent<SVGGElement, ForceNode, ForceNode>, node) => {
        node.fx = event.x
        node.fy = event.y
      })
      .on("end", (event: D3DragEvent<SVGGElement, ForceNode, ForceNode>, node) => {
        if (!event.active) {
          simulation.alphaTarget(0)
        }
        node.fx = null
        node.fy = null
      })

    const simulation = forceSimulation(simNodes)
      .force(
        "link",
        forceLink<ForceNode, ForceLink>(linkData)
          .id((node: any) => node.id)
          .distance(90)
          .strength(0.6)
      )
      .force("charge", forceManyBody().strength(-320).theta(0.9))
      .force("center", forceCenter(dimensions.width / 2, dimensions.height / 2))
      .force("collision", forceCollide().radius(32).strength(0.7))
      .force("x", forceX(dimensions.width / 2).strength(0.045))
      .force("y", forceY(dimensions.height / 2).strength(0.045))
      .alpha(1)
      .restart()

    nodeElements.call(dragBehavior as any)

    simulation.on("tick", () => {
      for (const node of simNodes) {
        nodePositionsRef.current.set(node.id, node)
      }
      nodeElements.attr("transform", (node) => `translate(${node.x ?? 0},${node.y ?? 0})`)
      linkElements
        .attr("x1", (link: ForceLink) => getPosition(link.source).x)
        .attr("y1", (link: ForceLink) => getPosition(link.source).y)
        .attr("x2", (link: ForceLink) => getPosition(link.target).x)
        .attr("y2", (link: ForceLink) => getPosition(link.target).y)
      if (edgeLabelSelectionRef.current) {
        edgeLabelSelectionRef.current
          .attr("x", (link: ForceLink) => {
            const source = getPosition(link.source)
            const target = getPosition(link.target)
            return (source.x + target.x) / 2
          })
          .attr("y", (link: ForceLink) => {
            const source = getPosition(link.source)
            const target = getPosition(link.target)
            return (source.y + target.y) / 2
          })
      }
      if (pendingFocusRef.current && selectedNodeIdRef.current) {
        const fulfilled = focusSelection(selectedNodeIdRef.current)
        pendingFocusRef.current = !fulfilled
      }
    })

    nodeSelectionRef.current = nodeElements as Selection<SVGGElement, ForceNode, SVGGElement, unknown>
    pendingFocusRef.current = Boolean(selectedNodeIdRef.current)

    return () => {
      simulation.stop()
      svg.on(".zoom", null)
      if (typeof window !== "undefined" && zoomFrameRef.current !== null) {
        window.cancelAnimationFrame(zoomFrameRef.current)
        zoomFrameRef.current = null
      }
      edgeLabelGroupRef.current = null
      edgeLabelSelectionRef.current = null
    }
  }, [nodes, edges, dimensions, ids, onNodeSelect, focusSelection, groupColors])

  useEffect(() => {
    showEdgeLabelsRef.current = showEdgeLabels
    if (edgeLabelGroupRef.current) {
      edgeLabelGroupRef.current.attr("display", showEdgeLabels ? null : "none")
    }
  }, [showEdgeLabels])

  useEffect(() => {
    setHoveredNodeId(null)
    setHoveredEdge(null)
  }, [nodes, edges])

  useEffect(() => {
    if (!nodeSelectionRef.current) {
      return
    }
    const focusSet = highlightContext?.focusNodeIds ?? null
    const primaryNodes = highlightContext?.primaryNodeIds ?? new Set<number>()
    nodeSelectionRef.current
      .attr("data-selected", (node) => (node.id === selectedNodeId ? "true" : null))
      .attr("opacity", (node) => {
        if (!focusSet) {
          return 1
        }
        return focusSet.has(node.id) ? 1 : 0.15
      })
    const coreSelection = nodeSelectionRef.current.selectAll<SVGCircleElement, ForceNode>(
      'circle[data-circle-role="core"]'
    )
    coreSelection
      .attr("stroke", (node) => (primaryNodes.has(node.id) ? "#fbbf24" : NODE_STROKE))
      .attr("stroke-width", (node) => (primaryNodes.has(node.id) ? 2.4 : 1.4))
      .attr("r", (node) => (primaryNodes.has(node.id) ? 18 : 16))
    const haloSelection = nodeSelectionRef.current.selectAll<SVGCircleElement, ForceNode>(
      'circle[data-circle-role="halo"]'
    )
    haloSelection.attr("opacity", (node) => {
      if (!focusSet) {
        return 0.4
      }
      if (primaryNodes.has(node.id)) {
        return 0.9
      }
      return focusSet.has(node.id) ? 0.5 : 0.05
    })
    const textSelection = nodeSelectionRef.current.selectAll<SVGTextElement, ForceNode>("text")
    textSelection.attr("fill-opacity", (node) => {
      if (!focusSet) {
        return 1
      }
      return focusSet.has(node.id) ? 1 : 0.35
    })
    if (linkSelectionRef.current) {
      linkSelectionRef.current
        .attr("stroke-opacity", (link) => {
          if (!focusSet) {
            return 0.55
          }
          if (highlightContext?.hoveredEdgeId && link.id === highlightContext.hoveredEdgeId) {
            return 1
          }
          return doesLinkTouchNodes(link, primaryNodes) ? 0.95 : 0.08
        })
        .attr("stroke-width", (link) => {
          if (!focusSet) {
            return 1.4
          }
          if (highlightContext?.hoveredEdgeId && link.id === highlightContext.hoveredEdgeId) {
            return 2.6
          }
          return doesLinkTouchNodes(link, primaryNodes) ? 2.2 : 0.8
        })
    }
    if (edgeLabelSelectionRef.current) {
      edgeLabelSelectionRef.current.attr("opacity", (link) => {
        if (!focusSet) {
          return 0.8
        }
        if (highlightContext?.hoveredEdgeId && link.id === highlightContext.hoveredEdgeId) {
          return 1
        }
        return doesLinkTouchNodes(link, primaryNodes) ? 1 : 0.1
      })
    }
  }, [selectedNodeId, nodes, highlightContext])

  useEffect(() => {
    if (!selectedNodeId) {
      pendingFocusRef.current = false
      return
    }
    const focused = focusSelection(selectedNodeId)
    pendingFocusRef.current = !focused
  }, [selectedNodeId, focusSelection])

  const handleZoomControl = (action: "in" | "out" | "reset") => {
    const svgElement = svgRef.current
    const zoomBehavior = zoomBehaviorRef.current
    if (!svgElement || !zoomBehavior) {
      return
    }
    const selection = select(svgElement)
    if (action === "reset") {
      selection.transition().duration(250).call(zoomBehavior.transform as any, zoomIdentity)
      return
    }
    const factor = action === "in" ? 1.25 : 0.8
    selection.transition().duration(200).call(zoomBehavior.scaleBy as any, factor)
  }

  const tooltipStyle = tooltip
    ? {
        left: Math.min(
          Math.max(tooltip.x + 16, 8),
          Math.max(dimensions.width - 220, 8)
        ),
        top: Math.min(
          Math.max(tooltip.y + 16, 8),
          Math.max(dimensions.height - 140, 8)
        ),
      }
    : null

  return (
    <div
      ref={wrapperRef}
      className="relative h-full min-h-[420px] overflow-hidden rounded-2xl border border-slate-800 bg-gradient-to-b from-slate-950 via-slate-900 to-slate-950 p-4 shadow-2xl shadow-black/40"
    >
      <svg
        ref={svgRef}
        width={dimensions.width}
        height={dimensions.height}
        role="img"
        aria-label="Graph visualization"
        className="size-full"
      />
      {nodes.length === 0 ? (
        <div className="text-muted-foreground absolute inset-0 flex items-center justify-center text-sm">
          Select at least one node column to render the graph.
        </div>
      ) : (
        <>
          <div className="pointer-events-none absolute left-4 top-4 hidden max-w-[220px] text-xs text-white/80 lg:block">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-white">Graph tips</p>
            <p>Drag nodes to rearrange. Scroll, pinch, or use the buttons to explore.</p>
          </div>
          {legendEntries.length > 0 && (
            <div className="pointer-events-none absolute right-4 top-4 max-w-[240px] rounded-2xl border border-white/10 bg-slate-950/70 p-3 text-[11px] text-white/80 shadow-lg backdrop-blur">
              <p className="text-[10px] font-semibold uppercase tracking-wide text-white">Legend</p>
              <div className="mt-2 space-y-1.5">
                {legendEntries.map(([group, color]) => (
                  <div key={group} className="flex items-center gap-2">
                    <span
                      className="h-2 w-2 rounded-full border border-white/40"
                      style={{ backgroundColor: color }}
                    />
                    <span className="truncate text-xs">{group}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          <div className="absolute bottom-4 right-4 flex flex-col items-end gap-2 text-white">
            <span className="rounded-full bg-white/10 px-3 py-1 text-xs font-medium tracking-wide">
              Zoom {zoomLevel.toFixed(2)}x
            </span>
            <div className="flex overflow-hidden rounded-full border border-white/15 bg-black/40 text-base shadow-lg backdrop-blur">
              <button
                type="button"
                className="h-10 w-10 border-r border-white/10 font-semibold leading-none text-white transition hover:bg-white/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
                aria-label="Zoom out"
                onClick={() => handleZoomControl("out")}
              >
                -
              </button>
              <button
                type="button"
                className="h-10 w-10 font-semibold leading-none text-white transition hover:bg-white/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
                aria-label="Reset zoom"
                onClick={() => handleZoomControl("reset")}
              >
                1x
              </button>
              <button
                type="button"
                className="h-10 w-10 border-l border-white/10 font-semibold leading-none text-white transition hover:bg-white/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
                aria-label="Zoom in"
                onClick={() => handleZoomControl("in")}
              >
                +
              </button>
            </div>
          </div>
        </>
      )}
      {tooltip && tooltipStyle && (
        <div
          className="pointer-events-none absolute z-20 w-52 rounded-xl border border-white/10 bg-slate-900/90 p-3 text-xs text-white shadow-xl backdrop-blur"
          style={tooltipStyle}
        >
          <p className="text-[0.65rem] uppercase tracking-wide text-slate-300">Node</p>
          <p className="text-sm font-semibold">{tooltip.node.label}</p>
          <p className="text-[0.65rem] text-slate-400">id {tooltip.node.id}</p>
          {tooltip.node.groups.length > 0 && (
            <p className="mt-1 text-[0.65rem] text-slate-300">
              Groups: {tooltip.node.groups.slice(0, 3).join(", ")}
            </p>
          )}
          <div className="mt-2 space-y-1">
            {Object.entries(tooltip.node.props)
              .slice(0, 2)
              .map(([key, value]) => (
                <div key={key}>
                  <p className="text-[0.6rem] uppercase tracking-wide text-slate-400">{key}</p>
                  <p className="truncate font-mono text-[0.65rem]">
                    {typeof value === "string" ||
                    typeof value === "number" ||
                    typeof value === "boolean"
                      ? String(value)
                      : JSON.stringify(value)}
                  </p>
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  )
}

function hexToRgba(hex: string, alpha: number) {
  const sanitized = hex.replace("#", "")
  const normalized =
    sanitized.length === 3
      ? sanitized
          .split("")
          .map((char) => `${char}${char}`)
          .join("")
      : sanitized.slice(0, 6).padEnd(6, "0")
  const value = Number.parseInt(normalized, 16)
  if (Number.isNaN(value)) {
    return `rgba(56, 189, 248, ${alpha})`
  }
  const r = (value >> 16) & 255
  const g = (value >> 8) & 255
  const b = value & 255
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

function formatEdgeLabel(edge: ForceLink) {
  if (edge.sourceColumn === edge.targetColumn) {
    return edge.sourceColumn
  }
  return `${edge.sourceColumn} → ${edge.targetColumn}`
}

function truncateLabel(label: string, max = 10) {
  if (label.length <= max) {
    return label
  }
  return `${label.slice(0, max - 1)}…`
}

function buildGroupColorMap(nodes: GraphNodeDatum[]) {
  const map = new Map<string, string>()
  const groups = new Set<string>()
  for (const node of nodes) {
    for (const group of node.groups) {
      groups.add(group)
    }
  }
  let index = 0
  for (const group of groups) {
    map.set(group, GROUP_COLORS[index % GROUP_COLORS.length])
    index += 1
  }
  return map
}

function getPosition(value: ForceLink["source"]) {
  if (typeof value === "object" && value !== null) {
    return { x: value.x ?? 0, y: value.y ?? 0 }
  }
  return { x: 0, y: 0 }
}

function getLinkNodeId(value: ForceLink["source"]): number {
  if (typeof value === "object" && value !== null) {
    return value.id
  }
  return value
}

function doesLinkTouchNodes(link: ForceLink, nodeIds: Set<number>) {
  if (nodeIds.size === 0) {
    return false
  }
  const sourceId = getLinkNodeId(link.source)
  const targetId = getLinkNodeId(link.target)
  return nodeIds.has(sourceId) || nodeIds.has(targetId)
}
