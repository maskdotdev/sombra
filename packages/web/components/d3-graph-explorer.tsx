'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import * as d3 from 'd3'
import { ZoomIn, ZoomOut, Maximize2, Network, Sun, Moon, Tag, ChevronDown, ChevronUp } from 'lucide-react'
import { Collapsible, CollapsibleTrigger, CollapsibleContent } from '@/components/ui/collapsible'
import { DatabaseSelector } from './database-selector'

interface NodeData extends d3.SimulationNodeDatum {
  id: string
  name: string
  type: string
  val: number
}

interface LinkData extends d3.SimulationLinkDatum<NodeData> {
  id: string
  source: string | NodeData
  target: string | NodeData
  label: string
}

interface GraphData {
  nodes: NodeData[]
  links: LinkData[]
}

export default function D3GraphExplorer() {
  const [selectedNode, setSelectedNode] = useState<NodeData | null>(null)
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null)
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [nodeMeta, setNodeMeta] = useState<Record<string, { label: string; labels: string[]; properties: Record<string, string | number | boolean>; nodeId: number }>>({})
  const [edgeMeta, setEdgeMeta] = useState<Record<string, { edgeId: number; edgeType: string; properties: Record<string, string | number | boolean> }>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [themeKey, setThemeKey] = useState<string>(typeof document !== 'undefined' && document.documentElement.classList.contains('dark') ? 'dark' : 'light')
  const [chargeStrength, setChargeStrength] = useState<number>(-300)
  const [linkDistance, setLinkDistance] = useState<number>(100)
  const svgRef = useRef<SVGSVGElement>(null)
  const simulationRef = useRef<d3.Simulation<NodeData, LinkData> | null>(null)
  const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null)
  const zoomTransformRef = useRef<d3.ZoomTransform | null>(null)
  const nodeSelRef = useRef<d3.Selection<SVGCircleElement, NodeData, SVGGElement, unknown> | null>(null)
  const linkSelRef = useRef<d3.Selection<SVGLineElement, LinkData, SVGGElement, unknown> | null>(null)
  const labelSelRef = useRef<d3.Selection<SVGTextElement, NodeData, SVGGElement, unknown> | null>(null)
  const edgeLabelSelRef = useRef<d3.Selection<SVGTextElement, LinkData, SVGGElement, unknown> | null>(null)
  const adjacencyRef = useRef<Map<string, Set<string>>>(new Map())
  const selectedNodeRef = useRef<NodeData | null>(null)
  const selectedEdgeIdRef = useRef<string | null>(null)
  const [showEdgeLabels, setShowEdgeLabels] = useState(false)

  // Observe theme changes (when 'dark' class toggles on html)
  useEffect(() => {
    if (typeof window === 'undefined') return
    // bootstrap from saved theme
    const saved = localStorage.getItem('theme')
    if (saved === 'dark' || saved === 'light') {
      document.documentElement.classList.toggle('dark', saved === 'dark')
      setThemeKey(saved)
    }
    const observer = new MutationObserver(() => {
      const isDark = document.documentElement.classList.contains('dark')
      setThemeKey(isDark ? 'dark' : 'light')
    })
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    return () => observer.disconnect()
  }, [])

  const readCssVar = (name: string, fallback: string) => {
    if (typeof window === 'undefined') return fallback
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
    return v || fallback
  }

  const getPaletteColor = (label: string) => {
    const palette = [
      readCssVar('--chart-1', '#3b82f6'),
      readCssVar('--chart-2', '#10b981'),
      readCssVar('--chart-3', '#f59e0b'),
      readCssVar('--chart-4', '#ef4444'),
      readCssVar('--chart-5', '#8b5cf6'),
    ]
    let hash = 0
    for (let i = 0; i < label.length; i++) hash = (hash * 31 + label.charCodeAt(i)) >>> 0
    return palette[hash % palette.length]
  }

  // Build a unique color per label map, using chart tokens first, then rainbow
  const labelColorMap = useMemo(() => {
    const counts = new Map<string, number>()
    const metas = Object.values(nodeMeta)
    if (metas.length) {
      for (const meta of metas) {
        for (const label of meta.labels || []) {
          counts.set(label, (counts.get(label) || 0) + 1)
        }
      }
    } else {
      for (const n of data.nodes) {
        counts.set(n.type, (counts.get(n.type) || 0) + 1)
      }
    }

    const labels = Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([label]) => label)

    const chartColors = [
      readCssVar('--chart-1', '#3b82f6'),
      readCssVar('--chart-2', '#10b981'),
      readCssVar('--chart-3', '#f59e0b'),
      readCssVar('--chart-4', '#ef4444'),
      readCssVar('--chart-5', '#8b5cf6'),
    ]

    const map: Record<string, string> = {}
    const total = labels.length
    for (let i = 0; i < labels.length; i++) {
      let color: string
      if (i < chartColors.length) {
        color = chartColors[i]
      } else {
        const t = total > chartColors.length ? i / total : 0
        color = (d3 as any).interpolateSinebow ? (d3 as any).interpolateSinebow(t) : getPaletteColor(labels[i])
      }
      map[labels[i]] = color
    }
    return map
  }, [nodeMeta, data.nodes, themeKey])

  const getNodeColor = (type: string) => labelColorMap[type] || getPaletteColor(type || 'Node')

  useEffect(() => {
    if (!svgRef.current) return
    const width = svgRef.current.clientWidth
    const height = svgRef.current.clientHeight

    d3.select(svgRef.current).selectAll('*').remove()

    const svg = d3.select(svgRef.current)

    const g = svg.append('g')

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        g.attr('transform', event.transform as any)
        zoomTransformRef.current = event.transform
      })

    svg.call(zoom as any)
    zoomRef.current = zoom
    // Reapply previous zoom transform if available
    if (zoomTransformRef.current) {
      g.attr('transform', zoomTransformRef.current as any)
      svg.call((zoom as any).transform, zoomTransformRef.current)
    }
    svg.on('click', () => {
      setSelectedNode(null)
      setSelectedEdgeId(null)
      resetDim()
    })

    const borderColor = readCssVar('--border', '#e5e7eb')
    const labelColor = readCssVar('--muted-foreground', '#6b7280')
    const nodeStroke = readCssVar('--card', '#ffffff')

    svg
      .append('defs')
      .selectAll('marker')
      .data(['arrow'])
      .join('marker')
      .attr('id', 'arrow')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 20)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('fill', borderColor)

    const simulation = d3
      .forceSimulation<NodeData>(data.nodes)
      .force(
        'link',
        d3
          .forceLink<NodeData, LinkData>(data.links)
          .id((d) => d.id)
          .distance(linkDistance)
      )
      .force('charge', d3.forceManyBody().strength(chargeStrength))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius(30))

    simulationRef.current = simulation

    const link = g
      .append('g')
      .selectAll('line')
      .data(data.links)
      .join('line')
      .attr('stroke', borderColor)
      .attr('stroke-width', 2)
      .attr('marker-end', 'url(#arrow)')
      .style('cursor', 'pointer')
      .on('mouseenter', function (_, d) {
        const src = typeof d.source === 'string' ? d.source : (d.source as NodeData).id
        const tgt = typeof d.target === 'string' ? d.target : (d.target as NodeData).id
        dimExcept(new Set([src, tgt]), new Set([d.id]))
      })
      .on('mouseleave', () => applyFocusFromSelection())
      .on('click', function (event, d) {
        event.stopPropagation()
        setSelectedEdgeId(d.id)
        setSelectedNode(null)
      })

    const node = g
      .append('g')
      .selectAll('circle')
      .data(data.nodes)
      .join('circle')
      .attr('r', (d) => Math.max(8, d.val / 2))
      .attr('fill', (d) => getNodeColor(d.type))
      .attr('stroke', nodeStroke)
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')
      .call(
        d3
          .drag<SVGCircleElement, NodeData>()
          .on('start', dragstarted as any)
          .on('drag', dragged as any)
          .on('end', dragended as any) as any
      )
      .on('mouseenter', function (_, d) {
        const nbrs = adjacencyRef.current.get(d.id) || new Set<string>()
        const nodesToKeep = new Set<string>([d.id, ...Array.from(nbrs)])
        const edgesToKeep = new Set<string>()
        for (const l of data.links) {
          const s = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
          const t = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
          if (s === d.id || t === d.id) edgesToKeep.add(l.id)
        }
        dimExcept(nodesToKeep, edgesToKeep)
      })
      .on('mouseleave', () => applyFocusFromSelection())
      .on('click', (event, d) => {
        event.stopPropagation()
        setSelectedNode(d)
        setSelectedEdgeId(null)
        focusOnNode(d)
      })
      .on('dblclick', function (event, d) {
        event.stopPropagation()
        // Toggle pin/unpin
        if (d.fx == null && d.fy == null) {
          d.fx = d.x
          d.fy = d.y
        } else {
          d.fx = null
          d.fy = null
        }
      })

    const label = g
      .append('g')
      .selectAll('text')
      .data(data.nodes)
      .join('text')
      .text((d) => d.name)
      .attr('font-size', 12)
      .attr('dx', 0)
      .attr('dy', (d) => Math.max(8, d.val / 2) + 15)
      .attr('text-anchor', 'middle')
      .attr('fill', labelColor)
      .style('pointer-events', 'none')
      .style('user-select', 'none')

    let edgeLabel: d3.Selection<SVGTextElement, LinkData, SVGGElement, unknown> | null = null
    if (showEdgeLabels) {
      edgeLabel = (g as unknown as d3.Selection<SVGGElement, unknown, null, undefined>)
        .append('g')
        .selectAll<SVGTextElement, LinkData>('text')
        .data(data.links)
        .join('text')
        .text((d) => d.label)
        .attr('font-size', 11)
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'central')
        .attr('fill', labelColor)
        .style('pointer-events', 'auto')
        .style('cursor', 'pointer')
        .on('click', function (event, d) {
          event.stopPropagation()
          setSelectedEdgeId(d.id)
          setSelectedNode(null)
        })
    }

    simulation.on('tick', () => {
      link
        .attr('x1', (d) => (d.source as NodeData).x!)
        .attr('y1', (d) => (d.source as NodeData).y!)
        .attr('x2', (d) => (d.target as NodeData).x!)
        .attr('y2', (d) => (d.target as NodeData).y!)

      node.attr('cx', (d) => d.x!).attr('cy', (d) => d.y!)

      label.attr('x', (d) => d.x!).attr('y', (d) => d.y!)

      if (edgeLabel) {
        edgeLabel.attr('transform', (d) => {
          const sx = (d.source as NodeData).x!
          const sy = (d.source as NodeData).y!
          const tx = (d.target as NodeData).x!
          const ty = (d.target as NodeData).y!
          const mx = (sx + tx) / 2
          const my = (sy + ty) / 2
          let angle = (Math.atan2(ty - sy, tx - sx) * 180) / Math.PI
          if (angle > 90 || angle < -90) angle += 180 // keep text upright
          return `translate(${mx},${my}) rotate(${angle})`
        })
      }
    })

    function dragstarted(event: d3.D3DragEvent<SVGCircleElement, NodeData, NodeData>) {
      if (!event.active) simulation.alphaTarget(0.3).restart()
      event.subject.fx = event.subject.x
      event.subject.fy = event.subject.y
    }

    function dragged(event: d3.D3DragEvent<SVGCircleElement, NodeData, NodeData>) {
      event.subject.fx = event.x
      event.subject.fy = event.y
    }

    function dragended(event: d3.D3DragEvent<SVGCircleElement, NodeData, NodeData>) {
      if (!event.active) simulation.alphaTarget(0)
      event.subject.fx = null
      event.subject.fy = null
    }

    // Save selections for interactions
    nodeSelRef.current = node as any
    linkSelRef.current = link as any
    labelSelRef.current = label as any
    edgeLabelSelRef.current = edgeLabel as any

    // Re-apply focus/dimming if there is an active selection
    const selNode = selectedNodeRef.current
    const selEdgeId = selectedEdgeIdRef.current
    if (selNode) {
      const nbrs = adjacencyRef.current.get(selNode.id) || new Set<string>()
      const nodesToKeep = new Set<string>([selNode.id, ...Array.from(nbrs)])
      const edgesToKeep = new Set<string>()
      for (const l of data.links) {
        const s = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
        const t = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
        if (s === selNode.id || t === selNode.id) edgesToKeep.add(l.id)
      }
      dimExcept(nodesToKeep, edgesToKeep, 0.15, 0)
    } else if (selEdgeId) {
      const edge = data.links.find((e) => e.id === selEdgeId)
      if (edge) {
        const s = typeof edge.source === 'string' ? edge.source : (edge.source as NodeData).id
        const t = typeof edge.target === 'string' ? edge.target : (edge.target as NodeData).id
        const nodesToKeep = new Set<string>([s, t])
        const edgesToKeep = new Set<string>([edge.id])
        dimExcept(nodesToKeep, edgesToKeep, 0.15, 0)
      }
    }

    return () => {
      simulation.stop()
    }
  }, [data, themeKey, chargeStrength, linkDistance, showEdgeLabels])

  // Update forces when sliders change
  useEffect(() => {
    const sim = simulationRef.current
    if (!sim) return
    const linkForce = sim.force('link') as d3.ForceLink<NodeData, LinkData> | null
    if (linkForce) (linkForce as any).distance(linkDistance)
    const chargeForce = sim.force('charge') as d3.ForceManyBody<NodeData> | null
    if (chargeForce) chargeForce.strength(chargeStrength)
    sim.alpha(0.5).restart()
  }, [linkDistance, chargeStrength])

  // Build adjacency for hover focus
  useEffect(() => {
    const map = new Map<string, Set<string>>()
    for (const n of data.nodes) map.set(n.id, new Set())
    for (const l of data.links) {
      const s = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
      const t = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
      map.get(s)?.add(t)
      map.get(t)?.add(s)
    }
    adjacencyRef.current = map
  }, [data])

  const dimExcept = (keepNodeIds: Set<string>, keepEdgeIds: Set<string>, nodeDimOpacity = 0.2, edgeDimOpacity = 0.1) => {
    if (nodeSelRef.current) {
      nodeSelRef.current.style('opacity', (d: any) => (keepNodeIds.has(d.id) ? 1 : nodeDimOpacity))
    }
    if (labelSelRef.current) {
      labelSelRef.current.style('opacity', (d: any) => (keepNodeIds.has(d.id) ? 1 : nodeDimOpacity))
    }
    if (linkSelRef.current) {
      linkSelRef.current.style('opacity', (d: any) => (keepEdgeIds.has(d.id) ? 1 : edgeDimOpacity))
    }
    if (edgeLabelSelRef.current) {
      edgeLabelSelRef.current.style('opacity', (d: any) => (keepEdgeIds.has(d.id) ? 1 : edgeDimOpacity))
    }
  }

  const resetDim = () => {
    nodeSelRef.current?.style('opacity', 1)
    labelSelRef.current?.style('opacity', 1)
    linkSelRef.current?.style('opacity', 1)
    edgeLabelSelRef.current?.style('opacity', 1)
  }

  const applyFocusFromSelection = () => {
    const selNode = selectedNodeRef.current
    const selEdgeId = selectedEdgeIdRef.current
    if (selNode) {
      const nbrs = adjacencyRef.current.get(selNode.id) || new Set<string>()
      const nodesToKeep = new Set<string>([selNode.id, ...Array.from(nbrs)])
      const edgesToKeep = new Set<string>()
      for (const l of data.links) {
        const s = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
        const t = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
        if (s === selNode.id || t === selNode.id) edgesToKeep.add(l.id)
      }
      dimExcept(nodesToKeep, edgesToKeep, 0.15, 0)
      return
    }
    if (selEdgeId) {
      const edge = data.links.find((e) => e.id === selEdgeId)
      if (edge) {
        const s = typeof edge.source === 'string' ? edge.source : (edge.source as NodeData).id
        const t = typeof edge.target === 'string' ? edge.target : (edge.target as NodeData).id
        const nodesToKeep = new Set<string>([s, t])
        const edgesToKeep = new Set<string>([edge.id])
        dimExcept(nodesToKeep, edgesToKeep, 0.15, 0)
        return
      }
    }
    resetDim()
  }

  const handleZoomIn = () => {
    if (!svgRef.current) return
    const svg = d3.select(svgRef.current)
    const zoom = zoomRef.current
    if (zoom) {
      svg.transition().duration(300).call(zoom.scaleBy as any, 1.3)
      const t = zoomTransformRef.current || d3.zoomIdentity
      zoomTransformRef.current = t.scale(1.3)
    }
  }

  const handleZoomOut = () => {
    if (!svgRef.current) return
    const svg = d3.select(svgRef.current)
    const zoom = zoomRef.current
    if (zoom) {
      svg.transition().duration(300).call(zoom.scaleBy as any, 0.7)
      const t = zoomTransformRef.current || d3.zoomIdentity
      zoomTransformRef.current = t.scale(0.7)
    }
  }

  const handleFitView = () => {
    if (!svgRef.current) return
    const svg = d3.select(svgRef.current)
    const zoom = zoomRef.current
    if (zoom) {
      svg.transition().duration(300).call(zoom.transform as any, d3.zoomIdentity)
      zoomTransformRef.current = d3.zoomIdentity
    }
  }

  const focusOnNode = (node: NodeData) => {
    if (!svgRef.current) return
    const svg = d3.select(svgRef.current)
    const width = svgRef.current.clientWidth
    const height = svgRef.current.clientHeight
    const zoom = zoomRef.current
    const scale = 1.5
    const transform = d3.zoomIdentity
      .translate(width / 2, height / 2)
      .scale(scale)
      .translate(-node.x!, -node.y!)
    if (zoom) svg.transition().duration(500).call(zoom.transform as any, transform)
    zoomTransformRef.current = transform
  }

  const isDark = themeKey === 'dark'
  const toggleTheme = () => {
    const next = isDark ? 'light' : 'dark'
    document.documentElement.classList.toggle('dark', next === 'dark')
    localStorage.setItem('theme', next)
    setThemeKey(next)
  }

  // Load data from API when dbPath changes
  const handleDatabaseChange = async (dbPath: string) => {
    if (!dbPath) return
    setLoading(true)
    setError(null)
    try {
      const headers = { 'X-Database-Path': dbPath }
      const [nodesRes, edgesRes] = await Promise.all([
        fetch('/api/graph/nodes', { headers }),
        fetch('/api/graph/edges', { headers }),
      ])
      if (!nodesRes.ok || !edgesRes.ok) throw new Error('Failed to load graph data')
      const nodesJson = await nodesRes.json()
      const edgesJson = await edgesRes.json()
      const reagraphNodes = nodesJson.nodes as Array<{ id: string; label: string; data: { nodeId: number; labels: string[]; properties: Record<string, string | number | boolean> } }>
      const reagraphEdges = edgesJson.edges as Array<{ id: string; source: string; target: string; label: string; data: { edgeId: number; edgeType: string; properties: Record<string, string | number | boolean> } }>

      // Degree map
      const degree = new Map<string, number>()
      for (const e of reagraphEdges) {
        degree.set(e.source, (degree.get(e.source) || 0) + 1)
        degree.set(e.target, (degree.get(e.target) || 0) + 1)
      }

      const nodes: NodeData[] = reagraphNodes.map((n) => {
        const firstLabel = n.data.labels[0] || 'Node'
        const deg = degree.get(n.id) || 1
        const val = Math.max(16, Math.min(48, 16 + deg * 4))
        return { id: n.id, name: n.label, type: firstLabel, val }
      })
      const links: LinkData[] = reagraphEdges.map((e) => ({ id: e.id, source: e.source, target: e.target, label: e.label }))

      const meta: Record<string, { label: string; labels: string[]; properties: Record<string, string | number | boolean>; nodeId: number }> = {}
      for (const n of reagraphNodes) {
        meta[n.id] = { label: n.label, labels: n.data.labels, properties: n.data.properties, nodeId: n.data.nodeId }
      }

      const eMeta: Record<string, { edgeId: number; edgeType: string; properties: Record<string, string | number | boolean> }> = {}
      for (const e of reagraphEdges) {
        eMeta[e.id] = { edgeId: e.data.edgeId, edgeType: e.data.edgeType, properties: e.data.properties }
      }

      setNodeMeta(meta)
      setEdgeMeta(eMeta)
      setData({ nodes, links })
      setSelectedNode(null)
      setSelectedEdgeId(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  // Keep refs in sync and (re)apply focus when selection changes
  useEffect(() => {
    selectedNodeRef.current = selectedNode
    selectedEdgeIdRef.current = selectedEdgeId
    applyFocusFromSelection()
  }, [selectedNode, selectedEdgeId])

  return (
    <div className="min-h-screen bg-background text-foreground">
      <DatabaseSelector onDatabaseChange={handleDatabaseChange} />

      <div className="container mx-auto p-6">
        <div className="mb-6">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary text-primary-foreground">
                <Network className="h-5 w-5" />
              </div>
              <div>
                <h1 className="text-3xl font-bold tracking-tight">Network Graph Explorer</h1>
                <p className="text-muted-foreground">Interactive visualization of system architecture</p>
              </div>
            </div>
          </div>
        </div>

        {error && (
          <div className="mb-4 rounded-lg border border-destructive/20 bg-destructive/10 text-destructive px-4 py-3">
            {error}
          </div>
        )}

        <div className="grid gap-6 lg:grid-cols-3">
          <div className="lg:col-span-2">
            <div className="bg-card border rounded-lg overflow-hidden relative">
              <div className="border-b bg-muted/50 px-4 py-3">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="font-semibold">System Network</div>
                    <div className="text-sm text-muted-foreground">
                      {data.nodes.length} nodes • {data.links.length} connections
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      className={`p-2 rounded-md ${showEdgeLabels ? 'bg-primary text-primary-foreground' : 'hover:bg-muted'}`}
                      onClick={() => setShowEdgeLabels((v) => !v)}
                      aria-pressed={showEdgeLabels}
                      title="Toggle edge types"
                    >
                      <Tag className="h-4 w-4" />
                    </button>
                    {/* Legend collapsible placed next to edge text toggle */}
                    <Collapsible>
                      <div className="relative">
                        <CollapsibleTrigger className="px-2 py-1 hover:bg-muted rounded-md text-xs flex items-center gap-1" aria-label="Toggle legend">
                          <span>Legend</span>
                          <ChevronUp className="h-3 w-3 data-[state=closed]:hidden" />
                          <ChevronDown className="h-3 w-3 hidden data-[state=closed]:block" />
                        </CollapsibleTrigger>
                        <CollapsibleContent>
                          <div className="absolute right-0 mt-2 bg-card border rounded-md shadow-sm p-3 space-y-2 text-xs z-10">
                            {(() => {
                              const labels = Object.keys(labelColorMap)
                              if (!labels.length) return <div className="text-muted-foreground">No labels yet</div>
                              return labels.map((label) => (
                                <div key={label} className="flex items-center gap-2">
                                  <div className="h-3 w-3 rounded-full" style={{ backgroundColor: labelColorMap[label] }} />
                                  <span>{label}</span>
                                </div>
                              ))
                            })()}
                            <div className="text-[10px] text-muted-foreground">Colors map to labels.</div>
                          </div>
                        </CollapsibleContent>
                      </div>
                    </Collapsible>
                    <button className="p-2 hover:bg-muted rounded-md" onClick={handleZoomIn} aria-label="Zoom in">
                      <ZoomIn className="h-4 w-4" />
                    </button>
                    <button className="p-2 hover:bg-muted rounded-md" onClick={handleZoomOut} aria-label="Zoom out">
                      <ZoomOut className="h-4 w-4" />
                    </button>
                    <button className="p-2 hover:bg-muted rounded-md" onClick={handleFitView} aria-label="Fit view">
                      <Maximize2 className="h-4 w-4" />
                    </button>
                    <button
                      className="p-2 hover:bg-muted rounded-md"
                      onClick={toggleTheme}
                      aria-label="Toggle theme"
                      title="Toggle theme"
                    >
                      {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
                    </button>
                  </div>
                </div>
              </div>
              <div className="p-0">
                {loading ? (
                  <div className="h-[600px] flex items-center justify-center text-muted-foreground">Loading graph…</div>
                ) : (
                  <svg
                    ref={svgRef}
                    className="w-full h-[600px]"
                    style={{ background: 'transparent' }}
                  />
                )}
              </div>
            </div>
          </div>

          <div className="space-y-6">
            <div className="bg-card border rounded-lg">
              <div className="border-b bg-muted/50 px-4 py-3">
                <div className="font-semibold">Details</div>
                <div className="text-sm text-muted-foreground">Click on a node or edge to view information</div>
              </div>
              <div className="p-4">
                {selectedNode ? (
                  <div className="space-y-4">
                    <div>
                      <h3 className="text-lg font-semibold mb-2">{nodeMeta[selectedNode.id]?.label || selectedNode.name}</h3>
                      <span className="inline-flex items-center rounded-md border px-2 py-1 text-xs">
                        {nodeMeta[selectedNode.id]?.labels?.[0] || selectedNode.type}
                      </span>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Node ID:</span>
                        <span className="font-mono">{nodeMeta[selectedNode.id]?.nodeId ?? selectedNode.id}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Connections:</span>
                        <span>
                          {
                            data.links.filter((l) => {
                              const sourceId = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
                              const targetId = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
                              return sourceId === selectedNode.id || targetId === selectedNode.id
                            }).length
                          }
                        </span>
                      </div>
                    </div>
                    <div className="pt-4 border-t">
                      <h4 className="text-sm font-medium mb-2">Connected Nodes</h4>
                      <div className="space-y-1">
                        {data.links
                          .filter((l) => {
                            const sourceId = typeof l.source === 'string' ? l.source : (l.source as NodeData).id
                            const targetId = typeof l.target === 'string' ? l.target : (l.target as NodeData).id
                            return sourceId === selectedNode.id || targetId === selectedNode.id
                          })
                          .map((link, idx) => {
                            const sourceId = typeof link.source === 'string' ? link.source : (link.source as NodeData).id
                            const targetId = typeof link.target === 'string' ? link.target : (link.target as NodeData).id
                            const connectedId = sourceId === selectedNode.id ? targetId : sourceId
                            const connectedNode = data.nodes.find((n) => n.id === connectedId)
                            return (
                              <div key={idx} className="text-sm flex items-center justify-between">
                                <span>{nodeMeta[connectedId]?.label || connectedNode?.name}</span>
                                <span className="inline-flex items-center rounded-md border px-2 py-1 text-xs">
                                  {link.label}
                                </span>
                              </div>
                            )
                          })}
                      </div>
                    </div>
                    {nodeMeta[selectedNode.id] && (
                      <div className="pt-4 border-t">
                        <h4 className="text-sm font-medium mb-2">Properties</h4>
                        {Object.keys(nodeMeta[selectedNode.id].properties || {}).length ? (
                          <div className="space-y-2">
                            {Object.entries(nodeMeta[selectedNode.id].properties).map(([k, v]) => (
                              <div key={k} className="bg-muted/30 rounded-md p-2">
                                <div className="text-xs text-muted-foreground mb-1">{k}</div>
                                <div className="text-sm font-mono break-all">{String(v)}</div>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="text-sm text-muted-foreground italic">No properties</div>
                        )}
                      </div>
                    )}
                  </div>
                ) : selectedEdgeId ? (
                  <div className="space-y-4">
                    <div className="border-b pb-3">
                      <div className="text-xs text-muted-foreground uppercase tracking-wide mb-1">Edge</div>
                      <h4 className="text-lg font-medium">{edgeMeta[selectedEdgeId]?.edgeType || 'Edge'}</h4>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">ID</span>
                        <span className="font-mono">{edgeMeta[selectedEdgeId]?.edgeId}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Type</span>
                        <span className="inline-flex items-center rounded-md border px-2 py-1 text-xs">
                          {edgeMeta[selectedEdgeId]?.edgeType}
                        </span>
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Properties</div>
                      {edgeMeta[selectedEdgeId] && Object.keys(edgeMeta[selectedEdgeId].properties).length > 0 ? (
                        <div className="space-y-2">
                          {Object.entries(edgeMeta[selectedEdgeId].properties).map(([key, value]) => (
                            <div key={key} className="bg-muted/30 rounded-md p-2">
                              <div className="text-xs font-medium text-muted-foreground mb-1">{key}</div>
                              <div className="text-sm font-mono break-all">{String(value)}</div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-sm text-muted-foreground italic">No properties</div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    <Network className="h-12 w-12 mx-auto mb-3 opacity-50" />
                    <p className="text-sm">Select a node or edge to view details</p>
                  </div>
                )}
              </div>
            </div>

            {/* Legend moved next to edge text toggle */}
            <div className="bg-card border rounded-lg">
              <div className="border-b bg-muted/50 px-4 py-3">
                <div className="font-semibold">Layout Controls</div>
              </div>
              <div className="p-4 space-y-4 text-sm">
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-muted-foreground">Charge strength</span>
                    <span className="font-mono">{chargeStrength}</span>
                  </div>
                  <input
                    type="range"
                    min="-800"
                    max="-50"
                    step="10"
                    value={chargeStrength}
                    onChange={(e) => setChargeStrength(parseInt(e.target.value, 10))}
                    className="w-full"
                  />
                </div>
                <div>
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-muted-foreground">Link distance</span>
                    <span className="font-mono">{linkDistance}</span>
                  </div>
                  <input
                    type="range"
                    min="40"
                    max="240"
                    step="5"
                    value={linkDistance}
                    onChange={(e) => setLinkDistance(parseInt(e.target.value, 10))}
                    className="w-full"
                  />
                </div>
                <button
                  className="px-3 py-2 border rounded-md"
                  onClick={() => {
                    setChargeStrength(-300)
                    setLinkDistance(100)
                  }}
                >
                  Reset forces
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}


