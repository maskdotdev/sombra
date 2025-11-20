"use client"

import { Zap, Shield, FileCode } from "lucide-react"
import { Card } from "@/components/ui/card"

export function FeatureGraph() {
  return (
    <div className="relative w-full max-w-5xl mx-auto h-[600px] md:h-[500px]">
      {/* SVG for edges */}
      <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ zIndex: 0 }}>
        <defs>
          <marker
            id="arrowhead"
            markerWidth="10"
            markerHeight="10"
            refX="9"
            refY="3"
            orient="auto"
            className="fill-muted-foreground"
          >
            <polygon points="0 0, 10 3, 0 6" />
          </marker>
        </defs>

        {/* Edge: Fast (top-left) -> Type Safe (bottom-center) */}
        <line
          x1="20%"
          y1="25%"
          x2="50%"
          y2="70%"
          stroke="currentColor"
          strokeWidth="2"
          strokeDasharray="4 4"
          className="text-muted-foreground"
          markerEnd="url(#arrowhead)"
        />
        <text x="30%" y="50%" className="fill-muted-foreground text-xs font-mono">
          with confidence
        </text>

        {/* Edge: Type Safe (bottom-center) -> Embedded (top-right) */}
        <line
          x1="50%"
          y1="70%"
          x2="80%"
          y2="25%"
          stroke="currentColor"
          strokeWidth="2"
          strokeDasharray="4 4"
          className="text-muted-foreground"
          markerEnd="url(#arrowhead)"
        />
        <text x="68%" y="50%" className="fill-muted-foreground text-xs font-mono">
          anywhere
        </text>

        {/* Edge: Embedded (top-right) -> Fast (top-left) */}
        <line
          x1="80%"
          y1="25%"
          x2="20%"
          y2="25%"
          stroke="currentColor"
          strokeWidth="2"
          strokeDasharray="4 4"
          className="text-muted-foreground"
          markerEnd="url(#arrowhead)"
        />
        <text x="48%" y="20%" className="fill-muted-foreground text-xs font-mono">
          instantly
        </text>
      </svg>

      {/* Node: Blazing Fast (top-left) */}
      <Card
        className="absolute top-0 left-0 md:left-[5%] p-6 bg-card border-border hover:border-primary transition-all duration-300 w-[280px]"
        style={{ zIndex: 1 }}
      >
        <div className="flex items-start gap-4">
          <div className="p-3 rounded-lg bg-secondary border border-border">
            <Zap className="w-6 h-6 text-foreground" />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-bold mb-2 font-mono">blazing fast</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Optimized query engine with sub-millisecond response times for complex graph traversals
            </p>
          </div>
        </div>
      </Card>

      {/* Node: Type Safe (bottom-center) */}
      <Card
        className="absolute bottom-0 left-1/2 -translate-x-1/2 p-6 bg-card border-border hover:border-primary transition-all duration-300 w-[280px]"
        style={{ zIndex: 1 }}
      >
        <div className="flex items-start gap-4">
          <div className="p-3 rounded-lg bg-secondary border border-border">
            <Shield className="w-6 h-6 text-foreground" />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-bold mb-2 font-mono">type safe</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Full TypeScript support with auto-generated types from your schema definitions
            </p>
          </div>
        </div>
      </Card>

      {/* Node: Embedded (top-right) */}
      <Card
        className="absolute top-0 right-0 md:right-[5%] p-6 bg-card border-border hover:border-primary transition-all duration-300 w-[280px]"
        style={{ zIndex: 1 }}
      >
        <div className="flex items-start gap-4">
          <div className="p-3 rounded-lg bg-secondary border border-border">
            <FileCode className="w-6 h-6 text-foreground" />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-bold mb-2 font-mono">embedded</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              File-based architecture with zero configuration. Deploy as a single binary anywhere
            </p>
          </div>
        </div>
      </Card>
    </div>
  )
}
