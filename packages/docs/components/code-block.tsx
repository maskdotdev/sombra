import { Card } from "@/components/ui/card"

export function CodeBlock() {
  return (
    <Card className="bg-card border-border overflow-hidden">
      <div className="bg-secondary px-4 py-2 border-b border-border flex items-center gap-2">
        <div className="flex gap-1.5">
          <div className="w-3 h-3 rounded-full bg-destructive/60" />
          <div className="w-3 h-3 rounded-full bg-primary/40" />
          <div className="w-3 h-3 rounded-full bg-chart-3/60" />
        </div>
        <span className="text-xs text-muted-foreground ml-2">query.ts</span>
      </div>
      <div className="p-6">
        <pre className="text-sm leading-relaxed">
          <code>
            <span className="text-primary">import</span> <span className="text-foreground">{"{ Sombra }"}</span>{" "}
            <span className="text-primary">from</span> <span className="text-chart-3">'@sombra/core'</span>
            {"\n\n"}
            <span className="text-primary">const</span> <span className="text-foreground">db</span>{" "}
            <span className="text-muted-foreground">=</span> <span className="text-primary">new</span>{" "}
            <span className="text-chart-2">Sombra</span>
            <span className="text-foreground">()</span>
            {"\n\n"}
            <span className="text-muted-foreground">// Find all users who follow each other</span>
            {"\n"}
            <span className="text-primary">const</span> <span className="text-foreground">mutualFollows</span>{" "}
            <span className="text-muted-foreground">=</span> <span className="text-primary">await</span>{" "}
            <span className="text-foreground">db</span>
            {"\n  "}
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">query</span>
            <span className="text-foreground">()</span>
            {"\n  "}
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">match</span>
            <span className="text-foreground">(</span>
            <span className="text-chart-3">'User'</span>
            <span className="text-foreground">)</span>
            {"\n  "}
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">where</span>
            <span className="text-foreground">(</span>
            <span className="text-chart-3">'FOLLOWS'</span>
            <span className="text-foreground">,</span> <span className="text-chart-3">'User'</span>
            <span className="text-foreground">)</span>
            {"\n  "}
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">bidirectional</span>
            <span className="text-foreground">()</span>
            {"\n  "}
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">execute</span>
            <span className="text-foreground">()</span>
            {"\n\n"}
            <span className="text-foreground">console</span>
            <span className="text-muted-foreground">.</span>
            <span className="text-chart-2">log</span>
            <span className="text-foreground">(mutualFollows)</span>
          </code>
        </pre>
      </div>
    </Card>
  )
}
