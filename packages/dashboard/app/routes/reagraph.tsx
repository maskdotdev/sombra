import { type ComponentProps, type ComponentType, useEffect, useState } from "react"
import { GraphView } from "../components/query/graph-view"
import { GraphExplorerPage } from "./graph"
import type { GraphCanvasProps } from "../components/query/graph-canvas"

type GraphViewProps = Omit<ComponentProps<typeof GraphView>, "GraphRenderer">
type GraphRendererComponent = ComponentType<GraphCanvasProps>

function GraphViewWithReagraph(props: GraphViewProps) {
    const [GraphRenderer, setGraphRenderer] = useState<GraphRendererComponent | null>(null)
    const [loadError, setLoadError] = useState<string | null>(null)

    useEffect(() => {
        let cancelled = false
        void import("../components/query/reagraph-canvas")
            .then((module) => {
                if (!cancelled) {
                    setGraphRenderer(() => module.ReagraphCanvas)
                }
            })
            .catch((error) => {
                console.error("Failed to load Reagraph renderer", error)
                if (!cancelled) {
                    setLoadError(error instanceof Error ? error.message : String(error))
                }
            })
        return () => {
            cancelled = true
        }
    }, [])

    const ActiveRenderer: GraphRendererComponent =
        GraphRenderer ??
        (({ height = 540 }) => (
            <div
                className="flex h-full w-full items-center justify-center rounded-2xl border bg-muted/30 text-sm text-muted-foreground"
                style={{ height }}
            >
                {loadError ? (
                    <span>Failed to load Reagraph. Showing fallback view.</span>
                ) : (
                    <span>Loading Reagraph…</span>
                )}
            </div>
        ))

    return <GraphView {...props} GraphRenderer={ActiveRenderer} />
}

export { loader } from "./graph"

export function meta() {
    return [
        { title: "Reagraph Explorer · Sombra" },
        {
            name: "description",
            content: "Render sampled rows with Reagraph for a fast, WebGL-powered graph experience.",
        },
    ]
}

export default function ReagraphExplorer() {
    return <GraphExplorerPage GraphComponent={GraphViewWithReagraph} />
}
