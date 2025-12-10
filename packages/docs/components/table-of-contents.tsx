"use client"

import type React from "react"
import { useEffect, useMemo, useState } from "react"
import { cn } from "@/lib/utils"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ArrowDownLeft } from "lucide-react"

type TableOfContentsItem = {
  href: string
  label: string
  description?: string
}

interface TableOfContentsProps {
  title?: string
  eyebrow?: string
  items: TableOfContentsItem[]
}

export function TableOfContents({
  items,
  title = "on this page",
  eyebrow = "docs",
}: TableOfContentsProps) {
  const anchors = useMemo(() => items.map((item) => item.href.replace("#", "")), [items])
  const [activeId, setActiveId] = useState<string>(anchors[0] ?? "")

  useEffect(() => {
    if (anchors.length === 0) return

    const headings = anchors
      .map((id) => document.getElementById(id))
      .filter((node): node is HTMLElement => Boolean(node))

    if (headings.length === 0) return

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort((a, b) => b.intersectionRatio - a.intersectionRatio)

        if (visible[0]?.target?.id) {
          setActiveId(visible[0].target.id)
        }
      },
      { rootMargin: "-40% 0px -45% 0px", threshold: [0, 0.25, 0.5, 1] },
    )

    headings.forEach((heading) => observer.observe(heading))

    return () => {
      headings.forEach((heading) => observer.unobserve(heading))
      observer.disconnect()
    }
  }, [anchors])

  const handleNavigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    const targetId = href.replace("#", "")
    const target = document.getElementById(targetId)

    if (!target) return

    event.preventDefault()
    target.scrollIntoView({ behavior: "smooth", block: "start" })
    window.history.replaceState({}, "", `#${targetId}`)
  }

  return (
    <Card className="relative overflow-hidden border-border bg-secondary/60 backdrop-blur">
      <div className="pointer-events-none absolute inset-0 bg-gradient-to-b from-primary/10 via-transparent to-transparent" />
      <div className="relative p-5 space-y-4">
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-1">
            <p className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">{title}</p>
            <p className="text-sm text-muted-foreground">Quick jumps through this page</p>
          </div>
          <Badge variant="outline" className="border-border bg-background/40 text-[11px] uppercase tracking-wide">
            {eyebrow}
          </Badge>
        </div>

        <div className="space-y-2">
          {items.map((item, index) => {
            const id = item.href.replace("#", "")
            const isActive = activeId === id

            return (
              <a
                key={item.href}
                href={item.href}
                onClick={(event) => handleNavigate(event, item.href)}
                className={cn(
                  "group relative flex items-start gap-3 rounded-md border border-transparent px-3 py-2 transition-colors",
                  "hover:border-primary/50 hover:bg-card/60",
                  isActive && "border-primary/70 bg-card/80 shadow-[0_0_0_1px_rgba(255,255,255,0.08)]",
                )}
              >
                <div className="flex h-8 w-8 items-center justify-center rounded-md border border-border bg-background/60 text-xs text-muted-foreground transition-colors group-hover:border-primary/40 group-hover:text-foreground">
                  <ArrowDownLeft className="h-4 w-4" />
                </div>
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">0{index + 1}</span>
                    <span className={cn("text-sm font-semibold capitalize", isActive && "text-foreground")}>
                      {item.label}
                    </span>
                  </div>
                  {item.description ? (
                    <p className="text-xs text-muted-foreground mt-1 leading-snug">{item.description}</p>
                  ) : null}
                </div>
              </a>
            )
          })}
        </div>
      </div>
    </Card>
  )
}
