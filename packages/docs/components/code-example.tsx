"use client"

import { useSearchParams } from "next/navigation"
import type { Language } from "./language-selector"

type CodeExamples = {
  [K in Language]?: string
}

interface CodeExampleProps {
  examples: CodeExamples
  label?: string
}

export function CodeExample({ examples, label }: CodeExampleProps) {
  const searchParams = useSearchParams()
  const lang = (searchParams.get("lang") as Language) || "typescript"
  const code = examples[lang] || examples.typescript || ""

  return (
    <div>
      {label && <div className="text-xs text-muted-foreground mb-2">{label}</div>}
      <pre className="text-sm text-foreground bg-background p-4 rounded border border-border overflow-x-auto">
        <code>{code}</code>
      </pre>
    </div>
  )
}
