import { useState, useMemo, Fragment } from "react"
import { ChevronDownIcon, ChevronRightIcon, CopyIcon, CheckIcon, FileCodeIcon, ImportIcon, FunctionSquareIcon, LinkIcon } from "lucide-react"
import { cn } from "~/lib/utils"
import { Button } from "../ui/button"
import { Badge } from "../ui/badge"

type PropertyValueProps = {
  value: unknown
  /** Optional property key name, used for heuristics (e.g., "codeText", "metadata") */
  propertyKey?: string
  /** Maximum height before collapsing with scroll, in pixels */
  maxHeight?: number
  /** Compact mode for tooltips */
  compact?: boolean
}

/** Known metadata keys that should be rendered with special formatting */
const METADATA_SPECIAL_KEYS = ["imports", "exports", "references", "docstring"] as const
type MetadataSpecialKey = typeof METADATA_SPECIAL_KEYS[number]

/** Type for import entries in metadata */
type ImportEntry = {
  module: string
  symbols?: string[]
  isTypeOnly?: boolean
}

/** Type for export entries in metadata */
type ExportEntry = {
  symbol: string
  isDefault?: boolean
  alias?: string
  source?: string
}

/** Type for reference entries in metadata */
type ReferenceEntry = {
  name: string
  kind: string
  range?: {
    start?: { row?: number; column?: number }
    end?: { row?: number; column?: number }
  }
}

/**
 * Smart component for rendering property values with special handling for:
 * - Stringified JSON (parsed and pretty-printed)
 * - Code blocks (syntax-highlighted with line numbers)
 * - Long strings (collapsible)
 * - Primitive values (inline)
 * - Metadata objects with imports/exports/references (structured view)
 */
export function PropertyValue({
  value,
  propertyKey,
  maxHeight = 200,
  compact = false,
}: PropertyValueProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const [copied, setCopied] = useState(false)

  const analyzed = useMemo(() => analyzeValue(value, propertyKey), [value, propertyKey])

  const handleCopy = async () => {
    const textToCopy =
      analyzed.type === "json"
        ? JSON.stringify(analyzed.parsed, null, 2)
        : analyzed.type === "metadata"
        ? JSON.stringify(analyzed.parsed, null, 2)
        : String(value)
    await navigator.clipboard.writeText(textToCopy)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  // Simple primitive values
  if (analyzed.type === "primitive") {
    return (
      <span className="font-mono text-xs break-words">{analyzed.display}</span>
    )
  }

  // Null/undefined
  if (analyzed.type === "empty") {
    return <span className="text-muted-foreground text-xs italic">null</span>
  }

  // Compact mode for tooltips - just show a preview
  if (compact) {
    return (
      <span className="font-mono text-xs truncate block">
        {analyzed.preview}
      </span>
    )
  }

  // Metadata with structured content (imports, exports, references)
  if (analyzed.type === "metadata") {
    return (
      <MetadataView
        data={analyzed.parsed}
        isExpanded={isExpanded}
        onToggleExpand={() => setIsExpanded(!isExpanded)}
        onCopy={handleCopy}
        copied={copied}
        maxHeight={maxHeight}
      />
    )
  }

  // JSON content (either parsed from string or object)
  if (analyzed.type === "json") {
    const formatted = JSON.stringify(analyzed.parsed, null, 2)
    const lines = formatted.split("\n")
    const isLong = lines.length > 8

    return (
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          {isLong && (
            <Button
              variant="ghost"
              size="sm"
              className="h-5 px-1"
              onClick={() => setIsExpanded(!isExpanded)}
            >
              {isExpanded ? (
                <ChevronDownIcon className="size-3" />
              ) : (
                <ChevronRightIcon className="size-3" />
              )}
              <span className="text-[0.65rem] text-muted-foreground ml-1">
                {lines.length} lines
              </span>
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-5 px-1 ml-auto"
            onClick={handleCopy}
          >
            {copied ? (
              <CheckIcon className="size-3 text-emerald-500" />
            ) : (
              <CopyIcon className="size-3" />
            )}
          </Button>
        </div>
        <div
          className={cn(
            "rounded-md bg-muted/50 overflow-auto",
            !isExpanded && isLong && "max-h-[160px]"
          )}
          style={{ maxHeight: isExpanded ? undefined : maxHeight }}
        >
          <pre className="p-2 text-xs font-mono">
            <code>{formatted}</code>
          </pre>
        </div>
      </div>
    )
  }

  // Code content
  if (analyzed.type === "code") {
    const lines = analyzed.content.split("\n")
    const isLong = lines.length > 10

    return (
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          {isLong && (
            <Button
              variant="ghost"
              size="sm"
              className="h-5 px-1"
              onClick={() => setIsExpanded(!isExpanded)}
            >
              {isExpanded ? (
                <ChevronDownIcon className="size-3" />
              ) : (
                <ChevronRightIcon className="size-3" />
              )}
              <span className="text-[0.65rem] text-muted-foreground ml-1">
                {lines.length} lines
              </span>
            </Button>
          )}
          {analyzed.language && (
            <span className="text-[0.6rem] uppercase tracking-wide text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
              {analyzed.language}
            </span>
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-5 px-1 ml-auto"
            onClick={handleCopy}
          >
            {copied ? (
              <CheckIcon className="size-3 text-emerald-500" />
            ) : (
              <CopyIcon className="size-3" />
            )}
          </Button>
        </div>
        <div
          className={cn(
            "rounded-md bg-slate-950 dark:bg-slate-900 overflow-auto border border-slate-800",
            !isExpanded && isLong && "max-h-[200px]"
          )}
          style={{ maxHeight: isExpanded ? undefined : maxHeight }}
        >
          <div className="flex">
            {/* Line numbers */}
            <div className="flex-none py-2 pl-2 pr-3 text-right select-none border-r border-slate-800">
              {lines.map((_, i) => (
                <div
                  key={i}
                  className="text-[0.65rem] leading-[1.4rem] text-slate-600 font-mono"
                >
                  {i + 1}
                </div>
              ))}
            </div>
            {/* Code content */}
            <pre className="flex-1 p-2 text-xs font-mono text-slate-200 overflow-x-auto">
              <code>
                {lines.map((line, i) => (
                  <div key={i} className="leading-[1.4rem]">
                    {line || " "}
                  </div>
                ))}
              </code>
            </pre>
          </div>
        </div>
      </div>
    )
  }

  // Long string (not code, not JSON)
  if (analyzed.type === "longstring") {
    const isLong = analyzed.content.length > 200

    return (
      <div className="space-y-1">
        {isLong && (
          <Button
            variant="ghost"
            size="sm"
            className="h-5 px-1"
            onClick={() => setIsExpanded(!isExpanded)}
          >
            {isExpanded ? (
              <ChevronDownIcon className="size-3" />
            ) : (
              <ChevronRightIcon className="size-3" />
            )}
            <span className="text-[0.65rem] text-muted-foreground ml-1">
              {analyzed.content.length} chars
            </span>
          </Button>
        )}
        <p
          className={cn(
            "font-mono text-xs break-words whitespace-pre-wrap",
            !isExpanded && isLong && "line-clamp-4"
          )}
        >
          {analyzed.content}
        </p>
      </div>
    )
  }

  // Fallback
  return (
    <span className="font-mono text-xs break-words">
      {JSON.stringify(value)}
    </span>
  )
}

type AnalyzedValue =
  | { type: "primitive"; display: string }
  | { type: "empty" }
  | { type: "json"; parsed: unknown; preview: string }
  | { type: "metadata"; parsed: Record<string, unknown>; preview: string }
  | { type: "code"; content: string; language: string | null; preview: string }
  | { type: "longstring"; content: string; preview: string }

/**
 * Checks if the parsed object looks like code metadata with special keys
 */
function isCodeMetadata(obj: unknown): obj is Record<string, unknown> {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    return false
  }
  const record = obj as Record<string, unknown>
  // Check if it has any of the special metadata keys
  return METADATA_SPECIAL_KEYS.some((key) => key in record)
}

function analyzeValue(value: unknown, propertyKey?: string): AnalyzedValue {
  // Handle null/undefined
  if (value === null || value === undefined) {
    return { type: "empty" }
  }

  // Handle primitives
  if (typeof value === "boolean" || typeof value === "number") {
    return { type: "primitive", display: String(value) }
  }

  // Handle strings
  if (typeof value === "string") {
    // Empty string
    if (value.trim() === "") {
      return { type: "primitive", display: '""' }
    }

    // Check if it's JSON
    const trimmed = value.trim()
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        const parsed = JSON.parse(value)
        // Check if this looks like code metadata (has imports/exports/references)
        if (isCodeMetadata(parsed)) {
          return {
            type: "metadata",
            parsed: parsed as Record<string, unknown>,
            preview: truncate(trimmed, 50),
          }
        }
        return {
          type: "json",
          parsed,
          preview: truncate(trimmed, 50),
        }
      } catch {
        // Not valid JSON, continue
      }
    }

    // Check for "metadata" property key - it's often a JSON string
    if (propertyKey?.toLowerCase() === "metadata") {
      try {
        const parsed = JSON.parse(value)
        if (isCodeMetadata(parsed)) {
          return {
            type: "metadata",
            parsed: parsed as Record<string, unknown>,
            preview: truncate(value, 50),
          }
        }
      } catch {
        // Not valid JSON, continue with other checks
      }
    }

    // Check if it looks like code based on property key
    const codePropertyKeys = [
      "codeText",
      "code",
      "source",
      "sourceCode",
      "content",
      "body",
      "snippet",
    ]
    const isCodeProperty = propertyKey
      ? codePropertyKeys.some(
          (k) => propertyKey.toLowerCase().includes(k.toLowerCase())
        )
      : false

    // Heuristics for code detection
    const hasCodeIndicators =
      // Multiple lines with consistent indentation
      (value.includes("\n") &&
        (value.includes("  ") || value.includes("\t"))) ||
      // Common code patterns
      /^(import|export|function|class|const|let|var|if|for|while|def |async |await )/.test(
        trimmed
      ) ||
      // JSX/TSX
      /<[A-Z][a-zA-Z]*/.test(value) ||
      // Arrow functions, type annotations
      /=>|:\s*(string|number|boolean|void|any|unknown)/.test(value)

    if (isCodeProperty || hasCodeIndicators) {
      // Try to detect language
      const language = detectLanguage(value, propertyKey)
      return {
        type: "code",
        content: value,
        language,
        preview: truncate(value.split("\n")[0] || value, 50),
      }
    }

    // Long string
    if (value.length > 100 || value.includes("\n")) {
      return {
        type: "longstring",
        content: value,
        preview: truncate(value, 50),
      }
    }

    // Short string
    return { type: "primitive", display: value }
  }

  // Handle objects/arrays
  if (typeof value === "object") {
    // Check if this looks like code metadata (has imports/exports/references)
    if (isCodeMetadata(value)) {
      return {
        type: "metadata",
        parsed: value as Record<string, unknown>,
        preview: truncate(JSON.stringify(value), 50),
      }
    }
    return {
      type: "json",
      parsed: value,
      preview: truncate(JSON.stringify(value), 50),
    }
  }

  // Fallback
  return { type: "primitive", display: String(value) }
}

function detectLanguage(
  content: string,
  propertyKey?: string
): string | null {
  // Check property key for hints
  if (propertyKey) {
    const key = propertyKey.toLowerCase()
    if (key.includes("typescript") || key.includes("ts")) return "typescript"
    if (key.includes("javascript") || key.includes("js")) return "javascript"
    if (key.includes("python") || key.includes("py")) return "python"
    if (key.includes("rust") || key.includes("rs")) return "rust"
    if (key.includes("sql")) return "sql"
    if (key.includes("json")) return "json"
  }

  // Content-based detection
  if (/:\s*(string|number|boolean|void|any|unknown|React\.)/.test(content)) {
    return "typescript"
  }
  if (
    /^(import|export|const|let|var|function|class|async|await)\s/.test(
      content.trim()
    )
  ) {
    return "javascript"
  }
  if (/^(def |class |import |from |if __name__)/.test(content.trim())) {
    return "python"
  }
  if (/^(fn |let |mut |impl |struct |enum |use |pub )/.test(content.trim())) {
    return "rust"
  }
  if (/^(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\s/i.test(content.trim())) {
    return "sql"
  }

  return null
}

function truncate(str: string, maxLen: number): string {
  const singleLine = str.replace(/\n/g, " ").replace(/\s+/g, " ")
  if (singleLine.length <= maxLen) return singleLine
  return singleLine.slice(0, maxLen - 1) + "..."
}

// ============================================================================
// Metadata View Component
// ============================================================================

type MetadataViewProps = {
  data: Record<string, unknown>
  isExpanded: boolean
  onToggleExpand: () => void
  onCopy: () => void
  copied: boolean
  maxHeight: number
}

/**
 * Structured view for code metadata (imports, exports, references, etc.)
 */
function MetadataView({
  data,
  isExpanded,
  onToggleExpand,
  onCopy,
  copied,
  maxHeight,
}: MetadataViewProps) {
  const imports = Array.isArray(data.imports) ? (data.imports as ImportEntry[]) : []
  const exports = Array.isArray(data.exports) ? (data.exports as ExportEntry[]) : []
  const references = Array.isArray(data.references) ? (data.references as ReferenceEntry[]) : []
  const docstring = typeof data.docstring === "string" ? data.docstring : null
  
  // Get other properties that aren't special
  const otherKeys = Object.keys(data).filter(
    (key) => !METADATA_SPECIAL_KEYS.includes(key as MetadataSpecialKey)
  )

  const hasContent = imports.length > 0 || exports.length > 0 || references.length > 0 || docstring || otherKeys.length > 0
  const totalItems = imports.length + exports.length + references.length + (docstring ? 1 : 0) + otherKeys.length

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <Button
          variant="ghost"
          size="sm"
          className="h-5 px-1"
          onClick={onToggleExpand}
        >
          {isExpanded ? (
            <ChevronDownIcon className="size-3" />
          ) : (
            <ChevronRightIcon className="size-3" />
          )}
          <span className="text-[0.65rem] text-muted-foreground ml-1">
            {totalItems} {totalItems === 1 ? "item" : "items"}
          </span>
        </Button>
        <div className="flex gap-1.5 ml-auto">
          {imports.length > 0 && (
            <Badge variant="outline" className="text-[0.6rem] px-1.5 py-0 h-4">
              <ImportIcon className="size-2.5 mr-1" />
              {imports.length}
            </Badge>
          )}
          {exports.length > 0 && (
            <Badge variant="outline" className="text-[0.6rem] px-1.5 py-0 h-4">
              <FileCodeIcon className="size-2.5 mr-1" />
              {exports.length}
            </Badge>
          )}
          {references.length > 0 && (
            <Badge variant="outline" className="text-[0.6rem] px-1.5 py-0 h-4">
              <LinkIcon className="size-2.5 mr-1" />
              {references.length}
            </Badge>
          )}
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-5 px-1"
          onClick={onCopy}
        >
          {copied ? (
            <CheckIcon className="size-3 text-emerald-500" />
          ) : (
            <CopyIcon className="size-3" />
          )}
        </Button>
      </div>

      {!hasContent && (
        <p className="text-xs text-muted-foreground italic">No metadata</p>
      )}

      {hasContent && (
        <div
          className={cn(
            "rounded-md bg-muted/30 overflow-auto",
            !isExpanded && "max-h-[180px]"
          )}
          style={{ maxHeight: isExpanded ? undefined : maxHeight }}
        >
          <div className="p-2.5 space-y-3">
            {/* Docstring */}
            {docstring && (
              <MetadataSection title="Docstring" icon={<FunctionSquareIcon className="size-3" />}>
                <p className="text-xs text-muted-foreground font-mono whitespace-pre-wrap">
                  {docstring}
                </p>
              </MetadataSection>
            )}

            {/* Imports */}
            {imports.length > 0 && (
              <MetadataSection title="Imports" icon={<ImportIcon className="size-3" />} count={imports.length}>
                <div className="space-y-1">
                  {imports.map((imp, idx) => (
                    <ImportItem key={`${imp.module}-${idx}`} entry={imp} />
                  ))}
                </div>
              </MetadataSection>
            )}

            {/* Exports */}
            {exports.length > 0 && (
              <MetadataSection title="Exports" icon={<FileCodeIcon className="size-3" />} count={exports.length}>
                <div className="flex flex-wrap gap-1.5">
                  {exports.map((exp, idx) => (
                    <ExportItem key={`${exp.symbol}-${idx}`} entry={exp} />
                  ))}
                </div>
              </MetadataSection>
            )}

            {/* References */}
            {references.length > 0 && (
              <MetadataSection title="References" icon={<LinkIcon className="size-3" />} count={references.length}>
                <div className="space-y-1">
                  {references.map((ref, idx) => (
                    <ReferenceItem key={`${ref.name}-${idx}`} entry={ref} />
                  ))}
                </div>
              </MetadataSection>
            )}

            {/* Other properties */}
            {otherKeys.length > 0 && (
              <MetadataSection title="Other">
                <div className="space-y-1.5">
                  {otherKeys.map((key) => (
                    <div key={key} className="text-xs">
                      <span className="text-muted-foreground">{key}:</span>{" "}
                      <span className="font-mono">
                        {typeof data[key] === "object"
                          ? JSON.stringify(data[key])
                          : String(data[key])}
                      </span>
                    </div>
                  ))}
                </div>
              </MetadataSection>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

type MetadataSectionProps = {
  title: string
  icon?: React.ReactNode
  count?: number
  children: React.ReactNode
}

function MetadataSection({ title, icon, count, children }: MetadataSectionProps) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-1.5 text-[0.65rem] uppercase tracking-wide text-muted-foreground font-medium">
        {icon}
        <span>{title}</span>
        {count !== undefined && (
          <span className="text-foreground/50">({count})</span>
        )}
      </div>
      {children}
    </div>
  )
}

function ImportItem({ entry }: { entry: ImportEntry }) {
  const symbols = entry.symbols ?? []
  return (
    <div className="flex items-start gap-2 text-xs">
      <span className="font-mono text-blue-500 dark:text-blue-400 shrink-0">
        {entry.module}
      </span>
      {symbols.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {symbols.map((symbol) => (
            <Badge
              key={symbol}
              variant="secondary"
              className="text-[0.6rem] px-1.5 py-0 h-4 font-mono"
            >
              {symbol}
            </Badge>
          ))}
        </div>
      )}
      {entry.isTypeOnly && (
        <Badge variant="outline" className="text-[0.55rem] px-1 py-0 h-3.5">
          type
        </Badge>
      )}
    </div>
  )
}

function ExportItem({ entry }: { entry: ExportEntry }) {
  return (
    <Badge
      variant={entry.isDefault ? "default" : "secondary"}
      className="text-[0.65rem] px-1.5 py-0.5 font-mono"
    >
      {entry.isDefault && <span className="opacity-60 mr-1">default</span>}
      {entry.symbol}
      {entry.alias && entry.alias !== entry.symbol && (
        <span className="opacity-60 ml-1">as {entry.alias}</span>
      )}
    </Badge>
  )
}

function ReferenceItem({ entry }: { entry: ReferenceEntry }) {
  const kindColors: Record<string, string> = {
    call: "text-amber-500 dark:text-amber-400",
    new: "text-purple-500 dark:text-purple-400",
    reference: "text-slate-500 dark:text-slate-400",
  }
  const kindColor = kindColors[entry.kind] ?? kindColors.reference

  return (
    <div className="flex items-center gap-2 text-xs">
      <Badge
        variant="outline"
        className={cn("text-[0.55rem] px-1 py-0 h-3.5", kindColor)}
      >
        {entry.kind}
      </Badge>
      <span className="font-mono">{entry.name}</span>
      {entry.range?.start && (
        <span className="text-[0.6rem] text-muted-foreground">
          L{entry.range.start.row}
          {entry.range.start.column !== undefined && `:${entry.range.start.column}`}
        </span>
      )}
    </div>
  )
}

/**
 * Simplified version for table cells - shows a smart preview with expand capability
 */
export function PropertyValueCell({ value, propertyKey }: PropertyValueProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const analyzed = useMemo(() => analyzeValue(value, propertyKey), [value, propertyKey])

  if (analyzed.type === "primitive" || analyzed.type === "empty") {
    return (
      <span className={cn(analyzed.type === "empty" && "text-muted-foreground italic")}>
        {analyzed.type === "empty" ? "null" : analyzed.display}
      </span>
    )
  }

  // For complex values, show a preview with expand option
  return (
    <div className="relative group">
      <button
        type="button"
        onClick={() => setIsExpanded(!isExpanded)}
        className="text-left font-mono hover:bg-muted/50 rounded px-1 -mx-1 transition-colors"
      >
        <span className="text-muted-foreground">
          {(analyzed.type === "json" || analyzed.type === "metadata") && (
            <span className="text-blue-500 dark:text-blue-400">{"{}"}</span>
          )}
          {analyzed.type === "code" && (
            <span className="text-emerald-500 dark:text-emerald-400">{"</>"}</span>
          )}
          {analyzed.type === "longstring" && (
            <span className="text-amber-500 dark:text-amber-400">{"..."}</span>
          )}{" "}
        </span>
        <span className="truncate max-w-[200px] inline-block align-bottom">
          {analyzed.preview}
        </span>
      </button>
      {isExpanded && (
        <div className="absolute z-50 left-0 top-full mt-1 w-[400px] max-h-[300px] overflow-auto bg-popover border rounded-lg shadow-lg p-3">
          <PropertyValue value={value} propertyKey={propertyKey} maxHeight={250} />
        </div>
      )}
    </div>
  )
}
