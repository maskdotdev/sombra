export type GraphEntity = {
  _id: number
  props?: Record<string, unknown> | null
  labels?: string[] | null
}

export type GraphNodeDatum = {
  id: number
  label: string
  props: Record<string, unknown>
  labels: string[]
  groups: string[]
  original: GraphEntity
}

export type GraphEdgeDatum = {
  id: string
  source: number
  target: number
  sourceColumn: string
  targetColumn: string
}

const PRIORITY_PROPS = ["name", "title", "label", "email", "handle"]

export function isGraphEntity(value: unknown): value is GraphEntity {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false
  }
  if (!("_id" in value)) {
    return false
  }
  const maybeId = (value as { _id?: unknown })._id
  return typeof maybeId === "number"
}

export function deriveNodeLabel(entity: GraphEntity): string {
  const props = entity.props ?? {}
  for (const key of PRIORITY_PROPS) {
    const candidate = props[key]
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate
    }
  }
  for (const value of Object.values(props)) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value
    }
  }
  return `#${entity._id}`
}
