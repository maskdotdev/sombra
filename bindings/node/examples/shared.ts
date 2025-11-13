export type DemoNode = {
  _id: number
  props: {
    name?: string
    [key: string]: unknown
  }
}

export function isDemoNode(value: unknown): value is DemoNode {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_id' in value &&
    typeof (value as { _id?: unknown })._id === 'number' &&
    'props' in value &&
    typeof (value as { props?: unknown }).props === 'object'
  )
}

