export interface ConnectOptions {
  createIfMissing?: boolean
  pageSize?: number
  cachePages?: number
  distinctNeighborsDefault?: boolean
  synchronous?: 'full' | 'normal' | 'off'
  commitCoalesceMs?: number
  commitMaxFrames?: number
  commitMaxCommits?: number
  autocheckpointMs?: number | null
}

export type Direction = 'out' | 'in' | 'both'

export type LiteralValue = string | number | boolean | null

export type TargetSpec =
  | string
  | {
      var?: string
      label?: string | null
    }

export type NodeLabels = string | string[]

export type ProjectionField =
  | string
  | {
      var: string
      as?: string | null
    }
  | {
      expr: string
      as: string
    }

export type QueryStream<T = Record<string, any>> = AsyncIterable<T>

export interface MutationSummary {
  createdNodes?: number[]
  createdEdges?: number[]
  updatedNodes?: number
  updatedEdges?: number
  deletedNodes?: number
  deletedEdges?: number
}

export interface CreateSummary {
  nodes: number[]
  edges: number[]
  aliases: Record<string, number>
  alias(name: string): number | undefined
}

export type PropsInput = Record<string, LiteralValue | null>

export type MutationOp =
  | { op: 'createNode'; labels: string[]; props?: PropsInput }
  | { op: 'updateNode'; id: number; set?: PropsInput; unset?: string[] }
  | { op: 'deleteNode'; id: number; cascade?: boolean }
  | { op: 'createEdge'; src: number; dst: number; ty: string; props?: PropsInput }
  | { op: 'updateEdge'; id: number; set?: PropsInput; unset?: string[] }
  | { op: 'deleteEdge'; id: number }

export interface MutationScript {
  ops: MutationOp[]
}

export interface MutationBuilder {
  queue(op: MutationOp): MutationBuilder
  createNode(labels: NodeLabels, props?: PropsInput): MutationBuilder
  updateNode(id: number, options?: { set?: PropsInput; unset?: string[] }): MutationBuilder
  deleteNode(id: number, cascade?: boolean): MutationBuilder
  createEdge(src: number, dst: number, ty: string, props?: PropsInput): MutationBuilder
  updateEdge(id: number, options?: { set?: PropsInput; unset?: string[] }): MutationBuilder
  deleteEdge(id: number): MutationBuilder
}

export interface CreateBuilder {
  node(labels: NodeLabels, props?: PropsInput, alias?: string): CreateNodeHandle
  nodeWithAlias(labels: NodeLabels, alias: string): CreateNodeHandle
  nodeWithAlias(labels: NodeLabels, props: PropsInput, alias: string): CreateNodeHandle
  edge(
    src: CreateNodeHandle | string | number,
    ty: string,
    dst: CreateNodeHandle | string | number,
    props?: PropsInput,
  ): CreateBuilder
  execute(): CreateSummary
}

export interface CreateNodeHandle extends CreateBuilder {}

export class QueryBuilder {
  match(target: TargetSpec): this
  where(edgeType: string | null, target: TargetSpec): this
  whereProp(
    varName: string,
    prop: string,
    op: '=' | 'eq' | 'between' | '>' | '>=' | '<' | '<=' | 'gt' | 'ge' | 'lt' | 'le',
    value: LiteralValue,
    value2?: LiteralValue,
  ): this
  direction(dir: Direction): this
  bidirectional(): this
  distinct(on?: 'nodes' | 'edges'): this
  select(fields: ProjectionField[]): this
  explain(): Promise<any>
  execute(): Promise<Array<Record<string, any>>>
  stream(): QueryStream
}

export class Database {
  static open(path: string, options?: ConnectOptions | null): Database
  query(): QueryBuilder
  create(): CreateBuilder
  intern(name: string): number
  seedDemo(): Database
  mutate(script: MutationScript): MutationSummary
  mutateMany(ops: MutationOp[]): MutationSummary
  transaction<T>(fn: (tx: MutationBuilder) => T | Promise<T>): Promise<{
    summary: MutationSummary
    result: T
  }>
  pragma(name: string, value?: any): any
  createNode(labels: NodeLabels, props?: PropsInput): number | null
  updateNode(id: number, options?: { set?: PropsInput; unset?: string[] }): this
  deleteNode(id: number, cascade?: boolean): this
  createEdge(src: number, dst: number, ty: string, props?: PropsInput): number | null
  deleteEdge(id: number): this
}

export function openDatabase(path: string, options?: ConnectOptions | null): Database

export const native: typeof import('./index.js')
