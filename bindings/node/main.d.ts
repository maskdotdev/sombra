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
  schema?: NodeSchema
}

export type Direction = 'out' | 'in' | 'both'

export type LiteralValue = string | number | boolean | null
export type ScalarValue = LiteralValue | Uint8Array | Buffer | Date

export type PredicateLiteral = LiteralValue | Date | Uint8Array | Buffer

export type NodeSchema = Record<string, Record<string, any>>
export type DefaultSchema = Record<string, Record<string, any>>

type TargetLabel<S extends NodeSchema> = keyof S & string

export type TargetSpec<S extends NodeSchema = DefaultSchema> =
  | TargetLabel<S>
  | {
      var?: string
      label?: TargetLabel<S> | null
    }

export type NodeLabels = string | string[]

type BaseVarProjectionField =
  | string
  | {
      var: string
      as?: string | null
    }

type BasePropProjectionField = {
  var: string
  prop: string
  as?: string | null
}

export interface PredicateBetweenOptions {
  inclusive?: [boolean, boolean]
}

export type QueryStream<T = Record<string, any>> = AsyncIterable<T>

type BindingMap<S extends NodeSchema> = Record<string, TargetLabel<S>>

type BindingLabel<
  S extends NodeSchema,
  B extends BindingMap<S>,
  V extends string,
> = V extends keyof B ? B[V] : TargetLabel<S>

type TypedPropProjectionField<
  S extends NodeSchema,
  B extends BindingMap<S>,
> = {
  [V in Extract<keyof B, string>]: {
    var: V
    prop: keyof S[BindingLabel<S, B, V>] & string
    as?: string | null
  }
}[Extract<keyof B, string>]

export type ProjectionField<
  S extends NodeSchema = DefaultSchema,
  B extends BindingMap<S> = BindingMap<S>,
> = BaseVarProjectionField | BasePropProjectionField | TypedPropProjectionField<S, B>

type ContainsNonPropField<Fields extends ReadonlyArray<ProjectionField>> =
  Exclude<Fields[number], BasePropProjectionField> extends never ? false : true

type QueryRow<HasVar extends boolean> = Record<string, HasVar extends true ? unknown : ScalarValue>

export interface ExplainOptions {
  redactLiterals?: boolean
}

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

export class PredicateBuilder<
  S extends NodeSchema = DefaultSchema,
  L extends TargetLabel<S> = TargetLabel<S>,
  Parent extends QueryBuilder<S, any, any> = QueryBuilder<S, any, any>,
> {
  eq(prop: keyof S[L] & string, value: PredicateLiteral): this
  ne(prop: keyof S[L] & string, value: PredicateLiteral): this
  lt(prop: keyof S[L] & string, value: PredicateLiteral): this
  lte(prop: keyof S[L] & string, value: PredicateLiteral): this
  le(prop: keyof S[L] & string, value: PredicateLiteral): this
  gt(prop: keyof S[L] & string, value: PredicateLiteral): this
  gte(prop: keyof S[L] & string, value: PredicateLiteral): this
  ge(prop: keyof S[L] & string, value: PredicateLiteral): this
  between(prop: keyof S[L] & string, low: PredicateLiteral, high: PredicateLiteral, opts?: PredicateBetweenOptions): this
  in(prop: keyof S[L] & string, values: ReadonlyArray<PredicateLiteral>): this
  exists(prop: keyof S[L] & string): this
  isNull(prop: keyof S[L] & string): this
  isNotNull(prop: keyof S[L] & string): this
  and(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  or(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  not(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  done(): Parent
}

type UpdateBindings<
  S extends NodeSchema,
  B extends BindingMap<S>,
  V extends string,
  L extends TargetLabel<S>,
> = Omit<B, V> & Record<V, L>

type KnownBindings<B extends Record<string, any>> = Extract<keyof B, string>

export class QueryBuilder<
  S extends NodeSchema = DefaultSchema,
  B extends BindingMap<S> = {},
  HasVar extends boolean = true,
> {
  match<L extends TargetLabel<S>>(label: L): QueryBuilder<S, B, HasVar>
  match<V extends string, L extends TargetLabel<S>>(target: { var: V; label: L }): QueryBuilder<S, UpdateBindings<S, B, V, L>, HasVar>
  match(target: TargetSpec<S>): QueryBuilder<S, B, HasVar>
  where<V extends KnownBindings<B>>(varName: V): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>
  where<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  where(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  where(varName: string, build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void): QueryBuilder<S, B, HasVar>
  where<V extends string, L extends TargetLabel<S>>(edgeType: string | null, target: { var: V; label: L }): QueryBuilder<S, UpdateBindings<S, B, V, L>, HasVar>
  where(edgeType: string | null, target: TargetSpec<S>): QueryBuilder<S, B, HasVar>
  andWhere<V extends KnownBindings<B>>(varName: V): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>
  andWhere<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  andWhere(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  andWhere(
    varName: string,
    build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  orWhere<V extends KnownBindings<B>>(varName: V): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>
  orWhere<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  orWhere(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  orWhere(
    varName: string,
    build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  direction(dir: Direction): QueryBuilder<S, B, HasVar>
  bidirectional(): QueryBuilder<S, B, HasVar>
  distinct(on?: 'nodes' | 'edges'): QueryBuilder<S, B, HasVar>
  requestId(id?: string | null): QueryBuilder<S, B, HasVar>
  select<Fields extends ReadonlyArray<ProjectionField<S, B>>>(
    fields: Fields,
  ): QueryBuilder<S, B, ContainsNonPropField<Fields> extends true ? true : false>
  explain(options?: ExplainOptions): Promise<any>
  execute(): Promise<Array<QueryRow<HasVar>>>
  stream(): QueryStream<QueryRow<HasVar>>
}

export class Database<S extends NodeSchema = DefaultSchema> {
  static open<T extends NodeSchema = DefaultSchema>(path: string, options?: ConnectOptions | null): Database<T>
  query(): QueryBuilder<S, {}, true>
  withSchema(schema: S | null): this
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
  cancelRequest(requestId: string): boolean
  createNode(labels: NodeLabels, props?: PropsInput): number | null
  updateNode(id: number, options?: { set?: PropsInput; unset?: string[] }): this
  deleteNode(id: number, cascade?: boolean): this
  createEdge(src: number, dst: number, ty: string, props?: PropsInput): number | null
  deleteEdge(id: number): this
}

export function openDatabase<S extends NodeSchema = DefaultSchema>(path: string, options?: ConnectOptions | null): Database<S>

export const native: typeof import('./index.js')
