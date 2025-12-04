export interface ConnectOptions {
  createIfMissing?: boolean
  pageSize?: number
  cachePages?: number
  distinctNeighborsDefault?: boolean
  synchronous?: 'full' | 'normal' | 'off'
  commitCoalesceMs?: number
  commitMaxFrames?: number
  commitMaxCommits?: number
  groupCommitMaxWriters?: number
  groupCommitMaxFrames?: number
  groupCommitMaxWaitMs?: number
  asyncFsync?: boolean
  walSegmentBytes?: number
  walPreallocateSegments?: number
  autocheckpointMs?: number | null
  schema?: NodeSchema
}

// ============================================================================
// Error Codes and Error Classes
// ============================================================================

/**
 * Error codes returned by the Sombra database engine.
 */
export const ErrorCode: {
  readonly UNKNOWN: 'UNKNOWN'
  readonly MESSAGE: 'MESSAGE'
  readonly ANALYZER: 'ANALYZER'
  readonly JSON: 'JSON'
  readonly IO: 'IO'
  readonly CORRUPTION: 'CORRUPTION'
  readonly CONFLICT: 'CONFLICT'
  readonly SNAPSHOT_TOO_OLD: 'SNAPSHOT_TOO_OLD'
  readonly CANCELLED: 'CANCELLED'
  readonly INVALID_ARG: 'INVALID_ARG'
  readonly NOT_FOUND: 'NOT_FOUND'
  readonly CLOSED: 'CLOSED'
}

export type ErrorCodeType = typeof ErrorCode[keyof typeof ErrorCode]

/**
 * Base error class for all Sombra database errors.
 */
export class SombraError extends Error {
  /** The error code identifying the type of error */
  readonly code: ErrorCodeType
  constructor(message: string, code?: ErrorCodeType)
}

/** Error thrown when a query analysis fails. */
export class AnalyzerError extends SombraError {
  constructor(message: string)
}

/** Error thrown when JSON serialization/deserialization fails. */
export class JsonError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an I/O operation fails. */
export class IoError extends SombraError {
  constructor(message: string)
}

/** Error thrown when data corruption is detected. */
export class CorruptionError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a transaction conflict occurs (write-write conflict). */
export class ConflictError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a snapshot is too old for an MVCC read. */
export class SnapshotTooOldError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an operation is cancelled. */
export class CancelledError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an invalid argument is provided. */
export class InvalidArgError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a requested resource is not found. */
export class NotFoundError extends SombraError {
  constructor(message: string)
}

/** Error thrown when operations are attempted on a closed database. */
export class ClosedError extends SombraError {
  constructor(message: string)
}

/**
 * Parses an error message from the native layer and returns a typed error.
 * Native errors have format: "[CODE_NAME] actual message"
 */
export function wrapNativeError(err: Error | string): SombraError

// ============================================================================
// Query Types
// ============================================================================

export type Direction = 'out' | 'in' | 'both'

export interface NeighborQueryOptions {
  direction?: Direction
  edgeType?: string
  distinct?: boolean
}

export interface NeighborEntry {
  nodeId: number
  edgeId: number
  typeId: number
}

export interface BfsTraversalOptions {
  direction?: Direction
  edgeTypes?: string[]
  maxResults?: number
}

export interface BfsVisit {
  nodeId: number
  depth: number
}

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

export type Expr = { readonly __expr: unique symbol; _node: unknown }

export type ScopedExpr<
  S extends NodeSchema = DefaultSchema,
  L extends TargetLabel<S> = TargetLabel<S>,
> = Expr & { __scope?: { label: L } }

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
export interface QueryResultMeta<Row = Record<string, any>> {
  rows: Array<Row>
  request_id: string | null
  features: ReadonlyArray<unknown>
  [key: string]: unknown
}

export function and(...exprs: ReadonlyArray<Expr>): Expr
export function or(...exprs: ReadonlyArray<Expr>): Expr
export function not(expr: Expr): Expr
export function eq<K extends string, V>(prop: K, value: V): Expr
export function ne<K extends string, V>(prop: K, value: V): Expr
export function lt<K extends string, V>(prop: K, value: V): Expr
export function le<K extends string, V>(prop: K, value: V): Expr
export function gt<K extends string, V>(prop: K, value: V): Expr
export function ge<K extends string, V>(prop: K, value: V): Expr
export function between<K extends string, V>(
  prop: K,
  low: V,
  high: V,
  opts?: PredicateBetweenOptions,
): Expr
export function inList<K extends string, V>(prop: K, values: ReadonlyArray<V>): Expr
export function exists<K extends string>(prop: K): Expr
export function isNull<K extends string>(prop: K): Expr
export function isNotNull<K extends string>(prop: K): Expr

type BindingMap<S extends NodeSchema> = Record<string, TargetLabel<S>>

type BindingLabel<
  S extends NodeSchema,
  B extends BindingMap<S>,
  V extends string,
> = V extends keyof B ? B[V] : TargetLabel<S>

type SpecLabel<
  S extends NodeSchema,
  Spec,
> = Spec extends TargetLabel<S>
  ? Spec
  : Spec extends { label?: infer L }
    ? L extends TargetLabel<S>
      ? L
      : TargetLabel<S>
    : TargetLabel<S>

type UpdateBindingsFromMap<
  S extends NodeSchema,
  B extends BindingMap<S>,
  M extends Record<string, any>,
> = Omit<B, keyof M & string> &
  {
    [K in keyof M & string]: SpecLabel<S, M[K]>
  }

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

export interface MutateBatchOptions {
  batchSize?: number
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

export interface NodeScope<
  S extends NodeSchema = DefaultSchema,
  L extends TargetLabel<S> = TargetLabel<S>,
  HasVar extends boolean = true,
> {
  where(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  andWhere(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  orWhere(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  select(...keys: Array<keyof S[L] & string>): NodeScope<S, L, false>
  distinct(): NodeScope<S, L, HasVar>
  direction(dir: Direction): NodeScope<S, L, HasVar>
  bidirectional(flag?: boolean): NodeScope<S, L, HasVar>
  requestId(id?: string | null): NodeScope<S, L, HasVar>
  explain(options?: ExplainOptions): Promise<any>
  execute(withMeta: true): Promise<QueryResultMeta<QueryRow<HasVar>>>
  execute(withMeta?: false): Promise<Array<QueryRow<HasVar>>>
  stream(): QueryStream<QueryRow<HasVar>>
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
  nodes<L extends TargetLabel<S>>(label: L): NodeScope<S, L, HasVar>
  match<L extends TargetLabel<S>>(label: L): QueryBuilder<S, B, HasVar>
  match<V extends string, L extends TargetLabel<S>>(target: { var: V; label: L }): QueryBuilder<S, UpdateBindings<S, B, V, L>, HasVar>
  match<M extends Record<string, TargetSpec<S>>>(
    targets: M,
  ): QueryBuilder<S, UpdateBindingsFromMap<S, B, M>, HasVar>
  match(target: TargetSpec<S>): QueryBuilder<S, B, HasVar>
  on<V extends KnownBindings<B>>(
    varName: V,
    scope: (ctx: NodeScope<S, BindingLabel<S, B, V>, HasVar>) => unknown,
  ): QueryBuilder<S, B, HasVar>
  on(varName: string, scope: (ctx: NodeScope<S, TargetLabel<S>, HasVar>) => unknown): QueryBuilder<S, B, HasVar>
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
  execute(withMeta: true): Promise<QueryResultMeta<QueryRow<HasVar>>>
  execute(withMeta?: false): Promise<Array<QueryRow<HasVar>>>
  stream(): QueryStream<QueryRow<HasVar>>
}

export class Database<S extends NodeSchema = DefaultSchema> {
  static open<T extends NodeSchema = DefaultSchema>(path: string, options?: ConnectOptions | null): Database<T>
  
  /**
   * Closes the database, releasing all resources.
   * After calling close(), all subsequent operations on this instance will fail.
   * Calling close() multiple times is safe (subsequent calls are no-ops).
   */
  close(): void
  
  /**
   * Returns true if the database has been closed.
   */
  readonly isClosed: boolean
  
  query(): QueryBuilder<S, {}, true>
  withSchema(schema: S | null): this
  create(): CreateBuilder
  intern(name: string): number
  seedDemo(): Database
  mutate(script: MutationScript): MutationSummary
  mutateMany(ops: MutationOp[]): MutationSummary
  mutateBatched(ops: MutationOp[], options?: MutateBatchOptions | null): MutationSummary
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
  getNodeRecord(nodeId: number): Record<string, any> | null
  getEdgeRecord(edgeId: number): Record<string, any> | null
  countNodesWithLabel(label: string): number
  countEdgesWithType(ty: string): number
  listNodesWithLabel(label: string): number[]
  neighbors(nodeId: number, options?: NeighborQueryOptions): NeighborEntry[]
  getOutgoingNeighbors(nodeId: number, edgeType?: string, distinct?: boolean): number[]
  getIncomingNeighbors(nodeId: number, edgeType?: string, distinct?: boolean): number[]
  bfsTraversal(nodeId: number, maxDepth: number, options?: BfsTraversalOptions): BfsVisit[]
}

export function openDatabase<S extends NodeSchema = DefaultSchema>(path: string, options?: ConnectOptions | null): Database<S>

export const native: typeof import('./index.js')
