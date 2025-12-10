/**
 * Configuration options for opening a Sombra database connection.
 */
export interface ConnectOptions {
  /** Create the database file if it doesn't exist (default: true) */
  createIfMissing?: boolean
  /** Database page size in bytes */
  pageSize?: number
  /** Number of pages to cache in memory */
  cachePages?: number
  /** Default distinct behavior for neighbor queries */
  distinctNeighborsDefault?: boolean
  /** Durability mode: 'full' (default), 'normal', or 'off' */
  synchronous?: 'full' | 'normal' | 'off'
  /** Milliseconds to coalesce commits */
  commitCoalesceMs?: number
  /** Maximum WAL frames before forcing a commit */
  commitMaxFrames?: number
  /** Maximum commits to coalesce */
  commitMaxCommits?: number
  /** Maximum concurrent writers for group commit */
  groupCommitMaxWriters?: number
  /** Maximum frames for group commit */
  groupCommitMaxFrames?: number
  /** Maximum wait time for group commit in milliseconds */
  groupCommitMaxWaitMs?: number
  /** Enable async fsync for better write throughput */
  asyncFsync?: boolean
  /** WAL segment size in bytes (16MB default) */
  walSegmentBytes?: number
  /** Number of WAL segments to preallocate */
  walPreallocateSegments?: number
  /** Auto-checkpoint interval in milliseconds (null to disable) */
  autocheckpointMs?: number | null
  /** Optional runtime schema for validation */
  schema?: NodeSchema
}

// ============================================================================
// Error Codes and Error Classes
// ============================================================================

/**
 * Error codes returned by the Sombra database engine.
 * Use these codes for programmatic error handling.
 *
 * @example
 * ```ts
 * import { ErrorCode, SombraError } from 'sombradb'
 *
 * try {
 *   await db.query().nodes('User').execute()
 * } catch (err) {
 *   if (err instanceof SombraError && err.code === ErrorCode.CONFLICT) {
 *     // Handle write-write conflict
 *   }
 * }
 * ```
 */
export const ErrorCode: {
  /** Unknown error type */
  readonly UNKNOWN: 'UNKNOWN'
  /** Generic message error */
  readonly MESSAGE: 'MESSAGE'
  /** Query analysis/parsing failed */
  readonly ANALYZER: 'ANALYZER'
  /** JSON serialization/deserialization error */
  readonly JSON: 'JSON'
  /** File/network I/O error */
  readonly IO: 'IO'
  /** Data corruption detected */
  readonly CORRUPTION: 'CORRUPTION'
  /** Write-write transaction conflict */
  readonly CONFLICT: 'CONFLICT'
  /** MVCC snapshot expired */
  readonly SNAPSHOT_TOO_OLD: 'SNAPSHOT_TOO_OLD'
  /** Operation was cancelled */
  readonly CANCELLED: 'CANCELLED'
  /** Invalid argument provided */
  readonly INVALID_ARG: 'INVALID_ARG'
  /** Resource not found */
  readonly NOT_FOUND: 'NOT_FOUND'
  /** Database is closed */
  readonly CLOSED: 'CLOSED'
}

/** Union type of all error code values */
export type ErrorCodeType = (typeof ErrorCode)[keyof typeof ErrorCode]

/**
 * Base error class for all Sombra database errors.
 * All errors thrown by the database are instances of this class or its subclasses.
 *
 * @example
 * ```ts
 * try {
 *   await db.query().nodes('User').execute()
 * } catch (err) {
 *   if (err instanceof SombraError) {
 *     console.log(`Error code: ${err.code}, message: ${err.message}`)
 *   }
 * }
 * ```
 */
export class SombraError extends Error {
  /** The error code identifying the type of error */
  readonly code: ErrorCodeType
  constructor(message: string, code?: ErrorCodeType)
}

/** Error thrown when a query analysis or parsing fails. */
export class AnalyzerError extends SombraError {
  constructor(message: string)
}

/** Error thrown when JSON serialization/deserialization fails. */
export class JsonError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an I/O operation fails (file, network, etc.). */
export class IoError extends SombraError {
  constructor(message: string)
}

/** Error thrown when data corruption is detected in the database. */
export class CorruptionError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a transaction conflict occurs (write-write conflict). Retry the transaction. */
export class ConflictError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a snapshot is too old for an MVCC read. The transaction took too long. */
export class SnapshotTooOldError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an operation is cancelled via cancelRequest(). */
export class CancelledError extends SombraError {
  constructor(message: string)
}

/** Error thrown when an invalid argument is provided to a function. */
export class InvalidArgError extends SombraError {
  constructor(message: string)
}

/** Error thrown when a requested resource (node, edge, etc.) is not found. */
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
 *
 * @param err - The error from the native layer
 * @returns A typed SombraError instance
 */
export function wrapNativeError(err: Error | string): SombraError

// ============================================================================
// Query Types
// ============================================================================

/** Direction for edge traversal: outgoing, incoming, or both */
export type Direction = 'out' | 'in' | 'both'

/**
 * Options for neighbor queries.
 */
export interface NeighborQueryOptions {
  /** Direction to traverse: 'out', 'in', or 'both' (default: 'out') */
  direction?: Direction
  /** Filter by edge type (optional) */
  edgeType?: string
  /** Deduplicate results (default: true) */
  distinct?: boolean
}

/**
 * A neighbor entry returned from neighbor queries.
 */
export interface NeighborEntry {
  /** The ID of the neighboring node */
  nodeId: number
  /** The ID of the connecting edge */
  edgeId: number
  /** The type ID of the edge (internal) */
  typeId: number
}

/**
 * Options for BFS (breadth-first search) traversal.
 */
export interface BfsTraversalOptions {
  /** Direction to traverse: 'out', 'in', or 'both' */
  direction?: Direction
  /** Filter by edge types (optional) */
  edgeTypes?: string[]
  /** Maximum number of results to return (optional) */
  maxResults?: number
}

/**
 * A visit record from BFS traversal.
 */
export interface BfsVisit {
  /** The ID of the visited node */
  nodeId: number
  /** The depth at which this node was visited */
  depth: number
}

/** Primitive literal values that can be stored in properties */
export type LiteralValue = string | number | boolean | null

/** Scalar values including binary and date types */
export type ScalarValue = LiteralValue | Uint8Array | Buffer | Date

/** Values that can be used in predicates */
export type PredicateLiteral = LiteralValue | Date | Uint8Array | Buffer

/** Schema definition mapping label names to property definitions */
export type NodeSchema = Record<string, Record<string, any>>

/** Default schema type when no schema is provided */
export type DefaultSchema = Record<string, Record<string, any>>

type TargetLabel<S extends NodeSchema> = keyof S & string

/**
 * Target specification for match clauses.
 * Can be a simple label string or an object with var and label.
 */
export type TargetSpec<S extends NodeSchema = DefaultSchema> =
  | TargetLabel<S>
  | {
      /** Variable name to bind this match to */
      var?: string
      /** Label to filter by (null for any label) */
      label?: TargetLabel<S> | null
    }

/** Opaque expression type for type-safe predicates */
export type Expr = { readonly __expr: unique symbol; _node: unknown }

/** Scoped expression type with label information */
export type ScopedExpr<S extends NodeSchema = DefaultSchema, L extends TargetLabel<S> = TargetLabel<S>> = Expr & {
  __scope?: { label: L }
}

/** Node labels can be a single string or array of strings */
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

/**
 * Options for the between() predicate.
 */
export interface PredicateBetweenOptions {
  /** Inclusive flags for [low, high] bounds. Default: [true, true] */
  inclusive?: [boolean, boolean]
}

/**
 * Async iterable stream for query results.
 * Supports early termination via close() and resource disposal.
 */
export interface QueryStream<T = Record<string, any>> extends AsyncIterable<T> {
  /** Closes the stream and releases resources */
  close(): void
  /** Returns from the async iterator, closing the stream */
  return?(value?: unknown): Promise<IteratorResult<T>>
  /** Symbol.dispose support for explicit resource management */
  [Symbol.dispose]?(): void
  /** Symbol.asyncDispose support for explicit resource management */
  [Symbol.asyncDispose]?(): Promise<void>
}

/**
 * Query result with metadata.
 * Returned when execute(true) is called.
 */
export interface QueryResultMeta<Row = Record<string, any>> {
  /** The result rows */
  rows: Array<Row>
  /** The request ID if one was set */
  request_id: string | null
  /** Query features used */
  features: ReadonlyArray<unknown>
  /** Additional metadata */
  [key: string]: unknown
}

// ============================================================================
// Predicate Functions
// ============================================================================

/**
 * Creates a logical AND expression combining multiple predicates.
 * @param exprs - One or more expressions to AND together
 * @returns Combined expression
 * @example
 * ```ts
 * and(eq('name', 'Ada'), gt('age', 18))
 * ```
 */
export function and(...exprs: ReadonlyArray<Expr>): Expr

/**
 * Creates a logical OR expression combining multiple predicates.
 * @param exprs - One or more expressions to OR together
 * @returns Combined expression
 * @example
 * ```ts
 * or(eq('status', 'active'), eq('status', 'pending'))
 * ```
 */
export function or(...exprs: ReadonlyArray<Expr>): Expr

/**
 * Creates a logical NOT expression negating a predicate.
 * @param expr - The expression to negate
 * @returns Negated expression
 * @example
 * ```ts
 * not(eq('deleted', true))
 * ```
 */
export function not(expr: Expr): Expr

/**
 * Creates an equality predicate (prop == value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Equality expression
 * @example
 * ```ts
 * eq('name', 'Ada Lovelace')
 * ```
 */
export function eq<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a not-equal predicate (prop != value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Not-equal expression
 */
export function ne<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a less-than predicate (prop < value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Less-than expression
 */
export function lt<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a less-than-or-equal predicate (prop <= value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Less-than-or-equal expression
 */
export function le<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a greater-than predicate (prop > value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Greater-than expression
 */
export function gt<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a greater-than-or-equal predicate (prop >= value).
 * @param prop - Property name
 * @param value - Value to compare
 * @returns Greater-than-or-equal expression
 */
export function ge<K extends string, V>(prop: K, value: V): Expr

/**
 * Creates a range predicate (low <= prop <= high by default).
 * @param prop - Property name
 * @param low - Lower bound
 * @param high - Upper bound
 * @param opts - Options for inclusive/exclusive bounds
 * @returns Between expression
 * @example
 * ```ts
 * // Inclusive on both ends [18, 65]
 * between('age', 18, 65)
 *
 * // Exclusive on high end [18, 65)
 * between('age', 18, 65, { inclusive: [true, false] })
 * ```
 */
export function between<K extends string, V>(prop: K, low: V, high: V, opts?: PredicateBetweenOptions): Expr

/**
 * Creates an IN predicate (prop IN values).
 * @param prop - Property name
 * @param values - Array of values to check against
 * @returns In-list expression
 * @example
 * ```ts
 * inList('status', ['active', 'pending', 'review'])
 * ```
 */
export function inList<K extends string, V>(prop: K, values: ReadonlyArray<V>): Expr

/**
 * Creates an EXISTS predicate (property exists on the node).
 * @param prop - Property name
 * @returns Exists expression
 */
export function exists<K extends string>(prop: K): Expr

/**
 * Creates an IS NULL predicate (prop IS NULL).
 * @param prop - Property name
 * @returns Is-null expression
 */
export function isNull<K extends string>(prop: K): Expr

/**
 * Creates an IS NOT NULL predicate (prop IS NOT NULL).
 * @param prop - Property name
 * @returns Is-not-null expression
 */
export function isNotNull<K extends string>(prop: K): Expr

type BindingMap<S extends NodeSchema> = Record<string, TargetLabel<S>>

type BindingLabel<S extends NodeSchema, B extends BindingMap<S>, V extends string> = V extends keyof B
  ? B[V]
  : TargetLabel<S>

type SpecLabel<S extends NodeSchema, Spec> =
  Spec extends TargetLabel<S>
    ? Spec
    : Spec extends { label?: infer L }
      ? L extends TargetLabel<S>
        ? L
        : TargetLabel<S>
      : TargetLabel<S>

type UpdateBindingsFromMap<S extends NodeSchema, B extends BindingMap<S>, M extends Record<string, any>> = Omit<
  B,
  keyof M & string
> & {
  [K in keyof M & string]: SpecLabel<S, M[K]>
}

type TypedPropProjectionField<S extends NodeSchema, B extends BindingMap<S>> = {
  [V in Extract<keyof B, string>]: {
    var: V
    prop: keyof S[BindingLabel<S, B, V>] & string
    as?: string | null
  }
}[Extract<keyof B, string>]

/** Projection field type for select() */
export type ProjectionField<S extends NodeSchema = DefaultSchema, B extends BindingMap<S> = BindingMap<S>> =
  | BaseVarProjectionField
  | BasePropProjectionField
  | TypedPropProjectionField<S, B>

type ContainsNonPropField<Fields extends ReadonlyArray<ProjectionField>> =
  Exclude<Fields[number], BasePropProjectionField> extends never ? false : true

type QueryRow<HasVar extends boolean> = Record<string, HasVar extends true ? unknown : ScalarValue>

/**
 * Options for query explanation.
 */
export interface ExplainOptions {
  /** Redact literal values in the plan for safe logging */
  redactLiterals?: boolean
}

/**
 * Summary of a mutation operation.
 */
export interface MutationSummary {
  /** IDs of created nodes */
  createdNodes?: number[]
  /** IDs of created edges */
  createdEdges?: number[]
  /** Count of updated nodes */
  updatedNodes?: number
  /** Count of updated edges */
  updatedEdges?: number
  /** Count of deleted nodes */
  deletedNodes?: number
  /** Count of deleted edges */
  deletedEdges?: number
}

/**
 * Options for batched mutations.
 */
export interface MutateBatchOptions {
  /** Number of operations per batch (default: 1024) */
  batchSize?: number
}

/**
 * Summary returned from the create() builder.
 */
export interface CreateSummary {
  /** IDs of created nodes (in order of creation) */
  nodes: number[]
  /** IDs of created edges (in order of creation) */
  edges: number[]
  /** Map of alias names to node IDs */
  aliases: Record<string, number>
  /**
   * Look up a node ID by alias name.
   * @param name - The alias name
   * @returns The node ID, or undefined if not found
   */
  alias(name: string): number | undefined
}

/** Property input type for mutations */
export type PropsInput = Record<string, LiteralValue | null>

/**
 * A single mutation operation.
 */
export type MutationOp =
  | { op: 'createNode'; labels: string[]; props?: PropsInput }
  | { op: 'updateNode'; id: number; set?: PropsInput; unset?: string[] }
  | { op: 'deleteNode'; id: number; cascade?: boolean }
  | { op: 'createEdge'; src: number; dst: number; ty: string; props?: PropsInput }
  | { op: 'updateEdge'; id: number; set?: PropsInput; unset?: string[] }
  | { op: 'deleteEdge'; id: number }

/**
 * A mutation script containing multiple operations.
 */
export interface MutationScript {
  ops: MutationOp[]
}

/**
 * Builder for queuing mutation operations in a transaction.
 */
export interface MutationBuilder {
  /** Queue a raw mutation operation */
  queue(op: MutationOp): MutationBuilder
  /** Queue a createNode operation */
  createNode(labels: NodeLabels, props?: PropsInput): MutationBuilder
  /** Queue an updateNode operation */
  updateNode(id: number, options?: { set?: PropsInput; unset?: string[] }): MutationBuilder
  /** Queue a deleteNode operation */
  deleteNode(id: number, cascade?: boolean): MutationBuilder
  /** Queue a createEdge operation */
  createEdge(src: number, dst: number, ty: string, props?: PropsInput): MutationBuilder
  /** Queue an updateEdge operation */
  updateEdge(id: number, options?: { set?: PropsInput; unset?: string[] }): MutationBuilder
  /** Queue a deleteEdge operation */
  deleteEdge(id: number): MutationBuilder
}

/**
 * Builder for bulk node and edge creation.
 * All operations are batched into a single transaction on execute().
 */
export interface CreateBuilder {
  /**
   * Add a node to the batch.
   * @param labels - Node labels
   * @param props - Node properties
   * @param alias - Optional alias for referencing in edges
   * @returns A handle for chaining and edge creation
   */
  node(labels: NodeLabels, props?: PropsInput, alias?: string): CreateNodeHandle

  /**
   * Add a node with an alias.
   * @param labels - Node labels
   * @param alias - Alias name for this node
   * @returns A handle for chaining and edge creation
   */
  nodeWithAlias(labels: NodeLabels, alias: string): CreateNodeHandle

  /**
   * Add a node with properties and an alias.
   * @param labels - Node labels
   * @param props - Node properties
   * @param alias - Alias name for this node
   * @returns A handle for chaining and edge creation
   */
  nodeWithAlias(labels: NodeLabels, props: PropsInput, alias: string): CreateNodeHandle

  /**
   * Add an edge between nodes.
   * @param src - Source node (handle, alias string, or node ID)
   * @param ty - Edge type
   * @param dst - Destination node (handle, alias string, or node ID)
   * @param props - Edge properties
   * @returns This builder for chaining
   */
  edge(
    src: CreateNodeHandle | string | number,
    ty: string,
    dst: CreateNodeHandle | string | number,
    props?: PropsInput,
  ): CreateBuilder

  /**
   * Execute all batched operations in a single transaction.
   * @returns Summary with created node and edge IDs
   */
  execute(): CreateSummary
}

/**
 * Handle returned from node() for chaining and edge creation.
 * Extends CreateBuilder with access to the node's batch index.
 */
export interface CreateNodeHandle extends CreateBuilder {}

/**
 * Fluent predicate builder for complex where clauses.
 *
 * @example
 * ```ts
 * db.query()
 *   .match({ u: 'User' })
 *   .where('u', (b) =>
 *     b.eq('status', 'active')
 *      .gt('age', 18)
 *      .or((nested) => nested.eq('role', 'admin').eq('role', 'moderator'))
 *   )
 *   .execute()
 * ```
 */
export class PredicateBuilder<
  S extends NodeSchema = DefaultSchema,
  L extends TargetLabel<S> = TargetLabel<S>,
  Parent extends QueryBuilder<S, any, any> = QueryBuilder<S, any, any>,
> {
  /** Add an equality predicate */
  eq(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a not-equal predicate */
  ne(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a less-than predicate */
  lt(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a less-than-or-equal predicate (alias for le) */
  lte(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a less-than-or-equal predicate */
  le(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a greater-than predicate */
  gt(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a greater-than-or-equal predicate (alias for ge) */
  gte(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a greater-than-or-equal predicate */
  ge(prop: keyof S[L] & string, value: PredicateLiteral): this
  /** Add a range predicate */
  between(
    prop: keyof S[L] & string,
    low: PredicateLiteral,
    high: PredicateLiteral,
    opts?: PredicateBetweenOptions,
  ): this
  /** Add an IN predicate */
  in(prop: keyof S[L] & string, values: ReadonlyArray<PredicateLiteral>): this
  /** Add an EXISTS predicate */
  exists(prop: keyof S[L] & string): this
  /** Add an IS NULL predicate */
  isNull(prop: keyof S[L] & string): this
  /** Add an IS NOT NULL predicate */
  isNotNull(prop: keyof S[L] & string): this
  /** Start a nested AND group */
  and(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  /** Start a nested OR group */
  or(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  /** Start a nested NOT group */
  not(callback: (builder: PredicateBuilder<S, L, Parent>) => void): this
  /** Finish building predicates and return to parent builder */
  done(): Parent
}

/**
 * Scoped query context for a single node variable.
 * Provides a simplified API when querying a single node type.
 */
export interface NodeScope<
  S extends NodeSchema = DefaultSchema,
  L extends TargetLabel<S> = TargetLabel<S>,
  HasVar extends boolean = true,
> {
  /** Add a WHERE predicate (AND with existing) */
  where(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  /** Add an AND WHERE predicate */
  andWhere(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  /** Add an OR WHERE predicate */
  orWhere(expr: ScopedExpr<S, L> | ((scope: NodeScope<S, L, HasVar>) => ScopedExpr<S, L>)): NodeScope<S, L, HasVar>
  /** Select specific properties to return */
  select(...keys: Array<keyof S[L] & string>): NodeScope<S, L, false>
  /** Enable distinct results */
  distinct(): NodeScope<S, L, HasVar>
  /** Set edge traversal direction */
  direction(dir: Direction): NodeScope<S, L, HasVar>
  /** Enable bidirectional traversal */
  bidirectional(flag?: boolean): NodeScope<S, L, HasVar>
  /** Set a request ID for cancellation support */
  requestId(id?: string | null): NodeScope<S, L, HasVar>
  /** Get the query execution plan */
  explain(options?: ExplainOptions): Promise<any>
  /** Execute and return results with metadata */
  execute(withMeta: true): Promise<QueryResultMeta<QueryRow<HasVar>>>
  /** Execute and return just the result rows */
  execute(withMeta?: false): Promise<Array<QueryRow<HasVar>>>
  /** Execute and return a streaming iterator */
  stream(): QueryStream<QueryRow<HasVar>>
}

type UpdateBindings<S extends NodeSchema, B extends BindingMap<S>, V extends string, L extends TargetLabel<S>> = Omit<
  B,
  V
> &
  Record<V, L>

type KnownBindings<B extends Record<string, any>> = Extract<keyof B, string>

/**
 * Fluent query builder for graph queries.
 *
 * @example
 * ```ts
 * // Simple query
 * const users = await db.query().nodes('User').execute()
 *
 * // Complex multi-variable query
 * const results = await db.query()
 *   .match({ u: 'User', p: 'Post' })
 *   .where('AUTHORED', { var: 'p' })
 *   .on('u', (scope) => scope.where(eq('name', 'Ada')))
 *   .select([{ var: 'u', prop: 'name', as: 'author' }])
 *   .execute()
 * ```
 */
export class QueryBuilder<
  S extends NodeSchema = DefaultSchema,
  B extends BindingMap<S> = {},
  HasVar extends boolean = true,
> {
  /**
   * Start a simple node query for a single label.
   * @param label - The node label to query
   * @returns A NodeScope for the query
   */
  nodes<L extends TargetLabel<S>>(label: L): NodeScope<S, L, HasVar>

  /**
   * Add a match clause for a node label.
   * @param label - The node label to match
   * @returns This builder for chaining
   */
  match<L extends TargetLabel<S>>(label: L): QueryBuilder<S, B, HasVar>

  /**
   * Add a match clause with explicit variable binding.
   * @param target - Object with var and label
   * @returns This builder with updated bindings
   */
  match<V extends string, L extends TargetLabel<S>>(target: {
    var: V
    label: L
  }): QueryBuilder<S, UpdateBindings<S, B, V, L>, HasVar>

  /**
   * Add multiple match clauses at once.
   * @param targets - Map of variable names to target specs
   * @returns This builder with updated bindings
   */
  match<M extends Record<string, TargetSpec<S>>>(targets: M): QueryBuilder<S, UpdateBindingsFromMap<S, B, M>, HasVar>

  /**
   * Add a match clause.
   * @param target - Target specification
   * @returns This builder for chaining
   */
  match(target: TargetSpec<S>): QueryBuilder<S, B, HasVar>

  /**
   * Apply operations to a specific bound variable.
   * @param varName - The variable name to operate on
   * @param scope - Callback receiving a NodeScope
   * @returns This builder for chaining
   */
  on<V extends KnownBindings<B>>(
    varName: V,
    scope: (ctx: NodeScope<S, BindingLabel<S, B, V>, HasVar>) => unknown,
  ): QueryBuilder<S, B, HasVar>
  on(varName: string, scope: (ctx: NodeScope<S, TargetLabel<S>, HasVar>) => unknown): QueryBuilder<S, B, HasVar>

  /**
   * Add WHERE predicates for a variable (returns PredicateBuilder).
   * @param varName - The variable to filter
   * @returns A PredicateBuilder for adding predicates
   */
  where<V extends KnownBindings<B>>(varName: V): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>

  /**
   * Add WHERE predicates for a variable with callback.
   * @param varName - The variable to filter
   * @param build - Callback to build predicates
   * @returns This builder for chaining
   */
  where<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  where(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  where(
    varName: string,
    build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>

  /**
   * Add an edge traversal clause.
   * @param edgeType - Edge type to traverse (null for any)
   * @param target - Target node specification
   * @returns This builder with updated bindings
   */
  where<V extends string, L extends TargetLabel<S>>(
    edgeType: string | null,
    target: { var: V; label: L },
  ): QueryBuilder<S, UpdateBindings<S, B, V, L>, HasVar>
  where(edgeType: string | null, target: TargetSpec<S>): QueryBuilder<S, B, HasVar>

  /** Add AND WHERE predicates (returns PredicateBuilder) */
  andWhere<V extends KnownBindings<B>>(
    varName: V,
  ): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>
  /** Add AND WHERE predicates with callback */
  andWhere<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  andWhere(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  andWhere(
    varName: string,
    build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>

  /** Add OR WHERE predicates (returns PredicateBuilder) */
  orWhere<V extends KnownBindings<B>>(
    varName: V,
  ): PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>
  /** Add OR WHERE predicates with callback */
  orWhere<V extends KnownBindings<B>>(
    varName: V,
    build: (builder: PredicateBuilder<S, BindingLabel<S, B, V>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>
  orWhere(varName: string): PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>
  orWhere(
    varName: string,
    build: (builder: PredicateBuilder<S, TargetLabel<S>, QueryBuilder<S, B, HasVar>>) => void,
  ): QueryBuilder<S, B, HasVar>

  /**
   * Set edge traversal direction for subsequent where clauses.
   * @param dir - 'out', 'in', or 'both'
   * @returns This builder for chaining
   */
  direction(dir: Direction): QueryBuilder<S, B, HasVar>

  /** Enable bidirectional edge traversal */
  bidirectional(): QueryBuilder<S, B, HasVar>

  /**
   * Enable distinct results.
   * @param on - What to deduplicate: 'nodes' or 'edges'
   * @returns This builder for chaining
   */
  distinct(on?: 'nodes' | 'edges'): QueryBuilder<S, B, HasVar>

  /**
   * Set a request ID for cancellation support.
   * @param id - Unique identifier for this request
   * @returns This builder for chaining
   */
  requestId(id?: string | null): QueryBuilder<S, B, HasVar>

  /**
   * Select which fields to return in results.
   * @param fields - Array of projection fields
   * @returns This builder for chaining
   */
  select<Fields extends ReadonlyArray<ProjectionField<S, B>>>(
    fields: Fields,
  ): QueryBuilder<S, B, ContainsNonPropField<Fields> extends true ? true : false>

  /**
   * Get the query execution plan without executing.
   * @param options - Explanation options
   * @returns The execution plan
   */
  explain(options?: ExplainOptions): Promise<any>

  /**
   * Execute the query and return results with metadata.
   * @param withMeta - true to include metadata
   * @returns Results with metadata
   */
  execute(withMeta: true): Promise<QueryResultMeta<QueryRow<HasVar>>>

  /**
   * Execute the query and return just the rows.
   * @param withMeta - false or omitted
   * @returns Array of result rows
   */
  execute(withMeta?: false): Promise<Array<QueryRow<HasVar>>>

  /**
   * Execute the query and return a streaming iterator.
   * @returns Async iterable stream of rows
   */
  stream(): QueryStream<QueryRow<HasVar>>
}

/**
 * Main database class for interacting with a Sombra graph database.
 *
 * @example
 * ```ts
 * // Open a database
 * const db = Database.open('/path/to/db')
 *
 * // Create some data
 * const userId = db.createNode('User', { name: 'Ada' })
 *
 * // Query the data
 * const users = await db.query().nodes('User').execute()
 *
 * // Close when done
 * db.close()
 * ```
 *
 * @example
 * ```ts
 * // Using with explicit resource management
 * {
 *   using db = Database.open('/path/to/db')
 *   // db.close() called automatically at end of block
 * }
 * ```
 */
export class Database<S extends NodeSchema = DefaultSchema> {
  /**
   * Opens a database at the specified path.
   * @param path - Path to the database file, or ':memory:' for in-memory
   * @param options - Connection options
   * @returns A new Database instance
   */
  static open<T extends NodeSchema = DefaultSchema>(path: string, options?: ConnectOptions | null): Database<T>

  /**
   * Closes the database, releasing all resources.
   * After calling close(), all subsequent operations on this instance will fail.
   * Calling close() multiple times is safe (subsequent calls are no-ops).
   */
  close(): void

  /** Symbol.dispose support for explicit resource management */
  [Symbol.dispose]?(): void

  /** Symbol.asyncDispose support for explicit resource management */
  [Symbol.asyncDispose]?(): Promise<void>

  /**
   * Returns true if the database has been closed.
   */
  readonly isClosed: boolean

  /**
   * Create a new query builder.
   * @returns A QueryBuilder instance
   */
  query(): QueryBuilder<S, {}, true>

  /**
   * Set or update the runtime schema for validation.
   * @param schema - The schema to use, or null to clear
   * @returns This database for chaining
   */
  withSchema(schema: S | null): this

  /**
   * Create a bulk creation builder.
   * @returns A CreateBuilder for batching node/edge creation
   */
  create(): CreateBuilder

  /**
   * Intern a string name and return its ID.
   * @param name - The name to intern
   * @returns The interned ID
   */
  intern(name: string): number

  /**
   * Seed the database with demo data for testing.
   * @returns This database for chaining
   */
  seedDemo(): Database

  /**
   * Execute a mutation script.
   * @param script - The mutation script to execute
   * @returns Summary of the mutation
   */
  mutate(script: MutationScript): MutationSummary

  /**
   * Execute multiple mutation operations.
   * @param ops - Array of mutation operations
   * @returns Summary of the mutation
   */
  mutateMany(ops: MutationOp[]): MutationSummary

  /**
   * Execute mutation operations in batches.
   * @param ops - Array of mutation operations
   * @param options - Batch options
   * @returns Combined summary of all batches
   */
  mutateBatched(ops: MutationOp[], options?: MutateBatchOptions | null): MutationSummary

  /**
   * Execute operations in a transaction.
   * @param fn - Callback that queues operations on the transaction
   * @returns The mutation summary and callback result
   */
  transaction<T>(fn: (tx: MutationBuilder) => T | Promise<T>): Promise<{
    summary: MutationSummary
    result: T
  }>

  /**
   * Get or set a database pragma value.
   * @param name - The pragma name
   * @param value - The value to set (omit to get current value)
   * @returns The pragma value
   */
  pragma(name: string, value?: any): any

  /**
   * Cancel a running request by ID.
   * @param requestId - The request ID to cancel
   * @returns true if cancellation was requested
   */
  cancelRequest(requestId: string): boolean

  /**
   * Create a node with labels and properties.
   * @param labels - Node labels (string or array)
   * @param props - Node properties
   * @returns The created node ID, or null on failure
   */
  createNode(labels: NodeLabels, props?: PropsInput): number | null

  /**
   * Update a node's properties.
   * @param id - The node ID to update
   * @param options - Properties to set and/or unset
   * @returns This database for chaining
   */
  updateNode(id: number, options?: { set?: PropsInput; unset?: string[] }): this

  /**
   * Delete a node.
   * @param id - The node ID to delete
   * @param cascade - If true, also delete connected edges
   * @returns This database for chaining
   */
  deleteNode(id: number, cascade?: boolean): this

  /**
   * Create an edge between two nodes.
   * @param src - Source node ID
   * @param dst - Destination node ID
   * @param ty - Edge type
   * @param props - Edge properties
   * @returns The created edge ID, or null on failure
   */
  createEdge(src: number, dst: number, ty: string, props?: PropsInput): number | null

  /**
   * Delete an edge.
   * @param id - The edge ID to delete
   * @returns This database for chaining
   */
  deleteEdge(id: number): this

  /**
   * Get a node's full record.
   * @param nodeId - The node ID
   * @returns The node record, or null if not found
   */
  getNodeRecord(nodeId: number): Record<string, any> | null

  /**
   * Get an edge's full record.
   * @param edgeId - The edge ID
   * @returns The edge record, or null if not found
   */
  getEdgeRecord(edgeId: number): Record<string, any> | null

  /**
   * Count nodes with a specific label.
   * @param label - The label to count
   * @returns The count
   */
  countNodesWithLabel(label: string): number

  /**
   * Count edges with a specific type.
   * @param ty - The edge type to count
   * @returns The count
   */
  countEdgesWithType(ty: string): number

  /**
   * List all node IDs with a specific label.
   * @param label - The label to list
   * @returns Array of node IDs
   */
  listNodesWithLabel(label: string): number[]

  /**
   * Get neighbors of a node.
   * @param nodeId - The node ID
   * @param options - Query options
   * @returns Array of neighbor entries
   */
  neighbors(nodeId: number, options?: NeighborQueryOptions): NeighborEntry[]

  /**
   * Get outgoing neighbor node IDs.
   * @param nodeId - The node ID
   * @param edgeType - Optional edge type filter
   * @param distinct - Deduplicate results (default: true)
   * @returns Array of neighbor node IDs
   */
  getOutgoingNeighbors(nodeId: number, edgeType?: string, distinct?: boolean): number[]

  /**
   * Get incoming neighbor node IDs.
   * @param nodeId - The node ID
   * @param edgeType - Optional edge type filter
   * @param distinct - Deduplicate results (default: true)
   * @returns Array of neighbor node IDs
   */
  getIncomingNeighbors(nodeId: number, edgeType?: string, distinct?: boolean): number[]

  /**
   * Perform a breadth-first search traversal.
   * @param nodeId - Starting node ID
   * @param maxDepth - Maximum traversal depth
   * @param options - Traversal options
   * @returns Array of visited nodes with depths
   */
  bfsTraversal(nodeId: number, maxDepth: number, options?: BfsTraversalOptions): BfsVisit[]
}

/**
 * Opens a database at the specified path.
 * Convenience function equivalent to Database.open().
 * @param path - Path to the database file
 * @param options - Connection options
 * @returns A new Database instance
 */
export function openDatabase<S extends NodeSchema = DefaultSchema>(
  path: string,
  options?: ConnectOptions | null,
): Database<S>

/** Access to the native NAPI bindings (advanced use) */
export const native: typeof import('./index.js')

// Typed facade (higher-level, schema-aware API)
export { SombraDB } from './typed'
