import type { BfsTraversalOptions, BfsVisit, ConnectOptions, Database, Direction } from './main'

/**
 * Definition for a node type in a graph schema.
 * @template Props - The property types for this node
 */
export interface NodeDefinition<Props extends Record<string, any>> {
  /** The properties this node type has */
  properties: Props
}

/**
 * Definition for an edge type in a graph schema.
 * @template From - The source node label
 * @template To - The target node label
 * @template Props - The property types for this edge
 */
export interface EdgeDefinition<From extends string, To extends string, Props extends Record<string, any>> {
  /** The source node label */
  from: From
  /** The target node label */
  to: To
  /** The properties this edge type has */
  properties: Props
}

/**
 * A complete graph schema definition with nodes and edges.
 *
 * @example
 * ```ts
 * interface MyGraph extends GraphSchema {
 *   nodes: {
 *     Person: { name: string; age: number }
 *     Company: { name: string; employees: number }
 *   }
 *   edges: {
 *     WORKS_AT: { from: 'Person'; to: 'Company'; properties: { role: string } }
 *   }
 * }
 * ```
 */
export interface GraphSchema {
  /** Node type definitions */
  nodes: Record<string, NodeDefinition<Record<string, any>>>
  /** Edge type definitions */
  edges: Record<string, EdgeDefinition<string, string, Record<string, any>>>
}

/** Extract node labels from a graph schema */
export type NodeLabel<S extends GraphSchema> = keyof S['nodes'] & string

/** Extract edge labels from a graph schema */
export type EdgeLabel<S extends GraphSchema> = keyof S['edges'] & string

/** Get property types for a node label */
export type NodeProps<S extends GraphSchema, L extends NodeLabel<S>> = S['nodes'][L]['properties']

/** Get property types for an edge label */
export type EdgeProps<S extends GraphSchema, E extends EdgeLabel<S>> = S['edges'][E]['properties']

/** Get the source node label for an edge type */
export type EdgeSourceLabel<S extends GraphSchema, E extends EdgeLabel<S>> = S['edges'][E]['from'] & string

/** Get the target node label for an edge type */
export type EdgeTargetLabel<S extends GraphSchema, E extends EdgeLabel<S>> = S['edges'][E]['to'] & string

/**
 * Branded node ID type for type safety.
 * The ID is a number but carries label information at the type level.
 */
export type NodeId<L extends string = string> = number & { __node?: L }

/**
 * A typed node instance with its ID, label, and properties.
 */
export interface NodeInstance<S extends GraphSchema, L extends NodeLabel<S>> {
  /** The node's unique ID */
  id: NodeId<L>
  /** The node's label */
  label: L
  /** The node's properties */
  properties: NodeProps<S, L>
}

/**
 * Options for creating a typed graph database.
 */
export interface TypedGraphOptions<S extends GraphSchema> {
  /** Connection options passed to Database.open() */
  connect?: ConnectOptions | null
  /** Runtime schema for validation */
  schema?: RuntimeGraphSchema<S>
}

/**
 * Runtime representation of a graph schema.
 * Used for validation at runtime.
 */
export type RuntimeGraphSchema<S extends GraphSchema> = {
  nodes: {
    [K in NodeLabel<S>]: {
      properties: NodeProps<S, K>
    }
  }
  edges: {
    [K in EdgeLabel<S>]: {
      from: EdgeSourceLabel<S, K>
      to: EdgeTargetLabel<S, K>
      properties: EdgeProps<S, K>
    }
  }
}

/**
 * Result from typed query operations.
 */
export interface TypedQueryResult {
  /** The IDs of matching nodes */
  nodeIds: number[]
}

/**
 * Typed query builder for traversal operations.
 *
 * @example
 * ```ts
 * const result = db.query()
 *   .startFromLabel('Person')
 *   .traverse(['WORKS_AT'], 'out', 1)
 *   .getIds()
 * ```
 */
export class TypedQueryBuilder<S extends GraphSchema> {
  /**
   * Set the starting label for the traversal.
   * @param label - The node label to start from
   * @returns This builder for chaining
   */
  startFromLabel<L extends NodeLabel<S>>(label: L): this

  /**
   * Traverse edges from the current nodes.
   * @param edgeTypes - Edge types to traverse
   * @param direction - Direction: 'out', 'in', or 'both'
   * @param depth - Maximum traversal depth
   * @returns This builder for chaining
   */
  traverse<E extends EdgeLabel<S>>(edgeTypes: ReadonlyArray<E>, direction?: Direction, depth?: number): this

  /**
   * Execute the traversal and return matching node IDs.
   * @returns Object containing nodeIds array
   */
  getIds(): TypedQueryResult
}

/**
 * Higher-level typed API for graph database operations.
 * Provides schema-aware CRUD operations with full TypeScript type checking.
 *
 * @example
 * ```ts
 * import { SombraDB } from 'sombradb/typed'
 *
 * interface Graph extends GraphSchema {
 *   nodes: {
 *     Person: { name: string; age: number }
 *     Company: { name: string; employees: number }
 *   }
 *   edges: {
 *     WORKS_AT: { from: 'Person'; to: 'Company'; properties: { role: string } }
 *   }
 * }
 *
 * const schema: Graph = {
 *   nodes: {
 *     Person: { properties: { name: '', age: 0 } },
 *     Company: { properties: { name: '', employees: 0 } },
 *   },
 *   edges: {
 *     WORKS_AT: { from: 'Person', to: 'Company', properties: { role: '' } },
 *   },
 * }
 *
 * const db = new SombraDB<Graph>('my.db', { schema })
 *
 * // All operations are fully typed
 * const alice = db.addNode('Person', { name: 'Alice', age: 30 })
 * const acme = db.addNode('Company', { name: 'Acme', employees: 100 })
 * db.addEdge(alice, acme, 'WORKS_AT', { role: 'Engineer' })
 * ```
 */
export class SombraDB<S extends GraphSchema> {
  /**
   * Create a new typed graph database.
   * @param path - Path to the database file, or ':memory:'
   * @param options - Database options including schema
   */
  constructor(path: string, options?: TypedGraphOptions<S>)

  /**
   * Access the underlying Database instance for advanced operations.
   * @returns The raw Database instance
   */
  raw(): Database<any>

  /**
   * Add a typed node to the database.
   * @param label - The node label (must be defined in schema)
   * @param props - The node properties (type-checked against schema)
   * @returns The created node's ID
   * @throws TypeError if label or properties are invalid
   */
  addNode<L extends NodeLabel<S>>(label: L, props: NodeProps<S, L>): NodeId<L>

  /**
   * Add a typed edge between nodes.
   * @param src - Source node ID (must match edge's 'from' label)
   * @param dst - Destination node ID (must match edge's 'to' label)
   * @param edgeType - The edge type (must be defined in schema)
   * @param props - The edge properties (type-checked against schema)
   * @returns The created edge's ID
   * @throws TypeError if edge type or properties are invalid
   */
  addEdge<E extends EdgeLabel<S>>(
    src: NodeId<EdgeSourceLabel<S, E>>,
    dst: NodeId<EdgeTargetLabel<S, E>>,
    edgeType: E,
    props: EdgeProps<S, E>,
  ): number

  /**
   * Get a typed node by ID.
   * @param id - The node ID
   * @param expectedLabel - Optional label to validate against
   * @returns The node instance, or null if not found
   */
  getNode<L extends NodeLabel<S>>(id: NodeId<L>, expectedLabel?: L): NodeInstance<S, L> | null

  /**
   * Find a node by a property value.
   * @param label - The node label to search
   * @param prop - The property name
   * @param value - The property value to match
   * @returns The matching node's ID, or null if not found
   */
  findNodeByProperty<L extends NodeLabel<S>, K extends keyof NodeProps<S, L> & string>(
    label: L,
    prop: K,
    value: NodeProps<S, L>[K],
  ): NodeId<L> | null

  /**
   * List all node IDs with a specific label.
   * @param label - The label to list
   * @returns Array of typed node IDs
   */
  listNodesWithLabel<L extends NodeLabel<S>>(label: L): Array<NodeId<L>>

  /**
   * Get incoming neighbor node IDs.
   * @param nodeId - The target node ID
   * @param edgeType - Optional edge type filter
   * @param distinct - Deduplicate results (default: true)
   * @returns Array of source node IDs
   */
  getIncomingNeighbors<E extends EdgeLabel<S>>(
    nodeId: NodeId<EdgeTargetLabel<S, E>>,
    edgeType?: E,
    distinct?: boolean,
  ): Array<NodeId<EdgeSourceLabel<S, E>>>

  /**
   * Get outgoing neighbor node IDs.
   * @param nodeId - The source node ID
   * @param edgeType - Optional edge type filter
   * @param distinct - Deduplicate results (default: true)
   * @returns Array of target node IDs
   */
  getOutgoingNeighbors<E extends EdgeLabel<S>>(
    nodeId: NodeId<EdgeSourceLabel<S, E>>,
    edgeType?: E,
    distinct?: boolean,
  ): Array<NodeId<EdgeTargetLabel<S, E>>>

  /**
   * Count nodes with a specific label.
   * @param label - The label to count
   * @returns The count
   */
  countNodesWithLabel<L extends NodeLabel<S>>(label: L): number

  /**
   * Count edges with a specific type.
   * @param edgeType - The edge type to count
   * @returns The count
   */
  countEdgesWithType<E extends EdgeLabel<S>>(edgeType: E): number

  /**
   * Perform a breadth-first search traversal.
   * @param nodeId - Starting node ID
   * @param maxDepth - Maximum traversal depth
   * @param options - Traversal options
   * @returns Array of visited nodes with depths
   */
  bfsTraversal(nodeId: NodeId<any>, maxDepth: number, options?: BfsTraversalOptions): BfsVisit[]

  /**
   * Create a typed query builder.
   * @returns A TypedQueryBuilder instance
   */
  query(): TypedQueryBuilder<S>

  /**
   * Flush pending operations (no-op, for API compatibility).
   * @returns This database for chaining
   */
  flush(): this
}
