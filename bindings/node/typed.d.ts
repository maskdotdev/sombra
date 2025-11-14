import type {
  BfsTraversalOptions,
  BfsVisit,
  ConnectOptions,
  Database,
  Direction,
  NeighborQueryOptions,
} from './main'

export interface NodeDefinition<Props extends Record<string, any>> {
  properties: Props
}

export interface EdgeDefinition<
  From extends string,
  To extends string,
  Props extends Record<string, any>,
> {
  from: From
  to: To
  properties: Props
}

export interface GraphSchema {
  nodes: Record<string, NodeDefinition<Record<string, any>>>
  edges: Record<string, EdgeDefinition<string, string, Record<string, any>>>
}

export type NodeLabel<S extends GraphSchema> = keyof S['nodes'] & string

export type EdgeLabel<S extends GraphSchema> = keyof S['edges'] & string

export type NodeProps<S extends GraphSchema, L extends NodeLabel<S>> = S['nodes'][L]['properties']

export type EdgeProps<S extends GraphSchema, E extends EdgeLabel<S>> = S['edges'][E]['properties']

export type EdgeSourceLabel<S extends GraphSchema, E extends EdgeLabel<S>> =
  S['edges'][E]['from'] & string

export type EdgeTargetLabel<S extends GraphSchema, E extends EdgeLabel<S>> =
  S['edges'][E]['to'] & string

export type NodeId<L extends string = string> = number & { __node?: L }

export interface NodeInstance<S extends GraphSchema, L extends NodeLabel<S>> {
  id: NodeId<L>
  label: L
  properties: NodeProps<S, L>
}

export interface TypedGraphOptions<S extends GraphSchema> {
  connect?: ConnectOptions | null
  schema?: RuntimeGraphSchema<S>
}

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

export interface TypedQueryResult {
  nodeIds: number[]
}

export class TypedQueryBuilder<S extends GraphSchema> {
  startFromLabel<L extends NodeLabel<S>>(label: L): this
  traverse<E extends EdgeLabel<S>>(
    edgeTypes: ReadonlyArray<E>,
    direction?: Direction,
    depth?: number,
  ): this
  getIds(): TypedQueryResult
}

export class SombraDB<S extends GraphSchema> {
  constructor(path: string, options?: TypedGraphOptions<S>)
  raw(): Database<any>
  addNode<L extends NodeLabel<S>>(label: L, props: NodeProps<S, L>): NodeId<L>
  addEdge<E extends EdgeLabel<S>>(
    src: NodeId<EdgeSourceLabel<S, E>>,
    dst: NodeId<EdgeTargetLabel<S, E>>,
    edgeType: E,
    props: EdgeProps<S, E>,
  ): number
  getNode<L extends NodeLabel<S>>(id: NodeId<L>, expectedLabel?: L): NodeInstance<S, L> | null
  findNodeByProperty<L extends NodeLabel<S>, K extends keyof NodeProps<S, L> & string>(
    label: L,
    prop: K,
    value: NodeProps<S, L>[K],
  ): NodeId<L> | null
  listNodesWithLabel<L extends NodeLabel<S>>(label: L): Array<NodeId<L>>
  getIncomingNeighbors<E extends EdgeLabel<S>>(
    nodeId: NodeId<EdgeTargetLabel<S, E>>,
    edgeType?: E,
    distinct?: boolean,
  ): Array<NodeId<EdgeSourceLabel<S, E>>>
  getOutgoingNeighbors<E extends EdgeLabel<S>>(
    nodeId: NodeId<EdgeSourceLabel<S, E>>,
    edgeType?: E,
    distinct?: boolean,
  ): Array<NodeId<EdgeTargetLabel<S, E>>>
  countNodesWithLabel<L extends NodeLabel<S>>(label: L): number
  countEdgesWithType<E extends EdgeLabel<S>>(edgeType: E): number
  bfsTraversal(nodeId: NodeId<any>, maxDepth: number, options?: BfsTraversalOptions): BfsVisit[]
  query(): TypedQueryBuilder<S>
  flush(): this
}
