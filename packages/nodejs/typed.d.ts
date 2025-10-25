import { 
  SombraDB as NativeSombraDB, 
  SombraPropertyValue, 
  SombraNode, 
  SombraEdge, 
  QueryBuilder as NativeQueryBuilder,
  SombraTransaction
} from './index';

export type PropertyType = string | number | boolean;

export type InferPropertyValue<T> = T extends string
  ? 'string'
  : T extends number
  ? 'int' | 'float'
  : T extends boolean
  ? 'bool'
  : never;

export type NodeSchema = Record<string, Record<string, PropertyType>>;
export type EdgeSchema = Record<
  string,
  {
    from: string;
    to: string;
    properties?: Record<string, PropertyType>;
  }
>;

export interface GraphSchema {
  nodes: NodeSchema;
  edges: EdgeSchema;
}

export type NodeLabel<Schema extends GraphSchema> = keyof Schema['nodes'] & string;
export type EdgeType<Schema extends GraphSchema> = keyof Schema['edges'] & string;

export type NodeProperties<
  Schema extends GraphSchema,
  Label extends NodeLabel<Schema>
> = Schema['nodes'][Label];

export type EdgeProperties<
  Schema extends GraphSchema,
  Edge extends EdgeType<Schema>
> = Schema['edges'][Edge]['properties'] extends Record<string, PropertyType>
  ? Schema['edges'][Edge]['properties']
  : Record<string, never>;

export type EdgeFrom<
  Schema extends GraphSchema,
  Edge extends EdgeType<Schema>
> = Schema['edges'][Edge]['from'];

export type EdgeTo<
  Schema extends GraphSchema,
  Edge extends EdgeType<Schema>
> = Schema['edges'][Edge]['to'];

export type TypedNode<
  Schema extends GraphSchema,
  Label extends NodeLabel<Schema>
> = {
  id: number;
  labels: Label[];
  properties: NodeProperties<Schema, Label>;
};

export type TypedEdge<
  Schema extends GraphSchema,
  Edge extends EdgeType<Schema>
> = {
  id: number;
  sourceNodeId: number;
  targetNodeId: number;
  typeName: Edge;
  properties: EdgeProperties<Schema, Edge>;
};

export interface TypedQueryBuilder<Schema extends GraphSchema> {
  startFrom(nodeIds: number[]): this;
  startFromLabel<L extends NodeLabel<Schema>>(label: L): this;
  startFromProperty<L extends NodeLabel<Schema>, K extends keyof NodeProperties<Schema, L>>(
    label: L,
    key: K,
    value: NodeProperties<Schema, L>[K]
  ): this;
  traverse<E extends EdgeType<Schema>>(
    edgeTypes: E[],
    direction: 'incoming' | 'outgoing' | 'both',
    depth: number
  ): this;
  limit(n: number): this;
  execute(): {
    startNodes: number[];
    nodeIds: number[];
    limited: boolean;
  };
}

export class SombraDB<Schema extends GraphSchema = any> {
  constructor(path: string);

  beginTransaction(): SombraTransaction;

  addNode<L extends NodeLabel<Schema>>(
    label: L,
    properties: NodeProperties<Schema, L>
  ): number;
  addNode(labels: string[], properties?: Record<string, SombraPropertyValue> | null): number;

  addEdge<E extends EdgeType<Schema>>(
    sourceNodeId: number,
    targetNodeId: number,
    edgeType: E,
    properties: EdgeProperties<Schema, E>
  ): number;
  addEdge(
    sourceNodeId: number,
    targetNodeId: number,
    label: string,
    properties?: Record<string, SombraPropertyValue> | null
  ): number;

  getNode<L extends NodeLabel<Schema> = NodeLabel<Schema>>(
    nodeId: number
  ): TypedNode<Schema, L> | null;
  getNode(nodeId: number): SombraNode | null;

  getNodesByLabel<L extends NodeLabel<Schema>>(label: L): number[];
  getNodesByLabel(label: string): number[];

  findNodeByProperty<L extends NodeLabel<Schema>, K extends keyof NodeProperties<Schema, L>>(
    label: L,
    key: K,
    value: NodeProperties<Schema, L>[K]
  ): number | undefined;

  findNodesByProperty<L extends NodeLabel<Schema>, K extends keyof NodeProperties<Schema, L>>(
    label: L,
    key: K,
    value: NodeProperties<Schema, L>[K]
  ): number[];

  getEdge<E extends EdgeType<Schema> = EdgeType<Schema>>(
    edgeId: number
  ): TypedEdge<Schema, E> | null;
  getEdge(edgeId: number): SombraEdge;

  getOutgoingEdges(nodeId: number): number[];
  getIncomingEdges(nodeId: number): number[];
  getNeighbors(nodeId: number): number[];
  getIncomingNeighbors(nodeId: number): number[];

  deleteNode(nodeId: number): void;
  deleteEdge(edgeId: number): void;

  setNodeProperty<L extends NodeLabel<Schema>, K extends keyof NodeProperties<Schema, L>>(
    nodeId: number,
    key: K,
    value: NodeProperties<Schema, L>[K]
  ): void;
  setNodeProperty(nodeId: number, key: string, value: SombraPropertyValue): void;

  removeNodeProperty(nodeId: number, key: string): void;

  flush(): void;
  checkpoint(): void;

  bfsTraversal(startNodeId: number, maxDepth: number): Array<{ nodeId: number; depth: number }>;

  query(): TypedQueryBuilder<Schema>;
  query(): NativeQueryBuilder;

  countNodesByLabel(): Record<string, number>;
  countEdgesByType(): Record<string, number>;
  countNodesWithLabel<L extends NodeLabel<Schema>>(label: L): number;
  countNodesWithLabel(label: string): number;
  countEdgesWithType<E extends EdgeType<Schema>>(edgeType: E): number;
  countEdgesWithType(edgeType: string): number;

  getAllNodeIdsOrdered(): number[];
  getFirstNode(): number | null;
  getLastNode(): number | null;

  getAncestors(startNodeId: number, edgeType: string, maxDepth?: number): number[];
  getDescendants(startNodeId: number, edgeType: string, maxDepth?: number): number[];

  shortestPath(start: number, end: number, edgeTypes?: string[]): number[] | null;
  findPaths(
    start: number,
    end: number,
    minDepth: number,
    maxDepth: number,
    edgeTypes?: string[]
  ): number[][];

  get db(): NativeSombraDB;
}

export { NativeSombraDB, SombraPropertyValue, SombraNode, SombraEdge, SombraTransaction };
