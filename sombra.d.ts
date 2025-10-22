export interface SombraPropertyValue {
  type: 'string' | 'int' | 'float' | 'bool' | 'bytes';
  value: any;
}

export interface SombraNode {
  id: number;
  labels: string[];
  properties: Record<string, SombraPropertyValue>;
}

export interface SombraEdge {
  id: number;
  sourceNodeId: number;
  targetNodeId: number;
  typeName: string;
  properties: Record<string, SombraPropertyValue>;
}

export interface BfsResult {
  nodeId: number;
  depth: number;
}

export interface DegreeEntry {
  nodeId: number;
  degree: number;
}

export interface DegreeDistribution {
  inDegree: DegreeEntry[];
  outDegree: DegreeEntry[];
  totalDegree: DegreeEntry[];
}

export interface HubNode {
  nodeId: number;
  degree: number;
}

export interface Subgraph {
  nodes: SombraNode[];
  edges: SombraEdge[];
  boundaryNodes: number[];
}

export interface PropertyBound {
  value: SombraPropertyValue;
  inclusive: boolean;
}

export interface PropertyRangeFilter {
  key: string;
  min?: PropertyBound;
  max?: PropertyBound;
}

export interface PropertyFilters {
  equals?: Record<string, SombraPropertyValue>;
  notEquals?: Record<string, SombraPropertyValue>;
  ranges?: PropertyRangeFilter[];
}

export interface NodePattern {
  varName: string;
  labels?: string[];
  properties?: PropertyFilters;
}

export interface EdgePattern {
  fromVar: string;
  toVar: string;
  types?: string[];
  properties?: PropertyFilters;
  direction: 'incoming' | 'outgoing' | 'both';
}

export interface Pattern {
  nodes: NodePattern[];
  edges: EdgePattern[];
}

export interface Match {
  nodeBindings: Record<string, number>;
  edgeIds: number[];
}

export interface QueryResult {
  startNodes: number[];
  nodeIds: number[];
  limited: boolean;
}

export declare class QueryBuilder {
  startFrom(nodeIds: number[]): this;
  startFromLabel(label: string): this;
  startFromProperty(label: string, key: string, value: string): this;
  traverse(edgeTypes: string[], direction: 'incoming' | 'outgoing' | 'both', depth: number): this;
  limit(n: number): this;
  execute(): QueryResult;
}

export declare class SombraDB {
  constructor(path: string);
  
  beginTransaction(): SombraTransaction;
  
  query(): QueryBuilder;
  
  addNode(labels: string[], properties?: Record<string, SombraPropertyValue>): number;
  
  addEdge(sourceNodeId: number, targetNodeId: number, label: string, properties?: Record<string, SombraPropertyValue>): number;
  
  getNode(nodeId: number): SombraNode;
  
  getEdge(edgeId: number): SombraEdge;
  
  getNeighbors(nodeId: number): number[];
  
  getIncomingNeighbors(nodeId: number): number[];
  
  getNeighborsTwoHops(nodeId: number): number[];
  
  getNeighborsThreeHops(nodeId: number): number[];
  
  bfsTraversal(startNodeId: number, maxDepth: number): BfsResult[];
  
  getNodesByLabel(label: string): number[];
  
  getNodesInRange(start: number, end: number): number[];
  
  getNodesFrom(start: number): number[];
  
  getNodesTo(end: number): number[];
  
  getFirstNode(): number | null;
  
  getLastNode(): number | null;
  
  getFirstNNodes(n: number): number[];
  
  getLastNNodes(n: number): number[];
  
  getAllNodeIdsOrdered(): number[];
  
  countOutgoingEdges(nodeId: number): number;
  
  countIncomingEdges(nodeId: number): number;
  
  getOutgoingEdges(nodeId: number): number[];
  
  getIncomingEdges(nodeId: number): number[];
  
  deleteNode(nodeId: number): void;
  
  deleteEdge(edgeId: number): void;
  
  setNodeProperty(nodeId: number, key: string, value: SombraPropertyValue): void;
  
  removeNodeProperty(nodeId: number, key: string): void;
  
  flush(): void;
  
  checkpoint(): void;
  
  countNodesByLabel(): Record<string, number>;
  
  countEdgesByType(): Record<string, number>;
  
  getTotalNodeCount(): number;
  
  getTotalEdgeCount(): number;
  
  degreeDistribution(): DegreeDistribution;
  
  findHubs(minDegree: number, degreeType: 'in' | 'out' | 'total'): HubNode[];
  
  findIsolatedNodes(): number[];
  
  findLeafNodes(direction: 'incoming' | 'outgoing' | 'both'): number[];
  
  getAverageDegree(): number;
  
  getDensity(): number;
  
  countNodesWithLabel(label: string): number;
  
  countEdgesWithType(edgeType: string): number;
  
  extractSubgraph(rootNodes: number[], depth: number, edgeTypes?: string[], direction?: 'incoming' | 'outgoing' | 'both'): Subgraph;
  
  extractInducedSubgraph(nodeIds: number[]): Subgraph;
  
  findAncestorByLabel(startNodeId: number, label: string, edgeType: string): number | null;
  
  getAncestors(startNodeId: number, edgeType: string, maxDepth?: number): number[];
  
  getDescendants(startNodeId: number, edgeType: string, maxDepth?: number): number[];
  
  getContainingFile(nodeId: number): number;

  matchPattern(pattern: Pattern): Match[];
}

export declare class SombraTransaction {
  id(): number;
  
  addNode(labels: string[], properties?: Record<string, SombraPropertyValue>): number;
  
  addEdge(sourceNodeId: number, targetNodeId: number, label: string, properties?: Record<string, SombraPropertyValue>): number;
  
  getNode(nodeId: number): SombraNode;
  
  getEdge(edgeId: number): SombraEdge;
  
  getNeighbors(nodeId: number): number[];
  
  getIncomingNeighbors(nodeId: number): number[];
  
  getNeighborsTwoHops(nodeId: number): number[];
  
  getNeighborsThreeHops(nodeId: number): number[];
  
  bfsTraversal(startNodeId: number, maxDepth: number): BfsResult[];
  
  getNodesByLabel(label: string): number[];
  
  getNodesInRange(start: number, end: number): number[];
  
  getNodesFrom(start: number): number[];
  
  getNodesTo(end: number): number[];
  
  getFirstNode(): number | null;
  
  getLastNode(): number | null;
  
  getFirstNNodes(n: number): number[];
  
  getLastNNodes(n: number): number[];
  
  getAllNodeIdsOrdered(): number[];
  
  countOutgoingEdges(nodeId: number): number;
  
  countIncomingEdges(nodeId: number): number;
  
  getOutgoingEdges(nodeId: number): number[];
  
  getIncomingEdges(nodeId: number): number[];
  
  deleteNode(nodeId: number): void;
  
  deleteEdge(edgeId: number): void;
  
  setNodeProperty(nodeId: number, key: string, value: SombraPropertyValue): void;
  
  removeNodeProperty(nodeId: number, key: string): void;
  
  commit(): void;
  
  rollback(): void;
}
