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

export declare class SombraDB {
  constructor(path: string);
  
  beginTransaction(): SombraTransaction;
  
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
