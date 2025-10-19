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

export declare class SombraDB {
  constructor(path: string);
  
  beginTransaction(): SombraTransaction;
  
  addNode(labels: string[], properties?: Record<string, SombraPropertyValue>): number;
  
  addEdge(sourceNodeId: number, targetNodeId: number, label: string, properties?: Record<string, SombraPropertyValue>): number;
  
  getNode(nodeId: number): SombraNode;
  
  getEdge(edgeId: number): SombraEdge;
  
  getNeighbors(nodeId: number): number[];
  
  getOutgoingEdges(nodeId: number): number[];
  
  getIncomingEdges(nodeId: number): number[];
  
  deleteNode(nodeId: number): void;
  
  deleteEdge(edgeId: number): void;
  
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
  
  getOutgoingEdges(nodeId: number): number[];
  
  getIncomingEdges(nodeId: number): number[];
  
  deleteNode(nodeId: number): void;
  
  deleteEdge(edgeId: number): void;
  
  commit(): void;
  
  rollback(): void;
}
