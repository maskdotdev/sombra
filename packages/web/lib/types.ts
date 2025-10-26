import type { GraphEdge, GraphNode } from 'reagraph';

export interface SombraPropertyValue {
  type: 'string' | 'int' | 'float' | 'bool';
  value: string | number | boolean;
}

export interface SombraNode {
  id: number;
  labels: string[];
  properties: Record<string, string | number | boolean>;
}

export interface SombraEdge {
  id: number;
  sourceNodeId: number;
  targetNodeId: number;
  typeName: string;
  properties: Record<string, string | number | boolean>;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface GraphStats {
  nodeCount: number;
  edgeCount: number;
  labels: string[];
  edgeTypes: string[];
}
