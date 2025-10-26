import type { GraphNode, GraphEdge } from 'reagraph';
import type { SombraNode, SombraEdge, SombraPropertyValue } from './types';

const LABEL_COLORS: Record<string, string> = {
  Person: '#3b82f6',
  File: '#10b981',
  Class: '#f59e0b',
  Function: '#ef4444',
  Organization: '#8b5cf6',
  Project: '#ec4899',
};

function getColorForLabel(labels: string[]): string {
  if (labels.length === 0) return '#6b7280';
  
  const firstLabel = labels[0];
  return LABEL_COLORS[firstLabel] || '#6b7280';
}

function propertyValueToString(prop: string | number | boolean): string {
  return String(prop);
}

export function transformNode(node: SombraNode): GraphNode {
  const nameProperty = node.properties.name || node.properties.label || node.properties.title;
  const labelText = nameProperty 
    ? propertyValueToString(nameProperty)
    : `${node.labels[0] || 'Node'} #${node.id}`;
  
  return {
    id: `n-${node.id}`,
    label: labelText,
    fill: getColorForLabel(node.labels),
    data: {
      nodeId: node.id,
      labels: node.labels,
      properties: node.properties,
    },
  };
}

export function transformEdge(edge: SombraEdge): GraphEdge {
  return {
    id: `e-${edge.id}`,
    source: `n-${edge.sourceNodeId}`,
    target: `n-${edge.targetNodeId}`,
    label: edge.typeName,
    data: {
      edgeId: edge.id,
      edgeType: edge.typeName,
      properties: edge.properties,
    },
  };
}

export function transformGraphData(nodes: SombraNode[], edges: SombraEdge[]): { nodes: GraphNode[], edges: GraphEdge[] } {
  return {
    nodes: nodes.map(transformNode),
    edges: edges.map(transformEdge),
  };
}
