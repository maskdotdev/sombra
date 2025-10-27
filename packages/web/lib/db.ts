import { SombraDB } from '@unyth/sombra';
import type { SombraNode, SombraEdge, GraphStats } from './types';

let db: SombraDB | null = null;
let currentPath: string | null = null;

export function getDatabase(path?: string): SombraDB {
  const dbPath = path || process.env.SOMBRA_DB_PATH;
  if (!dbPath) {
    throw new Error('Database path not provided and SOMBRA_DB_PATH environment variable is not set');
  }
  
  // Create new database instance if path changed
  if (!db || currentPath !== dbPath) {
    db = new SombraDB(dbPath);
    currentPath = dbPath;
  }
  
  return db;
}

export function getAllNodes(path?: string): SombraNode[] {
  const database = getDatabase(path);
  const nodeIds = database.getAllNodeIdsOrdered();
  
  return nodeIds
    .map((id: number) => database.getNode(id))
    .filter((node: any): node is SombraNode => node !== null);
}

export function getAllEdges(path?: string): SombraEdge[] {
  const database = getDatabase(path);
  const nodes = getAllNodes(path);
  const edgeSet = new Set<number>();
  const edges: SombraEdge[] = [];
  
  for (const node of nodes) {
    const outgoing = database.getOutgoingEdges(node.id);
    for (const edgeId of outgoing) {
      if (!edgeSet.has(edgeId)) {
        edgeSet.add(edgeId);
        const edge = database.getEdge(edgeId);
        if (edge) {
          edges.push(edge as SombraEdge);
        }
      }
    }
  }
  
  return edges;
}

export function getNodeById(id: number, path?: string): SombraNode | null {
  const database = getDatabase(path);
  return database.getNode(id) as SombraNode | null;
}

export function getNeighbors(nodeId: number, path?: string): number[] {
  const database = getDatabase(path);
  return database.getNeighbors(nodeId);
}

export function traverseFrom(nodeId: number, depth: number = 2, path?: string): { nodes: SombraNode[], edges: SombraEdge[] } {
  const database = getDatabase(path);
  const bfsResult = database.bfsTraversal(nodeId, depth);
  
  const nodeIds = new Set(bfsResult.map((r: any) => r.nodeId));
  const nodes = Array.from(nodeIds)
    .map(id => database.getNode(id))
    .filter((node): node is SombraNode => node !== null);
  
  const edgeSet = new Set<number>();
  const edges: SombraEdge[] = [];
  
  for (const node of nodes) {
    const outgoing = database.getOutgoingEdges(node.id);
    const incoming = database.getIncomingEdges(node.id);
    
    for (const edgeId of [...outgoing, ...incoming]) {
      if (!edgeSet.has(edgeId)) {
        const edge = database.getEdge(edgeId);
        if (edge) {
          const targetInSet = nodeIds.has(edge.sourceNodeId) && nodeIds.has(edge.targetNodeId);
          if (targetInSet) {
            edgeSet.add(edgeId);
            edges.push(edge as SombraEdge);
          }
        }
      }
    }
  }
  
  return { nodes, edges };
}

export function getGraphStats(path?: string): GraphStats {
  const database = getDatabase(path);
  const labelCounts = database.countNodesByLabel();
  const edgeTypeCounts = database.countEdgesByType();
  
  return {
    nodeCount: database.getAllNodeIdsOrdered().length,
    edgeCount: Object.values(edgeTypeCounts).reduce((sum: number, count: any) => sum + (count as number), 0),
    labels: Object.keys(labelCounts),
    edgeTypes: Object.keys(edgeTypeCounts),
  };
}
