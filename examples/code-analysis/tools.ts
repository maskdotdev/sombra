import { SombraDB, SombraPropertyValue } from 'sombradb';
import { tool } from 'ai';
import { z } from 'zod';

export function createSombraTools(db: SombraDB) {
  return {
    getNode: tool({
      description: 'Retrieve a node by its ID with all its properties and labels.',
      inputSchema: z.object({
        nodeId: z.number().describe('The ID of the node to retrieve')
      }),
      execute: async ({ nodeId }) => {
        const node = db.getNode(nodeId);
        return node;
      }
    }),

    getEdge: tool({
      description: 'Retrieve an edge by its ID with all its properties.',
      inputSchema: z.object({
        edgeId: z.number().describe('The ID of the edge to retrieve')
      }),
      execute: async ({ edgeId }) => {
        const edge = db.getEdge(edgeId);
        return edge;
      }
    }),

    getNeighbors: tool({
      description: 'Get all neighboring nodes connected to a given node (both incoming and outgoing).',
      inputSchema: z.object({
        nodeId: z.number().describe('The ID of the node to find neighbors for')
      }),
      execute: async ({ nodeId }) => {
        const neighbors = db.getNeighbors(nodeId);
        return { neighbors, count: neighbors.length };
      }
    }),

    getIncomingEdges: tool({
      description: 'Get all incoming edges to a node. Use this to find: what calls a function, what imports a file, what depends on a class.',
      inputSchema: z.object({
        nodeId: z.number().describe('The ID of the node')
      }),
      execute: async ({ nodeId }) => {
        const edgeIds = db.getIncomingEdges(nodeId);
        const edgeDetails = edgeIds.map((edgeId: number) => {
          const edge = db.getEdge(edgeId);
          const sourceNode = db.getNode(edge.sourceNodeId);
          return {
            edgeId,
            edgeType: edge.typeName,
            from: edge.sourceNodeId,
            fromNode: sourceNode,
            properties: edge.properties
          };
        });
        return { 
          count: edgeIds.length,
          edges: edgeDetails
        };
      }
    }),

    getOutgoingEdges: tool({
      description: 'Get all outgoing edges from a node. Use this to find: what a function calls, what a file imports, what a class depends on.',
      inputSchema: z.object({
        nodeId: z.number().describe('The ID of the node')
      }),
      execute: async ({ nodeId }) => {
        const edgeIds = db.getOutgoingEdges(nodeId);
        const edgeDetails = edgeIds.map((edgeId: number) => {
          const edge = db.getEdge(edgeId);
          const targetNode = db.getNode(edge.targetNodeId);
          return {
            edgeId,
            edgeType: edge.typeName,
            to: edge.targetNodeId,
            toNode: targetNode,
            properties: edge.properties
          };
        });
        return { 
          count: edgeIds.length,
          edges: edgeDetails
        };
      }
    }),

    traversePath: tool({
      description: 'Find paths between two nodes using BFS traversal. Useful for dependency analysis and impact analysis.',
      inputSchema: z.object({
        startId: z.number().describe('Starting node ID'),
        endId: z.number().describe('Target node ID'),
        maxDepth: z.number().default(5).describe('Maximum traversal depth')
      }),
      execute: async ({ startId, endId, maxDepth }) => {
        const bfsResult = db.bfsTraversal(startId, maxDepth);
        const visitedNodes = bfsResult.map(r => r.nodeId);
        const pathExists = visitedNodes.includes(endId);
        return {
          pathExists,
          visited: visitedNodes,
          message: pathExists ? `Path found from ${startId} to ${endId}` : `No path found within depth ${maxDepth}`
        };
      }
    }),

    queryByLabel: tool({
      description: 'Find all nodes with a specific label. Examples: find all Functions, all Classes, all Files.',
      inputSchema: z.object({
        label: z.string().describe('The label to search for (e.g., "Function", "Class", "File")')
      }),
      execute: async ({ label }) => {
        const nodeIds = db.getNodesByLabel(label);
        const nodes = nodeIds.map((id: number) => db.getNode(id));
        return {
          count: nodes.length,
          nodes: nodes.map((node) => ({
            id: node.id,
            labels: node.labels,
            properties: node.properties
          }))
        };
      }
    }),

    queryByProperty: tool({
      description: 'Find nodes by property name and value. Example: find all nodes with name="createUser".',
      inputSchema: z.object({
        propertyName: z.string().describe('Property name to search for'),
        propertyValue: z.string().describe('Property value to match')
      }),
      execute: async ({ propertyName, propertyValue }) => {
        const allNodeIds = db.getAllNodeIdsOrdered();
        const matchingNodes = allNodeIds
          .map(id => db.getNode(id))
          .filter((node) => {
            const prop = node.properties[propertyName];
            return prop && prop.value === propertyValue;
          });
        
        return {
          count: matchingNodes.length,
          nodes: matchingNodes.map((node) => ({
            id: node.id,
            labels: node.labels,
            properties: node.properties
          }))
        };
      }
    }),

    findCallChain: tool({
      description: 'Find the complete call chain from one function to another. Shows all intermediate calls.',
      inputSchema: z.object({
        startFunctionName: z.string().describe('Starting function name'),
        endFunctionName: z.string().describe('Target function name')
      }),
      execute: async ({ startFunctionName, endFunctionName }) => {
        const allNodeIds = db.getAllNodeIdsOrdered();
        const allNodes = allNodeIds.map(id => db.getNode(id));
        
        const startNode = allNodes.find((n) => 
          n.properties.name?.value === startFunctionName
        );
        const endNode = allNodes.find((n) => 
          n.properties.name?.value === endFunctionName
        );
        
        if (!startNode || !endNode) {
          return { 
            found: false, 
            message: `Could not find ${!startNode ? startFunctionName : endFunctionName}` 
          };
        }
        
        const bfsResult = db.bfsTraversal(startNode.id, 10);
        const visitedNodeIds = bfsResult.map(r => r.nodeId);
        const pathExists = visitedNodeIds.includes(endNode.id);
        
        if (pathExists) {
          const path = [];
          let current = endNode.id;
          const visited = new Set(visitedNodeIds);
          
          while (current !== startNode.id && visited.has(current)) {
            const node = db.getNode(current);
            path.unshift({
              id: current,
              name: node.properties.name?.value,
              labels: node.labels
            });
            
            const incomingEdgeIds = db.getIncomingEdges(current);
            const callEdge = incomingEdgeIds.find((edgeId: number) => {
              const edge = db.getEdge(edgeId);
              return edge.typeName === 'CALLS' && visited.has(edge.sourceNodeId);
            });
            
            if (callEdge) {
              const edge = db.getEdge(callEdge);
              current = edge.sourceNodeId;
            } else {
              break;
            }
          }
          
          if (current === startNode.id) {
            const node = db.getNode(current);
            path.unshift({
              id: current,
              name: node.properties.name?.value,
              labels: node.labels
            });
          }
          
          return {
            found: true,
            path,
            length: path.length
          };
        }
        
        return { 
          found: false, 
          message: `No call chain found from ${startFunctionName} to ${endFunctionName}` 
        };
      }
    }),

    findFunctionCallers: tool({
      description: 'Find all functions that call a specific function. Perfect for impact analysis.',
      inputSchema: z.object({
        functionName: z.string().describe('The function name to find callers for')
      }),
      execute: async ({ functionName }) => {
        const start = performance.now();
        const allNodeIds = db.getAllNodeIdsOrdered();
        const allNodes = allNodeIds.map(id => db.getNode(id));
        
        const targetNode = allNodes.find((n) => 
          n.properties.name?.value === functionName
        );
        
        if (!targetNode) {
          return { 
            found: false, 
            message: `Function ${functionName} not found`,
            executionTimeMs: performance.now() - start
          };
        }
        
        const incomingEdgeIds = db.getIncomingEdges(targetNode.id);
        const callers = incomingEdgeIds
          .map((edgeId: number) => {
            const edge = db.getEdge(edgeId);
            if (edge.typeName === 'CALLS') {
              const caller = db.getNode(edge.sourceNodeId);
              return {
                id: caller.id,
                name: caller.properties.name?.value,
                labels: caller.labels,
                lineNumber: edge.properties?.lineNumber?.value
              };
            }
            return null;
          })
          .filter((c) => c !== null);
        
        return {
          found: true,
          functionId: targetNode.id,
          callers,
          count: callers.length,
          executionTimeMs: performance.now() - start
        };
      }
    }),

    findFilesUsingFunction: tool({
      description: 'Find all files that contain code using a specific function. Traces through CONTAINS and CALLS relationships.',
      inputSchema: z.object({
        functionName: z.string().describe('The function name to search for')
      }),
      execute: async ({ functionName }) => {
        const allNodeIds = db.getAllNodeIdsOrdered();
        const allNodes = allNodeIds.map(id => db.getNode(id));
        
        const targetNode = allNodes.find((n) => 
          n.properties.name?.value === functionName
        );
        
        if (!targetNode) {
          return { 
            found: false, 
            message: `Function ${functionName} not found` 
          };
        }
        
        const incomingEdgeIds = db.getIncomingEdges(targetNode.id);
        const files = new Set<string>();
        const callingFunctions = [];
        
        for (const edgeId of incomingEdgeIds) {
          const edge = db.getEdge(edgeId);
          if (edge.typeName === 'CALLS') {
            const caller = db.getNode(edge.sourceNodeId);
            callingFunctions.push({
              id: caller.id,
              name: caller.properties.name?.value
            });
            
            let current = edge.sourceNodeId;
            let depth = 0;
            while (depth < 10) {
              const incomingToCurrentIds = db.getIncomingEdges(current);
              const containsEdgeId = incomingToCurrentIds.find((eid: number) => {
                const e = db.getEdge(eid);
                return e.typeName === 'CONTAINS';
              });
              
              if (containsEdgeId) {
                const e = db.getEdge(containsEdgeId);
                const parent = db.getNode(e.sourceNodeId);
                
                if (parent.labels.includes('File')) {
                  files.add(JSON.stringify({
                    id: parent.id,
                    name: parent.properties.name?.value,
                    path: parent.properties.path?.value
                  }));
                  break;
                }
                current = e.sourceNodeId;
              } else {
                break;
              }
              depth++;
            }
          }
        }
        
        return {
          found: true,
          functionId: targetNode.id,
          files: Array.from(files).map((f) => JSON.parse(f)),
          callingFunctions,
          count: files.size
        };
      }
    }),

    getGraphStats: tool({
      description: 'Get statistics about the graph including node count, edge count, and counts by label/type.',
      inputSchema: z.object({}),
      execute: async () => {
        const nodeCount = db.getTotalNodeCount();
        const edgeCount = db.getTotalEdgeCount();
        const nodesByLabel = db.countNodesByLabel();
        const edgesByType = db.countEdgesByType();
        
        return {
          nodeCount,
          edgeCount,
          nodesByLabel: Object.fromEntries(nodesByLabel),
          edgesByType: Object.fromEntries(edgesByType)
        };
      }
    })
  };
}
