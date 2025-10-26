'use client';

import { useState, useEffect, useMemo } from 'react';
import dynamic from 'next/dynamic';
import type { GraphData, GraphStats } from '@/lib/types';
import type { GraphNode, GraphEdge } from 'reagraph';
import { DatabaseSelector } from './database-selector';

const GraphCanvas = dynamic(
  () => import('reagraph').then((mod) => mod.GraphCanvas),
  { ssr: false }
);

interface GraphExplorerProps {
  initialData?: GraphData;
}

export function GraphExplorer({ initialData }: GraphExplorerProps) {
  const [graphData, setGraphData] = useState<GraphData>(
    initialData || { nodes: [], edges: [] }
  );
  const [stats, setStats] = useState<GraphStats | null>(null);
  const [loading, setLoading] = useState(!initialData);
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<GraphEdge | null>(null);
  const [dbPath, setDbPath] = useState<string>('');
  const [showProperties, setShowProperties] = useState(true);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [selectedLabel, setSelectedLabel] = useState<string>('all');
  const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null);
  const [isTraversalMode, setIsTraversalMode] = useState(false);
  const [traversalSourceNode, setTraversalSourceNode] = useState<GraphNode | null>(null);
  const [fullGraphData, setFullGraphData] = useState<GraphData>({ nodes: [], edges: [] });

  useEffect(() => {
    // Load saved database path from localStorage on mount
    const savedPath = localStorage.getItem('sombra-db-path');
    if (savedPath) {
      setDbPath(savedPath);
    }
  }, []);

  useEffect(() => {
    if (!initialData && dbPath) {
      loadFullGraph();
    }
    if (dbPath) {
      loadStats();
    }
  }, [dbPath]);

  const loadFullGraph = async () => {
    if (!dbPath) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const headers = { 'X-Database-Path': dbPath };
      const [nodesRes, edgesRes] = await Promise.all([
        fetch('/api/graph/nodes', { headers }),
        fetch('/api/graph/edges', { headers })
      ]);

      if (!nodesRes.ok || !edgesRes.ok) {
        throw new Error('Failed to load graph data');
      }

      const nodes = await nodesRes.json();
      const edges = await edgesRes.json();

      const newGraphData = { nodes: nodes.nodes, edges: edges.edges };
      setGraphData(newGraphData);
      setFullGraphData(newGraphData);
      setIsTraversalMode(false);
      setTraversalSourceNode(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  const loadStats = async () => {
    if (!dbPath) return;
    
    try {
      const res = await fetch('/api/graph/stats', {
        headers: { 'X-Database-Path': dbPath }
      });
      if (res.ok) {
        const data = await res.json();
        setStats(data);
      }
    } catch (err) {
      console.error('Failed to load stats:', err);
    }
  };

  const handleNodeClick = async (node: GraphNode) => {
    setSelectedNode(node);
    setSelectedEdge(null);
    setShowProperties(true);
  };

  const handleExploreConnections = async (node: GraphNode) => {
    try {
      const res = await fetch(`/api/graph/traverse?nodeId=${node.data.nodeId}&depth=2`, {
        headers: { 'X-Database-Path': dbPath }
      });
      if (res.ok) {
        const data = await res.json();
        setGraphData(data);
        setIsTraversalMode(true);
        setTraversalSourceNode(node);
      }
    } catch (err) {
      console.error('Failed to traverse from node:', err);
    }
  };

  const handleBackToFullGraph = () => {
    setGraphData(fullGraphData);
    setIsTraversalMode(false);
    setTraversalSourceNode(null);
    setSelectedNode(null);
    setSelectedEdge(null);
  };

  const handleEdgeClick = (edge: GraphEdge) => {
    setSelectedEdge(edge);
    setSelectedNode(null);
    setShowProperties(true);
  };

  const handleReset = () => {
    setSelectedNode(null);
    setSelectedEdge(null);
    setSearchQuery('');
    setSelectedLabel('all');
    if (isTraversalMode) {
      handleBackToFullGraph();
    }
  };

  const handleDatabaseChange = (path: string) => {
    setDbPath(path);
    setSelectedNode(null);
    setSelectedEdge(null);
    setSearchQuery('');
    setSelectedLabel('all');
  };

  // Filter graph data based on search and label selection
  const filteredGraphData = useMemo(() => {
    if (!graphData.nodes.length) return graphData;

    let filteredNodes = graphData.nodes;

    // Filter by label
    if (selectedLabel !== 'all') {
      filteredNodes = filteredNodes.filter(node => 
        node.data.labels.includes(selectedLabel)
      );
    }

    // Filter by search query
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      filteredNodes = filteredNodes.filter(node => {
        const labelMatch = (node.label ?? '').toLowerCase().includes(query);
        const idMatch = String(node.data.nodeId).includes(query);
        const propsMatch = Object.entries(node.data.properties).some(
          ([key, value]) => 
            key.toLowerCase().includes(query) || 
            String(value).toLowerCase().includes(query)
        );
        return labelMatch || idMatch || propsMatch;
      });
    }

    // Get IDs of filtered nodes
    const nodeIds = new Set(filteredNodes.map(n => n.id));

    // Filter edges to only include those between filtered nodes
    const filteredEdges = graphData.edges.filter(
      edge => nodeIds.has(edge.source) && nodeIds.has(edge.target)
    );

    return { nodes: filteredNodes, edges: filteredEdges };
  }, [graphData, searchQuery, selectedLabel]);

  if (loading && dbPath) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="max-w-7xl mx-auto">
          <div className="flex items-center justify-center h-96">
            <div className="text-lg">Loading graph...</div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 p-8">
        <div className="max-w-7xl mx-auto">
          <div className="flex items-center justify-center h-96">
            <div className="text-red-500">Error: {error}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <DatabaseSelector onDatabaseChange={handleDatabaseChange} />
      
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Breadcrumb Navigation */}
        {isTraversalMode && traversalSourceNode && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <svg className="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
              </svg>
              <div>
                <div className="text-sm text-blue-900 font-medium">
                  Exploring connections from: <span className="font-bold">{traversalSourceNode.label}</span>
                </div>
                <div className="text-xs text-blue-700">
                  Showing nodes within 2 hops • {filteredGraphData.nodes.length} nodes visible
                </div>
              </div>
            </div>
            <button
              onClick={handleBackToFullGraph}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
              Back to Full Graph
            </button>
          </div>
        )}

        {/* Header with stats */}
        <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2">Graph Explorer</h1>
              {stats && (
                <div className="flex gap-6 text-sm text-gray-600">
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
                    {isTraversalMode ? `${graphData.nodes.length} / ${stats.nodeCount}` : stats.nodeCount} nodes
                  </span>
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 bg-green-500 rounded-full"></span>
                    {isTraversalMode ? `${graphData.edges.length} / ${stats.edgeCount}` : stats.edgeCount} edges
                  </span>
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 bg-purple-500 rounded-full"></span>
                    {stats.labels.length} labels
                  </span>
                  {searchQuery || selectedLabel !== 'all' ? (
                    <span className="flex items-center gap-1">
                      <span className="w-2 h-2 bg-orange-500 rounded-full"></span>
                      {filteredGraphData.nodes.length} filtered
                    </span>
                  ) : null}
                </div>
              )}
            </div>
            <div className="flex gap-2">
              {isTraversalMode && (
                <button
                  onClick={handleBackToFullGraph}
                  className="px-4 py-2 bg-blue-600 text-white hover:bg-blue-700 rounded-lg transition-colors flex items-center gap-2"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
                  </svg>
                  Full Graph
                </button>
              )}
              <button
                onClick={() => setShowProperties(!showProperties)}
                className={`px-4 py-2 rounded-lg transition-colors ${
                  showProperties 
                    ? 'bg-blue-600 text-white hover:bg-blue-700' 
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                }`}
              >
                {showProperties ? '✓ Properties' : 'Properties'}
              </button>
              {(selectedNode || selectedEdge || searchQuery || selectedLabel !== 'all') && !isTraversalMode && (
                <button
                  onClick={handleReset}
                  className="px-4 py-2 bg-gray-200 text-gray-700 hover:bg-gray-300 rounded-lg transition-colors"
                >
                  Clear Filters
                </button>
              )}
            </div>
          </div>

          {/* Search and filter controls */}
          {graphData.nodes.length > 0 && (
            <div className="flex gap-4 items-center">
              <div className="flex-1">
                <input
                  type="text"
                  placeholder="Search nodes by label, ID, or properties..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              {stats && stats.labels.length > 0 && (
                <select
                  value={selectedLabel}
                  onChange={(e) => setSelectedLabel(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="all">All Labels</option>
                  {stats.labels.map(label => (
                    <option key={label} value={label}>{label}</option>
                  ))}
                </select>
              )}
            </div>
          )}
        </div>

        {/* Main content area */}
        <div className="flex gap-6">
          {/* Graph visualization */}
          <div className="flex-1">
            <div className="bg-white rounded-lg shadow-sm overflow-hidden" style={{ height: '600px' }}>
              {graphData.nodes.length > 0 ? (
                <GraphCanvas
                  nodes={filteredGraphData.nodes}
                  edges={filteredGraphData.edges}
                  onNodeClick={handleNodeClick}
                  onEdgeClick={handleEdgeClick}
                  onNodePointerOver={(node) => setHoveredNode(node)}
                  onNodePointerOut={() => setHoveredNode(null)}
                  labelType="all"
                  draggable
                />
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-gray-500 space-y-4">
                  {dbPath ? (
                    <div className="text-center">
                      <div className="text-lg mb-2">No graph data available</div>
                      <div className="text-sm">The database may be empty or there was an error loading the data.</div>
                    </div>
                  ) : (
                    <div className="text-center max-w-md">
                      <div className="text-lg mb-2">Welcome to SombraDB Graph Explorer</div>
                      <div className="text-sm mb-4">Enter a database path above to start exploring your graph data.</div>
                      <div className="text-xs text-gray-400 space-y-1">
                        <div>Try: <code className="bg-gray-100 px-1 rounded">./demo.db</code> (if you have demo data)</div>
                        <div>Or: <code className="bg-gray-100 px-1 rounded">:memory:</code> (for an empty in-memory database)</div>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Hover tooltip */}
            {hoveredNode && !selectedNode && (
              <div className="mt-4 bg-white rounded-lg shadow-sm p-4 border-l-4 border-blue-500">
                <div className="text-sm text-gray-600">Hovering:</div>
                <div className="font-semibold text-gray-900">{hoveredNode.label}</div>
                <div className="text-xs text-gray-500 mt-1">
                  {hoveredNode.data.labels.join(', ')} • ID: {hoveredNode.data.nodeId}
                </div>
              </div>
            )}
          </div>
          
          {/* Properties sidebar */}
          {showProperties && (
            <div className="w-80 bg-white rounded-lg shadow-sm p-6 overflow-y-auto" style={{ maxHeight: '600px' }}>
              <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <span className="w-1 h-5 bg-blue-500 rounded"></span>
                Details
              </h3>
              
              {selectedNode && (
                <div className="space-y-4">
                  <div className="border-b border-gray-200 pb-3">
                    <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Node</div>
                    <h4 className="text-lg font-medium text-gray-900">{selectedNode.label}</h4>
                  </div>

                  {/* Action button */}
                  {!isTraversalMode && (
                    <button
                      onClick={() => handleExploreConnections(selectedNode)}
                      className="w-full px-4 py-3 bg-gradient-to-r from-blue-500 to-blue-600 text-white rounded-lg hover:from-blue-600 hover:to-blue-700 transition-all flex items-center justify-center gap-2 font-medium shadow-sm"
                    >
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                      </svg>
                      Explore Connections
                    </button>
                  )}

                  <div>
                    <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Information</div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">ID</span>
                        <span className="font-mono text-gray-900">{selectedNode.data.nodeId}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">Labels</span>
                        <div className="flex gap-1 flex-wrap justify-end">
                          {selectedNode.data.labels.map((label: string) => (
                            <span key={label} className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs">
                              {label}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div>
                    <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Properties</div>
                    {Object.keys(selectedNode.data.properties).length > 0 ? (
                      <div className="space-y-2">
                        {Object.entries(selectedNode.data.properties).map(([key, value]) => (
                          <div key={key} className="bg-gray-50 rounded p-2">
                            <div className="text-xs font-medium text-gray-600 mb-1">{key}</div>
                            <div className="text-sm text-gray-900 font-mono break-all">{String(value)}</div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-gray-400 italic">No properties</div>
                    )}
                  </div>
                </div>
              )}
              
              {selectedEdge && (
                <div className="space-y-4">
                  <div className="border-b border-gray-200 pb-3">
                    <div className="text-xs text-gray-500 uppercase tracking-wide mb-1">Edge</div>
                    <h4 className="text-lg font-medium text-gray-900">{selectedEdge.label}</h4>
                  </div>

                  <div>
                    <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Information</div>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">ID</span>
                        <span className="font-mono text-gray-900">{selectedEdge.data.edgeId}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">Type</span>
                        <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs">
                          {selectedEdge.data.edgeType}
                        </span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">From</span>
                        <span className="font-mono text-gray-900">{selectedEdge.source}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-gray-600">To</span>
                        <span className="font-mono text-gray-900">{selectedEdge.target}</span>
                      </div>
                    </div>
                  </div>

                  <div>
                    <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">Properties</div>
                    {Object.keys(selectedEdge.data.properties).length > 0 ? (
                      <div className="space-y-2">
                        {Object.entries(selectedEdge.data.properties).map(([key, value]) => (
                          <div key={key} className="bg-gray-50 rounded p-2">
                            <div className="text-xs font-medium text-gray-600 mb-1">{key}</div>
                            <div className="text-sm text-gray-900 font-mono break-all">{String(value)}</div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-gray-400 italic">No properties</div>
                    )}
                  </div>
                </div>
              )}
              
              {!selectedNode && !selectedEdge && (
                <div className="text-gray-400 text-sm text-center py-8">
                  <svg className="w-12 h-12 mx-auto mb-2 opacity-50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122" />
                  </svg>
                  <p>Click on a node or edge to view its details</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
