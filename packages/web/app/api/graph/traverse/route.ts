import { NextRequest, NextResponse } from 'next/server';
import { traverseFrom } from '@/lib/db';
import { transformGraphData } from '@/lib/transforms';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const nodeIdParam = searchParams.get('nodeId');
    const depthParam = searchParams.get('depth');
    const dbPath = request.headers.get('X-Database-Path');
    
    if (!nodeIdParam) {
      return NextResponse.json(
        { error: 'nodeId query parameter is required' },
        { status: 400 }
      );
    }
    
    const nodeId = parseInt(nodeIdParam, 10);
    if (isNaN(nodeId)) {
      return NextResponse.json(
        { error: 'nodeId must be a valid number' },
        { status: 400 }
      );
    }
    
    const depth = depthParam ? parseInt(depthParam, 10) : 2;
    if (isNaN(depth) || depth < 1) {
      return NextResponse.json(
        { error: 'depth must be a positive number' },
        { status: 400 }
      );
    }
    
    const { nodes, edges } = traverseFrom(nodeId, depth, dbPath || undefined);
    const graphData = transformGraphData(nodes, edges);
    
    return NextResponse.json(graphData);
  } catch (error) {
    console.error('Error in traverse API:', error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
