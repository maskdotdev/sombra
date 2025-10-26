import { NextRequest, NextResponse } from 'next/server';
import { getAllEdges } from '@/lib/db';
import { transformGraphData } from '@/lib/transforms';

export async function GET(request: NextRequest) {
  try {
    const dbPath = request.headers.get('X-Database-Path');
    const edges = getAllEdges(dbPath || undefined);
    const nodes: any[] = []; // Empty nodes for edges-only endpoint
    const { edges: transformedEdges } = transformGraphData(nodes, edges);
    return NextResponse.json({ edges: transformedEdges });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to fetch edges' },
      { status: 500 }
    );
  }
}
