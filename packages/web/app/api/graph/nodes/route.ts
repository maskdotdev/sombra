import { NextRequest, NextResponse } from 'next/server';
import { getAllNodes } from '@/lib/db';
import { transformGraphData } from '@/lib/transforms';

export async function GET(request: NextRequest) {
  try {
    const dbPath = request.headers.get('X-Database-Path');
    const nodes = getAllNodes(dbPath || undefined);
    const edges: any[] = []; // Empty edges for nodes-only endpoint
    const { nodes: transformedNodes } = transformGraphData(nodes, edges);
    return NextResponse.json({ nodes: transformedNodes });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to fetch nodes' },
      { status: 500 }
    );
  }
}
