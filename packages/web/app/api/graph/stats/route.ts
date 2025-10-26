import { NextRequest, NextResponse } from 'next/server';
import { getGraphStats } from '@/lib/db';

export async function GET(request: NextRequest) {
  try {
    const dbPath = request.headers.get('X-Database-Path');
    const stats = getGraphStats(dbPath || undefined);
    return NextResponse.json(stats);
  } catch (error) {
    console.error('Error in stats API:', error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
