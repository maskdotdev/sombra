#!/usr/bin/env node
/**
 * Demonstrates batching 10,000 nodes and 20,000 edges with the fluent create builder.
 *
 * Usage:
 *   node examples/bulk_create.js [nodeCount] [edgeCount]
 *
 * Counts default to 10,000 nodes / 20,000 edges. Larger numbers are fine—the builder
 * submits everything in a single transaction.
 */
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { Database } = require('../main.js')

function tempDbPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sombra-bulk-'))
  return path.join(dir, 'graph.db')
}

function parseCount(arg, fallback) {
  if (arg === undefined) return fallback
  const parsed = Number(arg)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('counts must be positive numbers')
  }
  return Math.floor(parsed)
}

async function main() {
  const nodeCount = parseCount(process.argv[2], 10000)
  const edgeCount = parseCount(process.argv[3], 20000)
  if (edgeCount < 2) {
    throw new Error('need at least two edges to demonstrate connections')
  }

  const dbPath = tempDbPath()
  console.log('Creating database at', dbPath)
  const db = Database.open(dbPath, {
    synchronous: 'normal',
    commitCoalesceMs: 0,
    commitMaxFrames: 16384,
    cachePages: 16384,
  })

  const builder = db.create()
  const handles = []

  for (let i = 0; i < nodeCount; i++) {
    const props = {
      name: `Bulk User ${i}`,
      shard: i % 16,
      created_at: Date.now(),
    }
    handles.push(builder.node(['BulkUser'], props))
  }

  for (let i = 0; i < edgeCount; i++) {
    const src = handles[i % nodeCount]
    const dst = handles[(i * 13 + 7) % nodeCount]
    if (src === dst) {
      // Skip self loops by advancing the destination.
      const next = (i + 1) % nodeCount
      builder.edge(src, 'KNOWS', handles[next], { weight: i / edgeCount })
      continue
    }
    builder.edge(src, 'KNOWS', dst, { weight: i / edgeCount })
  }

  console.time('bulk-create')
  const summary = builder.execute()
  console.timeEnd('bulk-create')
  console.log(`Inserted ${summary.nodes.length} nodes and ${summary.edges.length} edges`)

  const sampleRows = await db
    .query()
    .match({ var: 'u', label: 'BulkUser' })
    .where('u', (pred) => pred.eq('shard', 0))
    .select(['u'])
    .execute()
  const sample = sampleRows.slice(0, 5)
  console.log('Sample query results (first 5 shard 0 users):')
  for (const row of sample) {
    console.log(JSON.stringify(row))
  }
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
