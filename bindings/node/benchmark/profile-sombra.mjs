/**
 * Profile Sombra Node.js binding performance to identify bottlenecks
 * 
 * Run with: node benchmark/profile-sombra.mjs
 */
import fs from 'node:fs'
import path from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Database } = require('../main.js')

const BENCH_ROOT = path.resolve(process.cwd(), '..', '..', 'target', 'bench')

function getDbPath(suffix) {
  return path.join(BENCH_ROOT, `profile-sombra-${suffix}.db`)
}

function cleanupDb(dbPath) {
  try {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(`${dbPath}-wal`)) fs.unlinkSync(`${dbPath}-wal`)
    if (fs.existsSync(`${dbPath}-shm`)) fs.unlinkSync(`${dbPath}-shm`)
  } catch {
    // Ignore
  }
}

function generateUsers(count) {
  const users = []
  for (let i = 0; i < count; i++) {
    users.push({
      name: `User_${i}_${Math.random().toString(36).substring(2, 10)}`,
      age: 18 + (i % 60),
      email: `user${i}@example.com`,
      score: Math.random() * 1000,
    })
  }
  return users
}

function formatNumber(n) {
  return n.toLocaleString('en-US')
}

async function main() {
  fs.mkdirSync(BENCH_ROOT, { recursive: true })
  
  const batchSize = 10000
  const users = generateUsers(batchSize)
  
  console.log('='.repeat(60))
  console.log(`Profiling Sombra with ${batchSize} nodes`)
  console.log('='.repeat(60))
  
  // ========================================================================
  // Test 1: Measure total time with create()
  // ========================================================================
  console.log('\n1. Total time with db.create() (JSON path)')
  const dbPath1 = getDbPath('test1')
  cleanupDb(dbPath1)
  const db1 = Database.open(dbPath1, { synchronous: 'normal' })
  
  const t1Start = performance.now()
  const builder1 = db1.create()
  const t1BuilderCreated = performance.now()
  
  for (const user of users) {
    builder1.node('User', user)
  }
  const t1NodesAdded = performance.now()
  
  builder1.execute()
  const t1Executed = performance.now()
  
  db1.close()
  
  const executeTime = t1Executed - t1NodesAdded
  const addNodesTime = t1NodesAdded - t1BuilderCreated
  const totalTime = t1Executed - t1Start
  
  console.log(`  Create builder: ${(t1BuilderCreated - t1Start).toFixed(2)}ms`)
  console.log(`  Add ${batchSize} nodes: ${addNodesTime.toFixed(2)}ms (${formatNumber(Math.round(batchSize / addNodesTime * 1000))} nodes/s)`)
  console.log(`  Execute: ${executeTime.toFixed(2)}ms`)
  console.log(`  Total: ${totalTime.toFixed(2)}ms (${formatNumber(Math.round(batchSize / totalTime * 1000))} ops/s)`)
  
  // ========================================================================
  // Test 2: Measure JSON serialization overhead
  // ========================================================================
  console.log('\n2. JSON serialization overhead (V8 only, no DB)')
  
  const t2Start = performance.now()
  const serialized = []
  for (const user of users) {
    serialized.push(JSON.stringify({ label: 'User', props: user }))
  }
  const t2End = performance.now()
  const jsonTime = t2End - t2Start
  
  console.log(`  Serialize ${batchSize} nodes: ${jsonTime.toFixed(2)}ms (${formatNumber(Math.round(batchSize / jsonTime * 1000))} ops/s)`)
  
  // ========================================================================
  // Test 3: Measure with minimal properties
  // ========================================================================
  console.log('\n3. Minimal properties (just {id: i})')
  const dbPath3 = getDbPath('test3')
  cleanupDb(dbPath3)
  const db3 = Database.open(dbPath3, { synchronous: 'normal' })
  
  const t3Start = performance.now()
  const builder3 = db3.create()
  for (let i = 0; i < batchSize; i++) {
    builder3.node('User', { id: i })
  }
  builder3.execute()
  const t3End = performance.now()
  
  db3.close()
  
  console.log(`  Total: ${(t3End - t3Start).toFixed(2)}ms (${formatNumber(Math.round(batchSize / (t3End - t3Start) * 1000))} ops/s)`)
  
  // ========================================================================
  // Test 4: Measure with no properties
  // ========================================================================
  console.log('\n4. No properties (empty {})')
  const dbPath4 = getDbPath('test4')
  cleanupDb(dbPath4)
  const db4 = Database.open(dbPath4, { synchronous: 'normal' })
  
  const t4Start = performance.now()
  const builder4 = db4.create()
  for (let i = 0; i < batchSize; i++) {
    builder4.node('User', {})
  }
  builder4.execute()
  const t4End = performance.now()
  
  db4.close()
  
  console.log(`  Total: ${(t4End - t4Start).toFixed(2)}ms (${formatNumber(Math.round(batchSize / (t4End - t4Start) * 1000))} ops/s)`)
  
  // ========================================================================
  // Test 5: Multiple small batches vs one large batch
  // ========================================================================
  console.log('\n5. Batch size comparison (same total nodes)')
  
  const batchSizes = [100, 500, 1000, 5000, 10000]
  for (const bs of batchSizes) {
    const dbPath = getDbPath(`batch-${bs}`)
    cleanupDb(dbPath)
    const db = Database.open(dbPath, { synchronous: 'normal' })
    
    const numBatches = Math.ceil(batchSize / bs)
    const tStart = performance.now()
    
    for (let b = 0; b < numBatches; b++) {
      const builder = db.create()
      const start = b * bs
      const end = Math.min(start + bs, batchSize)
      for (let i = start; i < end; i++) {
        builder.node('User', users[i])
      }
      builder.execute()
    }
    
    const tEnd = performance.now()
    db.close()
    
    const opsPerSec = Math.round(batchSize / (tEnd - tStart) * 1000)
    console.log(`  Batch size ${bs.toString().padStart(5)}: ${(tEnd - tStart).toFixed(2).padStart(8)}ms (${formatNumber(opsPerSec).padStart(8)} ops/s) - ${numBatches} commits`)
  }
  
  // ========================================================================
  // Test 6: Single-node transactions (worst case)
  // ========================================================================
  console.log('\n6. Single-node transactions (worst case, 1000 nodes only)')
  const dbPath6 = getDbPath('test6')
  cleanupDb(dbPath6)
  const db6 = Database.open(dbPath6, { synchronous: 'normal' })
  
  const smallBatch = 1000
  const t6Start = performance.now()
  for (let i = 0; i < smallBatch; i++) {
    db6.create().node('User', users[i]).execute()
  }
  const t6End = performance.now()
  
  db6.close()
  
  console.log(`  Total: ${(t6End - t6Start).toFixed(2)}ms (${formatNumber(Math.round(smallBatch / (t6End - t6Start) * 1000))} ops/s)`)
  
  // ========================================================================
  // Summary
  // ========================================================================
  console.log(`\n${'='.repeat(60)}`)
  console.log('SUMMARY')
  console.log('='.repeat(60))
  console.log(`
Raw B-tree (from compare-bench): ~666,000 ops/s
Node.js binding with create():   ~${formatNumber(Math.round(batchSize / totalTime * 1000))} ops/s

Time breakdown for ${batchSize} nodes:
- Adding nodes to builder (JS): ${addNodesTime.toFixed(2)}ms (${(addNodesTime / totalTime * 100).toFixed(1)}%)
- Execute (FFI + Rust):         ${executeTime.toFixed(2)}ms (${(executeTime / totalTime * 100).toFixed(1)}%)
- JSON serialization alone:     ${jsonTime.toFixed(2)}ms

The execute() call is ${(executeTime / jsonTime).toFixed(1)}x slower than pure JSON serialization.
This suggests the bottleneck is in the Rust graph layer, not FFI or serialization.
`)
  
  // Cleanup all temp databases
  cleanupDb(dbPath1)
  cleanupDb(getDbPath('test3'))
  cleanupDb(getDbPath('test4'))
  cleanupDb(getDbPath('test6'))
  for (const bs of batchSizes) {
    cleanupDb(getDbPath(`batch-${bs}`))
  }
  
  console.log('Profile complete!')
}

main().catch(console.error)
