/**
 * Benchmark comparing Sombra vs Neo4j
 * 
 * This benchmark measures:
 * 1. Batch node inserts (UNWIND for Neo4j)
 * 2. Single node inserts (per-node transactions)
 * 3. Node + edge creation
 * 
 * Prerequisites:
 *   docker run -d --name neo4j-bench \
 *     -p 7474:7474 -p 7687:7687 \
 *     -e NEO4J_AUTH=neo4j/testpassword \
 *     neo4j:5.15.0
 * 
 * Install deps:
 *   npm install neo4j-driver
 * 
 * Run with: node benchmark/neo4j-compare.mjs
 */
import fs from 'node:fs'
import path from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)

// Configuration
const NEO4J_URI = process.env.NEO4J_URI || 'bolt://localhost:7687'
const NEO4J_USER = process.env.NEO4J_USER || 'neo4j'
const NEO4J_PASSWORD = process.env.NEO4J_PASSWORD || 'testpassword'

const BENCH_ROOT = path.resolve(process.cwd(), '..', '..', 'target', 'bench')

const BENCH_ITERATIONS = 5

// Test configurations
const BATCH_SIZES = [100, 1000, 5000, 10000]

// ============================================================================
// Utilities
// ============================================================================

function ensureBenchDir() {
  fs.mkdirSync(BENCH_ROOT, { recursive: true })
}

function cleanupSombraDb(dbPath) {
  try {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(`${dbPath}-wal`)) fs.unlinkSync(`${dbPath}-wal`)
    if (fs.existsSync(`${dbPath}-shm`)) fs.unlinkSync(`${dbPath}-shm`)
  } catch {
    // Ignore cleanup errors
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

// ============================================================================
// Neo4j Functions
// ============================================================================

let neo4jDriver = null

async function initNeo4j() {
  try {
    const neo4j = (await import('neo4j-driver')).default
    neo4jDriver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(NEO4J_USER, NEO4J_PASSWORD))
    
    // Test connection
    const session = neo4jDriver.session()
    await session.run('RETURN 1')
    await session.close()
    
    console.log(`Connected to Neo4j at ${NEO4J_URI}`)
    return true
  } catch (err) {
    console.log(`\nNeo4j not available: ${err.message}`)
    console.log('To run Neo4j benchmarks, start Neo4j with:')
    console.log('  docker run -d --name neo4j-bench \\')
    console.log('    -p 7474:7474 -p 7687:7687 \\')
    console.log('    -e NEO4J_AUTH=neo4j/testpassword \\')
    console.log('    neo4j:5.15.0')
    console.log('')
    return false
  }
}

async function closeNeo4j() {
  if (neo4jDriver) {
    await neo4jDriver.close()
    neo4jDriver = null
  }
}

async function clearNeo4j() {
  const session = neo4jDriver.session()
  try {
    await session.run('MATCH (n) DETACH DELETE n')
  } finally {
    await session.close()
  }
}

async function neo4jBatchInsert(users) {
  const session = neo4jDriver.session()
  try {
    await session.run(
      'UNWIND $users AS u CREATE (:User {name: u.name, age: u.age, email: u.email, score: u.score})',
      { users }
    )
  } finally {
    await session.close()
  }
}

async function neo4jSingleInserts(users) {
  const session = neo4jDriver.session()
  try {
    for (const user of users) {
      await session.run(
        'CREATE (:User {name: $name, age: $age, email: $email, score: $score})',
        user
      )
    }
  } finally {
    await session.close()
  }
}

async function neo4jBatchInsertWithEdges(users) {
  const session = neo4jDriver.session()
  try {
    // Create users with temporary IDs
    await session.run(
      `UNWIND range(0, size($users)-1) AS idx
       CREATE (u:User {
         tmpId: idx,
         name: $users[idx].name,
         age: $users[idx].age,
         email: $users[idx].email,
         score: $users[idx].score
       })`,
      { users }
    )
    
    // Create KNOWS edges between consecutive users
    await session.run(
      `MATCH (a:User), (b:User)
       WHERE a.tmpId = b.tmpId - 1
       CREATE (a)-[:KNOWS {since: 2020 + (a.tmpId % 5)}]->(b)`
    )
    
    // Remove temporary IDs
    await session.run('MATCH (u:User) REMOVE u.tmpId')
  } finally {
    await session.close()
  }
}

// ============================================================================
// Sombra Functions
// ============================================================================

let Database = null

function initSombra() {
  try {
    const sombra = require('../main.js')
    Database = sombra.Database
    console.log('Sombra loaded successfully')
    return true
  } catch (err) {
    console.log(`Failed to load Sombra: ${err.message}`)
    return false
  }
}

function openFreshSombraDb(suffix) {
  const dbPath = path.join(BENCH_ROOT, `neo4j-compare-${suffix}.db`)
  cleanupSombraDb(dbPath)
  return Database.open(dbPath, {
    synchronous: 'normal',
  })
}

function sombraBatchInsert(db, users) {
  const builder = db.create()
  for (const user of users) {
    builder.node('User', user)
  }
  return builder.execute()
}

function sombraSingleInserts(db, users) {
  for (const user of users) {
    db.create().node('User', user).execute()
  }
}

function sombraBatchInsertWithEdges(db, users) {
  const builder = db.create()
  
  for (let i = 0; i < users.length; i++) {
    builder.node('User', users[i], `u${i}`)
  }
  
  for (let i = 0; i < users.length - 1; i++) {
    builder.edge(`u${i}`, 'KNOWS', `u${i + 1}`, { since: 2020 + (i % 5) })
  }
  
  return builder.execute()
}

// ============================================================================
// Benchmark Runner
// ============================================================================

function printResults(results) {
  console.table(results)
  
  // Print speedup summary
  console.log('\nSpeedup Analysis (Sombra vs Neo4j):')
  for (const row of results) {
    if (row.Speedup !== 'N/A') {
      console.log(`  ${row.Benchmark} @ ${row.BatchSize}: Sombra is ${row.Speedup} faster`)
    }
  }
}

async function main() {
  console.log('='.repeat(60))
  console.log('Sombra vs Neo4j Benchmark')
  console.log('='.repeat(60))
  
  ensureBenchDir()
  
  // Initialize databases
  const sombraOk = initSombra()
  const neo4jOk = await initNeo4j()
  
  if (!sombraOk && !neo4jOk) {
    console.log('\nNo databases available to benchmark!')
    process.exit(1)
  }
  
  const allResults = []
  
  // ========================================================================
  // Benchmark 1: Batch Insert (nodes only)
  // ========================================================================
  console.log(`\n${'='.repeat(60)}`)
  console.log('1. BATCH INSERT (nodes only)')
  console.log('='.repeat(60))
  
  for (const batchSize of BATCH_SIZES) {
    const users = generateUsers(batchSize)
    
    // Sombra benchmark - use a fresh database for each iteration
    let sombraAvg = null
    if (sombraOk) {
      console.log(`\nRunning Sombra batch insert (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < BENCH_ITERATIONS; i++) {
        // Each iteration gets its own fresh database
        const db = openFreshSombraDb(`batch-${batchSize}-${i}`)
        const start = performance.now()
        sombraBatchInsert(db, users)
        const elapsed = performance.now() - start
        times.push(elapsed)
        db.close()
      }
      sombraAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Sombra: ${sombraAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / sombraAvg) * 1000))} ops/s)`)
    }
    
    // Neo4j benchmark
    let neo4jAvg = null
    if (neo4jOk) {
      console.log(`Running Neo4j batch insert (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < BENCH_ITERATIONS; i++) {
        await clearNeo4j()
        const start = performance.now()
        await neo4jBatchInsert(users)
        const elapsed = performance.now() - start
        times.push(elapsed)
      }
      neo4jAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Neo4j: ${neo4jAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / neo4jAvg) * 1000))} ops/s)`)
    }
    
    const row = {
      Benchmark: 'Batch Insert',
      BatchSize: batchSize,
      SombraMs: sombraAvg ? sombraAvg.toFixed(2) : 'N/A',
      SombraOpsPerSec: sombraAvg ? formatNumber(Math.round((batchSize / sombraAvg) * 1000)) : 'N/A',
      Neo4jMs: neo4jAvg ? neo4jAvg.toFixed(2) : 'N/A',
      Neo4jOpsPerSec: neo4jAvg ? formatNumber(Math.round((batchSize / neo4jAvg) * 1000)) : 'N/A',
      Speedup: (sombraAvg && neo4jAvg) ? `${(neo4jAvg / sombraAvg).toFixed(2)}x` : 'N/A'
    }
    allResults.push(row)
  }
  
  // ========================================================================
  // Benchmark 2: Single Insert (per-node transactions)
  // ========================================================================
  console.log(`\n${'='.repeat(60)}`)
  console.log('2. SINGLE INSERT (per-node transaction)')
  console.log('='.repeat(60))
  
  // Only test small batch sizes for single inserts (too slow otherwise)
  const singleInsertSizes = [100, 500]
  
  for (const batchSize of singleInsertSizes) {
    const users = generateUsers(batchSize)
    const iterations = 3 // fewer iterations for slow test
    
    // Sombra benchmark
    let sombraAvg = null
    if (sombraOk) {
      console.log(`\nRunning Sombra single inserts (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < iterations; i++) {
        const db = openFreshSombraDb(`single-${batchSize}-${i}`)
        const start = performance.now()
        sombraSingleInserts(db, users)
        const elapsed = performance.now() - start
        times.push(elapsed)
        db.close()
      }
      sombraAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Sombra: ${sombraAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / sombraAvg) * 1000))} ops/s)`)
    }
    
    // Neo4j benchmark
    let neo4jAvg = null
    if (neo4jOk) {
      console.log(`Running Neo4j single inserts (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < iterations; i++) {
        await clearNeo4j()
        const start = performance.now()
        await neo4jSingleInserts(users)
        const elapsed = performance.now() - start
        times.push(elapsed)
      }
      neo4jAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Neo4j: ${neo4jAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / neo4jAvg) * 1000))} ops/s)`)
    }
    
    const row = {
      Benchmark: 'Single Insert',
      BatchSize: batchSize,
      SombraMs: sombraAvg ? sombraAvg.toFixed(2) : 'N/A',
      SombraOpsPerSec: sombraAvg ? formatNumber(Math.round((batchSize / sombraAvg) * 1000)) : 'N/A',
      Neo4jMs: neo4jAvg ? neo4jAvg.toFixed(2) : 'N/A',
      Neo4jOpsPerSec: neo4jAvg ? formatNumber(Math.round((batchSize / neo4jAvg) * 1000)) : 'N/A',
      Speedup: (sombraAvg && neo4jAvg) ? `${(neo4jAvg / sombraAvg).toFixed(2)}x` : 'N/A'
    }
    allResults.push(row)
  }
  
  // ========================================================================
  // Benchmark 3: Batch Insert with Edges
  // ========================================================================
  console.log(`\n${'='.repeat(60)}`)
  console.log('3. BATCH INSERT WITH EDGES')
  console.log('='.repeat(60))
  
  for (const batchSize of BATCH_SIZES) {
    const users = generateUsers(batchSize)
    
    // Sombra benchmark
    let sombraAvg = null
    if (sombraOk) {
      console.log(`\nRunning Sombra batch insert + edges (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < BENCH_ITERATIONS; i++) {
        const db = openFreshSombraDb(`edges-${batchSize}-${i}`)
        const start = performance.now()
        sombraBatchInsertWithEdges(db, users)
        const elapsed = performance.now() - start
        times.push(elapsed)
        db.close()
      }
      sombraAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Sombra: ${sombraAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / sombraAvg) * 1000))} ops/s)`)
    }
    
    // Neo4j benchmark
    let neo4jAvg = null
    if (neo4jOk) {
      console.log(`Running Neo4j batch insert + edges (${batchSize} nodes)...`)
      const times = []
      for (let i = 0; i < BENCH_ITERATIONS; i++) {
        await clearNeo4j()
        const start = performance.now()
        await neo4jBatchInsertWithEdges(users)
        const elapsed = performance.now() - start
        times.push(elapsed)
      }
      neo4jAvg = times.reduce((a, b) => a + b, 0) / times.length
      console.log(`  Neo4j: ${neo4jAvg.toFixed(2)}ms avg (${formatNumber(Math.round((batchSize / neo4jAvg) * 1000))} ops/s)`)
    }
    
    const row = {
      Benchmark: 'Batch + Edges',
      BatchSize: batchSize,
      SombraMs: sombraAvg ? sombraAvg.toFixed(2) : 'N/A',
      SombraOpsPerSec: sombraAvg ? formatNumber(Math.round((batchSize / sombraAvg) * 1000)) : 'N/A',
      Neo4jMs: neo4jAvg ? neo4jAvg.toFixed(2) : 'N/A',
      Neo4jOpsPerSec: neo4jAvg ? formatNumber(Math.round((batchSize / neo4jAvg) * 1000)) : 'N/A',
      Speedup: (sombraAvg && neo4jAvg) ? `${(neo4jAvg / sombraAvg).toFixed(2)}x` : 'N/A'
    }
    allResults.push(row)
  }
  
  // ========================================================================
  // Summary
  // ========================================================================
  console.log(`\n${'='.repeat(60)}`)
  console.log('SUMMARY')
  console.log('='.repeat(60))
  printResults(allResults)
  
  // Cleanup
  await closeNeo4j()
  
  // Clean up all temp databases
  for (const batchSize of BATCH_SIZES) {
    for (let i = 0; i < BENCH_ITERATIONS; i++) {
      cleanupSombraDb(path.join(BENCH_ROOT, `neo4j-compare-batch-${batchSize}-${i}.db`))
      cleanupSombraDb(path.join(BENCH_ROOT, `neo4j-compare-edges-${batchSize}-${i}.db`))
    }
  }
  for (const batchSize of [100, 500]) {
    for (let i = 0; i < 3; i++) {
      cleanupSombraDb(path.join(BENCH_ROOT, `neo4j-compare-single-${batchSize}-${i}.db`))
    }
  }
  
  console.log('\nBenchmark complete!')
}

main().catch(err => {
  console.error('Benchmark failed:', err)
  process.exit(1)
})
