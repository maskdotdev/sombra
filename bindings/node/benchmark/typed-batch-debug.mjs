/**
 * Benchmark comparing JSON path (create()) vs Typed path (batchCreate())
 * 
 * This benchmark measures the performance difference between:
 * 1. CreateBuilder (JSON serialization path)
 * 2. BatchCreateBuilder (typed FFI path)
 * 
 * Run with: node benchmark/typed-batch.mjs
 */
import { Bench } from 'tinybench'
import fs from 'node:fs'
import path from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Database, estimateBatchSize } = require('../main.js')

// Configuration
const BATCH_SIZES = [100, 500, 1000, 5000, 10000]
const ITERATIONS = 50
const BENCH_ROOT = path.resolve(process.cwd(), '..', '..', 'target', 'bench')
const DB_PATH_JSON = path.join(BENCH_ROOT, 'typed-bench-json.db')
const DB_PATH_TYPED = path.join(BENCH_ROOT, 'typed-bench-typed.db')

function ensureBenchDir() {
  fs.mkdirSync(BENCH_ROOT, { recursive: true })
}

function cleanupDb(dbPath) {
  try {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath)
    if (fs.existsSync(`${dbPath}-wal`)) fs.unlinkSync(`${dbPath}-wal`)
    if (fs.existsSync(`${dbPath}-shm`)) fs.unlinkSync(`${dbPath}-shm`)
  } catch {
    // Ignore cleanup errors
  }
}

function openDatabase(dbPath) {
  cleanupDb(dbPath)
  return Database.open(dbPath, {
    synchronous: 'normal',
    commitCoalesceMs: 5,
  })
}

/**
 * Generate sample user data
 */
function generateUserData(count) {
  const users = []
  for (let i = 0; i < count; i++) {
    users.push({
      name: `User_${i}_${Math.random().toString(36).substring(2, 10)}`,
      age: 18 + (i % 60),
      email: `user${i}@example.com`,
      score: Math.random() * 1000,
      active: i % 2 === 0,
    })
  }
  return users
}

/**
 * Benchmark: JSON path (CreateBuilder)
 */
function benchmarkJsonPath(db, users) {
  const builder = db.create()
  for (const user of users) {
    builder.node('User', user)
  }
  return builder.execute()
}

/**
 * Benchmark: Typed path (BatchCreateBuilder)
 */
function benchmarkTypedPath(db, users) {
  const builder = db.batchCreate()
  for (const user of users) {
    builder.node('User', user)
  }
  return builder.execute()
}

/**
 * Benchmark: JSON path with edges
 */
function benchmarkJsonPathWithEdges(db, users) {
  const builder = db.create()
  const handles = []
  
  for (let i = 0; i < users.length; i++) {
    const handle = builder.node('User', users[i], `user${i}`)
    handles.push(handle)
  }
  
  // Create edges between consecutive users
  for (let i = 0; i < handles.length - 1; i++) {
    builder.edge(`user${i}`, 'KNOWS', `user${i + 1}`, { since: 2020 + (i % 5) })
  }
  
  return builder.execute()
}

/**
 * Benchmark: Typed path with edges
 */
function benchmarkTypedPathWithEdges(db, users) {
  const builder = db.batchCreate()
  const handles = []
  
  for (let i = 0; i < users.length; i++) {
    const handle = builder.node('User', users[i], `$user${i}`)
    handles.push(handle)
  }
  
  // Create edges between consecutive users
  for (let i = 0; i < handles.length - 1; i++) {
    builder.edge(`$user${i}`, 'KNOWS', `$user${i + 1}`, { since: 2020 + (i % 5) })
  }
  
  return builder.execute()
}

/**
 * Run a single benchmark comparison
 */
async function runComparison(batchSize) {
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Batch size: ${batchSize} nodes`)
  console.log('='.repeat(60))
  
  const users = generateUserData(batchSize)
  
  // Estimate optimal batch size
  const sampleRecords = users.slice(0, 10).map(u => ({ label: 'User', props: u }))
  const estimatedSize = estimateBatchSize(sampleRecords)
  console.log(`Estimated optimal batch size: ${estimatedSize}`)
  
  const bench = new Bench({ iterations: ITERATIONS })
  
  // Nodes only benchmarks
  bench.add('JSON path (nodes only)', () => {
    const db = openDatabase(DB_PATH_JSON)
    try {
      benchmarkJsonPath(db, users)
    } finally {
      db.close()
    }
  })
  
  bench.add('Typed path (nodes only)', () => {
    const db = openDatabase(DB_PATH_TYPED)
    try {
      benchmarkTypedPath(db, users)
    } finally {
      db.close()
    }
  })
  
  // Nodes + edges benchmarks
  bench.add('JSON path (nodes + edges)', () => {
    const db = openDatabase(DB_PATH_JSON)
    try {
      benchmarkJsonPathWithEdges(db, users)
    } finally {
      db.close()
    }
  })
  
  bench.add('Typed path (nodes + edges)', () => {
    const db = openDatabase(DB_PATH_TYPED)
    try {
      benchmarkTypedPathWithEdges(db, users)
    } finally {
      db.close()
    }
  })
  
  await bench.run()
  console.log("TASK_ERRORS", bench.tasks.map(t => t.error))
  
  // Format results
  const results = bench.tasks
    .filter(task => task.result != null)
    .map(task => ({
      name: task.name,
      opsPerSec: Math.round(task.result.hz),
      avgMs: (task.result.mean * 1000).toFixed(3),
      samples: task.result.samples.length,
    }))
  
  if (results.length === 0) {
    console.log('No benchmark results available')
    return []
  }
  
  console.table(results)
  
  // Calculate speedup
  const jsonNodes = results.find(r => r.name.includes('JSON') && r.name.includes('nodes only'))
  const typedNodes = results.find(r => r.name.includes('Typed') && r.name.includes('nodes only'))
  const jsonEdges = results.find(r => r.name.includes('JSON') && r.name.includes('edges'))
  const typedEdges = results.find(r => r.name.includes('Typed') && r.name.includes('edges'))
  
  if (jsonNodes && typedNodes) {
    const speedup = (typedNodes.opsPerSec / jsonNodes.opsPerSec).toFixed(2)
    console.log(`\nNodes only speedup: ${speedup}x (typed vs JSON)`)
  }
  
  if (jsonEdges && typedEdges) {
    const speedup = (typedEdges.opsPerSec / jsonEdges.opsPerSec).toFixed(2)
    console.log(`Nodes + edges speedup: ${speedup}x (typed vs JSON)`)
  }
  
  return results
}

/**
 * Run throughput comparison (records per second)
 */
async function runThroughputComparison() {
  console.log(`\n${'='.repeat(60)}`)
  console.log('Throughput comparison (records/second)')
  console.log('='.repeat(60))
  
  const batchSize = 10000
  const users = generateUserData(batchSize)
  
  // Warmup
  console.log('Warming up...')
  for (let i = 0; i < 3; i++) {
    const db = openDatabase(DB_PATH_JSON)
    benchmarkJsonPath(db, users.slice(0, 100))
    db.close()
    
    const db2 = openDatabase(DB_PATH_TYPED)
    benchmarkTypedPath(db2, users.slice(0, 100))
    db2.close()
  }
  
  // Measure JSON path
  console.log('\nMeasuring JSON path...')
  const jsonTimes = []
  for (let i = 0; i < 10; i++) {
    const db = openDatabase(DB_PATH_JSON)
    const start = performance.now()
    benchmarkJsonPath(db, users)
    const elapsed = performance.now() - start
    jsonTimes.push(elapsed)
    db.close()
  }
  
  // Measure Typed path
  console.log('Measuring Typed path...')
  const typedTimes = []
  for (let i = 0; i < 10; i++) {
    const db = openDatabase(DB_PATH_TYPED)
    const start = performance.now()
    benchmarkTypedPath(db, users)
    const elapsed = performance.now() - start
    typedTimes.push(elapsed)
    db.close()
  }
  
  const jsonAvg = jsonTimes.reduce((a, b) => a + b, 0) / jsonTimes.length
  const typedAvg = typedTimes.reduce((a, b) => a + b, 0) / typedTimes.length
  
  const jsonRecordsPerSec = Math.round((batchSize / jsonAvg) * 1000)
  const typedRecordsPerSec = Math.round((batchSize / typedAvg) * 1000)
  
  console.log(`\nJSON path:  ${jsonRecordsPerSec.toLocaleString()} records/sec (avg ${jsonAvg.toFixed(1)}ms)`)
  console.log(`Typed path: ${typedRecordsPerSec.toLocaleString()} records/sec (avg ${typedAvg.toFixed(1)}ms)`)
  console.log(`Speedup: ${(typedRecordsPerSec / jsonRecordsPerSec).toFixed(2)}x`)
}

async function main() {
  console.log('Typed Batch API Benchmark')
  console.log('Comparing JSON path (create()) vs Typed path (batchCreate())')
  console.log(`Iterations per benchmark: ${ITERATIONS}`)
  
  ensureBenchDir()
  
  // Run comparisons for different batch sizes
  const allResults = []
  for (const batchSize of BATCH_SIZES) {
    const results = await runComparison(batchSize)
    allResults.push({ batchSize, results })
  }
  
  // Run throughput comparison
  await runThroughputComparison()
  
  // Summary
  console.log(`\n${'='.repeat(60)}`)
  console.log('Summary')
  console.log('='.repeat(60))
  
  console.log('\nSpeedup by batch size:')
  for (const { batchSize, results } of allResults) {
    const jsonNodes = results.find(r => r.name.includes('JSON') && r.name.includes('nodes only'))
    const typedNodes = results.find(r => r.name.includes('Typed') && r.name.includes('nodes only'))
    if (jsonNodes && typedNodes) {
      const speedup = (typedNodes.opsPerSec / jsonNodes.opsPerSec).toFixed(2)
      console.log(`  ${batchSize.toString().padStart(5)} nodes: ${speedup}x speedup`)
    }
  }
  
  // Cleanup
  cleanupDb(DB_PATH_JSON)
  cleanupDb(DB_PATH_TYPED)
  
  console.log('\nBenchmark complete!')
}

main().catch(console.error)
