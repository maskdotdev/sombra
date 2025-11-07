import { Bench } from 'tinybench'
import fs from 'node:fs'
import path from 'node:path'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)
const { Database } = require('../main.js')

const LABEL_USER = 'User'
const EDGE_TYPE_FOLLOWS = 'FOLLOWS'
const EDGE_ANCHOR_COUNT = 16
const OPS_PER_BATCH = parseEnvInt('BENCH_BATCH_SIZE', 256)
const PREFILL_BATCHES = parseEnvInt('BENCH_PREFILL_BATCHES', 512)
const BENCH_ROOT = path.resolve(process.cwd(), '..', '..', 'target', 'bench')
const DB_PATH = path.join(BENCH_ROOT, 'node-crud.db')

function parseEnvInt(name, fallback) {
  const raw = process.env[name]
  if (!raw) return fallback
  const parsed = Number.parseInt(raw, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function ensureBenchDir() {
  fs.mkdirSync(BENCH_ROOT, { recursive: true })
}

function openDatabase() {
  ensureBenchDir()
  const existed = fs.existsSync(DB_PATH)
  const db = Database.open(DB_PATH, {
    synchronous: 'normal',
    commitCoalesceMs: 5,
  })
  if (!existed) {
    db.seedDemo()
  }
  return db
}

class CrudHarness {
  constructor(db, batchSize) {
    this.db = db
    this.batchSize = batchSize
    this.counter = 0
    this.nodeDeletePool = []
    this.edgeDeletePool = []
    this.edgeAnchorNodes = []
    this.nodeUpdateTarget = 0
    this.edgeUpdateTarget = 0
    this.bootstrap()
  }

  bootstrap() {
    if (this.edgeAnchorNodes.length === 0) {
      for (let i = 0; i < EDGE_ANCHOR_COUNT; i++) {
        const id = this.createUser(`edge-anchor-${i}`)
        this.edgeAnchorNodes.push(id)
      }
    }
    this.nodeUpdateTarget = this.edgeAnchorNodes[0]
    this.edgeUpdateTarget = this.createEdgeBetween(
      this.edgeAnchorNodes[0],
      this.edgeAnchorNodes[1 % EDGE_ANCHOR_COUNT],
    )
    this.prefillDeletePools()
  }

  prefillDeletePools() {
    const target = this.batchSize * PREFILL_BATCHES
    if (this.nodeDeletePool.length < target) {
      const missing = target - this.nodeDeletePool.length
      this.nodeDeletePool.push(...this.createUsers(missing))
    }
    if (this.edgeDeletePool.length < target) {
      const missing = target - this.edgeDeletePool.length
      this.edgeDeletePool.push(...this.createEdges(missing))
    }
  }

  createNodeBatch() {
    const ops = []
    for (let i = 0; i < this.batchSize; i++) {
      const name = `bench-node-${this.bumpCounter()}`
      ops.push({ op: 'createNode', labels: [LABEL_USER], props: { name } })
    }
    const summary = this.db.mutateMany(ops)
    for (const id of summary.createdNodes ?? []) {
      this.nodeDeletePool.push(id)
    }
  }

  updateNodeBatch() {
    const ops = []
    for (let i = 0; i < this.batchSize; i++) {
      ops.push({
        op: 'updateNode',
        id: this.nodeUpdateTarget,
        set: { bio: `bio-${this.bumpCounter()}` },
        unset: [],
      })
    }
    this.db.mutateMany(ops)
  }

  deleteNodeBatch() {
    this.ensureNodeDeleteCapacity()
    const ids = this.nodeDeletePool.splice(0, this.batchSize)
    const ops = ids.map((id) => ({ op: 'deleteNode', id, cascade: true }))
    if (ops.length > 0) {
      this.db.mutateMany(ops)
    }
  }

  createEdgeBatch() {
    const ops = []
    for (let i = 0; i < this.batchSize; i++) {
      const { src, dst } = this.nextEdgePair()
      ops.push({ op: 'createEdge', src, dst, ty: EDGE_TYPE_FOLLOWS, props: {} })
    }
    const summary = this.db.mutateMany(ops)
    for (const id of summary.createdEdges ?? []) {
      this.edgeDeletePool.push(id)
    }
  }

  updateEdgeBatch() {
    const ops = []
    for (let i = 0; i < this.batchSize; i++) {
      const weight = this.bumpCounter() % 1_000
      ops.push({
        op: 'updateEdge',
        id: this.edgeUpdateTarget,
        set: { weight },
        unset: [],
      })
    }
    this.db.mutateMany(ops)
  }

  deleteEdgeBatch() {
    this.ensureEdgeDeleteCapacity()
    const ids = this.edgeDeletePool.splice(0, this.batchSize)
    const ops = ids.map((id) => ({ op: 'deleteEdge', id }))
    if (ops.length > 0) {
      this.db.mutateMany(ops)
    }
  }

  async readUsers() {
    const rows = await this.db.query().match(LABEL_USER).select(['a']).execute()
    return rows.length
  }

  createUser(name) {
    const summary = this.db.mutateMany([{ op: 'createNode', labels: [LABEL_USER], props: { name } }])
    const ids = summary.createdNodes ?? []
    const id = ids[ids.length - 1]
    if (id == null) {
      throw new Error('createUser must return an id')
    }
    return id
  }

  createUsers(count) {
    const created = []
    let remaining = count
    while (remaining > 0) {
      const chunk = Math.min(remaining, this.batchSize)
      const ops = []
      for (let i = 0; i < chunk; i++) {
        const name = `delete-pool-${this.bumpCounter()}`
        ops.push({ op: 'createNode', labels: [LABEL_USER], props: { name } })
      }
      const summary = this.db.mutateMany(ops)
      created.push(...(summary.createdNodes ?? []))
      remaining -= chunk
    }
    return created
  }

  createEdgeBetween(src, dst) {
    const summary = this.db.mutateMany([{ op: 'createEdge', src, dst, ty: EDGE_TYPE_FOLLOWS, props: {} }])
    const ids = summary.createdEdges ?? []
    const id = ids[ids.length - 1]
    if (id == null) {
      throw new Error('createEdgeBetween must return an id')
    }
    return id
  }

  createEdges(count) {
    const created = []
    let remaining = count
    while (remaining > 0) {
      const chunk = Math.min(remaining, this.batchSize)
      const ops = []
      for (let i = 0; i < chunk; i++) {
        const { src, dst } = this.nextEdgePair()
        ops.push({ op: 'createEdge', src, dst, ty: EDGE_TYPE_FOLLOWS, props: {} })
      }
      const summary = this.db.mutateMany(ops)
      created.push(...(summary.createdEdges ?? []))
      remaining -= chunk
    }
    return created
  }

  ensureNodeDeleteCapacity() {
    if (this.nodeDeletePool.length < this.batchSize) {
      this.nodeDeletePool.push(...this.createUsers(this.batchSize * 2))
    }
  }

  ensureEdgeDeleteCapacity() {
    if (this.edgeDeletePool.length < this.batchSize) {
      this.edgeDeletePool.push(...this.createEdges(this.batchSize * 2))
    }
  }

  nextEdgePair() {
    if (this.edgeAnchorNodes.length < 2) {
      throw new Error('edge anchor set must contain at least two nodes')
    }
    const idx = this.bumpCounter() % this.edgeAnchorNodes.length
    const src = this.edgeAnchorNodes[idx]
    const dst = this.edgeAnchorNodes[(idx + 1) % this.edgeAnchorNodes.length]
    return { src, dst }
  }

  bumpCounter() {
    const current = this.counter
    this.counter += 1
    return current
  }
}

const db = openDatabase()
const harness = new CrudHarness(db, OPS_PER_BATCH)

console.log(
  `CRUD benchmark using db=${DB_PATH}, batchSize=${OPS_PER_BATCH}, synchronous=normal, commitCoalesceMs=5`,
)

const bench = new Bench({ iterations: 200 })

bench.add(`create nodes x${OPS_PER_BATCH}`, () => {
  harness.createNodeBatch()
})

bench.add(`update nodes x${OPS_PER_BATCH}`, () => {
  harness.updateNodeBatch()
})

bench.add(`delete nodes x${OPS_PER_BATCH}`, () => {
  harness.deleteNodeBatch()
})

bench.add(`create edges x${OPS_PER_BATCH}`, () => {
  harness.createEdgeBatch()
})

bench.add(`update edges x${OPS_PER_BATCH}`, () => {
  harness.updateEdgeBatch()
})

bench.add(`delete edges x${OPS_PER_BATCH}`, () => {
  harness.deleteEdgeBatch()
})

bench.add('read users', async () => {
  await harness.readUsers()
})

await bench.run()
console.table(bench.table())
