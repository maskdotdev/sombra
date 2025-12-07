import test from 'ava'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import {
  Database,
  eq,
  ErrorCode,
  SombraError,
  AnalyzerError,
  JsonError,
  IoError,
  CorruptionError,
  ConflictError,
  SnapshotTooOldError,
  CancelledError,
  InvalidArgError,
  NotFoundError,
  ClosedError,
  wrapNativeError,
} from '..'
import { runFluentQueryExample } from '../examples/fluent_query'
import { reopenAndLogExample } from '../examples/reopen'

function tempPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sombra-node-'))
  return path.join(dir, 'db')
}

test('executing fluent query returns seeded rows', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const rows = await db
    .query()
    .nodes('User')
    .where(eq('name', 'Ada'))
    .execute()

  t.true(Array.isArray(rows))
  t.is(rows.length, 1)
  const entity = rows[0].n0 as { _id: number; props: Record<string, unknown> }
  t.truthy(entity)
  t.true(typeof entity._id === 'number')
  t.true(typeof entity.props === 'object')
})

test('execute optionally returns metadata payload', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const payload = await db
    .query()
    .nodes('User')
    .select('name')
    .requestId('req-meta')
    .execute(true)

  t.true(Array.isArray(payload.rows))
  t.true(payload.rows.length > 0)
  t.is(payload.request_id, 'req-meta')
  t.true(Array.isArray(payload.features))
})

test('streaming query iterates over results', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const stream = db.query().nodes('User').stream()

  const encountered: Array<number> = []
  for await (const row of stream) {
    const entity = row.n0 as { _id: number }
    encountered.push(entity._id)
  }

  t.true(encountered.length >= 3)
})

test('explain produces plan JSON', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const plan = await db
    .query()
    .match('User')
    .where('FOLLOWS', 'User')
    .direction('out')
    .select(['n0', 'n1'])
    .explain()

  t.true(Array.isArray(plan.plan))
  t.is(plan.plan[0]?.op, 'Project')
})

test('requestId flows through explain', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const plan = await db
    .query()
    .nodes('User')
    .requestId('req-node')
    .where(eq('name', 'Ada'))
    .explain()

  t.is(plan.request_id, 'req-node')
})

test('mutate supports basic CRUD operations', (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const summary = db.mutate({
    ops: [{ op: 'createNode', labels: ['User'], props: { name: 'Benchmark' } }],
  })
  t.true(Array.isArray(summary.createdNodes))
  const nodeId = summary.createdNodes[0]
  t.truthy(nodeId)

  db.updateNode(nodeId, { set: { bio: 'updated' } })
  db.deleteNode(nodeId, true)
  t.pass()
})

test('mutateMany batches operations', (t) => {
  const db = Database.open(tempPath())
  const summary = db.mutateMany([
    { op: 'createNode', labels: ['User'], props: { name: 'BatchA' } },
    { op: 'createNode', labels: ['User'], props: { name: 'BatchB' } },
  ])
  t.is(summary.createdNodes?.length, 2)
})

test('mutateBatched chunks large op lists', (t) => {
  const db = Database.open(tempPath())
  const ops = [
    { op: 'createNode', labels: ['User'], props: { name: 'Batch0' } },
    { op: 'createNode', labels: ['User'], props: { name: 'Batch1' } },
    { op: 'createNode', labels: ['User'], props: { name: 'Batch2' } },
  ]
  const summary = db.mutateBatched(ops, { batchSize: 2 })
  t.is(summary.createdNodes?.length, 3)
})

test('create builder supports handle references', (t) => {
  const db = Database.open(tempPath())
  const builder = db.create()
  const alice = builder.node('User', { name: 'Alice' })
  const bob = builder.node(['User'], { name: 'Bob' })
  builder.edge(alice, 'KNOWS', bob, { since: 2020 })
  const summary = builder.execute()
  t.is(summary.nodes.length, 2)
  t.is(summary.edges.length, 1)
  t.is(summary.alias('$missing'), undefined)
})

test('create builder supports alias chaining pattern', (t) => {
  const db = Database.open(tempPath())
  const summary = db
    .create()
    .node('User', { name: 'alice', age: 30 }, '$alice')
    .node('User', { name: 'bob', age: 25 }, '$bob')
    .node('User', { name: 'charlie', age: 35 }, '$charlie')
    .node('Company', { name: 'Acme Inc' }, '$company')
    .edge('$alice', 'FOLLOWS', '$bob')
    .edge('$bob', 'FOLLOWS', '$charlie')
    .edge('$alice', 'WORKS_AT', '$company', { role: 'Engineer' })
    .execute()

  t.is(summary.nodes.length, 4)
  t.is(summary.edges.length, 3)
  t.truthy(summary.aliases.$alice)
  t.truthy(summary.aliases.$bob)
  t.is(summary.alias('$alice'), summary.aliases.$alice)
})

test('transaction queues operations once', async (t) => {
  const db = Database.open(tempPath())
  const { summary, result } = await db.transaction(async (tx) => {
    tx.createNode('User', { name: 'TxUser1' })
    await Promise.resolve()
    tx.createNode('User', { name: 'TxUser2' })
    return 'ok'
  })
  t.is(summary.createdNodes?.length, 2)
  t.is(result, 'ok')
})

test('pragma toggles synchronous mode', (t) => {
  const db = Database.open(tempPath())
  const initial = db.pragma('synchronous')
  t.true(typeof initial === 'string')
  const updated = db.pragma('synchronous', 'normal')
  t.is(updated, 'normal')
  const current = db.pragma('synchronous')
  t.is(current, 'normal')
})

test('pragma toggles autocheckpoint window', (t) => {
  const db = Database.open(tempPath())
  db.pragma('autocheckpoint_ms', 5)
  t.is(db.pragma('autocheckpoint_ms'), 5)
  db.pragma('autocheckpoint_ms', null)
  t.is(db.pragma('autocheckpoint_ms'), null)
})

test('property projections return scalar columns', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const rows = await db
    .query()
    .nodes('User')
    .select('name')
    .execute()

  t.true(rows.length > 0)
  t.true(typeof rows[0].name === 'string')
})

test('DateTime literals support Date objects and ISO strings', (t) => {
  const db = Database.open(tempPath())
  const dateBuilder = db.query()
  dateBuilder.nodes('User').where(eq('created_at', new Date('2020-01-01T00:00:00Z')))
  const dateSpec = dateBuilder._build()
  t.is(dateSpec.predicate?.value?.t ?? dateSpec.predicate?.args?.[0]?.value?.t, 'DateTime')

  const isoBuilder = db.query()
  isoBuilder.nodes('User').where(eq('created_at', '2020-01-01T00:00:00Z'))
  const isoSpec = isoBuilder._build()
  t.is(isoSpec.predicate?.value?.t ?? isoSpec.predicate?.args?.[0]?.value?.t, 'DateTime')

  t.throws(() => db.query().nodes('User').where(eq('created_at', '2020-01-01T00:00:00')))
})

test('runtime schema validation rejects unknown properties', (t) => {
  const schema = {
    User: {
      name: { type: 'string' },
      created_at: { type: 'datetime' },
    },
  }
  const db = Database.open(tempPath(), { schema }).seedDemo()
  t.notThrows(() =>
    db
      .query()
      .nodes('User')
      .where(eq('name', 'Ada')),
  )
  t.throws(
    () => db.query().nodes('User').where(eq('unknown_prop', 'oops')),
    { message: /Unknown property 'unknown_prop'/ },
  )
  t.throws(
    () => db.query().match('User').select([{ var: 'n0', prop: 'bogus' }]),
    { message: /Unknown property 'bogus'/ },
  )
})

test('reopen example loads nodes and edges from an existing database', async (t) => {
  const dbPath = tempPath()
  await runFluentQueryExample(dbPath)
  const summary = await reopenAndLogExample(dbPath)
  t.true(summary.nodes.length > 0)
  t.true(summary.edges.length > 0)
})

// ============================================================================
// Database Lifecycle Tests
// ============================================================================

test('close() marks database as closed', (t) => {
  const db = Database.open(tempPath())
  t.false(db.isClosed)
  db.close()
  t.true(db.isClosed)
})

test('close() is idempotent', (t) => {
  const db = Database.open(tempPath())
  db.close()
  t.notThrows(() => db.close())
  t.notThrows(() => db.close())
  t.true(db.isClosed)
})

test('operations on closed database throw ClosedError', (t) => {
  const db = Database.open(tempPath())
  db.close()

  const err1 = t.throws(() => db.seedDemo())
  t.true(err1 instanceof ClosedError)
  t.is(err1?.message, 'database is closed')

  const err2 = t.throws(() => db.query())
  t.true(err2 instanceof ClosedError)

  const err3 = t.throws(() => db.create())
  t.true(err3 instanceof ClosedError)

  const err4 = t.throws(() => db.mutate({ ops: [] }))
  t.true(err4 instanceof ClosedError)

  const err5 = t.throws(() => db.pragma('synchronous'))
  t.true(err5 instanceof ClosedError)
})

test('create builder execute throws on closed database', (t) => {
  const db = Database.open(tempPath())
  const builder = db.create()
  builder.node('User', { name: 'Test' })
  db.close()

  const err = t.throws(() => builder.execute())
  t.true(err instanceof ClosedError)
  t.is(err?.message, 'database is closed')
})

// ============================================================================
// Persistence Tests
// ============================================================================

test('data persists after explicit close() and reopen', async (t) => {
  const dbPath = tempPath()

  // Create database, seed data, verify it exists
  const db1 = Database.open(dbPath)
  db1.seedDemo()

  const beforeClose = await db1.query().nodes('User').execute()
  t.is(beforeClose.length, 3, 'should have 3 users before close')

  // Explicitly close the database
  db1.close()

  // Reopen and verify data persisted
  const db2 = Database.open(dbPath, { createIfMissing: false })

  const afterReopen = await db2.query().nodes('User').execute()
  t.is(afterReopen.length, 3, 'should still have 3 users after reopen')

  db2.close()
})

test('node properties persist after close and reopen', async (t) => {
  const dbPath = tempPath()

  // Create database with custom data
  const db1 = Database.open(dbPath)
  db1.seedDemo()

  // Get original names
  const originalRows = await db1.query().nodes('User').execute()
  const originalNames = originalRows.map((r: { n0?: { props?: { name?: string } } }) =>
    r.n0?.props?.name
  ).sort()

  db1.close()

  // Reopen and verify properties
  const db2 = Database.open(dbPath, { createIfMissing: false })
  const reopenedRows = await db2.query().nodes('User').execute()
  const reopenedNames = reopenedRows.map((r: { n0?: { props?: { name?: string } } }) =>
    r.n0?.props?.name
  ).sort()

  t.deepEqual(reopenedNames, originalNames, 'property values should match after reopen')
  t.true(reopenedNames.includes('Ada'))
  t.true(reopenedNames.includes('Grace'))
  t.true(reopenedNames.includes('Alan'))

  db2.close()
})

test('edges persist after close and reopen', async (t) => {
  const dbPath = tempPath()

  // Create database with edges
  const db1 = Database.open(dbPath)
  db1.seedDemo()

  const edgesBefore = await db1.query()
    .match('User')
    .where('FOLLOWS', 'User')
    .select(['n0', 'n1'])
    .execute()
  t.true(edgesBefore.length > 0, 'should have edges before close')

  db1.close()

  // Reopen and verify edges
  const db2 = Database.open(dbPath, { createIfMissing: false })
  const edgesAfter = await db2.query()
    .match('User')
    .where('FOLLOWS', 'User')
    .select(['n0', 'n1'])
    .execute()

  t.is(edgesAfter.length, edgesBefore.length, 'edge count should match after reopen')

  db2.close()
})

test('multiple close/reopen cycles preserve data', async (t) => {
  const dbPath = tempPath()

  // First session - create data
  const db1 = Database.open(dbPath)
  db1.seedDemo()
  const count1 = (await db1.query().nodes('User').execute()).length
  t.is(count1, 3)
  db1.close()

  // Second session - verify and close
  const db2 = Database.open(dbPath, { createIfMissing: false })
  const count2 = (await db2.query().nodes('User').execute()).length
  t.is(count2, 3, 'data should persist after first reopen')
  db2.close()

  // Third session - verify again
  const db3 = Database.open(dbPath, { createIfMissing: false })
  const count3 = (await db3.query().nodes('User').execute()).length
  t.is(count3, 3, 'data should persist after second reopen')
  db3.close()
})

test('custom created nodes persist after close', async (t) => {
  const dbPath = tempPath()

  // Create database and add custom nodes
  const db1 = Database.open(dbPath)
  const result = db1.create()
    .node('Person', { name: 'John', age: 30 })
    .node('Person', { name: 'Jane', age: 25 })
    .execute()

  t.is(result.nodes.length, 2, 'should create 2 nodes')
  db1.close()

  // Reopen and verify
  const db2 = Database.open(dbPath, { createIfMissing: false })
  const persons = await db2.query().nodes('Person').execute()
  t.is(persons.length, 2, 'custom nodes should persist after reopen')

  db2.close()
})

// ============================================================================
// Error Code Tests
// ============================================================================

test('ErrorCode constants are defined', (t) => {
  t.is(ErrorCode.UNKNOWN, 'UNKNOWN')
  t.is(ErrorCode.MESSAGE, 'MESSAGE')
  t.is(ErrorCode.ANALYZER, 'ANALYZER')
  t.is(ErrorCode.JSON, 'JSON')
  t.is(ErrorCode.IO, 'IO')
  t.is(ErrorCode.CORRUPTION, 'CORRUPTION')
  t.is(ErrorCode.CONFLICT, 'CONFLICT')
  t.is(ErrorCode.SNAPSHOT_TOO_OLD, 'SNAPSHOT_TOO_OLD')
  t.is(ErrorCode.CANCELLED, 'CANCELLED')
  t.is(ErrorCode.INVALID_ARG, 'INVALID_ARG')
  t.is(ErrorCode.NOT_FOUND, 'NOT_FOUND')
  t.is(ErrorCode.CLOSED, 'CLOSED')
})

// ============================================================================
// Error Class Hierarchy Tests
// ============================================================================

test('SombraError has correct defaults', (t) => {
  const err = new SombraError('test message')
  t.is(err.message, 'test message')
  t.is(err.code, ErrorCode.UNKNOWN)
  t.is(err.name, 'SombraError')
  t.true(err instanceof Error)
})

test('SombraError accepts custom code', (t) => {
  const err = new SombraError('test', ErrorCode.IO)
  t.is(err.code, ErrorCode.IO)
})

test('AnalyzerError has correct code', (t) => {
  const err = new AnalyzerError('bad query')
  t.is(err.code, ErrorCode.ANALYZER)
  t.is(err.name, 'AnalyzerError')
  t.true(err instanceof SombraError)
  t.true(err instanceof Error)
})

test('JsonError has correct code', (t) => {
  const err = new JsonError('parse failed')
  t.is(err.code, ErrorCode.JSON)
  t.is(err.name, 'JsonError')
  t.true(err instanceof SombraError)
})

test('IoError has correct code', (t) => {
  const err = new IoError('disk error')
  t.is(err.code, ErrorCode.IO)
  t.is(err.name, 'IoError')
  t.true(err instanceof SombraError)
})

test('CorruptionError has correct code', (t) => {
  const err = new CorruptionError('data corrupt')
  t.is(err.code, ErrorCode.CORRUPTION)
  t.is(err.name, 'CorruptionError')
  t.true(err instanceof SombraError)
})

test('ConflictError has correct code', (t) => {
  const err = new ConflictError('write conflict')
  t.is(err.code, ErrorCode.CONFLICT)
  t.is(err.name, 'ConflictError')
  t.true(err instanceof SombraError)
})

test('SnapshotTooOldError has correct code', (t) => {
  const err = new SnapshotTooOldError('snapshot evicted')
  t.is(err.code, ErrorCode.SNAPSHOT_TOO_OLD)
  t.is(err.name, 'SnapshotTooOldError')
  t.true(err instanceof SombraError)
})

test('CancelledError has correct code', (t) => {
  const err = new CancelledError('request cancelled')
  t.is(err.code, ErrorCode.CANCELLED)
  t.is(err.name, 'CancelledError')
  t.true(err instanceof SombraError)
})

test('InvalidArgError has correct code', (t) => {
  const err = new InvalidArgError('bad argument')
  t.is(err.code, ErrorCode.INVALID_ARG)
  t.is(err.name, 'InvalidArgError')
  t.true(err instanceof SombraError)
})

test('NotFoundError has correct code', (t) => {
  const err = new NotFoundError('not found')
  t.is(err.code, ErrorCode.NOT_FOUND)
  t.is(err.name, 'NotFoundError')
  t.true(err instanceof SombraError)
})

test('ClosedError has correct code', (t) => {
  const err = new ClosedError('db closed')
  t.is(err.code, ErrorCode.CLOSED)
  t.is(err.name, 'ClosedError')
  t.true(err instanceof SombraError)
})

// ============================================================================
// wrapNativeError Tests
// ============================================================================

test('wrapNativeError parses [ANALYZER] prefix', (t) => {
  const err = wrapNativeError(new Error('[ANALYZER] invalid syntax'))
  t.true(err instanceof AnalyzerError)
  t.is(err.code, ErrorCode.ANALYZER)
  t.is(err.message, 'invalid syntax')
})

test('wrapNativeError parses [IO] prefix', (t) => {
  const err = wrapNativeError(new Error('[IO] file not found'))
  t.true(err instanceof IoError)
  t.is(err.code, ErrorCode.IO)
  t.is(err.message, 'file not found')
})

test('wrapNativeError parses [CORRUPTION] prefix', (t) => {
  const err = wrapNativeError(new Error('[CORRUPTION] page checksum mismatch'))
  t.true(err instanceof CorruptionError)
  t.is(err.code, ErrorCode.CORRUPTION)
  t.is(err.message, 'page checksum mismatch')
})

test('wrapNativeError parses [CONFLICT] prefix', (t) => {
  const err = wrapNativeError(new Error('[CONFLICT] write-write conflict'))
  t.true(err instanceof ConflictError)
  t.is(err.code, ErrorCode.CONFLICT)
  t.is(err.message, 'write-write conflict')
})

test('wrapNativeError parses [SNAPSHOT_TOO_OLD] prefix', (t) => {
  const err = wrapNativeError(new Error('[SNAPSHOT_TOO_OLD] reader evicted'))
  t.true(err instanceof SnapshotTooOldError)
  t.is(err.code, ErrorCode.SNAPSHOT_TOO_OLD)
  t.is(err.message, 'reader evicted')
})

test('wrapNativeError parses [CANCELLED] prefix', (t) => {
  const err = wrapNativeError(new Error('[CANCELLED] operation cancelled'))
  t.true(err instanceof CancelledError)
  t.is(err.code, ErrorCode.CANCELLED)
  t.is(err.message, 'operation cancelled')
})

test('wrapNativeError parses [INVALID_ARG] prefix', (t) => {
  const err = wrapNativeError(new Error('[INVALID_ARG] bad parameter'))
  t.true(err instanceof InvalidArgError)
  t.is(err.code, ErrorCode.INVALID_ARG)
  t.is(err.message, 'bad parameter')
})

test('wrapNativeError parses [NOT_FOUND] prefix', (t) => {
  const err = wrapNativeError(new Error('[NOT_FOUND] node does not exist'))
  t.true(err instanceof NotFoundError)
  t.is(err.code, ErrorCode.NOT_FOUND)
  t.is(err.message, 'node does not exist')
})

test('wrapNativeError parses [CLOSED] prefix', (t) => {
  const err = wrapNativeError(new Error('[CLOSED] database closed'))
  t.true(err instanceof ClosedError)
  t.is(err.code, ErrorCode.CLOSED)
  t.is(err.message, 'database closed')
})

test('wrapNativeError parses [JSON] prefix', (t) => {
  const err = wrapNativeError(new Error('[JSON] invalid json'))
  t.true(err instanceof JsonError)
  t.is(err.code, ErrorCode.JSON)
  t.is(err.message, 'invalid json')
})

test('wrapNativeError returns SombraError for unknown code', (t) => {
  const err = wrapNativeError(new Error('[UNKNOWN] something went wrong'))
  t.true(err instanceof SombraError)
  t.is(err.code, ErrorCode.UNKNOWN)
  t.is(err.message, 'something went wrong')
})

test('wrapNativeError returns SombraError for unprefixed message', (t) => {
  const err = wrapNativeError(new Error('no prefix here'))
  t.true(err instanceof SombraError)
  t.is(err.code, ErrorCode.UNKNOWN)
  t.is(err.message, 'no prefix here')
})

test('wrapNativeError handles string input', (t) => {
  const err = wrapNativeError('[ANALYZER] string error')
  t.true(err instanceof AnalyzerError)
  t.is(err.message, 'string error')
})

test('wrapNativeError preserves existing SombraError', (t) => {
  const original = new IoError('already typed')
  const wrapped = wrapNativeError(original)
  t.is(wrapped, original)
})
