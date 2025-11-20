import test from 'ava'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { Database, eq } from '..'
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
