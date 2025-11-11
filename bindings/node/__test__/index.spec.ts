import test from 'ava'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { Database } from '..'

function tempPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sombra-node-'))
  return path.join(dir, 'db')
}

test('executing fluent query returns seeded rows', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const result = await db
    .query()
    .match('User')
    .where('n0', (pred) => pred.eq('name', 'Ada'))
    .select(['n0'])
    .execute()

  t.true(Array.isArray(result.rows))
  t.is(result.rows.length, 1)
  const entity = result.rows[0].n0 as { _id: number; props: Record<string, unknown> }
  t.truthy(entity)
  t.true(typeof entity._id === 'number')
  t.true(typeof entity.props === 'object')
})

test('streaming query iterates over results', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const stream = db.query().match('User').select(['n0']).stream()

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
    .requestId('req-node')
    .match('User')
    .select(['n0'])
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
  const result = await db
    .query()
    .match({ var: 'a', label: 'User' })
    .select([{ var: 'a', prop: 'name', as: 'label' }])
    .execute()

  t.true(result.rows.length > 0)
  t.true(typeof result.rows[0].label === 'string')
})

test('DateTime literals support Date objects and ISO strings', (t) => {
  const db = Database.open(tempPath())
  const dateSpec = db
    .query()
    .match('User')
    .where('n0', (pred) => pred.eq('created_at', new Date('2020-01-01T00:00:00Z')))
    ._build()
  t.is(dateSpec.predicate?.value?.t ?? dateSpec.predicate?.args?.[0]?.value?.t, 'DateTime')

  const isoSpec = db
    .query()
    .match('User')
    .where('n0', (pred) => pred.eq('created_at', '2020-01-01T00:00:00Z'))
    ._build()
  t.is(isoSpec.predicate?.value?.t ?? isoSpec.predicate?.args?.[0]?.value?.t, 'DateTime')

  t.throws(() =>
    db.query().match('User').where('n0', (pred) => pred.eq('created_at', '2020-01-01T00:00:00')),
  )
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
      .match('User')
      .where('n0', (pred) => pred.eq('name', 'Ada'))
      .select(['n0']),
  )
  t.throws(
    () => db.query().match('User').where('n0', (pred) => pred.eq('unknown_prop', 'oops')),
    { message: /Unknown property 'unknown_prop'/ },
  )
  t.throws(
    () => db.query().match('User').select([{ var: 'n0', prop: 'bogus' }]),
    { message: /Unknown property 'bogus'/ },
  )
})
