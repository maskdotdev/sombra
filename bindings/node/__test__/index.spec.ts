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
  const rows = await db
    .query()
    .match('User')
    .whereProp('a', 'name', '=', 'Ada')
    .select(['a'])
    .execute()

  t.is(rows.length, 1)
  t.truthy(rows[0].a)
})

test('streaming query iterates over results', async (t) => {
  const db = Database.open(tempPath()).seedDemo()
  const stream = db.query().match('User').select(['a']).stream()

  const encountered: Array<number> = []
  for await (const row of stream) {
    encountered.push(row.a as number)
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
    .select(['a', 'b'])
    .explain()

  t.is(plan.plan.op, 'Project')
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
