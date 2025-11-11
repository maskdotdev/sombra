#!/usr/bin/env node
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const { Database } = require('../main.js')

function tempDbPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sombra-example-'))
  return path.join(dir, 'db')
}

async function main() {
  const db = Database.open(tempDbPath()).seedDemo()

  const newUserId = db.createNode('User', { name: 'Example User', bio: 'Hello from Node' })
  console.log('Created user id:', newUserId)

  db.updateNode(newUserId, { set: { bio: 'Updated bio' } })

  const rows = await db
    .query()
    .match('User')
    .where('n0', (pred) => pred.eq('name', 'Example User'))
    .select(['n0'])
    .execute()
  console.log('Query results:', rows)

  db.deleteNode(newUserId, true)
  console.log('Deleted user', newUserId)
}

main()
