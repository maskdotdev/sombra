import { Database } from '..'

type DemoNode = {
  _id: number
  props: {
    name?: string
    [key: string]: unknown
  }
}

function isDemoNode(value: unknown): value is DemoNode {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_id' in value &&
    typeof (value as { _id?: unknown })._id === 'number' &&
    'props' in value &&
    typeof (value as { props?: unknown }).props === 'object'
  )
}

async function main() {
  const dbPath = process.argv[2] ?? './fluent-query.db'
  const db = Database.open(dbPath)
  db.seedDemo()

  const rows = await db
    .query()
    .match({ var: 'a', label: 'User' })
    .where('FOLLOWS', { var: 'b', label: 'User' })
    .where('a', (pred) => pred.eq('name', 'Ada'))
    .where('b', (pred) => pred.between('name', 'Ada', 'Grace'))
    .select([
      { var: 'a', as: 'source' },
      { var: 'b', as: 'target' },
    ])
    .distinct()
    .execute()
  console.log('Query rows:', rows)

  for (const row of rows) {
    const source = row.source
    const target = row.target
    if (isDemoNode(source) && isDemoNode(target)) {
      const sourceName = String(source.props.name ?? '(unknown)')
      const targetName = String(target.props.name ?? '(unknown)')
      console.log(`source=${source._id} (${sourceName}) -> target=${target._id} (${targetName})`)
    }
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
