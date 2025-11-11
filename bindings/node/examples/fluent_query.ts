import { Database } from '..'

async function main() {
  const dbPath = process.argv[2] ?? './fluent-query.db'
  const db = Database.open(dbPath)
  db.seedDemo()

  const rows = await db
    .query()
    .match({ var: 'a', label: 'User' })
    .where('FOLLOWS', { var: 'b', label: 'User' })
    .where('a', (pred) => pred.eq('country', 'US'))
    .where('b', (pred) => pred.between('name', 'Ada', 'Grace'))
    .select([{ var: 'a', as: 'source' }, { var: 'b', as: 'target' }])
    .distinct()
    .execute()

  for (const row of rows) {
    const source = row.source
    const target = row.target
    if (source && target) {
      console.log(
        `source=${source._id} (${source.props.name}) -> target=${target._id} (${target.props.name})`,
      )
    }
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
