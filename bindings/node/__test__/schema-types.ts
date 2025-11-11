import { Database } from '..'

type Schema = {
  User: {
    _id: string
    name: string
    country: 'US' | 'CA'
    created_at: Date
  }
  Post: {
    _id: string
    title: string
    author_id: string
  }
}

async function typedExamples() {
  const db = Database.open<Schema>(':memory:')

  const nodes = await db.query().match('User').select(['n0']).execute()
  const entity = nodes.rows[0]?.n0
  const entityRecord = entity as Record<string, unknown> | undefined
  console.log(entityRecord)

  const scalars = await db
    .query()
    .match({ var: 'u', label: 'User' })
    .select([{ var: 'u', prop: 'name', as: 'userName' }])
    .execute()
  const label = scalars.rows[0]?.userName
  if (typeof label === 'string') {
    label.toUpperCase()
  }

  db.query()
    .match({ var: 'u', label: 'User' })
    .where('u', (pred) => pred.eq('country', 'US'))
  db.query()
    .match({ var: 'u', label: 'User' })
    // @ts-expect-error name typo should fail
    .where('u', (pred) => pred.eq('unknown_prop', 'value'))

  db.query()
    .match({ var: 'p', label: 'Post' })
    .where('p', (pred) => pred.eq('title', 'Hello'))
}

void typedExamples()
