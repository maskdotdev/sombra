import { Database, eq } from '..'

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

  const nodes = await db.query().nodes('User').execute()
  const entity = nodes[0]?.n0
  const entityRecord = entity as Record<string, unknown> | undefined
  console.log(entityRecord)

  const scalars = await db
    .query()
    .nodes('User')
    .select('name')
    .execute()
  const label = scalars[0]?.name
  if (typeof label === 'string') {
    label.toUpperCase()
  }

  db.query().nodes('User').where(eq('country', 'US'))
  db
    .query()
    .nodes('User')
    // @ts-expect-error invalid property selection should fail
    .select('unknown_prop')

  db.query().nodes('Post').where(eq('title', 'Hello'))
}

void typedExamples()
