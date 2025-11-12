import { and, Database, between, eq, inList, not } from '..'

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

  const scalarRows = await db
    .query()
    .nodes('User')
    .where(
      and(
        inList('name', ['Ada', 'Grace', 'Alan']),
        not(eq('name', 'Alan')),
      ),
    )
    .select('name')
    .execute()
  console.log('names returned by nodes() scope:')
  for (const row of scalarRows) {
    if (typeof row.name === 'string') {
      console.log(`- ${row.name}`)
    }
  }

  const { rows: followerRows, request_id: requestId } = await db
    .query()
    .match({ followee: 'User', follower: 'User' })
    .where('FOLLOWS', { var: 'followee', label: 'User' })
    .on('follower', (scope) => scope.where(eq('name', 'Ada')))
    .on('followee', (scope) => scope.where(between('name', 'Ada', 'Grace')))
    .select([
      { var: 'follower', as: 'follower' },
      { var: 'followee', as: 'followee' },
    ])
    .requestId('fluent-query')
    .distinct()
    .execute(true)
  console.log('follow relationships returned by match().on() scopes:')
  for (const row of followerRows) {
    const follower = row.follower
    const followee = row.followee
    if (isDemoNode(follower) && isDemoNode(followee)) {
      const followerName = String(follower.props.name ?? '(unknown)')
      const followeeName = String(followee.props.name ?? '(unknown)')
      console.log(`source=${follower._id} (${followerName}) -> target=${followee._id} (${followeeName})`)
    }
  }
  console.log(`request id for previous query: ${requestId}`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
