import path from 'node:path'

import { and, Database, between, eq, inList, not } from '..'
import { DemoNode, isDemoNode } from './shared'

const DEFAULT_DB_PATH = './fluent-query.db'

function invokedDirectly(scriptBase: string): boolean {
  const entry = process.argv[1]
  if (!entry) {
    return false
  }
  return path.basename(entry).startsWith(scriptBase)
}

export async function runFluentQueryExample(dbPath: string = DEFAULT_DB_PATH): Promise<void> {
  const db = Database.open(dbPath, { autocheckpointMs: 0 })
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

if (invokedDirectly('fluent_query')) {
  runFluentQueryExample(process.argv[2] ?? DEFAULT_DB_PATH).catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
