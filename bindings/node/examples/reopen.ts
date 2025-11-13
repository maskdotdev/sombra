import path from 'node:path'

import { Database } from '..'
import { DemoNode, isDemoNode } from './shared'

const DEFAULT_DB_PATH = './fluent-query.db'

export type ReopenSummary = {
  nodes: DemoNode[]
  edges: Array<{ source: DemoNode; target: DemoNode }>
}

function invokedDirectly(scriptBase: string): boolean {
  const entry = process.argv[1]
  if (!entry) {
    return false
  }
  return path.basename(entry).startsWith(scriptBase)
}

export async function reopenAndLogExample(dbPath: string = DEFAULT_DB_PATH): Promise<ReopenSummary> {
  const db = Database.open(dbPath, { createIfMissing: false })

  const nodeRows = (await db.query().nodes('User').execute()) as Array<{ n0?: unknown }>
  const nodes: DemoNode[] = []
  console.log('nodes stored in database:')
  for (const row of nodeRows) {
    const node = row.n0
    if (isDemoNode(node)) {
      nodes.push(node)
      const props = node.props ?? {}
      const name = typeof props.name === 'string' ? props.name : '(missing name)'
      console.log(`- id=${node._id} name=${name}`)
    }
  }

  const edgeRows = (await db
    .query()
    .match('User')
    .where('FOLLOWS', 'User')
    .direction('out')
    .select(['n0', 'n1'])
    .execute()) as Array<{ n0?: unknown; n1?: unknown }>
  const edges: Array<{ source: DemoNode; target: DemoNode }> = []
  console.log('edges stored in database:')
  for (const row of edgeRows) {
    const source = row.n0
    const target = row.n1
    if (isDemoNode(source) && isDemoNode(target)) {
      edges.push({ source, target })
      console.log(`- source=${source._id} -> target=${target._id}`)
    }
  }

  return { nodes, edges }
}

if (invokedDirectly('reopen')) {
  reopenAndLogExample(process.argv[2] ?? DEFAULT_DB_PATH).catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
