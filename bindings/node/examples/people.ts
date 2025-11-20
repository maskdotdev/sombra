import { rmSync } from 'node:fs'
import path from 'node:path'

import type { CreateSummary, NodeSchema } from '..'
import { Database, eq } from '..'

const DEFAULT_DB_PATH = './people-example.db'

interface PeopleSchema extends NodeSchema {
  Person: {
    name: string
    age: number
  }
  Company: {
    name: string
    employees: number
  }
  City: {
    name: string
    state: string
  }
  Pet: {
    name: string
    species: string
  }
}

type NodeIdMap = {
  fabian: number
  michelle: number
  levi: number
  sarah: number
  daniel: number
  carlos: number
  auroraTech: number
  omniCorp: number
  austin: number
  mochi: number
}

type GraphState = {
  nodes: NodeIdMap
}

function invokedDirectly(scriptBase: string): boolean {
  const entry = process.argv[1]
  if (!entry) {
    return false
  }
  return path.basename(entry).startsWith(scriptBase)
}

export async function runPeopleExample(dbPath: string = DEFAULT_DB_PATH): Promise<void> {
  if (dbPath === DEFAULT_DB_PATH) {
    resetDbFiles(dbPath)
  }
  const db = Database.open<PeopleSchema>(dbPath, { autocheckpointMs: 0, createIfMissing: true })
  const graph = seedPeopleGraph(db)
  await logQueries(db, graph)
}

function seedPeopleGraph(db: Database<PeopleSchema>): GraphState {
  const summary = db
    .create()
    .node('Person', { age: 32, name: 'Fabian' }, '$fabian')
    .node('Person', { age: 33, name: 'Michelle' }, '$michelle')
    .node('Person', { age: 4, name: 'Levi' }, '$levi')
    .node('Person', { age: 29, name: 'Sarah' }, '$sarah')
    .node('Person', { age: 31, name: 'Daniel' }, '$daniel')
    .node('Person', { age: 28, name: 'Carlos' }, '$carlos')
    .node('Company', { employees: 250, name: 'AuroraTech' }, '$auroraTech')
    .node('Company', { employees: 1200, name: 'OmniCorp' }, '$omniCorp')
    .node('City', { name: 'Austin', state: 'TX' }, '$austin')
    .node('Pet', { name: 'Mochi', species: 'Dog' }, '$mochi')
    .edge('$fabian', 'KNOWS', '$michelle', { since: 2015 })
    .edge('$fabian', 'KNOWS', '$sarah', { since: 2022 })
    .edge('$sarah', 'KNOWS', '$daniel', { since: 2023 })
    .edge('$carlos', 'KNOWS', '$fabian', { since: 2024 })
    .edge('$michelle', 'PARENT_OF', '$levi', { since: 2021 })
    .edge('$daniel', 'PARENT_OF', '$levi', { since: 2021 })
    .edge('$michelle', 'MARRIED_TO', '$fabian', { since: 2018 })
    .edge('$michelle', 'WORKS_AT', '$auroraTech', { role: 'Product Manager' })
    .edge('$daniel', 'WORKS_AT', '$omniCorp', { role: 'Data Engineer' })
    .edge('$sarah', 'WORKS_AT', '$auroraTech', { role: 'Design Lead' })
    .edge('$michelle', 'LIVES_IN', '$austin')
    .edge('$carlos', 'LIVES_IN', '$austin')
    .edge('$daniel', 'LIVES_IN', '$austin')
    .edge('$michelle', 'OWNS', '$mochi', { since: 2020 })
    .execute()

  const nodes: NodeIdMap = {
    fabian: requireAlias(summary, '$fabian'),
    michelle: requireAlias(summary, '$michelle'),
    levi: requireAlias(summary, '$levi'),
    sarah: requireAlias(summary, '$sarah'),
    daniel: requireAlias(summary, '$daniel'),
    carlos: requireAlias(summary, '$carlos'),
    auroraTech: requireAlias(summary, '$auroraTech'),
    omniCorp: requireAlias(summary, '$omniCorp'),
    austin: requireAlias(summary, '$austin'),
    mochi: requireAlias(summary, '$mochi'),
  }

  const edgeKeys = [
    'knowsFabianMichelle',
    'knowsFabianSarah',
    'knowsCarlosFabian',
    'knowsSarahDaniel',
    'parentMichelleLevi',
    'parentDanielLevi',
    'marriedMichelleFabian',
    'worksMichelleAurora',
    'worksDanielOmni',
    'worksSarahAurora',
    'livesMichelleAustin',
    'livesCarlosAustin',
    'livesDanielAustin',
    'ownsMichelleMochi',
  ] as const
  const edges: Record<(typeof edgeKeys)[number], number> = Object.create(null)
  edgeKeys.forEach((key, idx) => {
    const id = summary.edges[idx]
    if (typeof id !== 'number') {
      throw new Error(`missing edge id for ${key}`)
    }
    edges[key] = id
  })

  console.log('\n=== Node IDs ===')
  console.log(nodes)

  console.log('\n=== Edge IDs ===')
  console.log(edges)

  return { nodes }
}

async function logQueries(db: Database<PeopleSchema>, graph: GraphState): Promise<void> {
  const fabianRows = await db
    .query()
    .match('Person')
    .where('n0', (pred) => pred.eq('name', 'Fabian'))
    .select(['n0'])
    .execute()
  console.log('\n=== Fabian query (nodes scope) ===')
  console.log(fabianRows[0])

  const depthOneRows = await db
    .query()
    .match({ origin: 'Person' })
    .on('origin', (scope) => scope.where(eq('name', 'Fabian')))
    .where('KNOWS', { var: 'neighbor', label: 'Person' })
    .select([
      { var: 'origin', prop: 'name', as: 'source' },
      { var: 'neighbor', prop: 'name', as: 'neighbor' },
      { var: 'neighbor', prop: 'age', as: 'neighborAge' },
    ])
    .execute()
  console.log('\n=== Fabian KNOWS neighbors ===')
  console.log(depthOneRows)

  const depthTwoRows = await db
    .query()
    .match({ origin: 'Person' })
    .on('origin', (scope) => scope.where(eq('name', 'Fabian')))
    .where('KNOWS', { var: 'friend', label: 'Person' })
    .where('KNOWS', { var: 'friendOfFriend', label: 'Person' })
    .select([
      { var: 'friend', prop: 'name', as: 'via' },
      { var: 'friendOfFriend', prop: 'name', as: 'neighbor' },
    ])
    .execute()
  console.log('\n=== Depth=2 reachability from Fabian ===')
  console.log(depthTwoRows)

  const auroraEmployees = await db
    .query()
    .match({ company: 'Company' })
    .on('company', (scope) => scope.where(eq('name', 'AuroraTech')))
    .direction('in')
    .where('WORKS_AT', { var: 'employee', label: 'Person' })
    .select([
      { var: 'company', prop: 'name', as: 'company' },
      { var: 'employee', prop: 'name', as: 'employee' },
      { var: 'employee', prop: 'age', as: 'age' },
    ])
    .execute()
  console.log('\n=== WORKS_AT -> AuroraTech (incoming) ===')
  console.log(auroraEmployees)

  const austinResidents = await db
    .query()
    .match({ city: 'City' })
    .on('city', (scope) => scope.where(eq('name', 'Austin')))
    .direction('in')
    .where('LIVES_IN', { var: 'resident', label: 'Person' })
    .select([
      { var: 'city', prop: 'name', as: 'city' },
      { var: 'resident', prop: 'name', as: 'resident' },
    ])
    .execute()
  console.log('\n=== Austin residents (LIVES_IN incoming) ===')
  console.log(austinResidents)

  const pets = await db
    .query()
    .match({ pet: 'Pet' })
    .on('pet', (scope) => scope.where(eq('name', 'Mochi')))
    .direction('in')
    .where('OWNS', { var: 'owner', label: 'Person' })
    .select([
      { var: 'pet', prop: 'name', as: 'pet' },
      { var: 'pet', prop: 'species', as: 'species' },
      { var: 'owner', prop: 'name', as: 'owner' },
    ])
    .execute()
  console.log('\n=== Owners for Mochi ===')
  console.log(pets)

  console.log('\nUse these IDs for manual inspection:', graph.nodes)
}
function requireAlias(summary: CreateSummary, alias: string): number {
  const id = summary.alias(alias)
  if (typeof id !== 'number') {
    throw new Error(`failed to resolve alias ${alias}`)
  }
  return id
}

function resetDbFiles(basePath: string): void {
  for (const suffix of ['', '-wal', '-lock', '-shm']) {
    try {
      rmSync(`${basePath}${suffix}`)
    } catch (err) {
      if (!err || typeof err !== 'object' || !('code' in err) || (err as { code?: string }).code !== 'ENOENT') {
        throw err
      }
    }
  }
}

if (invokedDirectly('people')) {
  runPeopleExample(process.argv[2] ?? DEFAULT_DB_PATH).catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
