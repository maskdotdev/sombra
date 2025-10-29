"use client"

import { Terminal, Book, Database, GitBranch } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import Link from "next/link"
import { LanguageSelector } from "@/components/language-selector"
import { CodeExample } from "@/components/code-example"
import { SiteHeader } from "@/components/site-header"

export default function DocsPage() {
  return (
    <main className="min-h-screen bg-background">
      <SiteHeader />

      {/* Documentation Content */}
      <section className="container mx-auto px-4 py-20">
        <div className="max-w-6xl mx-auto">
          <div className="mb-16 flex items-start justify-between gap-8">
            <div>
              <h1 className="text-4xl font-bold mb-4">documentation</h1>
              <p className="text-lg text-muted-foreground">Everything you need to master Sombra graph database</p>
            </div>
            <LanguageSelector />
          </div>

          {/* Getting Started */}
          <div className="mb-20">
            <div className="flex items-center gap-2 mb-8">
              <GitBranch className="w-6 h-6 text-primary" />
              <h2 className="text-3xl font-bold">getting started</h2>
            </div>
            <Card className="p-8 bg-card border-border">
              <div className="space-y-8">
                <CodeExample
                  label="1. install sombra"
                  examples={{
                    typescript: `$ npm install @sombra/core`,
                    python: `$ pip install sombra`,
                    go: `$ go get github.com/sombra/sombra-go`,
                    rust: `$ cargo add sombra`,
                  }}
                />

                <CodeExample
                  label="2. initialize database"
                  examples={{
                    typescript: `import { Sombra } from '@sombra/core'

const db = new Sombra({
  url: 'sombra://localhost:7687',
  auth: { username: 'admin', password: 'secret' }
})`,
                    python: `from sombra import Sombra

db = Sombra(
    url='sombra://localhost:7687',
    auth={'username': 'admin', 'password': 'secret'}
)`,
                    go: `package main

import "github.com/sombra/sombra-go"

db, err := sombra.New(sombra.Config{
    URL: "sombra://localhost:7687",
    Auth: sombra.Auth{
        Username: "admin",
        Password: "secret",
    },
})`,
                    rust: `use sombra::Sombra;

let db = Sombra::new(Config {
    url: "sombra://localhost:7687",
    auth: Auth {
        username: "admin",
        password: "secret",
    },
})?;`,
                  }}
                />

                <CodeExample
                  label="3. create your first nodes"
                  examples={{
                    typescript: `await db.create()
  .node('User', { 
    id: 'u1', 
    name: 'alice',
    email: 'alice@example.com' 
  })
  .node('User', { 
    id: 'u2', 
    name: 'bob',
    email: 'bob@example.com' 
  })
  .execute()`,
                    python: `await db.create() \\
    .node('User', {
        'id': 'u1',
        'name': 'alice',
        'email': 'alice@example.com'
    }) \\
    .node('User', {
        'id': 'u2',
        'name': 'bob',
        'email': 'bob@example.com'
    }) \\
    .execute()`,
                    go: `err := db.Create().
    Node("User", map[string]interface{}{
        "id":    "u1",
        "name":  "alice",
        "email": "alice@example.com",
    }).
    Node("User", map[string]interface{}{
        "id":    "u2",
        "name":  "bob",
        "email": "bob@example.com",
    }).
    Execute()`,
                    rust: `db.create()
    .node("User", json!({
        "id": "u1",
        "name": "alice",
        "email": "alice@example.com"
    }))
    .node("User", json!({
        "id": "u2",
        "name": "bob",
        "email": "bob@example.com"
    }))
    .execute()
    .await?;`,
                  }}
                />

                <CodeExample
                  label="4. create relationships"
                  examples={{
                    typescript: `await db.create()
  .edge('User', { id: 'u1' }, 'FOLLOWS', 'User', { id: 'u2' }, {
    since: '2025-01-01'
  })
  .execute()`,
                    python: `await db.create() \\
    .edge('User', {'id': 'u1'}, 'FOLLOWS', 'User', {'id': 'u2'}, {
        'since': '2025-01-01'
    }) \\
    .execute()`,
                    go: `err := db.Create().
    Edge("User", map[string]interface{}{"id": "u1"},
         "FOLLOWS",
         "User", map[string]interface{}{"id": "u2"},
         map[string]interface{}{"since": "2025-01-01"}).
    Execute()`,
                    rust: `db.create()
    .edge("User", json!({"id": "u1"}),
          "FOLLOWS",
          "User", json!({"id": "u2"}),
          json!({"since": "2025-01-01"}))
    .execute()
    .await?;`,
                  }}
                />

                <CodeExample
                  label="5. query your graph"
                  examples={{
                    typescript: `const followers = await db.query()
  .match('User', { id: 'u2' })
  .traverse('FOLLOWS', { direction: 'inbound' })
  .return('*')

console.log(followers) // [{ id: 'u1', name: 'alice', ... }]`,
                    python: `followers = await db.query() \\
    .match('User', {'id': 'u2'}) \\
    .traverse('FOLLOWS', {'direction': 'inbound'}) \\
    .return_all()

print(followers)  # [{'id': 'u1', 'name': 'alice', ...}]`,
                    go: `followers, err := db.Query().
    Match("User", map[string]interface{}{"id": "u2"}).
    Traverse("FOLLOWS", TraverseOpts{Direction: "inbound"}).
    Return("*")

fmt.Println(followers) // [{id:u1 name:alice ...}]`,
                    rust: `let followers = db.query()
    .match_node("User", json!({"id": "u2"}))
    .traverse("FOLLOWS", TraverseOpts {
        direction: Direction::Inbound,
        ..Default::default()
    })
    .return_all()
    .await?;

println!("{:?}", followers); // [User { id: "u1", name: "alice", ... }]`,
                  }}
                />

                <div className="pt-6 border-t border-border">
                  <p className="text-sm text-muted-foreground mb-4">
                    You're now ready to build with Sombra! Continue reading to learn about core concepts and advanced
                    features.
                  </p>
                </div>
              </div>
            </Card>
          </div>

          {/* Core Concepts */}
          <div className="mb-20">
            <div className="flex items-center gap-2 mb-8">
              <Book className="w-6 h-6 text-primary" />
              <h2 className="text-3xl font-bold">core concepts</h2>
            </div>
            <div className="grid md:grid-cols-2 gap-6">
              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">nodes & edges</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Nodes represent entities in your graph. Edges define relationships between nodes with optional
                  properties and directionality.
                </p>
                <CodeExample
                  examples={{
                    typescript: `// define a node
const user = await db.create()
  .node('User', {
    id: 'u1',
    name: 'alice',
    email: 'alice@example.com'
  })
  .execute()

// create an edge
await db.create()
  .edge('User', { id: 'u1' }, 'FOLLOWS', 'User', { id: 'u2' }, {
    since: '2025-01-01',
    notificationsEnabled: true
  })
  .execute()`,
                    python: `# define a node
user = await db.create() \\
    .node('User', {
        'id': 'u1',
        'name': 'alice',
        'email': 'alice@example.com'
    }) \\
    .execute()

# create an edge
await db.create() \\
    .edge('User', {'id': 'u1'}, 'FOLLOWS', 'User', {'id': 'u2'}, {
        'since': '2025-01-01',
        'notificationsEnabled': True
    }) \\
    .execute()`,
                    go: `// define a node
user, err := db.Create().
    Node("User", map[string]interface{}{
        "id":    "u1",
        "name":  "alice",
        "email": "alice@example.com",
    }).
    Execute()

// create an edge
err = db.Create().
    Edge("User", map[string]interface{}{"id": "u1"},
         "FOLLOWS",
         "User", map[string]interface{}{"id": "u2"},
         map[string]interface{}{
             "since": "2025-01-01",
             "notificationsEnabled": true,
         }).
    Execute()`,
                    rust: `// define a node
let user = db.create()
    .node("User", json!({
        "id": "u1",
        "name": "alice",
        "email": "alice@example.com"
    }))
    .execute()
    .await?;

// create an edge
db.create()
    .edge("User", json!({"id": "u1"}),
          "FOLLOWS",
          "User", json!({"id": "u2"}),
          json!({
              "since": "2025-01-01",
              "notificationsEnabled": true
          }))
    .execute()
    .await?;`,
                  }}
                />
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">traversals</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Navigate your graph with powerful traversal patterns. Control depth, direction, and filtering at each
                  step.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// traverse with depth control
const friends = await db.query()
  .match('User', { id: 'alice' })
  .traverse('FOLLOWS', {
    depth: 3,
    direction: 'outbound',
    filter: { active: true }
  })
  .return('*')

// bidirectional traversal
const connections = await db.query()
  .match('User', { id: 'alice' })
  .traverse('FOLLOWS', { direction: 'both' })
  .return('*')`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">indexes</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Optimize query performance with automatic and custom indexes. Sombra intelligently indexes frequently
                  queried properties.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// create unique index
await db.createIndex('User', 'email', {
  unique: true,
  sparse: false
})

// composite index for complex queries
await db.createIndex('Post', ['userId', 'createdAt'], {
  name: 'user_posts_by_date'
})

// full-text search index
await db.createIndex('Post', 'content', {
  type: 'fulltext'
})`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">transactions</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  ACID-compliant transactions ensure data consistency. Batch operations for optimal performance.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// atomic transaction
await db.transaction(async (tx) => {
  const user = await tx.create()
    .node('User', { name: 'charlie' })
    .execute()
  
  const profile = await tx.create()
    .node('Profile', { userId: user.id })
    .execute()
  
  await tx.create()
    .edge('User', { id: user.id }, 'HAS_PROFILE', 'Profile', { id: profile.id })
    .execute()
})

// rollback on error
try {
  await db.transaction(async (tx) => {
    // operations...
  })
} catch (error) {
  // transaction automatically rolled back
}`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">query filtering</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Use powerful filter operators to narrow down results. Supports comparison, logical, and array
                  operators.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// comparison operators
const adults = await db.query()
  .match('User', { age: { $gte: 18 } })
  .return('*')

// logical operators
const activeUsers = await db.query()
  .match('User', {
    $and: [
      { verified: true },
      { lastLogin: { $gte: '2025-01-01' } }
    ]
  })
  .return('*')

// array operators
const tagged = await db.query()
  .match('Post', { tags: { $in: ['tech', 'ai'] } })
  .return('*')`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">aggregations</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Perform complex aggregations and analytics on your graph data with built-in functions.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// count and group
const stats = await db.query()
  .match('Post')
  .aggregate({
    count: 'count',
    totalLikes: { $sum: 'likes' },
    avgLikes: { $avg: 'likes' }
  })
  .groupBy('category')
  .return('*')

// distinct values
const categories = await db.query()
  .match('Post')
  .distinct('category')
  .return('*')`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Advanced Topics */}
          <div className="mb-20">
            <div className="flex items-center gap-2 mb-8">
              <Database className="w-6 h-6 text-primary" />
              <h2 className="text-3xl font-bold">advanced topics</h2>
            </div>
            <div className="grid md:grid-cols-2 gap-6">
              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">schema validation</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Define schemas for type safety and validation. Auto-generate TypeScript types from your schema.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`import { z } from 'zod'

const UserSchema = z.object({
  id: z.string(),
  email: z.string().email(),
  age: z.number().min(0).max(150),
  verified: z.boolean().default(false)
})

const db = new Sombra({
  schemas: {
    User: UserSchema
  }
})

// TypeScript types auto-generated
type User = z.infer<typeof UserSchema>

// validation on create
await db.create()
  .node('User', { 
    email: 'invalid' // throws validation error
  })
  .execute()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">real-time subscriptions</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Subscribe to graph changes in real-time. Perfect for reactive applications and live dashboards.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// subscribe to node changes
const subscription = db.subscribe()
  .match('User', { id: 'alice' })
  .on('update', (user) => {
    console.log('User updated:', user)
  })

// subscribe to edge changes
db.subscribe()
  .match('User', { id: 'alice' })
  .traverse('FOLLOWS')
  .on('create', (edge) => {
    console.log('New follower:', edge)
  })

// cleanup
subscription.unsubscribe()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">distributed queries</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Sombra automatically distributes queries across shards for optimal performance and scalability.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// configure sharding strategy
const db = new Sombra({
  sharding: {
    strategy: 'hash', // or 'range', 'consistent'
    key: 'userId',
    shards: 8
  },
  replication: {
    factor: 3,
    strategy: 'async'
  }
})

// queries automatically distributed
const users = await db.query()
  .match('User', { country: 'US' })
  .return('*')
// ^ executed across all shards in parallel`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">performance tuning</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Analyze and optimize queries with explain plans, performance metrics, and query hints.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// analyze query performance
const result = await db.query()
  .match('User')
  .traverse('FOLLOWS', { depth: 3 })
  .explain()

console.log({
  executionTime: result.executionTime,
  nodesScanned: result.nodesScanned,
  indexesUsed: result.indexesUsed,
  queryPlan: result.plan
})

// add query hints
const optimized = await db.query()
  .match('User')
  .hint({ useIndex: 'user_email_idx' })
  .traverse('FOLLOWS')
  .return('*')`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">path algorithms</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Find shortest paths, all paths, and weighted paths between nodes with built-in graph algorithms.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// shortest path
const path = await db.shortestPath()
  .from('User', { id: 'alice' })
  .to('User', { id: 'charlie' })
  .via(['FOLLOWS', 'FRIENDS_WITH'])
  .maxDepth(6)
  .execute()

// weighted path
const weighted = await db.shortestPath()
  .from('City', { name: 'NYC' })
  .to('City', { name: 'LA' })
  .via('CONNECTED_TO')
  .weight('distance')
  .execute()

// all paths
const allPaths = await db.allPaths()
  .from('User', { id: 'alice' })
  .to('User', { id: 'bob' })
  .maxDepth(4)
  .execute()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <h3 className="text-xl font-bold mb-3 text-foreground">batch operations</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Efficiently create, update, or delete large amounts of data with batch operations.
                </p>
                <pre className="text-xs text-foreground bg-background p-3 rounded border border-border">
                  <code>{`// batch create nodes
const users = Array.from({ length: 1000 }, (_, i) => ({
  id: \`u\${i}\`,
  name: \`user\${i}\`,
  email: \`user\${i}@example.com\`
}))

await db.batchCreate('User', users, {
  batchSize: 100 // process in chunks
})

// batch update
await db.batchUpdate('User', 
  { verified: false },
  { verified: true, verifiedAt: new Date() }
)`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Next Steps */}
          <div>
            <Card className="p-8 bg-secondary border-border">
              <h2 className="text-2xl font-bold mb-4">next steps</h2>
              <p className="text-muted-foreground mb-6">
                Now that you understand the core concepts, explore the API reference for detailed method documentation.
              </p>
              <div className="flex gap-4">
                <Button className="bg-primary text-primary-foreground hover:bg-primary/90" asChild>
                  <Link href="/api-reference">view api reference</Link>
                </Button>
                <Button
                  variant="outline"
                  className="border-border text-foreground hover:bg-muted bg-transparent"
                  asChild
                >
                  <Link href="/examples">browse examples</Link>
                </Button>
              </div>
            </Card>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border mt-20">
        <div className="container mx-auto px-4 py-8">
          <div className="flex flex-col md:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-2">
              <Terminal className="w-5 h-5 text-primary" />
              <span className="text-sm text-muted-foreground">sombra © 2025</span>
            </div>
            <div className="flex items-center gap-6 text-sm text-muted-foreground">
              <a href="#" className="hover:text-foreground transition-colors">
                github
              </a>
              <a href="#" className="hover:text-foreground transition-colors">
                twitter
              </a>
              <a href="#" className="hover:text-foreground transition-colors">
                discord
              </a>
            </div>
          </div>
        </div>
      </footer>
    </main>
  )
}
