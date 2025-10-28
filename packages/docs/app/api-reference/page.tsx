"use client"

import { Terminal, Code2 } from "lucide-react"
import { Card } from "@/components/ui/card"
import { LanguageSelector } from "@/components/language-selector"
import { CodeExample } from "@/components/code-example"
import { SiteHeader } from "@/components/site-header"

export default function APIReferencePage() {
  return (
    <main className="min-h-screen bg-background">
      {/* Site Header */}
      <SiteHeader />

      {/* API Reference Content */}
      <section className="container mx-auto px-4 py-20">
        <div className="max-w-6xl mx-auto">
          <div className="mb-16 flex items-start justify-between gap-8">
            <div>
              <div className="flex items-center gap-3 mb-4">
                <Code2 className="w-8 h-8 text-primary" />
                <h1 className="text-4xl font-bold">api reference</h1>
              </div>
              <p className="text-lg text-muted-foreground">Complete API documentation for Sombra graph database</p>
            </div>
            <LanguageSelector />
          </div>

          {/* Query Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">query methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.query()</code>
                  <span className="text-sm text-muted-foreground">→ QueryBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Initialize a new query builder. Chain methods to construct complex graph queries with filtering,
                  traversal, and aggregation.
                </p>
                <div className="space-y-4">
                  <CodeExample
                    label="basic query"
                    examples={{
                      typescript: `const users = await db.query()
  .match('User', { active: true })
  .return('*')`,
                      python: `users = await db.query() \\
    .match('User', {'active': True}) \\
    .return_all()`,
                      go: `users, err := db.Query().
    Match("User", map[string]interface{}{"active": true}).
    Return("*")`,
                      rust: `let users = db.query()
    .match_node("User", json!({"active": true}))
    .return_all()
    .await?;`,
                    }}
                  />
                  <CodeExample
                    label="with filtering and pagination"
                    examples={{
                      typescript: `const results = await db.query()
  .match('User', { age: { $gte: 18 } })
  .filter({ verified: true })
  .orderBy('createdAt', 'desc')
  .limit(10)
  .offset(20)
  .return(['id', 'name', 'email'])`,
                      python: `results = await db.query() \\
    .match('User', {'age': {'$gte': 18}}) \\
    .filter({'verified': True}) \\
    .order_by('createdAt', 'desc') \\
    .limit(10) \\
    .offset(20) \\
    .return_fields(['id', 'name', 'email'])`,
                      go: `results, err := db.Query().
    Match("User", map[string]interface{}{
        "age": map[string]interface{}{"$gte": 18},
    }).
    Filter(map[string]interface{}{"verified": true}).
    OrderBy("createdAt", "desc").
    Limit(10).
    Offset(20).
    Return([]string{"id", "name", "email"})`,
                      rust: `let results = db.query()
    .match_node("User", json!({"age": {"$gte": 18}}))
    .filter(json!({"verified": true}))
    .order_by("createdAt", Order::Desc)
    .limit(10)
    .offset(20)
    .return_fields(&["id", "name", "email"])
    .await?;`,
                    }}
                  />
                </div>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">.match()</code>
                  <span className="text-sm text-muted-foreground">→ QueryBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Match nodes by label and properties. Supports comparison operators, logical operators, and regex
                  patterns.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// simple match
.match('User', { id: 'u1' })

// comparison operators
.match('User', { 
  age: { $gte: 18, $lte: 65 },
  name: { $regex: '^A' }
})

// logical operators
.match('User', {
  $or: [
    { verified: true },
    { premium: true }
  ]
})`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">.traverse()</code>
                  <span className="text-sm text-muted-foreground">→ QueryBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Traverse edges from matched nodes. Control depth, direction, and apply filters during traversal.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// basic traversal
.traverse('FOLLOWS')

// with options
.traverse('FOLLOWS', {
  depth: 3,              // max depth
  direction: 'outbound', // 'inbound', 'both'
  filter: { active: true },
  minDepth: 1
})

// multiple edge types
.traverse(['FOLLOWS', 'FRIENDS_WITH'], {
  depth: 2
})`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">.aggregate()</code>
                  <span className="text-sm text-muted-foreground">→ QueryBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Perform aggregations on query results. Supports count, sum, avg, min, max, and custom functions.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const stats = await db.query()
  .match('Post')
  .aggregate({
    total: 'count',
    totalLikes: { $sum: 'likes' },
    avgLikes: { $avg: 'likes' },
    maxLikes: { $max: 'likes' },
    minLikes: { $min: 'likes' }
  })
  .groupBy('category')
  .return('*')`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Create Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">create methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.create()</code>
                  <span className="text-sm text-muted-foreground">→ CreateBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Create nodes and edges with a fluent interface. Supports batch operations and returns created
                  entities.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// create single node
const user = await db.create()
  .node('User', { 
    name: 'alice',
    email: 'alice@example.com' 
  })
  .execute()

// create multiple nodes and edges
await db.create()
  .node('User', { id: 'u1', name: 'alice' })
  .node('Post', { id: 'p1', title: 'Hello' })
  .edge('User', { id: 'u1' }, 'AUTHORED', 'Post', { id: 'p1' })
  .execute()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.batchCreate()</code>
                  <span className="text-sm text-muted-foreground">→ Promise&lt;Result[]&gt;</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Efficiently create large numbers of nodes or edges. Automatically batches operations for optimal
                  performance.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const users = Array.from({ length: 1000 }, (_, i) => ({
  id: \`u\${i}\`,
  name: \`user\${i}\`,
  email: \`user\${i}@example.com\`
}))

await db.batchCreate('User', users, {
  batchSize: 100,  // process in chunks
  parallel: true   // parallel execution
})`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Update Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">update methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.update()</code>
                  <span className="text-sm text-muted-foreground">→ UpdateBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Update node or edge properties. Supports atomic operations, conditional updates, and bulk updates.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// simple update
await db.update('User', { id: 'u1' })
  .set({ lastLogin: new Date() })
  .execute()

// atomic operations
await db.update('User', { id: 'u1' })
  .increment('loginCount', 1)
  .push('tags', 'premium')
  .execute()

// conditional update
await db.update('User', { verified: false })
  .set({ verified: true, verifiedAt: new Date() })
  .where({ email: { $regex: '@company.com$' } })
  .execute()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.batchUpdate()</code>
                  <span className="text-sm text-muted-foreground">→ Promise&lt;number&gt;</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Update multiple nodes matching a filter. Returns the count of updated nodes.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const updated = await db.batchUpdate(
  'User',
  { verified: false },
  { verified: true, verifiedAt: new Date() }
)

console.log(\`Updated \${updated} users\`)`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Delete Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">delete methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.delete()</code>
                  <span className="text-sm text-muted-foreground">→ DeleteBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Delete nodes and edges. Optionally cascade delete connected edges.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// delete node
await db.delete('User', { id: 'u1' })
  .execute()

// cascade delete edges
await db.delete('User', { id: 'u1' })
  .cascade()
  .execute()

// delete edges only
await db.delete()
  .edge('FOLLOWS', { 
    from: 'u1', 
    to: 'u2' 
  })
  .execute()`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Path Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">path methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.shortestPath()</code>
                  <span className="text-sm text-muted-foreground">→ PathBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Find the shortest path between two nodes using Dijkstra's algorithm. Supports weighted edges.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const path = await db.shortestPath()
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
  .execute()`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.allPaths()</code>
                  <span className="text-sm text-muted-foreground">→ PathBuilder</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Find all paths between two nodes up to a maximum depth. Useful for exploring all possible connections.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const paths = await db.allPaths()
  .from('User', { id: 'alice' })
  .to('User', { id: 'bob' })
  .via('FOLLOWS')
  .maxDepth(4)
  .limit(10)
  .execute()

console.log(\`Found \${paths.length} paths\`)`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Schema & Index Methods */}
          <div className="mb-16">
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">schema & index methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.createIndex()</code>
                  <span className="text-sm text-muted-foreground">→ Promise&lt;Index&gt;</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Create indexes to optimize query performance. Supports unique, sparse, and full-text indexes.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`// unique index
await db.createIndex('User', 'email', {
  unique: true,
  sparse: false
})

// composite index
await db.createIndex('Post', ['userId', 'createdAt'], {
  name: 'user_posts_by_date'
})

// full-text index
await db.createIndex('Post', 'content', {
  type: 'fulltext'
})`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.transaction()</code>
                  <span className="text-sm text-muted-foreground">→ Promise&lt;T&gt;</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Execute multiple operations atomically. Automatically rolls back on error.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`await db.transaction(async (tx) => {
  const user = await tx.create()
    .node('User', { name: 'alice' })
    .execute()
  
  await tx.create()
    .node('Profile', { userId: user.id })
    .execute()
  
  // if any operation fails, all are rolled back
})`}</code>
                </pre>
              </Card>
            </div>
          </div>

          {/* Utility Methods */}
          <div>
            <h2 className="text-2xl font-bold mb-8 pb-4 border-b border-border">utility methods</h2>

            <div className="space-y-8">
              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">.explain()</code>
                  <span className="text-sm text-muted-foreground">→ Promise&lt;ExplainResult&gt;</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Analyze query performance and execution plan without executing the query.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const plan = await db.query()
  .match('User')
  .traverse('FOLLOWS', { depth: 3 })
  .explain()

console.log({
  executionTime: plan.executionTime,
  nodesScanned: plan.nodesScanned,
  indexesUsed: plan.indexesUsed,
  queryPlan: plan.plan
})`}</code>
                </pre>
              </Card>

              <Card className="p-6 bg-card border-border">
                <div className="flex items-baseline gap-2 mb-3">
                  <code className="text-lg font-bold text-foreground">db.subscribe()</code>
                  <span className="text-sm text-muted-foreground">→ Subscription</span>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Subscribe to real-time changes in the graph. Supports create, update, and delete events.
                </p>
                <pre className="text-sm text-foreground bg-background p-4 rounded border border-border">
                  <code>{`const subscription = db.subscribe()
  .match('User', { id: 'alice' })
  .traverse('FOLLOWS')
  .on('create', (node) => {
    console.log('New node:', node)
  })
  .on('update', (node) => {
    console.log('Updated:', node)
  })
  .on('delete', (id) => {
    console.log('Deleted:', id)
  })

// cleanup
subscription.unsubscribe()`}</code>
                </pre>
              </Card>
            </div>
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
