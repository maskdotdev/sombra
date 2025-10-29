"use client"

import { Terminal } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import Link from "next/link"
import { LanguageSelector } from "@/components/language-selector"
import { CodeExample } from "@/components/code-example"
import { SiteHeader } from "@/components/site-header"

export default function ExamplesPage() {
  return (
    <main className="min-h-screen bg-background">
      {/* Site Header */}
      <SiteHeader />

      {/* Page Header */}
      <section className="container mx-auto px-4 py-12 border-b border-border">
        <div className="max-w-5xl mx-auto flex items-start justify-between gap-8">
          <div>
            <h1 className="text-4xl font-bold mb-4">real-world examples</h1>
            <p className="text-lg text-muted-foreground">
              See how Sombra powers complex applications across different industries and use cases
            </p>
          </div>
          <LanguageSelector />
        </div>
      </section>

      {/* Examples Content */}
      <section className="container mx-auto px-4 py-12">
        <div className="max-w-5xl mx-auto space-y-8">
          {/* Social Network Example */}
          <Card className="p-8 bg-card border-border">
            <div className="flex flex-col md:flex-row gap-6">
              <div className="flex-1">
                <h3 className="text-xl font-bold mb-2">social network recommendations</h3>
                <p className="text-muted-foreground mb-4">
                  Find mutual friends and suggest connections based on shared interests and network proximity
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <div>→ traverse friend networks</div>
                  <div>→ calculate relationship strength</div>
                  <div>→ rank by mutual connections</div>
                </div>
              </div>
              <div className="flex-1">
                <CodeExample
                  examples={{
                    typescript: `// find friend suggestions
const suggestions = await db
  .query()
  .match('User', { id: userId })
  .traverse('FRIENDS', { depth: 2 })
  .where('id', '!=', userId)
  .aggregate({
    mutualFriends: 'count',
    sharedInterests: 'intersect'
  })
  .orderBy('mutualFriends', 'desc')
  .limit(10)
  .execute()`,
                    python: `# find friend suggestions
suggestions = await db \\
    .query() \\
    .match('User', {'id': user_id}) \\
    .traverse('FRIENDS', {'depth': 2}) \\
    .where('id', '!=', user_id) \\
    .aggregate({
        'mutualFriends': 'count',
        'sharedInterests': 'intersect'
    }) \\
    .order_by('mutualFriends', 'desc') \\
    .limit(10) \\
    .execute()`,
                    go: `// find friend suggestions
suggestions, err := db.
    Query().
    Match("User", map[string]interface{}{"id": userId}).
    Traverse("FRIENDS", TraverseOpts{Depth: 2}).
    Where("id", "!=", userId).
    Aggregate(map[string]interface{}{
        "mutualFriends":   "count",
        "sharedInterests": "intersect",
    }).
    OrderBy("mutualFriends", "desc").
    Limit(10).
    Execute()`,
                    rust: `// find friend suggestions
let suggestions = db
    .query()
    .match_node("User", json!({"id": user_id}))
    .traverse("FRIENDS", TraverseOpts { depth: 2, ..Default::default() })
    .where_field("id", Operator::NotEqual, user_id)
    .aggregate(json!({
        "mutualFriends": "count",
        "sharedInterests": "intersect"
    }))
    .order_by("mutualFriends", Order::Desc)
    .limit(10)
    .execute()
    .await?;`,
                  }}
                />
              </div>
            </div>
          </Card>

          {/* E-commerce Example */}
          <Card className="p-8 bg-card border-border">
            <div className="flex flex-col md:flex-row gap-6">
              <div className="flex-1">
                <h3 className="text-xl font-bold mb-2">product recommendations</h3>
                <p className="text-muted-foreground mb-4">
                  Discover products based on purchase history, browsing patterns, and similar user behavior
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <div>→ analyze purchase patterns</div>
                  <div>→ find similar customers</div>
                  <div>→ collaborative filtering</div>
                </div>
              </div>
              <div className="flex-1">
                <CodeExample
                  examples={{
                    typescript: `// recommend products
const recommendations = await db
  .query()
  .match('User', { id: userId })
  .traverse('PURCHASED')
  .to('Product')
  .traverse('PURCHASED_BY', { reverse: true })
  .to('User')
  .traverse('PURCHASED')
  .to('Product')
  .where('id', 'not in', userPurchases)
  .aggregate({ score: 'frequency' })
  .orderBy('score', 'desc')
  .limit(20)
  .execute()`,
                    python: `# recommend products
recommendations = await db \\
    .query() \\
    .match('User', {'id': user_id}) \\
    .traverse('PURCHASED') \\
    .to('Product') \\
    .traverse('PURCHASED_BY', {'reverse': True}) \\
    .to('User') \\
    .traverse('PURCHASED') \\
    .to('Product') \\
    .where('id', 'not in', user_purchases) \\
    .aggregate({'score': 'frequency'}) \\
    .order_by('score', 'desc') \\
    .limit(20) \\
    .execute()`,
                    go: `// recommend products
recommendations, err := db.
    Query().
    Match("User", map[string]interface{}{"id": userId}).
    Traverse("PURCHASED").
    To("Product").
    Traverse("PURCHASED_BY", TraverseOpts{Reverse: true}).
    To("User").
    Traverse("PURCHASED").
    To("Product").
    Where("id", "not in", userPurchases).
    Aggregate(map[string]interface{}{"score": "frequency"}).
    OrderBy("score", "desc").
    Limit(20).
    Execute()`,
                    rust: `// recommend products
let recommendations = db
    .query()
    .match_node("User", json!({"id": user_id}))
    .traverse("PURCHASED", Default::default())
    .to("Product")
    .traverse("PURCHASED_BY", TraverseOpts { reverse: true, ..Default::default() })
    .to("User")
    .traverse("PURCHASED", Default::default())
    .to("Product")
    .where_field("id", Operator::NotIn, user_purchases)
    .aggregate(json!({"score": "frequency"}))
    .order_by("score", Order::Desc)
    .limit(20)
    .execute()
    .await?;`,
                  }}
                />
              </div>
            </div>
          </Card>

          {/* Fraud Detection Example */}
          <Card className="p-8 bg-card border-border">
            <div className="flex flex-col md:flex-row gap-6">
              <div className="flex-1">
                <h3 className="text-xl font-bold mb-2">fraud detection patterns</h3>
                <p className="text-muted-foreground mb-4">
                  Identify suspicious activity by analyzing transaction networks and detecting anomalous patterns
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <div>→ detect circular transactions</div>
                  <div>→ identify suspicious clusters</div>
                  <div>→ real-time pattern matching</div>
                </div>
              </div>
              <div className="flex-1">
                <CodeExample
                  examples={{
                    typescript: `// detect fraud rings
const suspiciousRings = await db
  .query()
  .match('Account', { flagged: true })
  .traverse('TRANSFERRED_TO', {
    depth: 3,
    circular: true
  })
  .where('amount', '>', 10000)
  .aggregate({
    totalAmount: 'sum',
    participants: 'collect'
  })
  .having('participants', '>=', 3)
  .execute()`,
                    python: `# detect fraud rings
suspicious_rings = await db \\
    .query() \\
    .match('Account', {'flagged': True}) \\
    .traverse('TRANSFERRED_TO', {'depth': 3, 'circular': True}) \\
    .where('amount', '>', 10000) \\
    .aggregate({'totalAmount': 'sum', 'participants': 'collect'}) \\
    .having('participants', '>=', 3) \\
    .execute()`,
                    go: `// detect fraud rings
suspiciousRings, err := db.
    Query().
    Match("Account", map[string]interface{}{"flagged": true}).
    Traverse("TRANSFERRED_TO", TraverseOpts{Depth: 3, Circular: true}).
    Where("amount", ">", 10000).
    Aggregate(map[string]interface{}{
        "totalAmount": "sum",
        "participants": "collect",
    }).
    Having("participants", ">=", 3).
    Execute()`,
                    rust: `// detect fraud rings
let suspicious_rings = db
    .query()
    .match_node("Account", json!({"flagged": true}))
    .traverse("TRANSFERRED_TO", TraverseOpts { depth: 3, circular: true, ..Default::default() })
    .where_field("amount", Operator::GreaterThan, 10000)
    .aggregate(json!({
        "totalAmount": "sum",
        "participants": "collect"
    }))
    .having("participants", json!({">=": 3}))
    .execute()
    .await?;`,
                  }}
                />
              </div>
            </div>
          </Card>

          {/* Knowledge Graph Example */}
          <Card className="p-8 bg-card border-border">
            <div className="flex flex-col md:flex-row gap-6">
              <div className="flex-1">
                <h3 className="text-xl font-bold mb-2">knowledge graph queries</h3>
                <p className="text-muted-foreground mb-4">
                  Build semantic search and entity relationship mapping for intelligent information retrieval
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <div>→ semantic entity linking</div>
                  <div>→ multi-hop reasoning</div>
                  <div>→ context-aware search</div>
                </div>
              </div>
              <div className="flex-1">
                <CodeExample
                  examples={{
                    typescript: `// find related concepts
const related = await db
  .query()
  .match('Concept', { name: 'AI' })
  .traverse('RELATED_TO', {
    depth: 3,
    weights: true
  })
  .aggregate({
    relevance: 'pathWeight',
    connections: 'count'
  })
  .orderBy('relevance', 'desc')
  .limit(15)
  .execute()`,
                    python: `# find related concepts
related = await db \\
    .query() \\
    .match('Concept', {'name': 'AI'}) \\
    .traverse('RELATED_TO', {'depth': 3, 'weights': True}) \\
    .aggregate({'relevance': 'pathWeight', 'connections': 'count'}) \\
    .order_by('relevance', 'desc') \\
    .limit(15) \\
    .execute()`,
                    go: `// find related concepts
related, err := db.
    Query().
    Match("Concept", map[string]interface{}{"name": "AI"}).
    Traverse("RELATED_TO", TraverseOpts{Depth: 3, Weights: true}).
    Aggregate(map[string]interface{}{
        "relevance": "pathWeight",
        "connections": "count",
    }).
    OrderBy("relevance", "desc").
    Limit(15).
    Execute()`,
                    rust: `// find related concepts
let related = db
    .query()
    .match_node("Concept", json!({"name": "AI"}))
    .traverse("RELATED_TO", TraverseOpts { depth: 3, weights: true, ..Default::default() })
    .aggregate(json!({
        "relevance": "pathWeight",
        "connections": "count"
    }))
    .order_by("relevance", Order::Desc)
    .limit(15)
    .execute()
    .await?;`,
                  }}
                />
              </div>
            </div>
          </Card>

          {/* Supply Chain Example */}
          <Card className="p-8 bg-card border-border">
            <div className="flex flex-col md:flex-row gap-6">
              <div className="flex-1">
                <h3 className="text-xl font-bold mb-2">supply chain tracking</h3>
                <p className="text-muted-foreground mb-4">
                  Track product origins, dependencies, and bottlenecks across complex supply networks
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <div>→ trace product lineage</div>
                  <div>→ identify bottlenecks</div>
                  <div>→ optimize logistics</div>
                </div>
              </div>
              <div className="flex-1">
                <CodeExample
                  examples={{
                    typescript: `// trace product origin
const origin = await db
  .query()
  .match('Product', { sku: 'ABC123' })
  .traverse('SOURCED_FROM', {
    direction: 'backward',
    depth: 10
  })
  .to('Supplier')
  .return({
    path: 'fullPath',
    suppliers: 'collect',
    leadTime: 'sum'
  })
  .execute()`,
                    python: `# trace product origin
origin = await db \\
    .query() \\
    .match('Product', {'sku': 'ABC123'}) \\
    .traverse('SOURCED_FROM', {'direction': 'backward', 'depth': 10}) \\
    .to('Supplier') \\
    .return_({'path': 'fullPath', 'suppliers': 'collect', 'leadTime': 'sum'}) \\
    .execute()`,
                    go: `// trace product origin
origin, err := db.
    Query().
    Match("Product", map[string]interface{}{"sku": "ABC123"}).
    Traverse("SOURCED_FROM", TraverseOpts{Direction: "backward", Depth: 10}).
    To("Supplier").
    Return(map[string]interface{}{
        "path": "fullPath",
        "suppliers": "collect",
        "leadTime": "sum",
    }).
    Execute()`,
                    rust: `// trace product origin
let origin = db
    .query()
    .match_node("Product", json!({"sku": "ABC123"}))
    .traverse("SOURCED_FROM", TraverseOpts { direction: "backward", depth: 10, ..Default::default() })
    .to("Supplier")
    .return_(json!({
        "path": "fullPath",
        "suppliers": "collect",
        "leadTime": "sum"
    }))
    .execute()
    .await?;`,
                  }}
                />
              </div>
            </div>
          </Card>
        </div>
      </section>

      {/* CTA Section */}
      <section className="container mx-auto px-4 py-20">
        <Card className="max-w-3xl mx-auto p-12 bg-secondary border-border text-center">
          <h2 className="text-3xl font-bold mb-4">ready to build?</h2>
          <p className="text-muted-foreground mb-8 text-balance">
            Start implementing these patterns in your own applications with our comprehensive documentation
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <Button
              size="lg"
              className="bg-primary text-primary-foreground hover:bg-primary/90 w-full sm:w-auto"
              asChild
            >
              <Link href="/docs">read the docs</Link>
            </Button>
            <Button
              size="lg"
              variant="outline"
              className="w-full sm:w-auto border-border text-foreground hover:bg-muted bg-transparent"
              asChild
            >
              <Link href="/api-reference">view api reference</Link>
            </Button>
          </div>
        </Card>
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
