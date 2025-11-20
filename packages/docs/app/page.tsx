"use client"

import { Terminal } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { CodeBlock } from "@/components/code-block"
import Link from "next/link"
import { SiteHeader } from "@/components/site-header"
import { InstallCommandSwitcher } from "@/components/install-command-switcher"
import { FeatureGraph } from "@/components/feature-graph"
import { Suspense } from "react"

export default function Home() {
  return (
    <main className="min-h-screen bg-background">
      <Suspense fallback={<div className="border-b border-border h-16" />}>
        <SiteHeader />
      </Suspense>

      {/* Hero Section */}
      <section className="container mx-auto px-4 py-20 md:py-32">
        <div className="max-w-4xl mx-auto text-center space-y-6">
          <InstallCommandSwitcher />
          <h1 className="text-4xl md:text-6xl font-bold text-balance leading-tight">
            The graph database
            <br />
            <span className="text-primary">built for developers</span>
          </h1>
          <p className="text-lg md:text-xl text-muted-foreground text-balance max-w-2xl mx-auto">
            Query complex relationships with simple syntax. Deploy anywhere. Scale infinitely.
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4 pt-4">
            <Button size="lg" className="bg-primary text-primary-foreground hover:bg-primary/90 w-full sm:w-auto">
              start building
            </Button>
            <Button
              size="lg"
              variant="outline"
              className="w-full sm:w-auto border-border text-foreground hover:bg-secondary bg-transparent"
              asChild
            >
              <Link href="/docs">view docs →</Link>
            </Button>
          </div>
        </div>

        {/* Code Example */}
        <div className="max-w-3xl mx-auto mt-16">
          <CodeBlock />
        </div>
      </section>

      {/* Features Graph */}
      <section className="container mx-auto px-4 py-20">
        <FeatureGraph />
      </section>

      {/* Query Examples */}
      <section id="examples" className="container mx-auto px-4 py-20">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl font-bold mb-2 text-center">intuitive query syntax</h2>
          <p className="text-muted-foreground text-center mb-12">Write queries that read like natural language</p>

          <div className="grid md:grid-cols-2 gap-6">
            <Card className="p-6 bg-card border-border">
              <div className="text-sm text-muted-foreground mb-2">// find connections</div>
              <pre className="text-sm text-foreground">
                <code>{`db.query()
  .match('User', { id: 'alice' })
  .traverse('FOLLOWS', { depth: 2 })
  .return('*')`}</code>
              </pre>
            </Card>

            <Card className="p-6 bg-card border-border">
              <div className="text-sm text-muted-foreground mb-2">// create relationships</div>
              <pre className="text-sm text-foreground">
                <code>{`db.create()
  .node('User', { name: 'bob' })
  .edge('LIKES', 'Post', { id: 123 })
  .execute()`}</code>
              </pre>
            </Card>

            <Card className="p-6 bg-card border-border">
              <div className="text-sm text-muted-foreground mb-2">// aggregate data</div>
              <pre className="text-sm text-foreground">
                <code>{`db.query()
  .match('Post')
  .aggregate({ likes: 'sum' })
  .groupBy('category')`}</code>
              </pre>
            </Card>

            <Card className="p-6 bg-card border-border">
              <div className="text-sm text-muted-foreground mb-2">// path finding</div>
              <pre className="text-sm text-foreground">
                <code>{`db.shortestPath()
  .from('User', { id: 'alice' })
  .to('User', { id: 'charlie' })
  .via('FOLLOWS')`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="container mx-auto px-4 py-20">
        <Card className="max-w-3xl mx-auto p-12 bg-secondary border-border text-center">
          <h2 className="text-3xl font-bold mb-4">ready to build?</h2>
          <p className="text-muted-foreground mb-8 text-balance">
            Start querying your data in minutes with our comprehensive documentation and examples
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
            >
              join discord
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
