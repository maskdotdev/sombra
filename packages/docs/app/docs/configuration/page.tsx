"use client"

import { Database, Settings } from "lucide-react"
import { Card } from "@/components/ui/card"
import { CodeExample } from "@/components/code-example"
import { SiteHeader } from "@/components/site-header"
import { LanguageSelector } from "@/components/language-selector"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Suspense } from "react"

export default function ConfigurationPage() {
  return (
    <main className="min-h-screen bg-background">
      <Suspense fallback={<div className="border-b border-border h-16" />}>
        <SiteHeader />
      </Suspense>

      <section className="container mx-auto px-4 py-20">
        <div className="max-w-6xl mx-auto">
          <div className="mb-16 flex items-start justify-between gap-8">
            <div>
              <div className="flex items-center gap-3 mb-4">
                <Settings className="w-8 h-8 text-primary" />
                <h1 className="text-4xl font-bold">configuration</h1>
              </div>
              <p className="text-lg text-muted-foreground">
                Flexible configuration options to optimize performance for different use cases.
              </p>
            </div>
            <Suspense fallback={<div className="w-40 h-10 bg-background border border-border rounded-lg" />}>
              <LanguageSelector />
            </Suspense>
          </div>

          <div className="space-y-12">
            {/* Overview */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">configuration overview</h2>
              <CodeExample
                label="basic configuration"
                examples={{
                  typescript: `import { Database } from "sombradb";

const db = Database.open("./data.db", {
    createIfMissing: true,
    pageSize: 4096,
    cachePages: 1024,
    synchronous: "normal",
    autocheckpointMs: 30000,
    // Advanced WAL / MVCC settings
    pagerGroupCommitMaxWriters: 10,
    pagerAsyncFsync: true,
    versionCodec: "snappy",
    snapshotPoolSize: 4
});`,
                  python: `from sombra import Database

db = Database.open('./data.db',
    create_if_missing=True,
    page_size=4096,
    cache_pages=1024,
    synchronous='normal',
    autocheckpoint_ms=30000,
    # Advanced WAL / MVCC settings
    pager_group_commit_max_writers=10,
    pager_async_fsync=True,
    version_codec='snappy',
    snapshot_pool_size=4
)`
                }}
              />
            </div>

            {/* Connection Options */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">connection options</h2>
              <Card className="p-6">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Option</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Default</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell className="font-mono">createIfMissing</TableCell>
                      <TableCell className="font-mono">boolean</TableCell>
                      <TableCell className="font-mono">true</TableCell>
                      <TableCell>Create database if it doesn't exist</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">pageSize</TableCell>
                      <TableCell className="font-mono">number</TableCell>
                      <TableCell className="font-mono">4096</TableCell>
                      <TableCell>Page size in bytes</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">cachePages</TableCell>
                      <TableCell className="font-mono">number</TableCell>
                      <TableCell className="font-mono">1024</TableCell>
                      <TableCell>Number of pages to cache</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">synchronous</TableCell>
                      <TableCell className="font-mono">string</TableCell>
                      <TableCell className="font-mono">'full'</TableCell>
                      <TableCell>Sync mode: 'full', 'normal', 'off'</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Card>
            </div>

            {/* MVCC Configuration */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">mvcc configuration</h2>
              <Card className="p-6">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Option</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Default</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell className="font-mono">inlineHistory</TableCell>
                      <TableCell className="font-mono">boolean</TableCell>
                      <TableCell>-</TableCell>
                      <TableCell>Embed newest history on page</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">versionCodec</TableCell>
                      <TableCell className="font-mono">string</TableCell>
                      <TableCell className="font-mono">'none'</TableCell>
                      <TableCell>Compression: 'none', 'snappy'</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">snapshotPoolSize</TableCell>
                      <TableCell className="font-mono">number</TableCell>
                      <TableCell>-</TableCell>
                      <TableCell>Cached snapshots to reuse for reads</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Card>
            </div>

            {/* WAL Configuration */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">wal configuration</h2>
              <Card className="p-6">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Option</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Default</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell className="font-mono">walSegmentBytes</TableCell>
                      <TableCell className="font-mono">number</TableCell>
                      <TableCell>-</TableCell>
                      <TableCell>WAL segment size in bytes</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">autocheckpointMs</TableCell>
                      <TableCell className="font-mono">number | null</TableCell>
                      <TableCell className="font-mono">30000</TableCell>
                      <TableCell>Auto-checkpoint interval</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Card>
            </div>

            {/* Performance Tuning */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">performance tuning</h2>
              <div className="grid md:grid-cols-2 gap-6">
                <Card className="p-6">
                  <h3 className="text-xl font-bold mb-3">write-heavy workloads</h3>
                  <CodeExample
                    examples={{
                      typescript: `const db = Database.open("./data.db", {
    synchronous: "normal",
    autocheckpointMs: 60000,
    commitCoalesceMs: 10,
    pagerGroupCommitMaxWriters: 16,
    pagerAsyncFsync: true
});`
                    }}
                  />
                </Card>
                <Card className="p-6">
                  <h3 className="text-xl font-bold mb-3">read-heavy workloads</h3>
                  <CodeExample
                    examples={{
                      typescript: `const db = Database.open("./data.db", {
    cachePages: 50000, // Large cache
    synchronous: "full", // Full durability
    autocheckpointMs: 30000,
});`
                    }}
                  />
                </Card>
              </div>
            </div>

          </div>
        </div>
      </section>
    </main>
  )
}
