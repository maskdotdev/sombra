"use client"

import { Terminal } from "lucide-react"
import { Card } from "@/components/ui/card"
import { CodeExample } from "@/components/code-example"
import { SiteHeader } from "@/components/site-header"
import { LanguageSelector } from "@/components/language-selector"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Suspense } from "react"

export default function CLIPage() {
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
                <Terminal className="w-8 h-8 text-primary" />
                <h1 className="text-4xl font-bold">cli reference</h1>
              </div>
              <p className="text-lg text-muted-foreground">
                Command line tools for managing your Sombra database.
              </p>
            </div>
            <Suspense fallback={<div className="w-40 h-10 bg-background border border-border rounded-lg" />}>
              <LanguageSelector />
            </Suspense>
          </div>

          <div className="space-y-12">
            {/* Installation */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">installation</h2>
              <CodeExample
                examples={{
                  typescript: "npm install -g @sombra/cli",
                  python: "pip install sombra-cli",
                  rust: "cargo install sombra-cli",
                  go: "go install github.com/sombra/cli@latest"
                }}
              />
            </div>

            {/* Global Options */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">global options</h2>
              <Card className="p-6">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Flag</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell className="font-mono">--pager-group-commit-max-writers</TableCell>
                      <TableCell>Max concurrent writers for group commit</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">--pager-async-fsync</TableCell>
                      <TableCell>Enable async fsync for WAL writes</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-mono">--version-codec</TableCell>
                      <TableCell>Compression codec ('none', 'snappy')</TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </Card>
            </div>

            {/* Profile Management */}
            <div className="space-y-6">
              <h2 className="text-2xl font-bold border-b border-border pb-4">profile management</h2>
              <p className="text-muted-foreground">
                Manage connection profiles for different environments.
              </p>
              <CodeExample
                label="save a profile"
                examples={{
                  typescript: `sombra profile save production \\
  --url sombra://prod-db:7687 \\
  --user admin \\
  --pager-async-fsync \\
  --version-codec snappy`
                }}
              />
            </div>
          </div>
        </div>
      </section>
    </main>
  )
}
