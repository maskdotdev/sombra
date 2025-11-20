"use client"

import { SombraLogo } from "@/components/sombra-logo"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { useSearchParams } from "next/navigation"

export function SiteHeader() {
  const searchParams = useSearchParams()
  const lang = searchParams.get("lang")

  const withLang = (path: string) => {
    if (lang) {
      return `${path}?lang=${lang}`
    }
    return path
  }

  return (
    <header className="border-b border-border">
      <div className="container mx-auto px-4 py-4 flex items-center justify-between">
        <Link href={withLang("/")} className="flex items-center gap-2">
          <SombraLogo className="text-primary" />
          <span className="text-xl font-bold text-foreground">sombra</span>
        </Link>
        <nav className="hidden md:flex items-center gap-6">
          <Link
            href={withLang("/docs")}
            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            docs
          </Link>
          <Link
            href={withLang("/api-reference")}
            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            api
          </Link>
          <Link
            href={withLang("/examples")}
            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            examples
          </Link>
          <Button size="sm" className="bg-primary text-primary-foreground hover:bg-primary/90">
            get started
          </Button>
        </nav>
      </div>
    </header>
  )
}
