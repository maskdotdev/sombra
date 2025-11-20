"use client"

import { useSearchParams, useRouter, usePathname } from "next/navigation"
import { Button } from "@/components/ui/button"

const LANGUAGES = [
  { id: "typescript", label: "TypeScript" },
  { id: "python", label: "Python" },
  { id: "go", label: "Go" },
  { id: "rust", label: "Rust" },
] as const

export type Language = (typeof LANGUAGES)[number]["id"]

export function LanguageSelector() {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const currentLang = (searchParams.get("lang") as Language) || "typescript"

  const setLanguage = (lang: Language) => {
    const params = new URLSearchParams(searchParams.toString())
    params.set("lang", lang)
    router.push(`${pathname}?${params.toString()}`)
  }

  return (
    <div className="flex items-center gap-2 p-1 bg-background border border-border rounded-lg">
      {LANGUAGES.map((lang) => (
        <Button
          key={lang.id}
          size="sm"
          variant="ghost"
          onClick={() => setLanguage(lang.id)}
          className={`text-xs ${
            currentLang === lang.id ? "bg-muted text-foreground" : "text-muted-foreground hover:text-foreground"
          }`}
        >
          {lang.label}
        </Button>
      ))}
    </div>
  )
}
