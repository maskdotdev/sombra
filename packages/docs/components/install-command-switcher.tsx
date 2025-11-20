"use client"

import { useState, useEffect } from "react"
import { Copy, Check } from "lucide-react"

const COMMANDS = [
  { manager: "npm", command: "npm install @sombra/core" },
  { manager: "pnpm", command: "pnpm add @sombra/core" },
  { manager: "bun", command: "bun add @sombra/core" },
  { manager: "uv", command: "uv pip install sombra" },
  { manager: "cargo", command: "cargo add sombra" },
]

const GLITCH_CHARS = "!@#$%^&*()_+-=[]{}|;:,.<>?/~`"

export function InstallCommandSwitcher() {
  const [currentIndex, setCurrentIndex] = useState(0)
  const [displayText, setDisplayText] = useState(COMMANDS[0].command)
  const [isGlitching, setIsGlitching] = useState(false)
  const [isHovered, setIsHovered] = useState(false)
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null)

  useEffect(() => {
    const interval = setInterval(() => {
      setIsGlitching(true)

      // Glitch effect
      let glitchCount = 0
      const glitchInterval = setInterval(() => {
        const currentCommand = COMMANDS[currentIndex].command
        const glitched = currentCommand
          .split("")
          .map((char) => (Math.random() > 0.7 ? GLITCH_CHARS[Math.floor(Math.random() * GLITCH_CHARS.length)] : char))
          .join("")
        setDisplayText(glitched)
        glitchCount++

        if (glitchCount > 8) {
          clearInterval(glitchInterval)
          const nextIndex = (currentIndex + 1) % COMMANDS.length
          setCurrentIndex(nextIndex)
          setDisplayText(COMMANDS[nextIndex].command)
          setIsGlitching(false)
        }
      }, 50)
    }, 3000)

    return () => clearInterval(interval)
  }, [currentIndex])

  const copyToClipboard = async (command: string, index: number) => {
    await navigator.clipboard.writeText(command)
    setCopiedIndex(index)
    setTimeout(() => setCopiedIndex(null), 2000)
  }

  return (
    <div
      className="relative inline-block pb-4 mb-12"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <div className="px-3 py-1 bg-secondary rounded-md text-sm text-muted-foreground font-mono">$ {displayText}</div>

      {isHovered && !isGlitching && (
        <div className="absolute top-full left-0 right-0 flex flex-wrap gap-2 justify-center animate-in fade-in slide-in-from-top-2 duration-200">
          {COMMANDS.map((cmd, index) => (
            <button
              key={cmd.manager}
              onClick={() => copyToClipboard(cmd.command, index)}
              className="px-2 py-1 bg-secondary hover:bg-muted border border-border rounded text-xs text-muted-foreground hover:text-foreground transition-colors flex items-center gap-1"
            >
              {copiedIndex === index ? (
                <>
                  <Check className="w-3 h-3" />
                  copied
                </>
              ) : (
                <>
                  <Copy className="w-3 h-3" />
                  {cmd.manager}
                </>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
