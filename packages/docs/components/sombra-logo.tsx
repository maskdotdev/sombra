"use client"

import { useEffect, useState } from "react"

export function SombraLogo({ className = "" }: { className?: string }) {
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  return (
    <svg
      width="32"
      height="32"
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
    >
      {/* Edges */}
      <g className="opacity-60">
        <line
          x1="8"
          y1="8"
          x2="16"
          y2="24"
          stroke="currentColor"
          strokeWidth="1"
          className="animate-pulse"
          style={{ animationDelay: "0s", animationDuration: "3s" }}
        />
        <line
          x1="16"
          y1="24"
          x2="26"
          y2="16"
          stroke="currentColor"
          strokeWidth="1"
          className="animate-pulse"
          style={{ animationDelay: "1.5s", animationDuration: "3s" }}
        />
      </g>

      {/* Nodes */}
      <g className={mounted ? "animate-pulse" : ""} style={{ animationDelay: "0s", animationDuration: "2s" }}>
        <circle cx="8" cy="8" r="3" fill="currentColor" className="opacity-90" />
        <circle cx="8" cy="8" r="4" fill="none" stroke="currentColor" strokeWidth="0.5" className="opacity-40" />
      </g>

      <g className={mounted ? "animate-pulse" : ""} style={{ animationDelay: "0.66s", animationDuration: "2s" }}>
        <circle cx="16" cy="24" r="3" fill="currentColor" className="opacity-90" />
        <circle cx="16" cy="24" r="4" fill="none" stroke="currentColor" strokeWidth="0.5" className="opacity-40" />
      </g>

      <g className={mounted ? "animate-pulse" : ""} style={{ animationDelay: "1.33s", animationDuration: "2s" }}>
        <circle cx="26" cy="16" r="3" fill="currentColor" className="opacity-90" />
        <circle cx="26" cy="16" r="4" fill="none" stroke="currentColor" strokeWidth="0.5" className="opacity-40" />
      </g>
    </svg>
  )
}
