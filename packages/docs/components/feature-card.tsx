import type React from "react"
import { Card } from "@/components/ui/card"

interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
}

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <Card className="p-6 bg-card border-border hover:border-primary/50 transition-colors">
      <div className="text-primary mb-4">{icon}</div>
      <h3 className="text-lg font-bold mb-2 text-foreground">{title}</h3>
      <p className="text-sm text-muted-foreground leading-relaxed">{description}</p>
    </Card>
  )
}
