export const DEMO_FOLLOWS_QUERY = {
  $schemaVersion: 1,
  matches: [
    { var: "follower", label: "User" },
    { var: "followee", label: "User" },
  ],
  edges: [
    {
      from: "follower",
      to: "followee",
      edge_type: "FOLLOWS",
      direction: "out",
    },
  ],
  projections: [
    { kind: "var", var: "follower" },
    { kind: "var", var: "followee" },
  ],
} as const;

type DirectionOption = "out" | "in" | "both";
export type AutoGraphSpecOptions = {
  sourceLabel?: string | null;
  targetLabel?: string | null;
  edgeType?: string | null;
  direction?: DirectionOption;
};

export function createAutoGraphSpec(options: AutoGraphSpecOptions) {
  const sourceLabel = options.sourceLabel?.trim();
  const targetLabel = options.targetLabel?.trim();
  if (!sourceLabel || !targetLabel) {
    throw new Error("Both source and target labels are required");
  }

  const matchSource: Record<string, string> = { var: "source", label: sourceLabel };
  const matchTarget: Record<string, string> = { var: "target", label: targetLabel };

  const edge: Record<string, string> = {
    from: "source",
    to: "target",
    direction: (options.direction ?? "out") as DirectionOption,
  };
  if (options.edgeType?.trim()) {
    edge.edge_type = options.edgeType.trim();
  }

  return {
    $schemaVersion: 1,
    matches: [matchSource, matchTarget],
    edges: [edge],
    projections: [
      { kind: "var", var: "source" },
      { kind: "var", var: "target" },
    ],
  } as const;
}
