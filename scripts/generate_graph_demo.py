#!/usr/bin/env python3
"""
Generate a synthetic social graph CSV pair (nodes + edges) for dashboard demos.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path
from typing import List, Sequence, Tuple


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    nodes = build_nodes(args.node_count, rng)
    edges = build_edges(nodes, args.edge_count, rng)
    write_nodes(args.nodes_out, nodes)
    write_edges(args.edges_out, edges)
    print(
        f"wrote {len(nodes)} nodes to {args.nodes_out} "
        f"and {len(edges)} edges to {args.edges_out}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-count", type=int, default=400)
    parser.add_argument("--edge-count", type=int, default=2400)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--nodes-out", type=Path, required=True)
    parser.add_argument("--edges-out", type=Path, required=True)
    return parser.parse_args()


def build_nodes(count: int, rng: random.Random) -> List[dict[str, str]]:
    first_names = [
        "Ada",
        "Alan",
        "Grace",
        "Edsger",
        "Anita",
        "Claude",
        "Joan",
        "Radia",
        "Linus",
        "Guido",
        "Barbara",
        "Donald",
        "Hedy",
        "Niklaus",
        "Margaret",
        "James",
    ]
    last_names = [
        "Lovelace",
        "Turing",
        "Hopper",
        "Dijkstra",
        "Borg",
        "Shannon",
        "Clarke",
        "Perlman",
        "Torvalds",
        "van Rossum",
        "Liskov",
        "Knuth",
        "Lamarr",
        "Wirth",
        "Hamilton",
        "Gosling",
    ]
    roles = ["Engineer", "Analyst", "PM", "Designer", "Evangelist", "SRE"]
    teams = ["Core", "Edge", "Atlas", "Nexus", "Pulse", "Signal", "Graph", "Storage"]
    cities = [
        "San Francisco",
        "New York",
        "London",
        "Berlin",
        "Tokyo",
        "Sydney",
        "Bangalore",
        "Toronto",
        "Dublin",
        "Austin",
    ]
    skills = ["python", "rust", "sql", "spark", "ml", "frontend", "backend", "systems"]

    nodes: List[dict[str, str]] = []
    start_date = date(2015, 1, 1)
    for idx in range(1, count + 1):
        first = rng.choice(first_names)
        last = rng.choice(last_names)
        name = f"{first} {last}"
        handle = f"@{first.lower()}{idx}"
        joined = start_date + timedelta(days=rng.randint(0, 365 * 9))
        skill_sample = ", ".join(sorted(rng.sample(skills, k=3)))
        label_suffix = rng.choice(["", "|Mentor", "|Manager", "|Contributor"])
        nodes.append(
            {
                "id": str(idx),
                "labels": f"Person{label_suffix}",
                "name": name,
                "handle": handle,
                "role": rng.choice(roles),
                "team": rng.choice(teams),
                "city": rng.choice(cities),
                "joined_at": joined.isoformat(),
                "skills": skill_sample,
            }
        )
    return nodes


def build_edges(
    nodes: Sequence[dict[str, str]], count: int, rng: random.Random
) -> List[dict[str, str]]:
    types = ["FOLLOWS", "MENTORS", "COLLABORATES_WITH"]
    projects = ["Atlas", "Beacon", "Circuit", "Drift", "Eon", "Flux"]
    pairs: set[Tuple[str, str, str]] = set()
    ids = [node["id"] for node in nodes]
    while len(pairs) < count:
        src, dst = rng.sample(ids, k=2)
        kind = rng.choice(types)
        key = (src, dst, kind)
        if key in pairs:
            continue
        pairs.add(key)
    edges: List[dict[str, str]] = []
    base_date = date(2017, 1, 1)
    for src, dst, kind in pairs:
        started = base_date + timedelta(days=rng.randint(0, 365 * 6))
        edges.append(
            {
                "src": src,
                "dst": dst,
                "type": kind,
                "project": rng.choice(projects),
                "strength": f"{rng.uniform(0.1, 1.0):.2f}",
                "since": started.isoformat(),
            }
        )
    return edges


def write_nodes(path: Path, rows: Sequence[dict[str, str]]) -> None:
    fieldnames = [
        "id",
        "labels",
        "name",
        "handle",
        "role",
        "team",
        "city",
        "joined_at",
        "skills",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_edges(path: Path, rows: Sequence[dict[str, str]]) -> None:
    fieldnames = ["src", "dst", "type", "project", "strength", "since"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
