#!/usr/bin/env python3
"""
Convert LDBC SNB CSV exports (sf0.1 bi composite snapshot) into the two files that
`sombra-cli import` expects: `nodes.csv` (Users) and `edges.csv` (FOLLOWS).

Usage:
    scripts/ldbc_to_sombra.py \
        --input out_sf0.1_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot \
        --nodes nodes.csv \
        --edges edges.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import pathlib
from typing import Dict, Iterable, Iterator, List


def main() -> None:
    args = _parse_args()
    snapshot = pathlib.Path(args.input).resolve()
    person_dir = snapshot / "dynamic" / "Person"
    knows_dir = snapshot / "dynamic" / "Person_knows_Person"

    if not person_dir.exists():
        raise SystemExit(f"Person directory not found: {person_dir}")
    if not knows_dir.exists():
        raise SystemExit(f"Person_knows_Person directory not found: {knows_dir}")

    persons = _load_person_rows(person_dir)
    _write_nodes(args.nodes, persons)

    edges = _load_knows_rows(knows_dir)
    _write_edges(args.edges, edges)

    print(
        f"wrote {len(persons)} nodes to {args.nodes} "
        f"and {len(edges) * 2} edges to {args.edges}"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="out_sf0.1_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot",
        help="Path to the LDBC SNB initial snapshot directory",
    )
    parser.add_argument(
        "--nodes",
        default="nodes.csv",
        help="Output path for the consolidated nodes file",
    )
    parser.add_argument(
        "--edges",
        default="edges.csv",
        help="Output path for the consolidated edges file",
    )
    return parser.parse_args()


def _iter_csv_rows(directory: pathlib.Path) -> Iterator[Dict[str, str]]:
    pattern = str(directory / "*.csv")
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="|")
            for row in reader:
                yield row


def _load_person_rows(directory: pathlib.Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for raw in _iter_csv_rows(directory):
        full_name = f"{raw['firstName']} {raw['lastName']}".strip()
        row = {
            "id": raw["id"],
            "label": "User",
            "name": full_name,
            "creationDate": raw["creationDate"],
            "gender": raw["gender"],
            "birthday": raw["birthday"],
            "emails": raw.get("email", ""),
            "languages": raw.get("language", ""),
            "locationCityId": raw.get("LocationCityId", ""),
            "ip": raw.get("locationIP", ""),
            "browser": raw.get("browserUsed", ""),
        }
        rows.append(row)
    return rows


def _load_knows_rows(directory: pathlib.Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for raw in _iter_csv_rows(directory):
        rows.append(
            {
                "src": raw["Person1Id"],
                "dst": raw["Person2Id"],
                "type": "FOLLOWS",
                "creationDate": raw["creationDate"],
            }
        )
    return rows


def _write_nodes(path: str, rows: Iterable[Dict[str, str]]) -> None:
    fieldnames = [
        "id",
        "label",
        "name",
        "creationDate",
        "gender",
        "birthday",
        "emails",
        "languages",
        "locationCityId",
        "ip",
        "browser",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_edges(path: str, rows: Iterable[Dict[str, str]]) -> None:
    fieldnames = ["src", "dst", "type", "creationDate"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            # Add the reverse direction so queries can traverse both ways.
            writer.writerow(
                {
                    "src": row["dst"],
                    "dst": row["src"],
                    "type": row["type"],
                    "creationDate": row["creationDate"],
                }
            )


if __name__ == "__main__":
    main()
