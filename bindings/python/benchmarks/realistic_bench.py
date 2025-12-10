"""Realistic benchmark comparing Python bindings vs Node.js and Rust core.

Run with: uv run python benchmarks/realistic_bench.py
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import List

from sombra import Database


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


NODE_COUNT = env_int("NODES", 5000)
EDGE_COUNT = env_int("EDGES", 20000)
READ_COUNT = env_int("READS", 10000)

CODE_TEXTS = [
    "function foo() { return 1; }",
    "const bar = () => { console.log('hello'); };",
    "class Baz { constructor() {} }",
    "export function qux() { return true; }",
    "const quux = (x, y) => x + y;",
]

METADATA = [
    '{"type": "function", "exported": true}',
    '{"type": "variable", "exported": false}',
    '{"type": "class", "exported": true}',
]


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "bench.sombra"
        print(f"📂 temp db: {db_path}")

        db = Database.open(
            str(db_path),
            synchronous="normal",
            commit_coalesce_ms=0,
            cache_pages=16384,
        )

        # Create nodes AND edges in a single transaction using the builder
        print(f"Creating {NODE_COUNT} nodes and {EDGE_COUNT} edges in single transaction...")
        write_start = time.perf_counter()

        builder = db.create()
        handles: List = []

        # Create nodes
        for i in range(NODE_COUNT):
            handle = builder.node(
                ["Node"],
                {
                    "name": f"fn_{i}",
                    "filePath": f"/tmp/file_{i // 50}.ts",
                    "startLine": i,
                    "endLine": i + 5,
                    "codeText": CODE_TEXTS[i % len(CODE_TEXTS)],
                    "language": "typescript",
                    "metadata": METADATA[i % len(METADATA)],
                },
            )
            handles.append(handle)

        node_prep_time = time.perf_counter() - write_start
        print(f"  prepared {NODE_COUNT} nodes: {node_prep_time * 1000:.1f} ms")

        # Create edges using handles
        edge_start = time.perf_counter()
        for i in range(EDGE_COUNT):
            src = handles[i % len(handles)]
            dst = handles[(i * 13 + 7) % len(handles)]
            # Skip self loops
            if src.index == dst.index:
                dst = handles[(i + 1) % len(handles)]
            builder.edge(
                src,
                "LINKS",
                dst,
                {
                    "weight": (i % 10) / 10.0,
                    "kind": "call" if i % 2 == 0 else "reference",
                },
            )

        edge_prep_time = time.perf_counter() - edge_start
        print(f"  prepared {EDGE_COUNT} edges: {edge_prep_time * 1000:.1f} ms")

        # Execute the transaction
        exec_start = time.perf_counter()
        summary = builder.execute()
        exec_time = time.perf_counter() - exec_start
        print(f"  executed transaction: {exec_time * 1000:.1f} ms")

        write_time = time.perf_counter() - write_start
        print(f"create total: {write_time * 1000:.1f} ms")

        node_ids = summary["nodes"]

        # Random reads using get_node_record
        print(f"Running {READ_COUNT} reads...")
        read_start = time.perf_counter()
        first_20_times: List[float] = []

        for i in range(READ_COUNT):
            node_id = node_ids[(i * 17) % len(node_ids)]
            single_start = time.perf_counter()
            record = db.get_node_record(node_id)
            if i < 20:
                first_20_times.append((time.perf_counter() - single_start) * 1_000_000)  # µs

        read_time = time.perf_counter() - read_start
        print(f"random reads: {read_time * 1000:.1f} ms")
        print(f"First 20 read times (µs): {[int(t) for t in first_20_times]}")

        db.close()

        # Print summary
        print("\n📊 Benchmark Summary (Python Bindings):")
        print(f"- Nodes: {NODE_COUNT} ({NODE_COUNT / write_time:.0f} nodes/sec)")
        print(f"- Edges: {EDGE_COUNT} ({EDGE_COUNT / write_time:.0f} edges/sec)")
        print(f"- Reads: {READ_COUNT} ({read_time * 1000:.1f}ms, {READ_COUNT / read_time:.0f} reads/sec)")
        print(f"- Total write time: {write_time * 1000:.1f}ms")


if __name__ == "__main__":
    main()
