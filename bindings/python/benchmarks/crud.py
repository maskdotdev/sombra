"""Batched CRUD micro-benchmark for the Python bindings."""

from __future__ import annotations

import os
import statistics
import time
from collections import deque
from pathlib import Path
from typing import Callable, Deque, List

from sombra_py import Database

LABEL_USER = "User"
EDGE_TYPE_FOLLOWS = "FOLLOWS"
EDGE_ANCHOR_COUNT = 16
BENCH_ROOT = Path(__file__).resolve().parents[3] / "target" / "bench"
DB_PATH = BENCH_ROOT / "python-crud.db"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


OPS_PER_BATCH = _env_int("BENCH_BATCH_SIZE", 256)
PREFILL_BATCHES = _env_int("BENCH_PREFILL_BATCHES", 512)
CACHE_PAGES = _env_int("BENCH_CACHE_PAGES", 4096)


def _format_ops_per_second(ops_per_second: float) -> str:
    if not ops_per_second or ops_per_second <= 0:
        return "n/a"
    for threshold, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if ops_per_second >= threshold:
            return f"{ops_per_second / threshold:.1f}{suffix}"
    return f"{ops_per_second:.0f}"


class CrudHarness:
    def __init__(self, db: Database, batch_size: int) -> None:
        self.db = db
        self.batch_size = batch_size
        self.counter = 0
        self.node_delete_pool: Deque[int] = deque()
        self.edge_delete_pool: Deque[int] = deque()
        self.edge_anchor_nodes: List[int] = []
        self.node_update_target = 0
        self.edge_update_target = 0
        self.bootstrap()

    def bootstrap(self) -> None:
        if not self.edge_anchor_nodes:
            for i in range(EDGE_ANCHOR_COUNT):
                self.edge_anchor_nodes.append(self.create_user(f"edge-anchor-{i}"))
        self.node_update_target = self.edge_anchor_nodes[0]
        self.edge_update_target = self.create_edge_between(
            self.edge_anchor_nodes[0],
            self.edge_anchor_nodes[1 % EDGE_ANCHOR_COUNT],
        )
        self.prefill_delete_pools()

    def prefill_delete_pools(self) -> None:
        target = self.batch_size * PREFILL_BATCHES
        if len(self.node_delete_pool) < target:
            missing = target - len(self.node_delete_pool)
            self.node_delete_pool.extend(self.create_users(missing))
        if len(self.edge_delete_pool) < target:
            missing = target - len(self.edge_delete_pool)
            self.edge_delete_pool.extend(self.create_edges(missing))

    def create_node_batch(self) -> None:
        ops = [
            {
                "op": "createNode",
                "labels": [LABEL_USER],
                "props": {"name": f"bench-node-{self.bump_counter()}"},
            }
            for _ in range(self.batch_size)
        ]
        summary = self.db.mutate_many(ops)
        self.node_delete_pool.extend(summary.get("createdNodes", []))

    def update_node_batch(self) -> None:
        ops = [
            {
                "op": "updateNode",
                "id": self.node_update_target,
                "set": {"bio": f"bio-{self.bump_counter()}"},
                "unset": [],
            }
            for _ in range(self.batch_size)
        ]
        self.db.mutate_many(ops)

    def delete_node_batch(self) -> None:
        self._ensure_node_capacity()
        ids = [self.node_delete_pool.popleft() for _ in range(self.batch_size)]
        ops = [{"op": "deleteNode", "id": node_id, "cascade": True} for node_id in ids]
        self.db.mutate_many(ops)

    def create_edge_batch(self) -> None:
        ops = []
        for _ in range(self.batch_size):
            src, dst = self.next_edge_pair()
            ops.append({"op": "createEdge", "src": src, "dst": dst, "ty": EDGE_TYPE_FOLLOWS, "props": {}})
        summary = self.db.mutate_many(ops)
        self.edge_delete_pool.extend(summary.get("createdEdges", []))

    def update_edge_batch(self) -> None:
        ops = [
            {
                "op": "updateEdge",
                "id": self.edge_update_target,
                "set": {"weight": self.bump_counter() % 1_000},
                "unset": [],
            }
            for _ in range(self.batch_size)
        ]
        self.db.mutate_many(ops)

    def delete_edge_batch(self) -> None:
        self._ensure_edge_capacity()
        ids = [self.edge_delete_pool.popleft() for _ in range(self.batch_size)]
        ops = [{"op": "deleteEdge", "id": edge_id} for edge_id in ids]
        self.db.mutate_many(ops)

    def read_users(self) -> None:
        self.db.query().match(LABEL_USER).select(["n0"]).execute().rows()

    def create_user(self, name: str) -> int:
        summary = self.db.mutate_many([{"op": "createNode", "labels": [LABEL_USER], "props": {"name": name}}])
        created = summary.get("createdNodes", [])
        if not created:
            raise RuntimeError("create_user must return an id")
        return int(created[-1])

    def create_users(self, count: int) -> List[int]:
        created: List[int] = []
        remaining = count
        while remaining > 0:
            chunk = min(remaining, self.batch_size)
            ops = [
                {
                    "op": "createNode",
                    "labels": [LABEL_USER],
                    "props": {"name": f"delete-pool-{self.bump_counter()}"},
                }
                for _ in range(chunk)
            ]
            summary = self.db.mutate_many(ops)
            created.extend(int(node_id) for node_id in summary.get("createdNodes", []))
            remaining -= chunk
        return created

    def create_edge_between(self, src: int, dst: int) -> int:
        summary = self.db.mutate_many(
            [{"op": "createEdge", "src": src, "dst": dst, "ty": EDGE_TYPE_FOLLOWS, "props": {}}]
        )
        created = summary.get("createdEdges", [])
        if not created:
            raise RuntimeError("create_edge_between must return an id")
        return int(created[-1])

    def create_edges(self, count: int) -> List[int]:
        created: List[int] = []
        remaining = count
        while remaining > 0:
            chunk = min(remaining, self.batch_size)
            ops = []
            for _ in range(chunk):
                src, dst = self.next_edge_pair()
                ops.append({"op": "createEdge", "src": src, "dst": dst, "ty": EDGE_TYPE_FOLLOWS, "props": {}})
            summary = self.db.mutate_many(ops)
            created.extend(int(edge_id) for edge_id in summary.get("createdEdges", []))
            remaining -= chunk
        return created

    def _ensure_node_capacity(self) -> None:
        if len(self.node_delete_pool) < self.batch_size:
            self.node_delete_pool.extend(self.create_users(self.batch_size * 2))

    def _ensure_edge_capacity(self) -> None:
        if len(self.edge_delete_pool) < self.batch_size:
            self.edge_delete_pool.extend(self.create_edges(self.batch_size * 2))

    def next_edge_pair(self) -> tuple[int, int]:
        if len(self.edge_anchor_nodes) < 2:
            raise RuntimeError("edge anchor set must contain at least two nodes")
        idx = self.bump_counter() % len(self.edge_anchor_nodes)
        src = self.edge_anchor_nodes[idx]
        dst = self.edge_anchor_nodes[(idx + 1) % len(self.edge_anchor_nodes)]
        return src, dst

    def bump_counter(self) -> int:
        value = self.counter
        self.counter += 1
        return value


def time_operation(
    label: str,
    fn: Callable[[], None],
    iterations: int = 200,
    ops_per_iter: int = 1,
) -> float:
    samples: List[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    mean = statistics.mean(samples)
    per_op = mean / max(ops_per_iter, 1)
    ops_per_second = ops_per_iter / mean if mean > 0 else float("inf")
    formatted_ops = _format_ops_per_second(ops_per_second)
    print(f"{label:>20}: {per_op * 1e6:.1f} µs/op | {formatted_ops} ops/s ({ops_per_iter} ops/txn)")
    return per_op


def main() -> None:
    BENCH_ROOT.mkdir(parents=True, exist_ok=True)
    fresh = not DB_PATH.exists()
    db = Database.open(
        str(DB_PATH),
        synchronous="normal",
        commit_coalesce_ms=5,
        cache_pages=CACHE_PAGES,
    )
    if fresh:
        db.seed_demo()
    harness = CrudHarness(db, OPS_PER_BATCH)

    print(
        f"Running Python CRUD micro-benchmarks with batch_size={OPS_PER_BATCH}, "
        f"synchronous=normal, commit_coalesce_ms=5, cache_pages={CACHE_PAGES}",
    )
    time_operation("create nodes", harness.create_node_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("update nodes", harness.update_node_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("delete nodes", harness.delete_node_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("create edges", harness.create_edge_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("update edges", harness.update_edge_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("delete edges", harness.delete_edge_batch, ops_per_iter=OPS_PER_BATCH)
    time_operation("read users", harness.read_users)


if __name__ == "__main__":
    main()
