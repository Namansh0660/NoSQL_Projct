"""
Benchmark Runner for NOSQL KG Project
-------------------------------------
- Uses existing MongoDB Atlas collections (papers, nodes, edges)
- Benchmarks inserts, lookups, traversals, and aggregations
- Prints JSON results for easy parsing
"""

import os
import time
import json
import random
from typing import Dict, Tuple
from pymongo.errors import PyMongoError
from api.db import papers_collection, nodes_collection, edges_collection  # ✅ Atlas connection

# -----------------------------
# Config
# -----------------------------
N_LOOKUPS = int(os.getenv("BENCH_N_LOOKUPS", "2000"))
SEED = int(os.getenv("BENCH_SEED", "42"))
random.seed(SEED)


# -----------------------------
# Timer utility
# -----------------------------
def timer(fn, *args, **kwargs) -> Tuple[float, any]:
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    return (time.perf_counter() - start), result


# -----------------------------
# Benchmark lookups
# -----------------------------
def do_lookups() -> Dict[str, float]:
    times = {}

    # Collect IDs for random access
    keys = [d["id"] for d in nodes_collection.find({"type": "Paper"}, {"id": 1}).limit(5000)]
    if not keys:
        raise RuntimeError("No paper nodes found in DB for benchmarking.")

    def rnd():
        return random.choice(keys)

    # 1️⃣ Point reads
    t, _ = timer(lambda: [nodes_collection.find_one({"id": rnd()}) for _ in range(N_LOOKUPS)])
    times["nodes_point_reads_s"] = t

    # 2️⃣ Traversals: paper -> authors
    t, _ = timer(lambda: [
        list(edges_collection.find({"source": rnd(), "relation": "authored_by"}).limit(5))
        for _ in range(N_LOOKUPS)
    ])
    times["edges_traversal_s"] = t

    # 3️⃣ Aggregation: count by keyword
    t, _ = timer(lambda: list(nodes_collection.aggregate([
        {"$match": {"type": "Paper"}},
        {"$unwind": "$properties.keywords"},
        {"$group": {"_id": "$properties.keywords", "c": {"$sum": 1}}},
        {"$sort": {"c": -1}},
        {"$limit": 10}
    ])))
    times["nodes_keywords_agg_s"] = t

    return times


# -----------------------------
# Print results
# -----------------------------
def print_result(label: str, metrics: Dict[str, float]):
    print(json.dumps({"label": label, **metrics}, indent=2))


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    try:
        metrics = do_lookups()
        print_result("real_data_benchmark", metrics)
    except PyMongoError as e:
        print(f"MongoDB error during benchmark: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

