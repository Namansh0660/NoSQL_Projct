#!/usr/bin/env python3
import os
import time
import json
import random
from typing import List, Dict, Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError

DB_NAME = os.getenv("MONGODB_DB", "NOSQL")
URI = os.getenv("MONGODB_URI", "mongodb://localhost:27018")

N_INSERT = int(os.getenv("BENCH_N_INSERT", "2000"))
N_LOOKUPS = int(os.getenv("BENCH_N_LOOKUPS", "2000"))
SEED = int(os.getenv("BENCH_SEED", "42"))
random.seed(SEED)


def timer(fn, *args, **kwargs) -> Tuple[float, any]:
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    return (time.perf_counter() - start), result


def connect_client() -> MongoClient:
    return MongoClient(URI)


def prepare_collections(client: MongoClient):
    db = client[DB_NAME]
    db.nodes.create_index("id")
    db.edges.create_index([("source", 1), ("target", 1), ("relation", 1)])
    db.papers.create_index("id")
    return db


def generate_docs(n: int) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    papers = []
    nodes = []
    edges = []
    for i in range(n):
        pid = f"paper_{i}"
        paper = {
            "id": pid,
            "title": f"Title {i}",
            "abstract": "x" * 200,
            "authors": [f"Author {i%10}", f"Author {(i+1)%10}"],
            "institutions": [f"Inst {i%5}"],
            "keywords": [f"KW{i%7}"]
        }
        papers.append(paper)
        node = {"id": pid, "type": "Paper", "properties": {"keywords": paper["keywords"], "authors": paper["authors"], "institutions": paper["institutions"]}}
        nodes.append(node)
        for a in paper["authors"]:
            edges.append({"source": pid, "target": f"author_{a.replace(' ', '_').lower()}", "relation": "authored_by"})
    return nodes, edges, papers


def add_zone_key(papers: List[Dict]) -> List[Dict]:
    zonal = []
    for p in papers:
        # Use first letter of first keyword/title as zone key
        base = (p.get("keywords", [p.get("title", "z")])[0] or "z").lower()
        zk = base[0] if base else "z"
        d = dict(p)
        d["zoneKey"] = zk
        zonal.append(d)
    return zonal


def load_data(db, nodes, edges, papers) -> Dict[str, float]:
    times = {}
    t, _ = timer(db.nodes.delete_many, {})
    times["nodes_clear_s"] = t
    t, _ = timer(db.edges.delete_many, {})
    times["edges_clear_s"] = t
    t, _ = timer(db.papers.delete_many, {})
    times["papers_clear_s"] = t

    t, _ = timer(db.nodes.insert_many, nodes, ordered=False)
    times["nodes_insert_s"] = t
    t, _ = timer(db.edges.insert_many, edges, ordered=False)
    times["edges_insert_s"] = t
    t, _ = timer(db.papers.insert_many, papers, ordered=False)
    times["papers_insert_s"] = t
    return times


def load_zonal(db, papers_zonal: List[Dict]) -> Dict[str, float]:
    times = {}
    t, _ = timer(db.papers_zonal.delete_many, {})
    times["papers_zonal_clear_s"] = t
    t, _ = timer(db.papers_zonal.insert_many, papers_zonal, ordered=False)
    times["papers_zonal_insert_s"] = t
    return times


def do_lookups(db, keys: List[str]) -> Dict[str, float]:
    times = {}
    def rnd(keys):
        return keys[random.randrange(0, len(keys))]

    t, _ = timer(lambda: [db.nodes.find_one({"id": rnd(keys)}) for _ in range(N_LOOKUPS)])
    times["nodes_point_reads_s"] = t

    # Simple traversals: paper -> authors via edges
    edge_keys = [(f"paper_{i}", "authored_by") for i in range(len(keys))]
    t, _ = timer(lambda: [list(db.edges.find({"source": rnd(keys), "relation": "authored_by"}).limit(5)) for _ in range(N_LOOKUPS)])
    times["edges_traversal_s"] = t

    # Aggregate: count by keyword
    t, _ = timer(lambda: list(db.nodes.aggregate([
        {"$match": {"type": "Paper"}},
        {"$unwind": "$properties.keywords"},
        {"$group": {"_id": "$properties.keywords", "c": {"$sum": 1}}},
        {"$sort": {"c": -1}},
        {"$limit": 10}
    ])))
    times["nodes_keywords_agg_s"] = t

    return times


def print_result(label: str, metrics: Dict[str, float]):
    print(json.dumps({"label": label, **metrics}, indent=2))


if __name__ == "__main__":
    client = connect_client()
    db = prepare_collections(client)
    nodes, edges, papers = generate_docs(N_INSERT)
    papers_zonal = add_zone_key(papers)

    keys = [d["id"] for d in nodes]

    # Baseline (existing shard keys from init): hashed id on nodes/papers, compound on edges
    m1 = load_data(db, nodes, edges, papers)
    m1.update(do_lookups(db, keys))
    print_result("hashed_id_baseline", m1)

    # Range sharding: reshard nodes by range-like key using a new collection copy
    # Note: true reshard requires 5.0+ and admin ops; here we approximate using a temp collection
    db.nodes_range = db["nodes_range"]
    db.nodes_range.drop()
    db.nodes_range.create_index([("type", 1), ("id", 1)])
    t_copy, _ = timer(lambda: db.nodes_range.insert_many(nodes, ordered=False))
    t_reads, _ = timer(lambda: [db.nodes_range.find_one({"type": "Paper", "id": random.choice(keys)}) for _ in range(N_LOOKUPS)])
    print_result("range_key_nodes_copy", {"copy_insert_s": t_copy, "point_reads_s": t_reads})

    # Zonal sharding: use papers_zonal with zoneKey to route to shards (true zones configured in cluster)
    mz_ins = load_zonal(db, papers_zonal)
    # Route queries by zoneKey initial
    letters = ["a", "b", "c", "m", "n", "t", "z"]
    t_zonal_reads, _ = timer(lambda: [db.papers_zonal.find_one({"zoneKey": random.choice(letters)}) for _ in range(N_LOOKUPS)])
    print_result("zonal_true", {**mz_ins, "zonal_point_reads_s": t_zonal_reads})
