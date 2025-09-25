import os
import time
import json
import random
import statistics
import datetime
import streamlit as st
from typing import Dict, List, Callable
from pymongo import MongoClient
from ingestion.kafka_api_fetcher import fetch_arxiv, fetch_pubmed, fetch_crossref

st.set_page_config(page_title="NOSQL KG Sharding Dashboard", layout="wide")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27018")
MONGODB_DB = os.getenv("MONGODB_DB", "NOSQL")

@st.cache_resource
def get_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DB]

def add_zone_key(papers: List[Dict]) -> List[Dict]:
    out = []
    for p in papers:
        base = (p.get("keywords", [p.get("title", "z")])[0] or "z").lower()
        zk = base[0] if base else "z"
        d = dict(p)
        d["zoneKey"] = zk
        out.append(d)
    return out

def try_fetch(fn: Callable, **kwargs) -> List[Dict]:
    try:
        return fn(**kwargs)
    except Exception as e:
        st.warning(f"Fetch error for {fn.__name__}: {e}")
        return []

def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * p
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[int(k)]
    return values[f] * (c - k) + values[c] * (k - f)

def run_read_benchmark(coll, build_query: Callable[[], Dict], n: int, explain_sample: int = 50) -> Dict:
    latencies: List[float] = []
    errors = 0
    shard_hits: Dict[str, int] = {}
    start = time.perf_counter()
    for i in range(n):
        q = build_query()
        t0 = time.perf_counter()
        try:
            _ = coll.find_one(q)
        except Exception:
            errors += 1
        latencies.append(time.perf_counter() - t0)
        if i < explain_sample:
            try:
                exp = coll.find(q).limit(1).explain()
                qp = exp.get("queryPlanner", {})
                shards = qp.get("winningPlan", {}).get("shards") or exp.get("executionStats", {}).get("shards")
                if isinstance(shards, list):
                    for shard in shards:
                        name = shard.get("shardName") or shard.get("shardNameHint") or "unknown"
                        shard_hits[name] = shard_hits.get(name, 0) + 1
                else:
                    shard_hits["single"] = shard_hits.get("single", 0) + 1
            except Exception:
                pass
    total_time = time.perf_counter() - start
    ops_sec = (n / total_time) if total_time > 0 else 0.0
    fanout = (sum(shard_hits.values()) / explain_sample) if explain_sample else 1.0
    return {
        "avg_s": statistics.fmean(latencies) if latencies else 0.0,
        "p50_s": percentile(latencies, 0.50),
        "p95_s": percentile(latencies, 0.95),
        "p99_s": percentile(latencies, 0.99),
        "ops_per_sec": ops_sec,
        "errors": errors,
        "fanout_avg": fanout,
        "fanout_breakdown": shard_hits,
    }

def coll_shard_stats(db, coll_name: str) -> Dict:
    try:
        stats = db.command({"collStats": coll_name, "scale": 1, "verbose": True})
        shards = stats.get("shards") or {}
        counts = []
        sizes = []
        shard_rows = []
        for shard, s in shards.items():
            c = s.get("count", 0)
            sz = s.get("size", 0)
            counts.append(c)
            sizes.append(sz)
            shard_rows.append({"shard": shard, "count": c, "size_bytes": sz})
        sd_count = statistics.pstdev(counts) if counts else 0.0
        total = sum(counts) if counts else 0
        balance_index = (sd_count / total) if total else 0.0
        return {
            "per_shard": shard_rows,
            "count_stddev": sd_count,
            "balance_index": balance_index,
            "total_docs": total,
            "total_size_bytes": sum(sizes) if sizes else 0,
        }
    except Exception as e:
        return {"error": str(e)}

st.title("NOSQL Knowledge Graph - Sharding & Fetch Dashboard")

with st.sidebar:
    st.header("Settings")
    topic = st.text_input("Topic / Query", value="graph neural networks")
    source_opts = st.multiselect("Sources", ["arxiv", "pubmed", "crossref"], default=["arxiv", "crossref"])
    batch_size = st.number_input("Batch size per source", 1, 200, 50)
    n_lookups = st.number_input("Lookup count (per strategy)", 10, 10000, 1000, step=100)
    seed = st.number_input("Random seed", 0, 10_000, 42)
    run_btn = st.button("Fetch, Insert & Benchmark")

if "results" not in st.session_state:
    st.session_state["results"] = []

col1, col2 = st.columns(2)

with col1:
    st.subheader("Last Fetch")
    if run_btn:
        random.seed(int(seed))
        db = get_db()

        # Fetch papers (robust)
        fetched: List[Dict] = []
        if "arxiv" in source_opts:
            fetched += try_fetch(fetch_arxiv, batch_size=batch_size, search_query=topic)
        if "pubmed" in source_opts:
            fetched += try_fetch(fetch_pubmed, batch_size=batch_size, term=topic)
        if "crossref" in source_opts:
            fetched += try_fetch(fetch_crossref, batch_size=batch_size, query=topic)

        # Convert fetched_at to human-readable for display/storage
        for d in fetched:
            ts = d.get("fetched_at")
            if isinstance(ts, (int, float)):
                try:
                    d["fetched_at_human"] = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    d["fetched_at_human"] = None

        st.write(f"Fetched {len(fetched)} papers")
        st.json([{k: v for k, v in d.items() if k in ("title","doi","source","fetched_at_human")} for d in fetched[:3]])

        # Insert hashed baseline into papers
        t0 = time.perf_counter()
        if fetched:
            db.papers.insert_many(fetched, ordered=False)
        t_ins_baseline = time.perf_counter() - t0

        # Zonal copy into papers_zonal with zoneKey
        zonal = add_zone_key(fetched)
        t0 = time.perf_counter()
        if zonal:
            db.papers_zonal.insert_many(zonal, ordered=False)
        t_ins_zonal = time.perf_counter() - t0

        # Benchmarks
        keys = [p.get("id") for p in fetched if p.get("id")]
        if not keys:
            keys = [f"paper_{i}" for i in range(len(fetched))]

        # Hashed baseline
        def q_id():
            return {"id": random.choice(keys)}
        hashed_metrics = run_read_benchmark(db.papers, q_id, int(n_lookups))

        # Zonal
        letters = list({(topic or "a")[0].lower(), "a", "m", "n", "z"})
        def q_zone():
            return {"zoneKey": random.choice(letters)}
        zonal_metrics = run_read_benchmark(db.papers_zonal, q_zone, int(n_lookups))

        # Distribution & sizes
        dist_papers = coll_shard_stats(db, "papers")
        dist_papers_zonal = coll_shard_stats(db, "papers_zonal")

        result = {
            "topic": topic,
            "num_papers": len(fetched),
            # inserts
            "hashed_insert_s": t_ins_baseline,
            "zonal_insert_s": t_ins_zonal,
            # latency/throughput/errors/fanout
            "hashed_avg_s": hashed_metrics["avg_s"],
            "hashed_p95_s": hashed_metrics["p95_s"],
            "hashed_p99_s": hashed_metrics["p99_s"],
            "hashed_ops_sec": hashed_metrics["ops_per_sec"],
            "hashed_errors": hashed_metrics["errors"],
            "hashed_fanout": hashed_metrics["fanout_avg"],
            "zonal_avg_s": zonal_metrics["avg_s"],
            "zonal_p95_s": zonal_metrics["p95_s"],
            "zonal_p99_s": zonal_metrics["p99_s"],
            "zonal_ops_sec": zonal_metrics["ops_per_sec"],
            "zonal_errors": zonal_metrics["errors"],
            "zonal_fanout": zonal_metrics["fanout_avg"],
            # distribution
            "papers_stddev": dist_papers.get("count_stddev", 0),
            "papers_balance": dist_papers.get("balance_index", 0),
            "papers_zonal_stddev": dist_papers_zonal.get("count_stddev", 0),
            "papers_zonal_balance": dist_papers_zonal.get("balance_index", 0),
            # sizes
            "papers_total_bytes": dist_papers.get("total_size_bytes", 0),
            "papers_zonal_total_bytes": dist_papers_zonal.get("total_size_bytes", 0),
            # breakdowns
            "hashed_fanout_breakdown": hashed_metrics.get("fanout_breakdown", {}),
            "zonal_fanout_breakdown": zonal_metrics.get("fanout_breakdown", {}),
            "papers_per_shard": dist_papers.get("per_shard", []),
            "papers_zonal_per_shard": dist_papers_zonal.get("per_shard", []),
            # time
            "timestamp": time.time(),
        }
        st.session_state["results"].append(result)

    if st.session_state["results"]:
        st.subheader("Per Retrieval Metrics")
        cols_show = [
            "topic","num_papers",
            "hashed_insert_s","zonal_insert_s",
            "hashed_avg_s","hashed_p95_s","hashed_p99_s","hashed_ops_sec","hashed_errors","hashed_fanout",
            "zonal_avg_s","zonal_p95_s","zonal_p99_s","zonal_ops_sec","zonal_errors","zonal_fanout",
            "papers_stddev","papers_balance","papers_zonal_stddev","papers_zonal_balance",
            "papers_total_bytes","papers_zonal_total_bytes",
        ]
        import pandas as pd
        df = pd.DataFrame(st.session_state["results"]) 
        # Use human-readable datetime on x-axis
        if "timestamp" in df.columns:
            try:
                df["ts"] = pd.to_datetime(df["timestamp"], unit="s")
            except Exception:
                df["ts"] = df["timestamp"]
        # Ensure missing columns are created to avoid KeyError when selecting
        df_show = df.reindex(columns=cols_show)
        st.dataframe(df_show)

        st.markdown("**Fan-out breakdown (last run)**")
        last = st.session_state["results"][-1]
        st.json({
            "hashed": last.get("hashed_fanout_breakdown", {}),
            "zonal": last.get("zonal_fanout_breakdown", {}),
        })

        st.markdown("**Shard distribution (last run)**")
        st.json({
            "papers": last.get("papers_per_shard", []),
            "papers_zonal": last.get("papers_zonal_per_shard", []),
        })

with col2:
    st.subheader("Charts")
    if st.session_state["results"]:
        import pandas as pd
        import altair as alt
        df = pd.DataFrame(st.session_state["results"]) 

        def cols_available(df, cols):
            return [c for c in cols if c in df.columns]

        # Ensure human-readable datetime column exists
        if "timestamp" in df.columns and "ts" not in df.columns:
            try:
                df["ts"] = pd.to_datetime(df["timestamp"], unit="s")
            except Exception:
                df["ts"] = df["timestamp"]

        def draw_line(df, ycols, title, ytitle):
            cols = cols_available(df, ycols)
            if not cols or "ts" not in df.columns:
                return
            mdf = df[["ts"] + cols].melt("ts", var_name="metric", value_name="value")
            chart = alt.Chart(mdf).mark_line(point=True).encode(
                x=alt.X("ts:T", title="Time"),
                y=alt.Y("value:Q", title=ytitle),
                color=alt.Color("metric:N", title="Metric")
            ).properties(title=title, height=260)
            st.altair_chart(chart, use_container_width=True)

        def draw_bar(df, xcol, ycols, title, xtitle, ytitle):
            cols = cols_available(df, ycols)
            if not cols or xcol not in df.columns:
                return
            mdf = df[[xcol] + cols].melt(xcol, var_name="metric", value_name="value")
            chart = alt.Chart(mdf).mark_bar().encode(
                x=alt.X(f"{xcol}:N", title=xtitle),
                y=alt.Y("value:Q", title=ytitle),
                color=alt.Color("metric:N", title="Metric")
            ).properties(title=title, height=260)
            st.altair_chart(chart, use_container_width=True)

        st.markdown("**Latency over time**")
        draw_line(df, ["hashed_avg_s","zonal_avg_s","hashed_p95_s","zonal_p95_s","hashed_p99_s","zonal_p99_s"], "Latency (s)", "Seconds")

        st.markdown("**Throughput (ops/sec) over time**")
        draw_line(df, ["hashed_ops_sec","zonal_ops_sec"], "Throughput (ops/sec)", "Ops/sec")

        st.markdown("**Balance index (lower is better) over time**")
        draw_line(df, ["papers_balance","papers_zonal_balance"], "Balance Index", "StdDev/Total")

        st.markdown("**Insert time per topic**")
        draw_bar(df, "topic", ["hashed_insert_s","zonal_insert_s"], "Insert Time by Topic", "Topic", "Seconds")

st.caption("Data source: local mongos. Set env MONGODB_URI to point elsewhere.")
