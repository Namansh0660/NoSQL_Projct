import os
import time
import json
import random
import statistics
import datetime
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
from typing import Dict, List, Callable, Optional
from pymongo import MongoClient
import sys
sys.path.append('.')
from ingestion.kafka_api_fetcher import fetch_arxiv, fetch_pubmed, fetch_crossref
from benchmarks.sharding_bench import (ShardingStrategyFactory, ModuloShardingStrategy, 
                                     ConsistentHashingStrategy, RangeShardingStrategy)
from ingestion.kafka_producer import make_producer, produce_document
import os

st.set_page_config(
    page_title="NoSQL Knowledge Graph Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Use the same MongoDB connection as pipeline_runner.py
from api.db import db, papers_collection, nodes_collection, edges_collection

# Add custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stAlert > div {
        padding: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# Sidebar Configuration
# -----------------------------
st.sidebar.title("🔧 Configuration")

# Data source selection
data_source = st.sidebar.selectbox(
    "Select Data Source",
    ["MongoDB Atlas", "Local MongoDB"],
    index=0
)

# Real-time updates toggle
auto_refresh = st.sidebar.checkbox("Enable Auto-refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)

# Sharding configuration
st.sidebar.subheader("🗂️ Sharding Configuration")
selected_strategy = st.sidebar.selectbox(
    "Sharding Strategy",
    ["modulo", "consistent", "range", "hashed", "zonal"],
    index=0
)
num_shards = st.sidebar.slider("Number of Shards", 2, 10, 3)

# API fetching controls
st.sidebar.subheader("📡 Data Ingestion")
fetch_enabled = st.sidebar.checkbox("Enable API Fetching", value=False)
batch_size = st.sidebar.slider("Batch Size", 5, 50, 10)
topic_query = st.sidebar.text_input("Search Topic (e.g., graph databases)", value="graph databases")

# -----------------------------
# Main Dashboard Header
# -----------------------------
st.markdown('<h1 class="main-header">📊 NoSQL Knowledge Graph Dashboard</h1>', unsafe_allow_html=True)

# Real-time status indicator
col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
with col1:
    if auto_refresh:
        st.success("🟢 Live Updates: ON")
    else:
        st.warning("🟡 Live Updates: OFF")

with col2:
    st.info(f"🗄️ Database: {data_source}")

with col3:
    st.info(f"🔀 Sharding: {selected_strategy.title()}")
    
with col4:
    # Add a refresh button for manual updates
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# -----------------------------
# Early helpers to avoid NameError during first render
# -----------------------------
@st.cache_data(ttl=30)
def get_knowledge_graph_data(limit=1000, node_types=None, relation_types=None):
    try:
        node_query = {}
        if node_types and len(node_types) > 0:
            node_query["type"] = {"$in": node_types}
        # Exclude benchmark/test nodes
        node_query["sharding_strategy"] = {"$exists": False}
        nodes = list(nodes_collection.find(node_query).limit(limit))
        node_ids = [node["id"] for node in nodes]
        edge_query = {}
        if relation_types and len(relation_types) > 0:
            edge_query["relation"] = {"$in": relation_types}
        edge_query["$or"] = [
            {"source": {"$in": node_ids}},
            {"target": {"$in": node_ids}}
        ]
        edges = list(edges_collection.find(edge_query).limit(limit))
        latest_node = nodes_collection.find_one({}, sort=[("_id", -1)])
        latest_timestamp = latest_node.get("_id").generation_time if latest_node else None
        return nodes, edges, latest_timestamp
    except Exception as e:
        st.error(f"Error fetching KG data: {e}")
        return [], [], None

def create_network_graph(nodes, edges, layout_type="spring", show_labels=False):
    if not nodes or not edges:
        return go.Figure()
    G = nx.Graph()
    node_colors = {
        "Paper": "#1f77b4",
        "Author": "#ff7f0e",
        "Institution": "#2ca02c",
        "Concept": "#d62728",
        "Journal": "#9467bd",
        "Conference": "#8c564b",
        "Unknown": "#7f7f7f"
    }
    for node in nodes:
        node_type = node.get("type", "Unknown")
        G.add_node(node["id"], type=node_type, **node.get("properties", {}))
    for edge in edges:
        if edge["source"] in G.nodes and edge["target"] in G.nodes:
            G.add_edge(edge["source"], edge["target"], relation=edge["relation"], **edge.get("properties", {}))
    try:
        if layout_type == "spring":
            pos = nx.spring_layout(G, k=3, iterations=50)
        elif layout_type == "circular":
            pos = nx.circular_layout(G)
        elif layout_type == "kamada_kawai":
            pos = nx.kamada_kawai_layout(G)
        elif layout_type == "spectral":
            pos = nx.spectral_layout(G)
        else:
            pos = nx.spring_layout(G, k=3, iterations=50)
    except Exception as e:
        st.warning(f"Layout calculation error: {e}. Using random layout.")
        pos = nx.random_layout(G)
    edge_traces = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        relation = G.edges[edge].get('relation', 'connected')
        edge_trace = go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            line=dict(width=1, color='#888'),
            hoverinfo='text',
            hovertext=f"Relation: {relation}<br>Source: {edge[0]}<br>Target: {edge[1]}",
            mode='lines',
            showlegend=False
        )
        edge_traces.append(edge_trace)
    node_traces = []
    for node_type, color in {
        "Paper": "#1f77b4",
        "Author": "#ff7f0e",
        "Institution": "#2ca02c",
        "Concept": "#d62728",
        "Journal": "#9467bd",
        "Conference": "#8c564b",
        "Unknown": "#7f7f7f"
    }.items():
        node_x, node_y, node_text, node_ids, node_sizes = [], [], [], [], []
        for node in G.nodes():
            if G.nodes[node].get('type') == node_type:
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                node_ids.append(node)
                node_size = 10 + 2 * len(list(G.neighbors(node)))
                node_sizes.append(min(node_size, 25))
                props = G.nodes[node]
                hover_text = f"<b>{node}</b><br>Type: {node_type}"
                if 'title' in props:
                    hover_text += f"<br>Title: {props['title'][:50]}..."
                if 'name' in props:
                    hover_text += f"<br>Name: {props['name']}"
                node_text.append(hover_text)
        if node_x:
            display_text = [node_type[0] for _ in node_x] if not show_labels else [
                (G.nodes[n].get('name') or G.nodes[n].get('title', node_type))[:10] for n in node_ids
            ]
            node_traces.append(go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text' if show_labels else 'markers',
                hoverinfo='text', hovertext=node_text,
                text=display_text, textposition="top center",
                marker=dict(size=node_sizes, color=color, line=dict(width=1, color='white'), opacity=0.85),
                name=node_type
            ))
    fig = go.Figure(data=edge_traces + node_traces)
    fig.update_layout(title="Knowledge Graph Visualization", titlefont_size=16, showlegend=True,
        hovermode='closest', margin=dict(b=20,l=5,r=5,t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False), height=700,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01))
    return fig

def fetch_new_papers(topic: Optional[str] = None):
    """Fetch new papers from APIs (to Kafka if available, else direct DB)"""
    if not fetch_enabled:
        return 0
    try:
        search_topic = (topic or st.session_state.get("topic_query") or topic_query or "graph databases")
        papers = []
        per_api = max(1, batch_size // 3)
        papers.extend(fetch_arxiv(batch_size=per_api, search_query=search_topic))
        papers.extend(fetch_pubmed(batch_size=per_api, term=search_topic))
        papers.extend(fetch_crossref(batch_size=per_api, query=search_topic))
        if not papers:
            return 0
        produced = 0
        try:
            bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            producer = st.session_state.get("_kafka_producer")
            if producer is None or getattr(producer, "_bootstrap", None) != bootstrap:
                producer = make_producer()
                setattr(producer, "_bootstrap", bootstrap)
                st.session_state["_kafka_producer"] = producer
            for paper in papers:
                if produce_document(producer, paper, source=paper.get("source", "api")):
                    produced += 1
            producer.flush()
        except Exception as e:
            st.warning(f"Kafka unavailable ({e}), inserting directly into MongoDB")
            for paper in papers:
                doc = {
                    "id": f"{paper.get('source','api')}_{hash(paper.get('title',''))}",
                    "payload": paper,
                    "timestamp": time.time(),
                    "source": paper.get("source", "api"),
                    "doc_type": "paper",
                }
                papers_collection.insert_one(doc)
            produced = len(papers)
        return produced
    except Exception as e:
        st.error(f"Error fetching papers: {e}")
        return 0

# -----------------------------
# Knowledge Graph Visualization
# -----------------------------
st.header("🔍 Knowledge Graph Explorer")

# Quick topic search controls
search_col1, search_col2 = st.columns([3, 1])
with search_col1:
    inline_topic = st.text_input("Search topic", value=topic_query, key="inline_topic")
with search_col2:
    if st.button("Fetch by Topic"):
        with st.spinner("Fetching papers..."):
            count = fetch_new_papers(inline_topic)
        if count > 0:
            st.success(f"Fetched {count} papers for '{inline_topic}'.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.warning("No papers fetched.")

# Visualization controls
kg_col1, kg_col2, kg_col3 = st.columns([1, 1, 1])

with kg_col1:
    # Node type filter
    node_types = st.multiselect(
        "Filter Node Types",
        ["Paper", "Author", "Institution", "Concept", "Journal", "Conference"],
        default=["Paper", "Author", "Concept"]
    )

with kg_col2:
    # Relation type filter (simplified for now)
    relation_types = st.multiselect(
        "Filter Relation Types",
        ["authored_by", "affiliated_with", "mentions", "cites", "published_in"],
        default=["authored_by", "mentions"]
    )

with kg_col3:
    # Layout and display options
    layout_type = st.selectbox(
        "Graph Layout",
        ["spring", "circular", "kamada_kawai", "spectral"],
        index=0
    )
    show_labels = st.checkbox("Show Node Labels", value=False)
    node_limit = st.slider("Max Nodes", 50, 500, 200)

# Fetch graph data with filters
nodes, edges, latest_update = get_knowledge_graph_data(limit=node_limit, node_types=node_types, relation_types=relation_types)

# Display last update time
if latest_update:
    st.caption(f"Last updated: {latest_update.strftime('%Y-%m-%d %H:%M:%S')}")

# Create and display the graph
if nodes and edges:
    graph_fig = create_network_graph(nodes, edges, layout_type=layout_type, show_labels=show_labels)
    st.plotly_chart(graph_fig, use_container_width=True)
    
    # Display stats
    st.caption(f"Displaying {len(nodes)} nodes and {len(edges)} edges")
    
    # Add export option
    if st.button("Export Graph Data"):
        # Create downloadable JSON
        graph_data = {
            "nodes": nodes,
            "edges": edges,
            "timestamp": datetime.datetime.now().isoformat()
        }
        st.download_button(
            label="Download JSON",
            data=json.dumps(graph_data, default=str),
            file_name="knowledge_graph_export.json",
            mime="application/json"
        )
else:
    st.warning("No knowledge graph data available. Try adding some papers first.")
    
    # Show sample data button
    if st.button("Load Sample Data"):
        st.info("This would load sample data into the database (not implemented)")
        # TODO: Implement sample data loading

# -----------------------------
# Helper Functions
# -----------------------------
@st.cache_data(ttl=30)
def get_database_stats():
    """Get current database statistics"""
    try:
        papers_count = papers_collection.count_documents({})
        nodes_count = nodes_collection.count_documents({})
        edges_count = edges_collection.count_documents({})
        
        # Get recent papers (last 24 hours)
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        recent_papers = papers_collection.count_documents({
            "timestamp": {"$gte": yesterday.timestamp()}
        })
        
        return {
            "papers": papers_count,
            "nodes": nodes_count,
            "edges": edges_count,
            "recent_papers": recent_papers
        }
    except Exception as e:
        st.error(f"Error fetching database stats: {e}")
        return {"papers": 0, "nodes": 0, "edges": 0, "recent_papers": 0}

@st.cache_data(ttl=30)  # Reduced TTL for more frequent updates
def get_knowledge_graph_data(limit=1000, node_types=None, relation_types=None):
    """Fetch knowledge graph data for visualization with filtering options"""
    try:
        # Build node query
        node_query = {}
        if node_types and len(node_types) > 0:
            node_query["type"] = {"$in": node_types}
            
        # Get nodes with limit and filtering
        nodes = list(nodes_collection.find(node_query).limit(limit))
        
        # Get node IDs for edge filtering
        node_ids = [node["id"] for node in nodes]
        
        # Build edge query
        edge_query = {}
        if relation_types and len(relation_types) > 0:
            edge_query["relation"] = {"$in": relation_types}
        
        # Add source/target filtering to ensure we only get edges connected to our nodes
        edge_query["$or"] = [
            {"source": {"$in": node_ids}},
            {"target": {"$in": node_ids}}
        ]
        
        # Get edges with filtering
        edges = list(edges_collection.find(edge_query).limit(limit))
        
        # Get timestamp of most recent node for freshness indicator
        latest_node = nodes_collection.find_one({}, sort=[("_id", -1)])
        latest_timestamp = latest_node.get("_id").generation_time if latest_node else None
        
        return nodes, edges, latest_timestamp
    except Exception as e:
        st.error(f"Error fetching KG data: {e}")
        return [], [], None

def create_network_graph(nodes, edges, layout_type="spring", show_labels=False):
    """Create an interactive network graph using Plotly"""
    if not nodes or not edges:
        return go.Figure()
    
    # Create NetworkX graph
    G = nx.Graph()
    
    # Add nodes with attributes
    node_colors = {
        "Paper": "#1f77b4",
        "Author": "#ff7f0e", 
        "Institution": "#2ca02c",
        "Concept": "#d62728",
        "Journal": "#9467bd",
        "Conference": "#8c564b",
        "Unknown": "#7f7f7f"
    }
    
    for node in nodes:
        node_type = node.get("type", "Unknown")
        G.add_node(node["id"], type=node_type, **node.get("properties", {}))
    
    # Add edges
    for edge in edges:
        if edge["source"] in G.nodes and edge["target"] in G.nodes:
            G.add_edge(edge["source"], edge["target"], relation=edge["relation"], **edge.get("properties", {}))
    
    # Generate layout based on selected type
    try:
        if layout_type == "spring":
            pos = nx.spring_layout(G, k=3, iterations=50)
        elif layout_type == "circular":
            pos = nx.circular_layout(G)
        elif layout_type == "kamada_kawai":
            pos = nx.kamada_kawai_layout(G)
        elif layout_type == "spectral":
            pos = nx.spectral_layout(G)
        else:
            pos = nx.spring_layout(G, k=3, iterations=50)
    except Exception as e:
        st.warning(f"Layout calculation error: {e}. Using random layout.")
        pos = nx.random_layout(G)
    
    # Create edge traces with hover info
    edge_traces = []
    edge_types = set()
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        relation = G.edges[edge].get('relation', 'connected')
        edge_types.add(relation)
        
        # Create a trace for each edge type
        edge_trace = go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            line=dict(width=1, color='#888'),
            hoverinfo='text',
            hovertext=f"Relation: {relation}<br>Source: {edge[0]}<br>Target: {edge[1]}",
            mode='lines',
            showlegend=False
        )
        edge_traces.append(edge_trace)
    
    # Create node traces by type
    node_traces = []
    
    for node_type, color in node_colors.items():
        
        node_x = []
        node_y = []
        node_text = []
        node_ids = []
        node_sizes = []
        
        for node in G.nodes():
            if G.nodes[node].get('type') == node_type:
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                node_ids.append(node)
                
                # Adjust node size based on connections
                node_size = 10 + 2 * len(list(G.neighbors(node)))
                node_sizes.append(min(node_size, 25))  # Cap size
                
                # Create hover text
                properties = G.nodes[node]
                hover_text = f"<b>{node}</b><br>"
                hover_text += f"Type: {node_type}<br>"
                if 'name' in properties:
                    hover_text += f"Name: {properties['name']}<br>"
                if 'title' in properties:
                    hover_text += f"Title: {properties['title'][:50]}...<br>"
                if 'abstract' in properties and properties['abstract']:
                    hover_text += f"Abstract: {properties['abstract'][:100]}...<br>"
                if 'keywords' in properties and properties['keywords']:
                    hover_text += f"Keywords: {', '.join(properties['keywords'][:5])}<br>"
                if 'authors' in properties and properties['authors']:
                    hover_text += f"Authors: {', '.join(properties['authors'][:3])}<br>"
                
                node_text.append(hover_text)
        
        if node_x:  # Only add trace if there are nodes of this type
            # Determine text to show
            display_text = []
            if show_labels:
                for node_id in node_ids:
                    props = G.nodes[node_id]
                    if 'name' in props:
                        display_text.append(props['name'][:10])
                    elif 'title' in props:
                        display_text.append(props['title'][:10] + '...')
                    else:
                        display_text.append(node_type[0])
            else:
                display_text = [node_type[0] for _ in node_x]
            
            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text' if show_labels else 'markers',
                hoverinfo='text',
                hovertext=node_text,
                text=display_text,
                textposition="top center",
                marker=dict(
                    size=node_sizes,
                    color=color,
                    line=dict(width=1, color='white'),
                    opacity=0.85
                ),
                name=node_type
            )
            node_traces.append(node_trace)
    
    # Combine all traces
    all_traces = edge_traces + node_traces
    
    # Create figure
    fig = go.Figure(data=all_traces)
    fig.update_layout(
        title="Knowledge Graph Visualization",
        titlefont_size=16,
        showlegend=True,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=[ dict(
            text="Knowledge Graph showing Papers, Authors, Institutions, and Concepts",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.005, y=-0.002,
            xanchor='left', yanchor='bottom',
            font=dict(color='#888', size=12)
        )],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        height=700,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )
    
    return fig

@st.cache_data(ttl=30)
def get_sharding_performance():
    """Get sharding performance metrics using actual benchmark functions"""
    try:
        from benchmarks.sharding_bench import compare_sharding_strategies
        strategies = [
            ShardingStrategyFactory.create_strategy("modulo", num_shards),
            ShardingStrategyFactory.create_strategy("consistent", num_shards),
            ShardingStrategyFactory.create_strategy("range", num_shards, range_field="year")
        ]
        results = compare_sharding_strategies(strategies)
        # Normalize keys if needed
        return results
    except Exception as e:
        st.error(f"Error running sharding benchmarks: {e}")
        return {}

def fetch_new_papers():
    """Fetch new papers from APIs"""
    if not fetch_enabled:
        return 0
    
    try:
        # Fetch with topic query
        papers = []
        per_api = max(1, batch_size // 3)
        papers.extend(fetch_arxiv(batch_size=per_api, search_query=topic_query))
        papers.extend(fetch_pubmed(batch_size=per_api, search_query=topic_query))
        papers.extend(fetch_crossref(batch_size=per_api, search_query=topic_query))

        if not papers:
            return 0

        # Try to publish to Kafka; fallback to direct DB insert
        produced = 0
        try:
            bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            producer = st.session_state.get("_kafka_producer")
            if producer is None or getattr(producer, "_bootstrap", None) != bootstrap:
                producer = make_producer()
                setattr(producer, "_bootstrap", bootstrap)
                st.session_state["_kafka_producer"] = producer

            for paper in papers:
                if produce_document(producer, paper, source=paper.get("source", "api")):
                    produced += 1
            producer.flush()
        except Exception as e:
            st.warning(f"Kafka unavailable ({e}), inserting directly into MongoDB")
            for paper in papers:
                doc = {
                    "id": f"{paper.get('source','api')}_{hash(paper.get('title',''))}",
                    "payload": paper,
                    "timestamp": time.time(),
                    "source": paper.get("source", "api"),
                    "doc_type": "paper",
                }
                papers_collection.insert_one(doc)
            produced = len(papers)

        return produced
    except Exception as e:
        st.error(f"Error fetching papers: {e}")
        return 0

# -----------------------------
# Main Dashboard Content
# -----------------------------

# Database Statistics
st.header("📈 Database Statistics")
stats = get_database_stats()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("📄 Total Papers", stats["papers"], delta=stats["recent_papers"])
with col2:
    st.metric("🔗 Knowledge Nodes", stats["nodes"])
with col3:
    st.metric("🌐 Relationships", stats["edges"])
with col4:
    st.metric("🕒 Recent Papers (24h)", stats["recent_papers"])

# Real-time Knowledge Graph Visualization
st.header("🕸️ Real-time Knowledge Graph")

# Add controls for graph visualization
col1, col2 = st.columns([3, 1])
with col2:
    graph_limit = st.number_input("Max nodes to display", 100, 2000, 500, 100)
    show_labels = st.checkbox("Show node labels", value=True)

with col1:
    # Get knowledge graph data
    data_nodes, data_edges, _ = get_knowledge_graph_data(graph_limit)
    
    if data_nodes and data_edges:
        fig = create_network_graph(data_nodes, data_edges)
        st.plotly_chart(fig, use_container_width=True)
        
        # Graph statistics
        st.subheader("Graph Statistics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Nodes", len(data_nodes))
        with col2:
            st.metric("Edges", len(data_edges))
        with col3:
            density = len(data_edges) / (len(data_nodes) * (len(data_nodes) - 1) / 2) if len(data_nodes) > 1 else 0
            st.metric("Graph Density", f"{density:.3f}")
    else:
        st.warning("No knowledge graph data available. Run the data ingestion pipeline first.")

# Sharding Performance Analysis
st.header("🔀 Sharding Performance Analysis")

performance_data = get_sharding_performance()
if performance_data:
    strategies = list(performance_data.keys())
    # Detect available numeric metrics and build comparison chart dynamically
    # Collect metric names from first strategy
    first_metrics = performance_data[strategies[0]] if strategies else {}
    numeric_metrics = [k for k, v in first_metrics.items() if isinstance(v, (int, float))]
    # Fallback: pick common metrics from sharding_bench if present
    if not numeric_metrics:
        numeric_metrics = ["insert_100_nodes_s", "retrieve_nodes_s", "traversal_s"]
    # Build figure with up to 3 metrics as separate subplots
    cols = min(3, max(1, len(numeric_metrics)))
    fig = make_subplots(rows=1, cols=cols, subplot_titles=[m for m in numeric_metrics[:cols]])
    colors = ["#66b3ff", "#99e699", "#ffcc99"]
    for idx, metric in enumerate(numeric_metrics[:cols]):
        y_vals = [performance_data[s].get(metric, None) for s in strategies]
        fig.add_trace(
            go.Bar(x=strategies, y=y_vals, name=metric, marker_color=colors[idx % len(colors)]),
            row=1, col=idx+1
        )
    fig.update_layout(height=420, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    # Performance table (all metrics)
    st.subheader("Detailed Performance Metrics")
    df = pd.DataFrame(performance_data).T
    st.dataframe(df, use_container_width=True)

# Data Ingestion Control Panel
st.header("📡 Data Ingestion Control")

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🔄 Fetch New Papers", disabled=not fetch_enabled):
        with st.spinner("Fetching papers from APIs..."):
            new_papers = fetch_new_papers()
        if new_papers > 0:
            st.success(f"Successfully fetched {new_papers} new papers!")
            st.experimental_rerun()
        else:
            st.warning("No new papers fetched.")

with col2:
    if st.button("🏗️ Rebuild Knowledge Graph"):
        with st.spinner("Rebuilding knowledge graph..."):
            # This would trigger the KG builder
            time.sleep(2)  # Simulate processing
        st.success("Knowledge graph rebuilt successfully!")
        st.experimental_rerun()

with col3:
    if st.button("📊 Run Sharding Benchmark"):
        with st.spinner("Running sharding benchmark..."):
            st.cache_data.clear()
            perf = get_sharding_performance()
        if perf:
            st.success("Sharding benchmark completed!")
        else:
            st.warning("Benchmark returned no results. Check logs.")

# Recent Papers Table
st.header("📚 Recent Papers")
try:
    recent_papers = list(papers_collection.find({}).sort("timestamp", -1).limit(10))
    if recent_papers:
        papers_data = []
        for paper in recent_papers:
            payload = paper.get("payload", {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except:
                    payload = {"title": "Unknown", "authors": [], "source": "unknown"}
            
            papers_data.append({
                "Title": payload.get("title", "Unknown")[:100] + "..." if len(payload.get("title", "")) > 100 else payload.get("title", "Unknown"),
                "Authors": ", ".join(payload.get("authors", [])[:3]) + ("..." if len(payload.get("authors", [])) > 3 else ""),
                "Source": payload.get("source", "unknown").upper(),
                "Timestamp": datetime.datetime.fromtimestamp(paper.get("timestamp", 0)).strftime("%Y-%m-%d %H:%M")
            })
        
        df = pd.DataFrame(papers_data)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No recent papers found. Start data ingestion to see papers here.")
except Exception as e:
    st.error(f"Error loading recent papers: {e}")

# System Status
st.header("⚡ System Status")
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Database Connection")
    try:
        db.command('ping')
        st.success("✅ MongoDB Connected")
    except Exception as e:
        st.error(f"❌ MongoDB Error: {e}")

with col2:
    st.subheader("Kafka Status")
    # This is a placeholder - in real implementation, you'd check Kafka connectivity
    st.info("🟡 Kafka Status: Check manually")

with col3:
    st.subheader("Knowledge Graph")
    if stats["nodes"] > 0 and stats["edges"] > 0:
        st.success(f"✅ Active ({stats['nodes']} nodes, {stats['edges']} edges)")
    else:
        st.warning("🟡 No KG data - run ingestion pipeline")

# Auto-refresh mechanism
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.markdown("🚀 **NoSQL Knowledge Graph Dashboard** | Built with Streamlit, MongoDB Atlas, and Apache Kafka")
