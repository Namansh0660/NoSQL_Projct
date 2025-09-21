"""
NOSQL/ingestion/kafka_consumer_kg.py

- Kafka consumer for raw_papers topic
- Normalizes data, builds KG nodes & edges, inserts into MongoDB
- Runs continuously for real-time ingestion
"""

import json
import logging
import sys
import time
from hashlib import sha256
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("nosql.kafka_consumer_kg")

# -----------------------------
# MongoDB
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

PAPERS_COLLECTION = db.papers
KG_NODES = db.kg_nodes
KG_EDGES = db.kg_edges

# -----------------------------
# Kafka
# -----------------------------
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw_papers"
GROUP_ID = "nosql_kg_consumer_v1"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id=GROUP_ID,
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# -----------------------------
# Helper functions
# -----------------------------
def deterministic_id(*parts: str) -> str:
    combined = "|".join(parts)
    return sha256(combined.encode("utf-8")).hexdigest()

def _safe_text(payload: Any) -> str:
    """Ensure payload is string for KG"""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, ensure_ascii=False)
    return str(payload)

# -----------------------------
# KG Node & Edge helpers
# -----------------------------
def create_node(node_id: str, node_type: str, properties: dict) -> dict:
    return {"id": node_id, "type": node_type, "properties": properties}

def create_edge(source_id: str, target_id: str, relation: str, properties: dict = None) -> dict:
    if properties is None:
        properties = {}
    return {"source": source_id, "target": target_id, "relation": relation, "properties": properties}

def upsert_node(node: dict):
    try:
        KG_NODES.update_one({"id": node["id"]}, {"$set": node}, upsert=True)
    except Exception as e:
        logger.error(f"Failed to upsert node {node['id']}: {e}")

def upsert_edge(edge: dict):
    try:
        KG_EDGES.update_one(
            {"source": edge["source"], "target": edge["target"], "relation": edge["relation"]},
            {"$set": edge},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to upsert edge {edge['source']} -> {edge['target']}: {e}")

# -----------------------------
# Process a single paper
# -----------------------------
def process_paper(doc: dict):
    payload = doc.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {"text": payload}  # fallback

    paper_id = doc.get("id") or deterministic_id(_safe_text(payload.get("title", "")), str(time.time()))
    
    # --- Paper node ---
    paper_node = create_node(
        node_id=paper_id,
        node_type="Paper",
        properties={
            "title": payload.get("title"),
            "abstract": payload.get("abstract"),
            "doi": payload.get("doi"),
            "metadata": payload.get("metadata", {}),
            "provenance": doc.get("provenance", {}),
        }
    )
    upsert_node(paper_node)

    # --- Authors & edges ---
    authors = payload.get("authors") or []
    for author_name in authors:
        author_id = f"author_{author_name}"
        author_node = create_node(author_id, "Author", {"name": author_name})
        upsert_node(author_node)
        upsert_edge(create_edge(paper_id, author_id, "authored_by"))

    # --- Institutions ---
    institutions = payload.get("institutions") or []
    for inst in institutions:
        inst_id = f"institution_{inst}"
        inst_node = create_node(inst_id, "Institution", {"name": inst})
        upsert_node(inst_node)
        for author_name in authors:
            upsert_edge(create_edge(f"author_{author_name}", inst_id, "affiliated_with"))

    # --- Concepts / keywords ---
    concepts = payload.get("keywords") or []
    for concept in concepts:
        concept_id = f"concept_{concept.lower().replace(' ', '_')}"
        concept_node = create_node(concept_id, "Concept", {"name": concept})
        upsert_node(concept_node)
        upsert_edge(create_edge(paper_id, concept_id, "mentions"))

# -----------------------------
# Main loop
# -----------------------------
def main():
    logger.info("Kafka Consumer for NOSQL KG started...")
    for message in consumer:
        try:
            doc = message.value
            process_paper(doc)
            logger.info(f"Processed document id={doc.get('id')}")
        except Exception as e:
            logger.exception(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
