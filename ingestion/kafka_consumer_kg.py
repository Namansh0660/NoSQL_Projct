"""
Kafka → MongoDB Consumer for NOSQL KG (Atlas-ready)
--------------------------------------------------
- Consumes messages from 'raw_papers' Kafka topic
- Normalizes data, builds KG nodes & edges
- Inserts into MongoDB Atlas collections
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging
import sys
import time
from hashlib import sha256
from typing import Any, Dict

from kafka import KafkaConsumer
from api.db import papers_collection, nodes_collection as kg_nodes_collection, edges_collection as kg_edges_collection  # ✅ Atlas collections

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("kafka_consumer_kg")

# -----------------------------
# Kafka Config
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
    if isinstance(payload, str):
        return payload
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, ensure_ascii=False)
    return str(payload)

# -----------------------------
# KG Node & Edge helpers
# -----------------------------
def create_node(node_id: str, node_type: str, properties: Dict) -> Dict:
    return {"id": node_id, "type": node_type, "properties": properties}

def create_edge(source_id: str, target_id: str, relation: str, properties: Dict = None) -> Dict:
    if properties is None:
        properties = {}
    return {"source": source_id, "target": target_id, "relation": relation, "properties": properties}

def upsert_node(node: Dict):
    try:
        kg_nodes_collection.update_one({"id": node["id"]}, {"$set": node}, upsert=True)
    except Exception as e:
        logger.error(f"Failed to upsert node {node['id']}: {e}")

def upsert_edge(edge: Dict):
    try:
        kg_edges_collection.update_one(
            {"source": edge["source"], "target": edge["target"], "relation": edge["relation"]},
            {"$set": edge},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to upsert edge {edge['source']} -> {edge['target']}: {e}")

# -----------------------------
# Process a single paper
# -----------------------------
def process_paper(doc: Dict):
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
            "authors": payload.get("authors", []),
            "institutions": payload.get("institutions", []),
            "keywords": payload.get("keywords", []),
            "metadata": payload.get("metadata", {}),
            "provenance": doc.get("provenance", {}),
        }
    )
    upsert_node(paper_node)

    # --- Authors & edges ---
    authors = payload.get("authors") or []
    for author_name in authors:
        author_id = f"author_{author_name.replace(' ', '_').lower()}"
        author_node = create_node(author_id, "Author", {"name": author_name})
        upsert_node(author_node)
        upsert_edge(create_edge(paper_id, author_id, "authored_by"))

    # --- Institutions ---
    institutions = payload.get("institutions") or []
    for inst in institutions:
        inst_id = f"institution_{inst.replace(' ', '_').lower()}"
        inst_node = create_node(inst_id, "Institution", {"name": inst})
        upsert_node(inst_node)
        for author_name in authors:
            author_id = f"author_{author_name.replace(' ', '_').lower()}"
            upsert_edge(create_edge(author_id, inst_id, "affiliated_with"))

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
    logger.info("Kafka Consumer for NOSQL KG started (Atlas)...")
    for message in consumer:
        try:
            doc = message.value
            process_paper(doc)
            logger.info(f"Processed document id={doc.get('id')}")
        except Exception as e:
            logger.exception(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
