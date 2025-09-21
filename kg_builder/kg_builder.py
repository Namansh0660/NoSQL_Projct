"""
Knowledge Graph Builder for NOSQL KG project

- Converts normalized papers and PDFs into nodes and edges
- Deduplicates nodes and edges
- Stores KG in MongoDB collections: kg_nodes, kg_edges
- Handles edge cases: missing authors, missing papers, duplicate edges
"""

import logging
import sys
from pymongo import MongoClient
import json
# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kg_builder")

# -----------------------------
# MongoDB connection
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

PAPERS_COLLECTION = db.papers
PDFS_COLLECTION = db.pdfs
KG_NODES = db.kg_nodes
KG_EDGES = db.kg_edges

# -----------------------------
# Helper: Generate node dict
# -----------------------------
def create_node(node_id: str, node_type: str, properties: dict):
    return {
        "id": node_id,
        "type": node_type,
        "properties": properties
    }

# -----------------------------
# Helper: Generate edge dict
# -----------------------------
def create_edge(source_id: str, target_id: str, relation: str, properties: dict = None):
    if properties is None:
        properties = {}
    return {
        "source": source_id,
        "target": target_id,
        "relation": relation,
        "properties": properties
    }

# -----------------------------
# Deduplicate and upsert node
# -----------------------------
def upsert_node(node):
    try:
        KG_NODES.update_one(
            {"id": node["id"]},
            {"$set": node},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to upsert node {node['id']}: {e}")

# -----------------------------
# Deduplicate and upsert edge
# -----------------------------
def upsert_edge(edge):
    try:
        KG_EDGES.update_one(
            {
                "source": edge["source"],
                "target": edge["target"],
                "relation": edge["relation"]
            },
            {"$set": edge},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to upsert edge {edge['source']} -> {edge['target']} ({edge['relation']}): {e}")

# -----------------------------
# Build KG from a single paper
# -----------------------------

def process_paper(doc):
    paper_id = doc.get("id")
    if not paper_id:
        logger.warning("Skipping paper with missing id")
        return

    payload = doc.get("payload", {})

    # If payload is a string, try to parse JSON; else keep as text
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            # Treat as raw text for abstract
            payload = {"title": None, "abstract": payload, "doi": None, "authors": []}

    # Now safe to do .get() on payload
    paper_node = create_node(
        node_id=paper_id,
        node_type="Paper",
        properties={
            "title": payload.get("title"),
            "abstract": payload.get("abstract"),
            "doi": payload.get("doi"),
            "metadata": doc.get("metadata", {}),
            "provenance": doc.get("provenance", {})
        }
    )
    upsert_node(paper_node)

    # --- Authors ---
    authors = payload.get("authors", [])
    if not isinstance(authors, list):
        authors = []

    for author_name in authors:
        author_id = f"author_{author_name.replace(' ', '_').lower()}"
        author_node = create_node(
            node_id=author_id,
            node_type="Author",
            properties={"name": author_name}
        )
        upsert_node(author_node)

        edge = create_edge(
            source_id=paper_id,
            target_id=author_id,
            relation="authored_by"
        )
        upsert_edge(edge)

    # --- Institutions ---
    institutions = payload.get("institutions", [])
    if not isinstance(institutions, list):
        institutions = []

    for inst in institutions:
        inst_id = f"institution_{inst.replace(' ', '_').lower()}"
        inst_node = create_node(
            node_id=inst_id,
            node_type="Institution",
            properties={"name": inst}
        )
        upsert_node(inst_node)

        for author_name in authors:
            edge = create_edge(
                source_id=f"author_{author_name.replace(' ', '_').lower()}",
                target_id=inst_id,
                relation="affiliated_with"
            )
            upsert_edge(edge)

    # --- Keywords / concepts ---
    concepts = payload.get("keywords", [])
    if not isinstance(concepts, list):
        concepts = []

    for concept in concepts:
        concept_id = f"concept_{concept.lower().replace(' ', '_')}"
        concept_node = create_node(
            node_id=concept_id,
            node_type="Concept",
            properties={"name": concept}
        )
        upsert_node(concept_node)

        edge = create_edge(
            source_id=paper_id,
            target_id=concept_id,
            relation="mentions"
        )
        upsert_edge(edge)


# -----------------------------
# Main KG Builder
# -----------------------------
def build_kg():
    papers = list(PAPERS_COLLECTION.find({}))
    pdfs = list(PDFS_COLLECTION.find({}))

    logger.info(f"Building KG from {len(papers)} papers and {len(pdfs)} PDFs")

    # Process all papers
    for doc in papers:
        process_paper(doc)

    # Process PDFs (as separate Paper nodes if not already present)
    for doc in pdfs:
        process_paper(doc)

    logger.info("✅ Knowledge Graph construction completed")

# -----------------------------
# Run KG Builder
# -----------------------------
if __name__ == "__main__":
    build_kg()
