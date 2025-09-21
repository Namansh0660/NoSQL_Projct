"""
KG Edge Builder Script
----------------------
Scans all Paper nodes in the MongoDB KG and builds edges for:
1. authored_by (Paper -> Author)
2. affiliated_with (Author -> Institution) [if institution info exists]
3. mentions/concept (Paper -> Concept) [if concept info exists]

Handles edge cases:
- Creates missing nodes (authors, institutions, concepts)
- Avoids duplicate edges
- Normalizes author names and IDs
"""

from pymongo import MongoClient
import re

# -----------------------------
# MongoDB Connection
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"
KG_NODES = "kg_nodes"
KG_EDGES = "kg_edges"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
nodes_col = db[KG_NODES]
edges_col = db[KG_EDGES]

# -----------------------------
# Utility Functions
# -----------------------------
def normalize_name(name: str) -> str:
    """Normalize names to lowercase and underscores."""
    return re.sub(r'\s+', '_', name.strip().lower())

def create_node(node_id: str, node_type: str, properties: dict):
    """Create node if it does not exist."""
    existing = nodes_col.find_one({"id": node_id})
    if not existing:
        nodes_col.insert_one({
            "id": node_id,
            "type": node_type,
            "properties": properties
        })

def create_edge(source: str, target: str, relation: str, properties: dict = {}):
    """Create edge safely (avoid duplicates)."""
    existing = edges_col.find_one({"source": source, "target": target, "relation": relation})
    if not existing:
        edges_col.insert_one({
            "source": source,
            "target": target,
            "relation": relation,
            "properties": properties
        })

# -----------------------------
# Main Edge Building Logic
# -----------------------------
def build_edges():
    papers = list(nodes_col.find({"type": "Paper"}))
    print(f"[INFO] Found {len(papers)} papers to process.")

    for paper in papers:
        paper_id = paper["id"]
        properties = paper.get("properties", {})

        # -----------------------------
        # 1. Authored_by edges
        # -----------------------------
        authors = properties.get("authors", [])
        if not isinstance(authors, list):
            authors = []
        for author_name in authors:
            author_id = f"author_{normalize_name(author_name)}"
            # Create author node if missing
            create_node(author_id, "Author", {"name": author_name})
            # Create edge: Paper -> Author
            create_edge(paper_id, author_id, "authored_by")

        # -----------------------------
        # 2. Affiliated_with edges (optional)
        # -----------------------------
        institutions = properties.get("institutions", [])
        if isinstance(institutions, list):
            for inst_name in institutions:
                inst_id = f"institution_{normalize_name(inst_name)}"
                create_node(inst_id, "Institution", {"name": inst_name})
                for author_name in authors:
                    author_id = f"author_{normalize_name(author_name)}"
                    create_edge(author_id, inst_id, "affiliated_with")

        # -----------------------------
        # 3. Mentions / Concepts edges (optional)
        # -----------------------------
        concepts = properties.get("concepts", [])
        if isinstance(concepts, list):
            for concept in concepts:
                concept_id = f"concept_{normalize_name(concept)}"
                create_node(concept_id, "Concept", {"name": concept})
                create_edge(paper_id, concept_id, "mentions")

    print("[INFO] Edge building complete!")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    build_edges()
