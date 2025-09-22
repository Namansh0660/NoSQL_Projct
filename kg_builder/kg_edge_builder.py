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

import re
from api.db import kg_nodes_collection, kg_edges_collection  # ✅ Atlas collections

# -----------------------------
# Utility Functions
# -----------------------------
def normalize_name(name: str) -> str:
    """Normalize names to lowercase and underscores."""
    return re.sub(r'\s+', '_', name.strip().lower())

def create_node(node_id: str, node_type: str, properties: dict):
    """Create node if it does not exist."""
    if not kg_nodes_collection.find_one({"id": node_id}):
        kg_nodes_collection.insert_one({
            "id": node_id,
            "type": node_type,
            "properties": properties
        })

def create_edge(source: str, target: str, relation: str, properties: dict = None):
    """Create edge safely (avoid duplicates)."""
    if properties is None:
        properties = {}
    if not kg_edges_collection.find_one({"source": source, "target": target, "relation": relation}):
        kg_edges_collection.insert_one({
            "source": source,
            "target": target,
            "relation": relation,
            "properties": properties
        })

# -----------------------------
# Main Edge Building Logic
# -----------------------------
def build_edges():
    papers = list(kg_nodes_collection.find({"type": "Paper"}))
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
        concepts = properties.get("concepts", []) or properties.get("keywords", [])
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
