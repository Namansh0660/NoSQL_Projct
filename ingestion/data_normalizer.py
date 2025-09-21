"""
Data Normalizer for NOSQL KG ingestion pipeline

- Normalizes authors, DOI, and text
- Deduplicates across PDFs and papers
- Merges multiple sources into a canonical KG-ready document
- Updates MongoDB collections in-place
"""

import re
import logging
import sys
from pymongo import MongoClient
from typing import List, Dict, Any

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("data_normalizer")

# -----------------------------
# MongoDB connection
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

PAPERS_COLLECTION = db.papers
PDFS_COLLECTION = db.pdfs

# -----------------------------
# Helper: Normalize authors
# -----------------------------
def normalize_authors(authors: List[str]) -> List[str]:
    """
    Normalize author names:
    - lowercase
    - strip spaces
    - replace spaces with underscore
    """
    normalized = []
    for a in authors:
        if not a:
            continue
        clean = a.strip().lower().replace(" ", "_")
        normalized.append(clean)
    return list(set(normalized))  # remove duplicates

# -----------------------------
# Helper: Normalize DOI
# -----------------------------
def normalize_doi(doi: str) -> str:
    """
    Normalize DOI string
    - Remove URL prefixes
    - Lowercase
    """
    if not doi:
        return None
    doi = doi.lower().strip()
    doi = re.sub(r"https?://(dx\.)?doi\.org/", "", doi)
    return doi

# -----------------------------
# Helper: Clean text
# -----------------------------
def clean_text(text) -> str:
    """
    Remove non-printable characters, normalize whitespace.
    Safely handles strings, bytes, dicts, lists, or None.
    """
    if not text:
        return ""
    if isinstance(text, dict) or isinstance(text, list):
        # Convert dict/list to JSON string
        import json
        try:
            text = json.dumps(text, ensure_ascii=False)
        except Exception:
            text = str(text)
    elif not isinstance(text, str):
        text = str(text)

    import re
    text = re.sub(r"\s+", " ", text)
    text = ''.join(c for c in text if c.isprintable())
    return text.strip()


# -----------------------------
# Merge metadata
# -----------------------------
def merge_metadata(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge metadata from new document into existing
    """
    merged = existing.copy()
    # Merge authors
    existing_authors = existing.get("authors", [])
    new_authors = new.get("authors", [])
    merged["authors"] = list(set(existing_authors + new_authors))

    # Merge other fields
    for key in ["title", "abstract", "doi", "payload", "metadata", "provenance"]:
        if key in new and new[key]:
            if key in merged and merged[key]:
                # Special handling for payload text: append if different
                if key == "payload" and new[key] not in merged[key]:
                    merged[key] += "\n" + new[key]
                elif key != "payload":
                    merged[key] = new[key]
            else:
                merged[key] = new[key]

    return merged

# -----------------------------
# Normalize and merge a single document
# -----------------------------
def normalize_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize authors, DOI, text, and metadata
    """
    # Normalize authors
    if "authors" in doc:
        doc["authors"] = normalize_authors(doc.get("authors", []))

    # Normalize DOI
    if "doi" in doc:
        doc["doi"] = normalize_doi(doc.get("doi"))

    # Clean payload
    if "payload" in doc:
        doc["payload"] = clean_text(doc.get("payload"))

    # Clean abstract if present
    if "abstract" in doc:
        doc["abstract"] = clean_text(doc.get("abstract"))

    return doc

# -----------------------------
# Normalize entire collection
# -----------------------------
def normalize_collection(collection):
    """
    Normalize and deduplicate documents in a MongoDB collection
    """
    docs = list(collection.find({}))
    logger.info(f"Normalizing {len(docs)} documents in collection '{collection.name}'")

    for doc in docs:
        doc_id = doc.get("id")
        if not doc_id:
            logger.warning(f"Skipping document without id: {doc}")
            continue

        # Normalize document
        normalized_doc = normalize_document(doc)

        # Check for duplicates by DOI if available
        doi = normalized_doc.get("doi")
        if doi:
            existing = collection.find_one({"doi": doi, "id": {"$ne": doc_id}})
            if existing:
                # Merge existing document with current
                merged = merge_metadata(existing, normalized_doc)
                collection.replace_one({"_id": existing["_id"]}, merged)
                # Delete current duplicate
                collection.delete_one({"_id": doc["_id"]})
                logger.info(f"Merged duplicate doc_id={doc_id} with existing DOI={doi}")
                continue

        # Update normalized document in-place
        collection.replace_one({"_id": doc["_id"]}, normalized_doc)
        logger.info(f"Normalized doc_id={doc_id}")

# -----------------------------
# Run normalization on all relevant collections
# -----------------------------
if __name__ == "__main__":
    normalize_collection(PAPERS_COLLECTION)
    normalize_collection(PDFS_COLLECTION)
    logger.info("✅ Normalization and deduplication completed for all collections")
