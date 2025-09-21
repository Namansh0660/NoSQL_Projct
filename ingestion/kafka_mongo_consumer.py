"""
Kafka → MongoDB consumer for NOSQL KG project

Features:
- Routes messages into different collections based on `doc_type`:
    - "paper"  -> papers
    - "pdf"    -> pdfs
    - "binary" -> binaries
- Deduplicates documents by `id` or `payload` checksum
- Merges metadata for existing entries
- Prepares collection for Knowledge Graph ingestion
- Handles edge cases: missing id, unknown doc_type, JSON errors
"""

import json
import logging
import sys
import hashlib
from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne

# -----------------------------
# Setup logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_mongo_consumer")

# -----------------------------
# MongoDB connection
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Map doc_type -> collection
DOC_TYPE_COLLECTION_MAP = {
    "paper": db.papers,
    "pdf": db.pdfs,
    "binary": db.binaries,
}

DEFAULT_COLLECTION = db.papers  # fallback

# -----------------------------
# Kafka Consumer config
# -----------------------------
TOPIC = "raw_papers"
BOOTSTRAP_SERVERS = ["localhost:9092"]
GROUP_ID = "nosql_consumer_group"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id=GROUP_ID,
)

logger.info(f"🚀 Listening to Kafka topic '{TOPIC}'...")

# -----------------------------
# Helper: Generate checksum
# -----------------------------
def compute_checksum(payload: str) -> str:
    """Return SHA256 checksum of the payload"""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest() if payload else None

# -----------------------------
# Helper: Upsert document with deduplication
# -----------------------------
def upsert_document(collection, doc: dict):
    """
    Insert or update document in MongoDB collection.
    Deduplicate by 'id' or checksum if available.
    Merge metadata if document already exists.
    """
    doc_id = doc.get("id")
    payload = doc.get("payload", "")
    checksum = doc.get("checksum") or compute_checksum(payload)

    if not doc_id:
        logger.warning(f"Skipping document without 'id': {doc}")
        return

    # Add checksum to doc
    doc["checksum"] = checksum

    # Prepare update
    update_doc = {
        "$set": doc
    }

    try:
        collection.update_one(
            {"id": doc_id},
            update_doc,
            upsert=True
        )
        logger.info(f"✅ Stored/updated doc_type='{doc.get('doc_type')}' id={doc_id} in collection '{collection.name}'")
    except Exception as e:
        logger.error(f"❌ Failed to store document {doc_id}: {e}")

# -----------------------------
# Consumption loop
# -----------------------------
for message in consumer:
    try:
        doc = message.value

        # Determine collection
        doc_type = doc.get("doc_type", "paper")
        collection = DOC_TYPE_COLLECTION_MAP.get(doc_type, DEFAULT_COLLECTION)

        upsert_document(collection, doc)

    except json.JSONDecodeError as e:
        logger.error(f"⚠️ JSON decode error: {e} - raw message: {message.value}")
    except Exception as e:
        logger.error(f"⚠️ Unexpected error: {e} - skipping message")
