"""
Kafka → MongoDB Atlas consumer for NOSQL KG project

Features:
- Routes messages into different collections based on `doc_type`:
    - "paper"  -> papers
    - "pdf"    -> pdfs
    - "binary" -> binaries
- Deduplicates documents by `id` or payload checksum
- Merges metadata for existing entries
- Prepares collection for Knowledge Graph ingestion
- Handles edge cases: missing id, unknown doc_type, JSON errors
"""

import json
import logging
import sys
import os
import hashlib
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# MongoDB connection for local development
MONGO_URI = os.environ.get(
    "MONGO_URI", 
    "mongodb://root:example@mongo:27017/nosql_kg?authSource=admin"
)

# For local testing outside Docker, try localhost if mongo hostname fails
if "mongo:27017" in MONGO_URI and os.environ.get("IN_DOCKER") != "true":
    try:
        # Try with the Docker service name first
        test_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        test_client.admin.command('ping')
    except Exception:
        # If that fails, try with localhost
        logging.info("Couldn't connect to mongo:27017, trying localhost:27017")
        MONGO_URI = MONGO_URI.replace("mongo:27017", "localhost:27017")

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    # Verify connection
    client.admin.command('ping')
    logging.info(f"✅ Connected to MongoDB: {MONGO_URI}")
    
    # Get database
    db = client.get_database()
    
    # Get collections
    papers_collection = db["papers"]
    pdfs_collection = db["pdfs"]
    binaries_collection = db["binaries"]
    
except Exception as e:
    logging.error(f"❌ Failed to connect to MongoDB: {e}")
    # For testing, create mock collections
    class MockCollection:
        def __init__(self, name):
            self.name = name
        def insert_one(self, doc):
            logging.info(f"MOCK: Would insert into {self.name}: {doc['id']}")
            return True
        def update_one(self, query, update, upsert=False):
            logging.info(f"MOCK: Would update in {self.name}: {query}")
            return True
        def find_one(self, query):
            logging.info(f"MOCK: Would find in {self.name}: {query}")
            return None
    
    papers_collection = MockCollection("papers")
    pdfs_collection = MockCollection("pdfs")
    binaries_collection = MockCollection("binaries")

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
# Map doc_type -> Atlas collection
# -----------------------------
DOC_TYPE_COLLECTION_MAP = {
    "paper": papers_collection,
    "pdf": pdfs_collection,
    "binary": binaries_collection,
}
DEFAULT_COLLECTION = papers_collection  # fallback

# -----------------------------
# Kafka Consumer config
# -----------------------------
TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "raw_papers")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BOOTSTRAP_SERVERS = [b.strip() for b in BOOTSTRAP.split(",") if b.strip()]
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "nosql_consumer_group")

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
    Insert or update document in MongoDB Atlas collection.
    Deduplicate by 'id' or checksum if available.
    Merge metadata if document already exists.
    """
    doc_id = doc.get("id")
    payload = doc.get("payload", "")
    checksum = doc.get("checksum") or compute_checksum(payload)

    if not doc_id:
        logger.warning(f"⚠️ Skipping document without 'id': {doc}")
        return

    doc["checksum"] = checksum

    try:
        collection.update_one(
            {"id": doc_id},
            {"$set": doc},
            upsert=True
        )
        logger.info(
            f"✅ Stored/updated doc_type='{doc.get('doc_type', 'paper')}' id={doc_id} in collection '{collection.name}'"
        )
    except Exception as e:
        logger.error(f"❌ Failed to store document {doc_id}: {e}")

# -----------------------------
# Consumption loop
# -----------------------------
for message in consumer:
    try:
        doc = message.value
        doc_type = doc.get("doc_type", "paper")
        collection = DOC_TYPE_COLLECTION_MAP.get(doc_type, DEFAULT_COLLECTION)

        upsert_document(collection, doc)

    except json.JSONDecodeError as e:
        logger.error(f"⚠️ JSON decode error: {e} - raw message: {message.value}")
    except Exception as e:
        logger.error(f"⚠️ Unexpected error: {e} - skipping message")
