# api/db.py
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
MONGODB_DB = os.getenv("MONGODB_DB", "NOSQL")  # default to NOSQL if not set

# Allow explicit URI override for local/Atlas
MONGODB_URI = os.getenv("MONGODB_URI")

MONGODB_USER = os.getenv("MONGODB_USER")
MONGODB_PASS = os.getenv("MONGODB_PASS")
MONGODB_CLUSTER = os.getenv("MONGODB_CLUSTER")

# -----------------------------
# Build MongoDB URI with sensible fallbacks
# -----------------------------
if not MONGODB_URI:
    # If Atlas credentials are present, build an Atlas SRV URI
    if all([MONGODB_USER, MONGODB_PASS, MONGODB_CLUSTER]):
        MONGODB_URI = (
            f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASS}@{MONGODB_CLUSTER}/"
            f"?retryWrites=true&w=majority&appName=NOSQL"
        )
    else:
        # Fallback to local MongoDB (use Docker service name by default)
        # This matches docker-compose defaults; replace host with localhost when running outside Docker
        MONGODB_URI = "mongodb://root:example@mongo:27017/nosql_kg?authSource=admin&retryWrites=true&w=majority"

# -----------------------------
# Connect to MongoDB
# -----------------------------
try:
    # Use Server API for Atlas SRV URIs; plain client for mongodb://
    if MONGODB_URI.startswith("mongodb+srv://"):
        client = MongoClient(MONGODB_URI, server_api=ServerApi('1'))
    else:
        client = MongoClient(MONGODB_URI)
    # Test connection
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB! URI=", MONGODB_URI)
except Exception as e:
    print("❌ Connection failed:", e)
    raise e

# -----------------------------
# Database and collections
# -----------------------------
db = client[MONGODB_DB]

papers_collection = db["papers"]
nodes_collection = db["nodes"]
edges_collection = db["edges"]

# Aliases for backward compatibility
KG_PAPERS = papers_collection
KG_NODES = nodes_collection
KG_EDGES = edges_collection
