# api/db.py
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()
MONGODB_USER = os.getenv("MONGODB_USER")
MONGODB_PASS = os.getenv("MONGODB_PASS")
MONGODB_CLUSTER = os.getenv("MONGODB_CLUSTER")
MONGODB_DB = os.getenv("MONGODB_DB", "NOSQL")  # default to NOSQL if not set

# Validate env
if not all([MONGODB_USER, MONGODB_PASS, MONGODB_CLUSTER]):
    raise ValueError("MongoDB credentials are not fully set in .env!")

# -----------------------------
# Build MongoDB Atlas URI
# -----------------------------
MONGODB_URI = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASS}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority&appName=NOSQL"

# -----------------------------
# Connect to MongoDB Atlas
# -----------------------------
try:
    client = MongoClient(MONGODB_URI, server_api=ServerApi('1'))
    # Test connection
    client.admin.command('ping')
    print("✅ Successfully connected and authenticated to MongoDB Atlas!")
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
