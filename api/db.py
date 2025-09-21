from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

KG_NODES = db.kg_nodes
KG_EDGES = db.kg_edges
