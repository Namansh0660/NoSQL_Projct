from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://nosql_db:nosql_db@nosql.vojsy9y.mongodb.net/?retryWrites=true&w=majority&appName=NOSQL"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("✅ Successfully connected and authenticated!")
except Exception as e:
    print("❌ Connection failed:", e)
