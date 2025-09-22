# 📚 NoSQL Knowledge Graph Pipeline

A full-stack **NoSQL Knowledge Graph (KG) system** for academic papers, built using **MongoDB Atlas**, **Kafka**, **FastAPI**, and Python.
This project ingests papers, normalizes data, builds KG nodes & edges, and provides API endpoints for querying and traversing the KG.

---

## **Table of Contents**

1. [Project Structure](#project-structure)
2. [Environment Setup](#environment-setup)
3. [MongoDB Atlas Setup](#mongodb-atlas-setup)
4. [Kafka Setup](#kafka-setup)
5. [Running the Pipeline](#running-the-pipeline)
6. [API](#api)
7. [Sharding Notes](#sharding-notes)
8. [Troubleshooting](#troubleshooting)

---

## **Project Structure**

```
NOSQL/
├── api/                       # FastAPI backend
│   ├── routes/                # API routes
│   │   ├── nodes.py
│   │   ├── edges.py
│   │── search.py
│   │── search_embeddings.py
│   │── traverse.py
│   ├── db.py                  # MongoDB connection
│   ├── main.py                # FastAPI app entry
│   └── models.py              # Pydantic models
├── ingestion/                 # Data ingestion and Kafka
│   ├── pdf_parser.py
│   ├── data_normalizer.py
│   ├── kafka_producer.py
│   ├── kafka_pdf_producer.py
│   ├── kafka_consumer_kg.py
│   ├── kafka_mongo_consumer.py
│   └── kafka_api_fetcher.py
├── kg_builder/                # KG builder scripts
│   ├── kg_builder.py
│   └── kg_edge_builder.py
├── mongo-init-scripts/        # Optional MongoDB init scripts
├── nosqlenv/                  # Python virtual environment
├── samples/                   # Example papers / PDFs
├── .env                       # Environment variables
└── docker-compose.yml         # Optional Docker setup
```

---

## **Environment Setup**

1. **Create a virtual environment**

```bash
python -m venv nosqlenv
```

2. **Activate the environment**

```bash
# Windows
nosqlenv\Scripts\activate
# Linux/Mac
source nosqlenv/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Configure environment variables** (`.env` file):

```
MONGODB_USER=nosql_db
MONGODB_PASS=nosql_db
MONGODB_CLUSTER=nosql.vojsy9y.mongodb.net
MONGODB_DB=NOSQL
KAFKA_BOOTSTRAP=localhost:9092
```

---

## **MongoDB Atlas Setup**

1. Create a MongoDB Atlas cluster (M2 or higher for sharding).
2. Create a user (`nosql_db`) with **readWrite** permissions on the `NOSQL` database.
3. Whitelist your IP in Atlas network access.
4. Update `.env` with your credentials.

**Test connection:**

```python
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://nosql_db:nosql_db@nosql.vojsy9y.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("✅ Successfully connected and authenticated!")
except Exception as e:
    print("❌ Connection failed:", e)
```

---

## **Kafka Setup**

1. Install Kafka and Zookeeper locally or via Docker.
2. Start Zookeeper:

```bash
zookeeper-server-start.sh config/zookeeper.properties
```

3. Start Kafka broker:

```bash
kafka-server-start.sh config/server.properties
```

4. Create topics:

```bash
kafka-topics.sh --create --topic raw_papers --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

---

## **Running the Pipeline**

### **1. Start the full pipeline**

```bash
python pipeline_runner.py
```

* Starts Kafka producer and consumer threads.
* Monitors MongoDB `papers` collection and triggers KG builder.
* Logs will show ingestion, normalization, and KG updates.

### **2. Verify MongoDB collections**

Collections automatically created:

* `papers`
* `kg_nodes`
* `kg_edges`

---

## **API**

### **Run FastAPI server**

```bash
uvicorn api.main:app --reload
```

* Available at: [http://127.0.0.1:8000](http://127.0.0.1:8000)

### **Example Endpoints**

* **Nodes:** `/nodes`
* **Edges:** `/edges`
* **Search by text:** `/search`
* **Search embeddings:** `/search_embeddings`
* **Traverse KG:** `/traverse`

Test via browser or Postman.

---

## **Sharding Notes**

> ⚠️ Only possible on **M2 or higher clusters**, not free-tier (M0).

* Enable sharding via Atlas UI:

  1. Navigate to **Clusters → Collections → NOSQL → Collection → Shard Collection**.
  2. Choose shard key:

     * `papers`: `"id"` (hashed)
     * `kg_nodes`: `"id"` (hashed)
     * `kg_edges`: `{ "source": 1, "target": 1 }`

---

## **Troubleshooting**

1. **SSL errors connecting to Atlas**

   * Ensure Python OpenSSL >= 3.0.
   * Use correct MongoDB URI format with `mongodb+srv://`.

2. **Authentication errors**

   * Check `.env` credentials match Atlas user.
   * Ensure user has readWrite on `NOSQL` database.

3. **Kafka connection issues**

   * Verify broker is running and topic exists.
   * Check `bootstrap_servers` in `.env`.

4. **Module import errors**

   * Run scripts from the **project root**.

   ```bash
   python -m api.main
   ```

---

## **References**

* [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)
* [Kafka Python Client](https://kafka-python.readthedocs.io/en/master/)
* [FastAPI Documentation](https://fastapi.tiangolo.com/)

---
