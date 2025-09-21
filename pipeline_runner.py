# NOSQL/pipeline_runner.py

import threading
import subprocess
import time
import signal
import sys
import logging
from pymongo import MongoClient

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline_runner")

# -----------------------------
# MongoDB setup
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nosql_kg"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
PAPERS_COLLECTION = db.papers

# -----------------------------
# Graceful shutdown
# -----------------------------
_shutdown = False

def _signal_handler(signum, frame):
    global _shutdown
    logger.info("Received signal %s - shutting down...", signum)
    _shutdown = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# -----------------------------
# Function to run a script in a separate thread
# -----------------------------
def run_script(script_path):
    """
    Run a Python script as a subprocess
    """
    logger.info(f"Starting {script_path} ...")
    process = subprocess.Popen([sys.executable, script_path])
    return process

# -----------------------------
# Monitor papers collection and trigger KG builder
# -----------------------------
def monitor_and_build_kg(poll_interval=10):
    """
    Monitor papers collection for new papers and run KG builder.
    """
    last_count = PAPERS_COLLECTION.count_documents({})
    logger.info(f"Initial papers count: {last_count}")

    while not _shutdown:
        time.sleep(poll_interval)
        current_count = PAPERS_COLLECTION.count_documents({})
        if current_count > last_count:
            new_papers = current_count - last_count
            logger.info(f"Detected {new_papers} new papers → running KG builder...")
            # Run KG builder scripts sequentially
            subprocess.run([sys.executable, "kg_builder/kg_builder.py"])
            subprocess.run([sys.executable, "kg_builder/kg_edge_builder.py"])
            last_count = current_count

# -----------------------------
# Main pipeline
# -----------------------------
def main():
    # 1️⃣ Start Kafka producer in background thread
    producer_thread = threading.Thread(target=run_script, args=("ingestion/kafka_producer.py",), daemon=True)
    producer_thread.start()

    # 2️⃣ Start Kafka consumer (data_normalizer)
    consumer_thread = threading.Thread(target=run_script, args=("ingestion/kafka_mongo_consumer.py",), daemon=True)
    consumer_thread.start()

    # 3️⃣ Monitor papers and run KG builder automatically
    try:
        monitor_and_build_kg()
    finally:
        logger.info("Shutting down pipeline runner...")

if __name__ == "__main__":
    main()
