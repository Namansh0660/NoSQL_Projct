"""
Kafka PDF Producer for NOSQL KG

- Takes a PDF file (or folder of PDFs)
- Uses pdf_parser.extract_pdf_text for extraction
- Sends parsed PDF as a JSON message to Kafka topic `raw_papers`
- Includes metadata and provenance
- Handles errors, empty PDFs, encrypted PDFs, and corrupted PDFs
- Auto-creates folder if it doesn't exist
"""

import os
import sys
import json
import time
import logging
from kafka import KafkaProducer
from pdf_parser import extract_pdf_text

# -----------------------------
# Setup logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_pdf_producer")

# -----------------------------
# Kafka Producer setup
# -----------------------------
TOPIC = "raw_papers"
BOOTSTRAP_SERVERS = ["localhost:9092"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# -----------------------------
# Produce PDF message
# -----------------------------
def produce_pdf(file_path: str):
    """
    Parse a single PDF and produce to Kafka
    """
    try:
        result = extract_pdf_text(file_path)
        doc_id = f"pdf_{os.path.basename(file_path)}_{int(time.time())}"

        message = {
            "id": doc_id,
            "source": "pdf_parser",
            "timestamp": time.time(),
            "checksum": None,  # Optional: add file checksum later
            "provenance": result.get("provenance", {}),
            "payload": result.get("text", ""),
            "metadata": result.get("metadata", {}),
            "doc_type": "pdf"
        }

        producer.send(TOPIC, value=message)
        producer.flush()
        logger.info(f"✅ Produced PDF to Kafka: {file_path} (id={doc_id})")

    except Exception as e:
        logger.error(f"❌ Failed to produce PDF {file_path}: {e}")

# -----------------------------
# Produce all PDFs in a folder
# -----------------------------
def produce_folder(folder_path: str):
    """
    Walks a folder and produces all PDF files to Kafka
    """
    # Auto-create folder if missing
    if not os.path.exists(folder_path):
        logger.warning(f"Folder does not exist, creating: {folder_path}")
        os.makedirs(folder_path)
        logger.info(f"Folder created. Please place your PDFs inside: {folder_path}")
        return  # Exit, user should add PDFs

    for root, _, files in os.walk(folder_path):
        for f in files:
            if f.lower().endswith(".pdf"):
                file_path = os.path.join(root, f)
                produce_pdf(file_path)

# -----------------------------
# CLI usage
# -----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Produce PDF(s) to Kafka topic 'raw_papers'.")
    parser.add_argument("--file", help="Single PDF file path", type=str)
    parser.add_argument("--folder", help="Folder containing PDFs", type=str)

    args = parser.parse_args()

    if args.file:
        produce_pdf(args.file)
    elif args.folder:
        produce_folder(args.folder)
    else:
        logger.error("Please provide --file or --folder")
