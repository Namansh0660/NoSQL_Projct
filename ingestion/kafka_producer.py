"""
NOSQL/ingestion/kafka_producer.py

Enhanced Kafka producer:
- Fetch papers from ArXiv, PubMed, CrossRef in batches
- Deduplicate and publish continuously
- Reuses robust MessageEnvelope & Kafka producer logic
"""

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, asdict
from hashlib import sha256
from typing import Any, Dict, Optional

from kafka_api_fetcher import fetch_arxiv, fetch_pubmed, fetch_crossref
from kafka import KafkaProducer
from kafka.errors import KafkaError

# -------------------------
# Configuration (can be moved into .env)
# -------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_papers")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "nosql_producer_v1")
RETRIES = int(os.getenv("KAFKA_RETRIES", "5"))
ACKS = os.getenv("KAFKA_ACKS", "all")
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "20"))
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "10"))  # 10 papers per batch
DELAY_SEC = int(os.getenv("KAFKA_DELAY_SEC", "10"))    # delay between batches

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("nosql.kafka_producer")

# -------------------------
# Message envelope
# -------------------------
@dataclass
class MessageEnvelope:
    id: str
    source: str
    timestamp: float
    checksum: str
    provenance: Dict[str, Any]
    payload: Any
    doc_type: str = "paper"

    def to_json(self) -> str:
        obj = asdict(self)
        try:
            return json.dumps(obj, default=_json_serializer, ensure_ascii=False)
        except (TypeError, ValueError):
            obj['payload'] = _safe_serialize(self.payload)
            return json.dumps(obj, default=_json_serializer, ensure_ascii=False)

def _json_serializer(obj):
    try:
        return str(obj)
    except Exception:
        return None

def _safe_serialize(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (dict, list, str, int, float, bool)):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        return {"__bytes_hex__": payload.hex()}
    try:
        return str(payload)
    except Exception:
        return {"__repr__": repr(payload)}

def compute_checksum_bytes(data: bytes) -> str:
    return sha256(data).hexdigest()

def compute_checksum_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()

def deterministic_id(*parts: str) -> str:
    combined = "|".join(parts)
    return sha256(combined.encode("utf-8")).hexdigest()

# -------------------------
# Kafka producer
# -------------------------
def make_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=CLIENT_ID,
        retries=RETRIES,
        acks=ACKS,
        linger_ms=LINGER_MS,
        batch_size=16384,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        max_request_size=10 * 1024 * 1024,
    )
    logger.info("KafkaProducer created - bootstrap_servers=%s client_id=%s", BOOTSTRAP_SERVERS, CLIENT_ID)
    return producer

# -------------------------
# Publish envelope
# -------------------------
def publish_envelope(producer: KafkaProducer, envelope: MessageEnvelope, topic: str = DEFAULT_TOPIC, key: Optional[str] = None, timeout: int = 10) -> bool:
    raw = envelope.to_json()
    key = key or envelope.id
    attempt = 0
    backoff = 0.5
    while attempt <= RETRIES:
        attempt += 1
        try:
            result = producer.send(topic, key=key, value=raw).get(timeout=timeout)
            logger.info("Published message id=%s topic=%s partition=%s offset=%s", envelope.id, topic, result.topic, result.offset)
            return True
        except KafkaError as e:
            logger.error("Kafka send failed attempt %d/%d: %s", attempt, RETRIES, e)
            time.sleep(min(backoff, 10))
            backoff *= 2
        except Exception as e:
            logger.exception("Unexpected exception while sending: %s", e)
            time.sleep(min(backoff, 10))
            backoff *= 2
    logger.error("Failed to publish message id=%s after %d attempts", envelope.id, RETRIES)
    return False

# -------------------------
# Graceful shutdown
# -------------------------
_shutdown = False
def _signal_handler(signum, frame):
    global _shutdown
    logger.info("Received signal %s - graceful shutdown", signum)
    _shutdown = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# -------------------------
# Produce document helper
# -------------------------
def produce_document(producer: KafkaProducer, payload: Any, source: str = "crawler", provenance: Optional[Dict[str, Any]] = None, topic: str = DEFAULT_TOPIC) -> bool:
    provenance = provenance or {}
    timestamp = time.time()
    if isinstance(payload, (bytes, bytearray)):
        checksum = compute_checksum_bytes(payload)
        doc_id = deterministic_id(source, checksum, str(int(timestamp)))
    elif isinstance(payload, str):
        checksum = compute_checksum_text(payload)
        doc_id = deterministic_id(source, checksum, str(int(timestamp)))
    else:
        serialized = json.dumps(payload, sort_keys=True, default=_json_serializer)
        checksum = compute_checksum_text(serialized)
        doc_id = deterministic_id(source, checksum, str(int(timestamp)))

    envelope = MessageEnvelope(id=doc_id, source=source, timestamp=timestamp, checksum=checksum, provenance=provenance, payload=payload)
    return publish_envelope(producer, envelope, topic=topic, key=envelope.id)

# -------------------------
# Main continuous loop
# -------------------------
def main():
    producer = make_producer()
    seen_ids = set()

    logger.info("Starting continuous API fetch & Kafka publishing loop...")

    try:
        while not _shutdown:
            # Fetch batches
            papers = fetch_arxiv(BATCH_SIZE) + fetch_pubmed(BATCH_SIZE) + fetch_crossref(BATCH_SIZE)
            unique_papers = []
            for p in papers:
                key = p.get("doi") or p.get("fetch_url") or p.get("title")
                if key and key not in seen_ids:
                    unique_papers.append(p)
                    seen_ids.add(key)

            for paper in unique_papers:
                produce_document(producer, paper, source=paper.get("source", "api"), provenance={"fetch_url": paper.get("fetch_url")})

            producer.flush()
            time.sleep(DELAY_SEC)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received - shutting down...")

    finally:
        logger.info("Flushing and closing Kafka producer...")
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        except Exception as e:
            logger.exception("Exception closing producer: %s", e)
        logger.info("Producer closed. Exiting.")

if __name__ == "__main__":
    main()
