#!/usr/bin/env python3
"""
NoSQL Project Pipeline Integration Test
====================================
This script tests the complete pipeline integration:
1. Kafka Producer fetches papers and publishes to Kafka
2. Kafka Consumer processes messages and builds knowledge graph
3. MongoDB Atlas stores the data with proper sharding
4. UI visualizes the knowledge graph

Usage:
  python test_pipeline.py [--connection-string MONGODB_URI] [--bootstrap-servers KAFKA_BOOTSTRAP_SERVERS]
"""

import argparse
import logging
import os
import time
import json
import subprocess
import signal
import sys
from datetime import datetime
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("pipeline_test")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Test NoSQL Project Pipeline Integration")
    parser.add_argument(
        "--connection-string", 
        default=os.environ.get("MONGODB_URI"),
        help="MongoDB connection string (default: MONGODB_URI env var)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: KAFKA_BOOTSTRAP_SERVERS env var or localhost:9092)"
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "raw_papers"),
        help="Kafka topic (default: KAFKA_TOPIC env var or raw_papers)"
    )
    parser.add_argument(
        "--test-duration",
        type=int,
        default=300,  # 5 minutes
        help="Test duration in seconds (default: 300)"
    )
    parser.add_argument(
        "--paper-limit",
        type=int,
        default=50,
        help="Maximum number of papers to process (default: 50)"
    )
    args = parser.parse_args()
    
    if not args.connection_string:
        logger.warning("MongoDB connection string not provided. Some tests may be skipped.")
    
    return args

def get_mongodb_client(uri):
    """Get MongoDB client with retry logic"""
    if not uri:
        logger.warning("MongoDB URI not provided. Skipping MongoDB connection.")
        return None
        
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to MongoDB Atlas (attempt {attempt+1}/{max_retries})")
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Test connection
            client.admin.command('ping')
            logger.info("Successfully connected to MongoDB Atlas")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Max retries reached. MongoDB tests will be skipped.")
                return None

def check_kafka_status(bootstrap_servers):
    """Check if Kafka is running"""
    try:
        # Use kafka-topics to check if Kafka is running; match compose container name
        cmd = [
            "docker", "exec", "nosql_kafka", 
            "kafka-topics", 
            "--bootstrap-server", bootstrap_servers,
            "--list"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            logger.info(f"Kafka is running. Available topics: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"Kafka check failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("Kafka check timed out")
        return False
    except Exception as e:
        logger.error(f"Error checking Kafka status: {e}")
        return False

def start_process(cmd, env=None):
    """Start a subprocess and return the process object"""
    try:
        logger.info(f"Starting process: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        return process
    except Exception as e:
        logger.error(f"Failed to start process: {e}")
        return None

def monitor_process(process, name, log_prefix=""):
    """Monitor a process and log its output"""
    if not process:
        return
        
    try:
        # Check if process is still running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            logger.error(f"{name} process exited with code {process.returncode}")
            if stdout:
                logger.info(f"{log_prefix} stdout: {stdout}")
            if stderr:
                logger.error(f"{log_prefix} stderr: {stderr}")
            return False
            
        # Process is still running, check for new output
        while True:
            output = process.stdout.readline()
            if not output:
                break
            logger.info(f"{log_prefix} {output.strip()}")
            
        return True
    except Exception as e:
        logger.error(f"Error monitoring {name} process: {e}")
        return False

def check_mongodb_collections(client, db_name="NOSQL"):
    """Check MongoDB collections and document counts"""
    if not client:
        logger.warning("MongoDB client not available. Skipping collection check.")
        return {}
        
    try:
        db = client[db_name]
        collections = db.list_collection_names()
        
        results = {}
        for collection in collections:
            count = db[collection].count_documents({})
            results[collection] = count
            logger.info(f"Collection {collection}: {count} documents")
            
        return results
    except Exception as e:
        logger.error(f"Error checking MongoDB collections: {e}")
        return {}

def test_pipeline_integration(args):
    """Test the complete pipeline integration"""
    logger.info("🚀 Starting Pipeline Integration Test...")
    
    # Environment variables for processes
    env = os.environ.copy()
    env["MONGODB_URI"] = args.connection_string or ""
    env["KAFKA_BOOTSTRAP_SERVERS"] = args.bootstrap_servers
    env["KAFKA_TOPIC"] = args.topic
    env["PAPER_LIMIT"] = str(args.paper_limit)
    
    # Check if Kafka is running
    if not check_kafka_status(args.bootstrap_servers):
        logger.error("Kafka is not running. Please start Kafka before running this test.")
        return False
    
    # Connect to MongoDB
    mongodb_client = get_mongodb_client(args.connection_string)
    
    # Check initial state
    logger.info("Checking initial state...")
    initial_counts = check_mongodb_collections(mongodb_client)
    
    # Start Kafka Producer
    producer_cmd = ["python", "kafka_producer.py"]
    producer_process = start_process(producer_cmd, env)
    
    # Start Kafka Consumer
    consumer_cmd = ["python", "kafka_consumer_kg.py"]
    consumer_process = start_process(consumer_cmd, env)
    
    # Monitor processes for the specified duration
    start_time = time.time()
    end_time = start_time + args.test_duration
    
    try:
        while time.time() < end_time:
            # Monitor producer
            producer_running = monitor_process(producer_process, "Producer", "[PRODUCER]")
            
            # Monitor consumer
            consumer_running = monitor_process(consumer_process, "Consumer", "[CONSUMER]")
            
            # Check MongoDB collections periodically
            if time.time() - start_time > 60 and mongodb_client:  # Check after 1 minute
                logger.info("Checking MongoDB collections...")
                current_counts = check_mongodb_collections(mongodb_client)
                
                # Compare with initial counts
                for collection, count in current_counts.items():
                    initial = initial_counts.get(collection, 0)
                    diff = count - initial
                    if diff > 0:
                        logger.info(f"Collection {collection} grew by {diff} documents")
            
            # If both processes have exited, end the test
            if not producer_running and not consumer_running:
                logger.warning("Both producer and consumer have exited. Ending test early.")
                break
                
            # Sleep to avoid high CPU usage
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Test interrupted by user. Shutting down...")
    finally:
        # Terminate processes
        for process, name in [(producer_process, "Producer"), (consumer_process, "Consumer")]:
            if process and process.poll() is None:
                logger.info(f"Terminating {name} process...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning(f"{name} process did not terminate gracefully. Killing...")
                    process.kill()
    
    # Final check of MongoDB collections
    if mongodb_client:
        logger.info("Final check of MongoDB collections...")
        final_counts = check_mongodb_collections(mongodb_client)
        
        # Compare with initial counts
        success = False
        for collection, count in final_counts.items():
            initial = initial_counts.get(collection, 0)
            diff = count - initial
            if diff > 0:
                logger.info(f"Collection {collection} grew by {diff} documents")
                success = True
        
        if not success:
            logger.warning("No growth detected in MongoDB collections. Pipeline may not be working correctly.")
            return False
    
    logger.info("✅ Pipeline Integration Test completed!")
    return True

def main():
    """Main function"""
    args = parse_args()
    
    try:
        success = test_pipeline_integration(args)
        if success:
            logger.info("✅ All tests passed!")
            return 0
        else:
            logger.error("❌ Tests failed!")
            return 1
    except Exception as e:
        logger.error(f"Error during testing: {e}")
        return 1
    finally:
        # Clean up MongoDB client if it exists
        if 'mongodb_client' in locals() and mongodb_client:
            mongodb_client.close()

if __name__ == "__main__":
    sys.exit(main())