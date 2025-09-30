#!/usr/bin/env python3
"""
Enable Sharding on MongoDB Atlas Collections
==========================================
This script enables sharding for collections in MongoDB Atlas.
Run this once to set up sharding for the knowledge graph collections.

Requirements:
- MongoDB Atlas cluster (M2+ tier - sharding not available on M0)
- Collections must contain data before enabling sharding
- Proper permissions to run sharding commands

Usage:
  python enable_atlas_sharding.py [--connection-string MONGODB_URI]
"""

import argparse
import logging
import os
import time
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("atlas_sharding")

# Get MongoDB connection string from environment or argument
def get_connection_string():
    parser = argparse.ArgumentParser(description="Enable sharding on MongoDB Atlas")
    parser.add_argument(
        "--connection-string", 
        default=os.environ.get("MONGODB_URI"),
        help="MongoDB connection string (default: MONGODB_URI env var)"
    )
    args = parser.parse_args()
    
    if not args.connection_string:
        raise ValueError("MongoDB connection string not provided. Use --connection-string or set MONGODB_URI env var.")
    
    return args.connection_string

def get_client():
    """Get MongoDB client with retry logic"""
    uri = get_connection_string()
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
            logger.error(f"Failed to connect (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Max retries reached. Exiting.")
                raise

def enable_database_sharding(client, db_name="NOSQL"):
    """Enable sharding for the database"""
    try:
        result = client.admin.command("enableSharding", db_name)
        logger.info(f"✅ Database sharding enabled for {db_name}: {result}")
        return True
    except OperationFailure as e:
        if "already enabled" in str(e):
            logger.info(f"ℹ️ Sharding already enabled for database {db_name}")
            return True
        logger.error(f"❌ Failed to enable database sharding: {e}")
        return False

def shard_collection(client, db_name, coll_name, shard_key, strategy="hashed"):
    """Shard a collection with given shard key"""
    try:
        # Check if collection exists and has data
        db = client[db_name]
        if coll_name not in db.list_collection_names():
            logger.warning(f"⚠️ Collection {coll_name} does not exist. Creating it...")
            db.create_collection(coll_name)
        
        # Enable sharding for the collection
        result = client.admin.command({
            "shardCollection": f"{db_name}.{coll_name}",
            "key": shard_key
        })
        logger.info(f"✅ Collection {coll_name} sharded with key {shard_key}: {result}")
        return True
    except OperationFailure as e:
        if "already sharded" in str(e):
            logger.info(f"ℹ️ Collection {db_name}.{coll_name} is already sharded")
            return True
        logger.error(f"❌ Failed to shard collection {coll_name}: {e}")
        return False

def main():
    """Enable sharding for all collections"""
    logger.info("🚀 Enabling MongoDB Atlas Sharding...")
    
    try:
        client = get_client()
        db_name = "NOSQL"
        
        # Enable database sharding
        if not enable_database_sharding(client, db_name):
            logger.error("Failed to enable database sharding. Exiting.")
            return
        
        # Shard collections with appropriate keys
        collections_to_shard = [
            # Collection name, shard key, description
            ("papers", {"id": "hashed"}, "Papers collection (hashed ID for even distribution)"),
            ("nodes", {"id": "hashed"}, "KG Nodes collection (hashed ID for even distribution)"),
            ("edges", {"source": "hashed"}, "KG Edges collection (hashed source for lookups)"),
            ("papers_zonal", {"zoneKey": 1, "_id": 1}, "Zonal papers collection (for zonal queries)")
        ]
        
        success_count = 0
        for coll_name, shard_key, description in collections_to_shard:
            logger.info(f"Sharding {coll_name}: {description}")
            if shard_collection(client, db_name, coll_name, shard_key):
                success_count += 1
        
        logger.info(f"✅ Sharding setup complete! {success_count}/{len(collections_to_shard)} collections sharded")
        logger.info("\nNext steps:")
        logger.info("1. Check shard status: db.adminCommand({shardingState: 1})")
        logger.info("2. Monitor shard distribution: db.papers.getShardDistribution()")
        logger.info("3. Run benchmarks to compare performance")
        
    except Exception as e:
        logger.error(f"Error during sharding setup: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()
