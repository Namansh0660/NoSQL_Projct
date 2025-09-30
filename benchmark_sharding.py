#!/usr/bin/env python3
"""
MongoDB Atlas Sharding Benchmark
==============================
This script benchmarks different sharding strategies for MongoDB Atlas.
It measures performance for various operations across different sharding configurations.

Usage:
  python benchmark_sharding.py [--connection-string MONGODB_URI]
"""

import argparse
import logging
import os
import time
import random
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("sharding_benchmark")

# Get MongoDB connection string from environment or argument
def get_connection_string():
    parser = argparse.ArgumentParser(description="Benchmark MongoDB Atlas sharding strategies")
    parser.add_argument(
        "--connection-string", 
        default=os.environ.get("MONGODB_URI"),
        help="MongoDB connection string (default: MONGODB_URI env var)"
    )
    parser.add_argument(
        "--output",
        default="benchmark_results.json",
        help="Output file for benchmark results (default: benchmark_results.json)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations for each benchmark (default: 5)"
    )
    args = parser.parse_args()
    
    if not args.connection_string:
        raise ValueError("MongoDB connection string not provided. Use --connection-string or set MONGODB_URI env var.")
    
    return args

def get_client(uri):
    """Get MongoDB client with retry logic"""
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

def benchmark_operation(func, iterations=5):
    """Benchmark a function by running it multiple times and measuring performance"""
    times = []
    for i in range(iterations):
        start_time = time.time()
        result = func()
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
        logger.debug(f"Iteration {i+1}/{iterations}: {execution_time:.4f} seconds")
    
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    
    return {
        "avg_time": avg_time,
        "min_time": min_time,
        "max_time": max_time,
        "times": times
    }

def run_benchmarks(client, db_name, iterations):
    """Run benchmarks for different operations and sharding strategies"""
    db = client[db_name]
    results = {
        "timestamp": datetime.now().isoformat(),
        "database": db_name,
        "benchmarks": {}
    }
    
    # Get collection names
    collections = [
        "papers",
        "nodes",
        "edges",
        "papers_zonal"
    ]
    
    # Benchmark operations for each collection
    for collection_name in collections:
        if collection_name not in db.list_collection_names():
            logger.warning(f"Collection {collection_name} does not exist. Skipping benchmarks.")
            continue
        
        collection = db[collection_name]
        count = collection.count_documents({})
        
        if count == 0:
            logger.warning(f"Collection {collection_name} is empty. Skipping benchmarks.")
            continue
        
        logger.info(f"Benchmarking {collection_name} ({count} documents)...")
        
        # Get a sample document to use for queries
        sample_doc = collection.find_one()
        if not sample_doc:
            continue
        
        # Prepare benchmark operations
        operations = {
            "count_all": lambda: collection.count_documents({}),
            "find_one": lambda: collection.find_one(),
            "find_limit_100": lambda: list(collection.find().limit(100)),
        }
        
        # Add collection-specific operations
        if collection_name == "papers":
            # Find papers by ID
            if "id" in sample_doc:
                paper_id = sample_doc["id"]
                operations["find_by_id"] = lambda: collection.find_one({"id": paper_id})
            
            # Find papers by title (if exists)
            if "title" in sample_doc:
                title_fragment = sample_doc["title"].split()[0] if sample_doc["title"] else ""
                if title_fragment:
                    operations["find_by_title_fragment"] = lambda: list(
                        collection.find({"title": {"$regex": title_fragment, "$options": "i"}}).limit(10)
                    )
        
        elif collection_name == "nodes":
            # Find nodes by type
            if "type" in sample_doc:
                node_type = sample_doc["type"]
                operations["find_by_type"] = lambda: list(
                    collection.find({"type": node_type}).limit(100)
                )
        
        elif collection_name == "edges":
            # Find edges by source
            if "source" in sample_doc:
                source = sample_doc["source"]
                operations["find_by_source"] = lambda: list(
                    collection.find({"source": source}).limit(100)
                )
        
        # Run benchmarks for each operation
        collection_results = {}
        for op_name, op_func in operations.items():
            logger.info(f"  Benchmarking operation: {op_name}")
            try:
                benchmark_result = benchmark_operation(op_func, iterations)
                collection_results[op_name] = benchmark_result
                logger.info(f"    Avg time: {benchmark_result['avg_time']:.4f} seconds")
            except Exception as e:
                logger.error(f"Error benchmarking {op_name}: {e}")
        
        results["benchmarks"][collection_name] = collection_results
    
    return results

def save_results(results, output_file):
    """Save benchmark results to a file"""
    try:
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Benchmark results saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving results: {e}")

def generate_report(results, output_file):
    """Generate a visual report of benchmark results"""
    try:
        # Create a DataFrame for each collection
        dfs = []
        for collection_name, operations in results["benchmarks"].items():
            for op_name, metrics in operations.items():
                df = pd.DataFrame({
                    "Collection": collection_name,
                    "Operation": op_name,
                    "Avg Time (s)": metrics["avg_time"],
                    "Min Time (s)": metrics["min_time"],
                    "Max Time (s)": metrics["max_time"]
                }, index=[0])
                dfs.append(df)
        
        if not dfs:
            logger.warning("No benchmark data to generate report")
            return
        
        # Combine all DataFrames
        df_all = pd.concat(dfs, ignore_index=True)
        
        # Create plots
        plt.figure(figsize=(12, 8))
        
        # Plot average times by collection and operation
        ax = plt.subplot(111)
        df_pivot = df_all.pivot(index="Operation", columns="Collection", values="Avg Time (s)")
        df_pivot.plot(kind="bar", ax=ax)
        
        plt.title("MongoDB Atlas Sharding Benchmark Results")
        plt.ylabel("Average Time (seconds)")
        plt.xlabel("Operation")
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save plot
        report_file = output_file.replace(".json", ".png")
        plt.savefig(report_file)
        logger.info(f"Benchmark report saved to {report_file}")
        
        # Also save as CSV for further analysis
        csv_file = output_file.replace(".json", ".csv")
        df_all.to_csv(csv_file, index=False)
        logger.info(f"Benchmark data saved to {csv_file}")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")

def main():
    """Run MongoDB Atlas sharding benchmarks"""
    logger.info("🚀 Starting MongoDB Atlas Sharding Benchmarks...")
    
    try:
        args = get_connection_string()
        client = get_client(args.connection_string)
        db_name = "NOSQL"
        
        # Run benchmarks
        results = run_benchmarks(client, db_name, args.iterations)
        
        # Save results
        save_results(results, args.output)
        
        # Generate report
        generate_report(results, args.output)
        
        logger.info("✅ Benchmarks completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during benchmarking: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()