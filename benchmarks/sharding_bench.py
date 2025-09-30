"""
Application-Level Sharding Benchmark for NOSQL KG Project
-------------------------------------------------------
- Implements three sharding strategies for knowledge graph nodes
- Supports: Modulo Hashing, Consistent Hashing, Range-based Partitioning
- Provides routing logic and benchmarking for each strategy
- Uses existing MongoDB Atlas collections (papers, nodes, edges)
"""

import os
import time
import json
import random
import hashlib
import bisect
from typing import Dict, Tuple, List, Optional, Callable
from pymongo.errors import PyMongoError
from api.db import papers_collection, nodes_collection, edges_collection

# -----------------------------
# Config
# -----------------------------
N_LOOKUPS = int(os.getenv("BENCH_N_LOOKUPS", "2000"))
SEED = int(os.getenv("BENCH_SEED", "42"))
random.seed(SEED)
NUM_SHARDS = int(os.getenv("NUM_SHARDS", "3"))
DEFAULT_SHARD_KEY = os.getenv("SHARD_KEY", "id")

# -----------------------------
# Timer utility
# -----------------------------
def timer(fn, *args, **kwargs) -> Tuple[float, any]:
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    return (time.perf_counter() - start), result

# -----------------------------
# Sharding Strategy Base Class
# -----------------------------
class ShardingStrategy:
    def __init__(self, num_shards: int = NUM_SHARDS):
        self.num_shards = num_shards
        self.name = "base"
    
    def get_shard(self, key: str, document: Optional[Dict] = None) -> int:
        """Determine which shard a document should be routed to"""
        raise NotImplementedError("Subclasses must implement get_shard method")
    
    def get_collection_name(self, base_name: str, shard_id: int) -> str:
        """Generate collection name for a specific shard"""
        return f"{base_name}_shard{shard_id}"
    
    def __str__(self):
        return f"{self.name}_sharding_{self.num_shards}_shards"

# -----------------------------
# 1. Modulo Hashing Strategy
# -----------------------------
class ModuloShardingStrategy(ShardingStrategy):
    def __init__(self, num_shards: int = NUM_SHARDS):
        super().__init__(num_shards)
        self.name = "modulo"
    
    def get_shard(self, key: str, document: Optional[Dict] = None) -> int:
        """Modulo-based sharding: hash(key) % num_shards"""
        if not key:
            raise ValueError("Key cannot be empty for modulo sharding")
        
        # Create a consistent hash from the key
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return key_hash % self.num_shards

# -----------------------------
# 2. Consistent Hashing Strategy
# -----------------------------
class ConsistentHashingStrategy(ShardingStrategy):
    def __init__(self, num_shards: int = NUM_SHARDS, virtual_nodes_per_shard: int = 100):
        super().__init__(num_shards)
        self.name = "consistent"
        self.virtual_nodes_per_shard = virtual_nodes_per_shard
        self.ring = []
        self.shard_map = {}
        self._build_ring()
    
    def _build_ring(self):
        """Build the consistent hashing ring with virtual nodes"""
        self.ring = []
        self.shard_map = {}
        
        for shard_id in range(self.num_shards):
            for vnode in range(self.virtual_nodes_per_shard):
                # Create virtual node hash
                vnode_key = f"shard{shard_id}_vnode{vnode}"
                vnode_hash = int(hashlib.md5(vnode_key.encode()).hexdigest(), 16)
                
                self.ring.append(vnode_hash)
                self.shard_map[vnode_hash] = shard_id
        
        # Sort the ring for binary search
        self.ring.sort()
    
    def get_shard(self, key: str, document: Optional[Dict] = None) -> int:
        """Consistent hashing: map key to ring position"""
        if not key:
            raise ValueError("Key cannot be empty for consistent hashing")
        
        # Hash the key
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        # Find the position in the ring using binary search
        pos = bisect.bisect_right(self.ring, key_hash)
        if pos == len(self.ring):
            pos = 0  # Wrap around to first shard
            
        # Get the virtual node hash and corresponding shard
        vnode_hash = self.ring[pos]
        return self.shard_map[vnode_hash]

# -----------------------------
# 3. Range-based Partitioning Strategy
# -----------------------------
class RangeShardingStrategy(ShardingStrategy):
    def __init__(self, num_shards: int = NUM_SHARDS, range_field: str = "year"):
        super().__init__(num_shards)
        self.name = "range"
        self.range_field = range_field
        self.ranges = self._calculate_ranges()
    
    def _calculate_ranges(self) -> List[Tuple]:
        """Calculate range boundaries based on existing data distribution"""
        # For paper nodes, we might use publication year, citation count, etc.
        # This is a simplified version - in production, you'd analyze actual data
        ranges = []
        
        # Example: assume papers from 2000-2025, distribute evenly
        if self.range_field == "year":
            min_year, max_year = 2000, 2025
            year_range = max_year - min_year + 1
            shard_range = year_range / self.num_shards
            
            for i in range(self.num_shards):
                start = min_year + i * shard_range
                end = min_year + (i + 1) * shard_range
                ranges.append((start, end))
        
        return ranges

    def get_shard(self, key: str, document: Optional[Dict] = None) -> int:
        """Range-based sharding based on document field values"""
        # If we only have a key and no document, use consistent fallback
        # This ensures we always get the same shard for the same key
        if document is None:
            # Use modulo hashing for consistent shard assignment
            key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
            return key_hash % self.num_shards
        
        # Try to get the field value from different possible document structures
        field_value = None
        
        # Check in properties (common structure)
        if "properties" in document and isinstance(document["properties"], dict):
            field_value = document["properties"].get(self.range_field)
        
        # Check directly in document
        if field_value is None and self.range_field in document:
            field_value = document[self.range_field]
            
        # Check in metadata
        if field_value is None and "metadata" in document:
            field_value = document.get("metadata", {}).get(self.range_field)
        
        # If we still don't have a value, use consistent hash fallback
        if field_value is None:
            key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
            return key_hash % self.num_shards
        
        # Find which range the value falls into
        for shard_id, (start, end) in enumerate(self.ranges):
            if start <= field_value < end:
                return shard_id
        
        # If value is outside all ranges, use consistent hash
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return key_hash % self.num_shards

# -----------------------------
# Sharding Strategy Factory
# -----------------------------
class ShardingStrategyFactory:
    @staticmethod
    def create_strategy(strategy_name: str, num_shards: int = NUM_SHARDS, **kwargs) -> ShardingStrategy:
        strategy_name = strategy_name.lower()
        
        if strategy_name == "modulo":
            return ModuloShardingStrategy(num_shards, **kwargs)
        elif strategy_name == "consistent":
            return ConsistentHashingStrategy(num_shards, **kwargs)
        elif strategy_name == "range":
            return RangeShardingStrategy(num_shards, **kwargs)
        else:
            raise ValueError(f"Unknown sharding strategy: {strategy_name}")

# -----------------------------
# Node Insertion with Sharding
# -----------------------------
def insert_node_with_sharding(node: Dict, strategy: ShardingStrategy) -> bool:
    """Insert a node using the specified sharding strategy"""
    try:
        shard_id = strategy.get_shard(node["id"], node)
        shard_collection_name = strategy.get_collection_name("nodes", shard_id)
        
        # In a real implementation, you'd insert into the sharded collection
        # For now, we'll simulate by storing shard info in the document
        node["shard_id"] = shard_id
        node["sharding_strategy"] = strategy.name
        
        nodes_collection.insert_one(node)
        return True
    except Exception as e:
        print(f"Error inserting node: {e}")
        return False

# -----------------------------
# Node Retrieval with Sharding
# -----------------------------
def retrieve_node_with_sharding(node_id: str, strategy: ShardingStrategy) -> Optional[Dict]:
    """Retrieve a node using the specified sharding strategy"""
    try:
        # First, find which shard the node should be in
        expected_shard = strategy.get_shard(node_id)
        
        # In a real implementation, you'd query the specific shard collection
        # For simulation, we'll query all nodes but filter by expected shard
        node = nodes_collection.find_one({"id": node_id})
        
        if node:
            # Calculate actual shard based on the full document
            # This ensures we use the same logic for both storage and retrieval
            actual_shard = strategy.get_shard(node_id, node)
            
            # Store the correct shard ID in the document if it's missing or incorrect
            if node.get("shard_id") != actual_shard:
                nodes_collection.update_one(
                    {"id": node_id},
                    {"$set": {"shard_id": actual_shard, "sharding_strategy": strategy.name}}
                )
            
            # Only log if there's a real mismatch between calculation methods
            if actual_shard != expected_shard:
                # This is just for debugging - in production we'd use proper logging
                print(f"Shard calculation mismatch: expected {expected_shard}, calculated {actual_shard}")
            
            return node
        return None
    except Exception as e:
        print(f"Error retrieving node: {e}")
        return None

# -----------------------------
# Benchmark Functions
# -----------------------------
def benchmark_sharding_strategy(strategy: ShardingStrategy) -> Dict[str, float]:
    """Benchmark a specific sharding strategy"""
    times = {}
    
    # Get sample node IDs for testing
    sample_nodes = list(nodes_collection.find({}, {"id": 1}).limit(1000))
    if not sample_nodes:
        raise RuntimeError("No nodes found for benchmarking")
    
    node_ids = [node["id"] for node in sample_nodes]
    
    def get_random_node_id():
        return random.choice(node_ids)
    
    # 1️⃣ Benchmark insertion simulation
    def insert_benchmark():
        test_node = {
            "id": f"test_node_{random.randint(100000, 999999)}",
            "type": "Paper",
            "properties": {"title": "Test", "year": random.randint(2000, 2025)}
        }
        return insert_node_with_sharding(test_node, strategy)
    
    t_insert, _ = timer(lambda: [insert_benchmark() for _ in range(100)])
    times["insert_100_nodes_s"] = t_insert
    
    # 2️⃣ Benchmark retrieval
    def retrieve_benchmark():
        node_id = get_random_node_id()
        return retrieve_node_with_sharding(node_id, strategy)
    
    t_retrieve, _ = timer(lambda: [retrieve_benchmark() for _ in range(N_LOOKUPS)])
    times["retrieve_nodes_s"] = t_retrieve
    
    # 3️⃣ Benchmark traversal (paper -> authors)
    def traversal_benchmark():
        paper_id = get_random_node_id()
        edges = list(edges_collection.find({
            "source": paper_id, 
            "relation": "authored_by"
        }).limit(5))
        return edges
    
    t_traversal, _ = timer(lambda: [traversal_benchmark() for _ in range(N_LOOKUPS // 2)])
    times["traversal_s"] = t_traversal
    
    return times

def compare_sharding_strategies(strategies: List[ShardingStrategy]) -> Dict[str, Dict[str, float]]:
    """Compare multiple sharding strategies"""
    results = {}
    
    for strategy in strategies:
        print(f"Benchmarking {strategy}...")
        try:
            metrics = benchmark_sharding_strategy(strategy)
            results[str(strategy)] = metrics
        except Exception as e:
            print(f"Error benchmarking {strategy}: {e}")
            results[str(strategy)] = {"error": str(e)}
    
    return results

# -----------------------------
# Print results
# -----------------------------
def print_result(label: str, metrics: Dict[str, float]):
    print(json.dumps({"label": label, **metrics}, indent=2))

def print_comparison_results(results: Dict[str, Dict[str, float]]):
    print("\n" + "="*60)
    print("SHARDING STRATEGY COMPARISON RESULTS")
    print("="*60)
    
    for strategy_name, metrics in results.items():
        print(f"\n{strategy_name}:")
        if "error" in metrics:
            print(f"  ERROR: {metrics['error']}")
        else:
            for metric, value in metrics.items():
                print(f"  {metric}: {value:.4f}s")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    try:
        # Create all three sharding strategies
        strategies = [
            ShardingStrategyFactory.create_strategy("modulo"),
            ShardingStrategyFactory.create_strategy("consistent"),
            ShardingStrategyFactory.create_strategy("range", range_field="year")
        ]
        
        # Benchmark and compare
        results = compare_sharding_strategies(strategies)
        print_comparison_results(results)
        
    except PyMongoError as e:
        print(f"MongoDB error during benchmark: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
