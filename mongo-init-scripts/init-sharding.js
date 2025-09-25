// init-sharding.js
// Initialize replica sets, add shards to mongos, and enable sharding

// 1) Initialize config server replset
try {
  rs.initiate({
    _id: "cfgRS",
    configsvr: true,
    members: [{ _id: 0, host: "configsvr:27019" }]
  });
} catch (e) {}

// 2) Initialize shard1 replset
try {
  const shard1 = new Mongo("mongodb://shard1:27017/admin");
  shard1.getDB("admin").runCommand({ replSetInitiate: { _id: "shardRS1", members: [{ _id: 0, host: "shard1:27017" }] } });
} catch (e) {}

// 3) Initialize shard2 replset
try {
  const shard2 = new Mongo("mongodb://shard2:27017/admin");
  shard2.getDB("admin").runCommand({ replSetInitiate: { _id: "shardRS2", members: [{ _id: 0, host: "shard2:27017" }] } });
} catch (e) {}

// 4) Add shards to mongos and enable sharding
try {
  const s = new Mongo("mongodb://mongos:27017/admin");
  const admin = s.getDB("admin");
  admin.runCommand({ addShard: "shardRS1/shard1:27017" });
  admin.runCommand({ addShard: "shardRS2/shard2:27017" });

  // Enable sharding for database NOSQL
  admin.runCommand({ enableSharding: "NOSQL" });

  // Example shard keys: hashed on id, range on type+id
  admin.runCommand({ shardCollection: "NOSQL.nodes", key: { id: "hashed" } });
  admin.runCommand({ shardCollection: "NOSQL.edges", key: { source: 1, target: 1, relation: 1 } });
  admin.runCommand({ shardCollection: "NOSQL.papers", key: { id: "hashed" } });
} catch (e) {}
