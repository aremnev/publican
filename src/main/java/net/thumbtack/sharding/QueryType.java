package net.thumbtack.sharding;

public enum QueryType {
	selectSpecShard,    // select from specific shard
	selectShard,        // select from undefined shard
	selectAnyShard,     // select from any shard
	selectAllShards,    // select from all shards with results union to list
	selectAllShardsSum, // select from all shards with summation of results to int
	updateSpecShard,    // update on specific shard
	updateAllShards     // update on all shards
}
