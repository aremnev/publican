package net.thumbtack;

import net.thumbtack.sharding.core.Shard;

public interface ShardService {

    void publicate(String shardId, Shard shard);
    void remove(String shardId);
    String findActiveShardId(int bucketIndex);
    boolean isShardAlive(String shardId);
    Shard findShard(String shardId);
}
