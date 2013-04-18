package net.thumbtack.sharding.core;

public interface ShardFactory {

    Shard createShard(ShardConfig config);
}
