package net.thumbtack.sharding.core;

/**
 * Creates new shard from config.
 */
public interface ShardFactory {

    /**
     * Creates new shard.
     * @param config The shard configuration.
     * @return The new shard.
     */
    Shard createShard(ShardConfig config);
}
