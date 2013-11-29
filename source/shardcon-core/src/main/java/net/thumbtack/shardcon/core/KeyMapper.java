package net.thumbtack.shardcon.core;

/**
 * Maps key to some shard.
 */
public interface KeyMapper {

    /**
     * Maps key to shard.
     * @param key The key.
     * @return The shard ids.
     */
    public int shard(long key);
}
