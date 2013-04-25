package net.thumbtack.sharding.core.map;

/**
 * Maps key to some shard.
 */
public interface KeyMapper {

    /**
     * Maps key to shard.
     * @param key The key.
     * @return The shard id.
     */
    public int shard(long key);
}
