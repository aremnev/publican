package net.thumbtack.sharding.core;

import java.util.List;

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
