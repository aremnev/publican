package net.thumbtack.sharding.core;

import net.thumbtack.sharding.core.query.Connection;

/**
 * General interface for any shards.
 */
public interface Shard {

    /**
     * Get the shard id.
     * @return The shard id.
     */
    long getId();

    /**
     * Get the connection.
     * @return The connection.
     */
    Connection getConnection();
}
