package net.thumbtack.shardcon.core;

import net.thumbtack.shardcon.core.query.Connection;

import java.io.Serializable;

/**
 * General interface for any shards.
 */
public interface Shard extends Serializable {

    /**
     * Gets the shard id.
     * @return The shard id.
     */
    int getId();

    /**
     * Gets the connection.
     * @return The connection.
     */
    Connection getConnection();
}
