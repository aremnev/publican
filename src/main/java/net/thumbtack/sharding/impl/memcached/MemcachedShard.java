package net.thumbtack.sharding.impl.memcached;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;

/**
 * The jdbc shard.
 */
public class MemcachedShard implements Shard {

    private final long id;
    private String host;
    private int port;

    /**
     * Constructor.
     * @param config The jdbc shard configuration.
     */
    public MemcachedShard(MemcachedShardConfig config) {
        this.id = config.getId();
        this.host = config.getHost();
        this.port = config.getPort();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new MemcachedConnection(host, port);
    }

    @Override
    public String toString() {
        return "JdbcShard{" +
                "id=" + id +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
