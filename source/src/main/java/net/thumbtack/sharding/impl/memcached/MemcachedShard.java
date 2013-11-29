package net.thumbtack.sharding.impl.memcached;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;

import java.util.*;

/**
 * The jdbc shard.
 */
public class MemcachedShard implements Shard {

    private static final String SHARD = "shard.";
    private static final String HOST = ".host";
    private static final String PORT = ".port";

    private final int id;
    private String host;
    private int port;

    /**
     * The constructor.
     * @param id the id of shard.
     * @param host the memcached server url.
     * @param port the memcached server port.
     */
    public MemcachedShard(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new MemcachedConnection(host, port);
    }

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<MemcachedShard> fromProperties(Properties props) {
        Set<Integer> shardIds = new HashSet<Integer>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                int id = Integer.parseInt(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<MemcachedShard> result = new ArrayList<MemcachedShard>(shardIds.size());
        for (int shardId : shardIds) {
            String host = props.getProperty(SHARD + shardId + HOST);
            int port = Integer.parseInt(props.getProperty(SHARD + shardId + PORT));
            result.add(new MemcachedShard(shardId, host, port));
        }
        return result;
    }

    @Override
    public String toString() {
        return "MemcachedShard{" +
                "id=" + id +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
