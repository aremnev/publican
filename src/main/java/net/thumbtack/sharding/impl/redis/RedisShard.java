package net.thumbtack.sharding.impl.redis;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * The jdbc shard.
 */
public class RedisShard implements Shard {

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
    public RedisShard(int id, String host, int port) {
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
        return new RedisConnection(host, port);
    }

    /**
     * Builds list of jdbc shard configurations from properties.
     * @param props The properties.
     * @return The list of jdbc shard configurations.
     */
    public static List<RedisShard> fromProperties(Properties props) {
        Set<Integer> shardIds = new HashSet<Integer>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(SHARD)) {
                int id = Integer.parseInt(name.split("\\.")[1]);
                shardIds.add(id);
            }
        }
        List<RedisShard> result = new ArrayList<RedisShard>(shardIds.size());
        for (int shardId : shardIds) {
            String host = props.getProperty(SHARD + shardId + HOST);
            int port = Integer.parseInt(props.getProperty(SHARD + shardId + PORT));
            result.add(new RedisShard(shardId, host, port));
        }
        return result;
    }

    @Override
    public String toString() {
        return "RedisShard{" +
                "id=" + id +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
