package net.thumbtack.sharding.impl.redis;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
    private static final int MAX_ACTIVE = 20;
    private static final int MAX_IDLE = 5;
    private static final int MIN_IDLE = 5;
    private static final int NUM_TESTS_PER_EVICTION_RUN = 10;
    private static final int TIME_BETWEEN_EVICTION_RUNS_MILLIS = 10000;

    private final int id;

    private JedisPool jedisPool;

    /**
     * The constructor.
     * @param id the id of shard.
     * @param host the memcached server url.
     * @param port the memcached server port.
     */
    public RedisShard(int id, String host, int port) {
        this.id = id;
        setup(host, port);
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Connection getConnection() {
        return new RedisConnection(jedisPool);
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

    private void setup(String host, int port) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxActive(MAX_ACTIVE);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setMaxIdle(MAX_IDLE);
        poolConfig.setMinIdle(MIN_IDLE);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setNumTestsPerEvictionRun(NUM_TESTS_PER_EVICTION_RUN);
        poolConfig.setTimeBetweenEvictionRunsMillis(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        jedisPool = new JedisPool(poolConfig, host, port);
    }

    @Override
    public String toString() {
        return "RedisShard{" +
                "id=" + id +
                ", jedisPool='" + jedisPool + '\'' +
                '}';
    }
}
