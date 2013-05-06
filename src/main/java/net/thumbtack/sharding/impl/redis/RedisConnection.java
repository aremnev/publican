package net.thumbtack.sharding.impl.redis;

import net.thumbtack.sharding.core.query.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Encapsulates the memcached client.
 */
public class RedisConnection extends Connection {

    private JedisPool jedisPool;
    private Jedis jedis;


    /**
     *
     * @return the MemcachedClient.
     */
    public Jedis getClient() {
        return jedis;
    }



    /**
     * Constructor.
     * @param jedisPool The jedisPool.
     */
    public RedisConnection(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void open() {
        if (jedis != null) {
            throw new RuntimeException("Connection already is opened.");
        }
        jedis = jedisPool.getResource();
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.returnResource(jedis);
        }
    }


    @Override
    public String toString() {
        return "RedisConnection{" +
                "jedis='" + jedis + '\'' +
                '}';
    }
}
