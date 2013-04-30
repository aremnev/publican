package net.thumbtack.sharding.impl.redis;

import net.thumbtack.sharding.core.query.Connection;
import redis.clients.jedis.Jedis;

/**
 * Encapsulates the memcached client.
 */
public class RedisConnection extends Connection {

    /**
     * the memcached server url.
     */
    private String host;

    /**
     * the memcached server port.
     */
    private int port;

    /**
     * the the instance of MemcachedClient.
     */
    private Jedis client;


    /**
     *
     * @return the MemcachedClient.
     */
    public Jedis getClient() {
        return client;
    }



    /**
     * Constructor.
     * @param host The url to memcached server.
     * @param port The port of memcached server.
     */
    public RedisConnection(String host, int port) {
        this.host = host;
        this.port = port;
        client = new Jedis(host, port);
    }

    @Override
    public void open() {
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void close() {

    }


    @Override
    public String toString() {
        return "RedisConnection{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
