package net.thumbtack.sharding.impl.memcached;

import net.rubyeye.xmemcached.MemcachedClient;
import net.thumbtack.sharding.core.query.Connection;

/**
 * Encapsulates the memcached client.
 */
public class MemcachedConnection extends Connection {

    /**
     * the the instance of MemcachedClient.
     */
    private MemcachedClient memcachedClient;


    /**
     *
     * @return the MemcachedClient.
     */
    public MemcachedClient getClient() {
        return memcachedClient;
    }



    /**
     * Constructor.
     * @param memcachedClient The memcachedClient.
     */
    public MemcachedConnection(MemcachedClient memcachedClient) {
        this.memcachedClient = memcachedClient;
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
        return "MemcachedConnection{" +
                "memcachedClient='" + memcachedClient + '\'' +
                '}';
    }
}
