package net.thumbtack.sharding.impl.memcached;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.thumbtack.sharding.core.query.Connection;

import java.io.IOException;

/**
 * Encapsulates the memcached client.
 */
public class MemcachedConnection extends Connection {

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
    private MemcachedClient client;


    /**
     *
     * @return the MemcachedClient.
     */
    public MemcachedClient getClient() {
        return client;
    }



    /**
     * Constructor.
     * @param host The url to memcached server.
     * @param port The port of memcached server.
     */
    public MemcachedConnection(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            client = new XMemcachedClient(host, port);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
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
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}
