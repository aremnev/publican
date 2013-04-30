package net.thumbtack.sharding.test;

import net.thumbtack.sharding.H2Server;
import net.thumbtack.sharding.MemcachedServer;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.test.jdbc.JdbcStorage;
import net.thumbtack.sharding.test.memcached.MemcachedStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    H2Server h2Server = new H2Server();

    public Storage jdbcStorageAsync;
    public Storage jdbcStorageSync;

    public Storage memcachedStorageAsync;
    public Storage memcachedStorageSync;

    MemcachedServer memcachedServer1 = new MemcachedServer(11212);
    MemcachedServer memcachedServer2 = new MemcachedServer(11213);

    private static ShardingSuite instance;

    static {
        try {
            instance = new ShardingSuite();
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private boolean started;

    private ShardingSuite() throws Exception {
        jdbcStorageAsync = new JdbcStorage(false);
        jdbcStorageSync = new JdbcStorage(true);
        memcachedStorageAsync = new MemcachedStorage(false);
        memcachedStorageSync = new MemcachedStorage(true);
    }

    static public ShardingSuite getInstance() {
        return instance;
    }

    synchronized public void start() throws Exception {
        if (!started) {
            h2Server.start();
            memcachedServer1.start();
            memcachedServer2.start();
            logger.debug("embedded db server started");
            started = true;
        }
    }

    synchronized public void stop() throws Exception {
        if (started) {
            h2Server.stop();
            memcachedServer1.stop();
            memcachedServer2.stop();
            logger.debug("embedded db server stopped");
            started = false;
        }
    }
}
