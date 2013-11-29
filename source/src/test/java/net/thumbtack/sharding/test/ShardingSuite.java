package net.thumbtack.sharding.test;

import net.thumbtack.sharding.H2Server;
import net.thumbtack.sharding.MemcachedServer;
import net.thumbtack.sharding.Storage;
import net.thumbtack.sharding.test.jdbc.JdbcStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    H2Server h2Server = new H2Server();

    public Storage jdbcStorageAsync;
    public Storage jdbcStorageSync;

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
    }

    static public ShardingSuite getInstance() {
        return instance;
    }

    synchronized public void start() throws Exception {
        if (!started) {
            h2Server.start();
            logger.debug("embedded db server started");
            started = true;
        }
    }

    synchronized public void stop() throws Exception {
        if (started) {
            h2Server.stop();
            logger.debug("embedded db server stopped");
            started = false;
        }
    }
}
