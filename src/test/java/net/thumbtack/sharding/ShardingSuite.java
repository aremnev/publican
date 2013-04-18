package net.thumbtack.sharding;

import net.thumbtack.sharding.common.Storage;
import net.thumbtack.sharding.jdbc.JdbcStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardingSuite {

    private static final Logger logger = LoggerFactory.getLogger(ShardingSuite.class);

    H2Server h2Server = new H2Server();

    public Storage jdbcStorageAsync;
    public Storage jdbcStorageSync;

    public ShardingSuite() throws Exception {
        jdbcStorageAsync = new JdbcStorage("query-async.properties");
        jdbcStorageSync = new JdbcStorage("query-sync.properties");
    }

    public void start() throws Exception {
        h2Server.start();
        logger.debug("embedded db server started");
    }

    public void stop() throws Exception {
        h2Server.stop();
        logger.debug("embedded db server stopped");
    }
}
